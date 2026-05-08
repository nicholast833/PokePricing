#!/usr/bin/env python3
"""
Export Supabase pokemon_sets + pokemon_cards into pokemon_sets_data.json (Explorer shape),
and apply merge fields from that JSON back onto Supabase (Wizard + GemRate).

CI usage:
  python supabase_wizard_dataset_bridge.py export --output pokemon_sets_data.json
  python scrape/sync_pokemon_wizard.py ...  # or poll_wizard_tracked_cards_all_sets.py
  python supabase_wizard_dataset_bridge.py apply-wizard --input pokemon_sets_data.json
  python scrape/gemrate_scraper.py --data pokemon_sets_data.json
  python supabase_wizard_dataset_bridge.py apply-gemrate --input pokemon_sets_data.json

Env (same as run_daily_api_queue / backup scripts):
  SUPABASE_URL
  SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY (service role recommended for bulk updates)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from supabase import Client, create_client

ROOT = Path(__file__).resolve().parents[1]


def _load_env() -> None:
    if not load_dotenv:
        return
    load_dotenv(ROOT / ".env")
    load_dotenv(ROOT / "scrape" / "ebay_listing_checker.env")


def _supabase() -> Client:
    _load_env()
    url = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
    key = (
        (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
        or (os.environ.get("SUPABASE_KEY") or "").strip()
    )
    if not url or not key:
        print("Missing SUPABASE_URL or SUPABASE_KEY / SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
        raise SystemExit(1)
    return create_client(url, key)


def _fetch_paginated(
    client: Client,
    table: str,
    select: str,
    *,
    order: Optional[str] = None,
    desc: bool = False,
) -> List[Dict[str, Any]]:
    page_size = 1000
    page = 0
    out: List[Dict[str, Any]] = []
    while True:
        q = client.table(table).select(select)
        if order:
            q = q.order(order, desc=desc)
        start = page * page_size
        end = start + page_size - 1
        res = q.range(start, end).execute()
        batch = res.data or []
        out.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
    return out


def _card_row_to_toplist_shape(row: Dict[str, Any]) -> Dict[str, Any]:
    """Match shared.js fetchPokemonSetsFromSupabase: spread metrics onto the card."""
    base = {k: v for k, v in row.items() if k != "metrics"}
    m = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    return {**base, **m}


def _set_row_to_export_shape(row: Dict[str, Any]) -> Dict[str, Any]:
    """Include set row as in DB."""
    return dict(row)


def _merge_set_metadata_for_export(row: Dict[str, Any]) -> Dict[str, Any]:
    """Match supabase-config.js: { ...set, ...(set.metadata || {}) } so GemRate set fields round-trip."""
    base = dict(row)
    meta = base.get("metadata")
    if isinstance(meta, dict):
        return {**base, **meta}
    return base


def export_json(output_path: Path) -> None:
    client = _supabase()
    print("Fetching pokemon_sets ...", flush=True)
    sets = _fetch_paginated(client, "pokemon_sets", "*", order="set_code")
    print(f"Fetching pokemon_cards ({len(sets)} sets) ...", flush=True)
    # Omit price_history: tcggo_market_history can be huge; Wizard poll re-fetches chart history anyway.
    cards = _fetch_paginated(
        client,
        "pokemon_cards",
        "unique_card_id,set_code,name,number,rarity,market_price,artist,image_url,metrics",
        order="unique_card_id",
    )
    by_set: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for c in cards:
        sc = str(c.get("set_code") or "").strip().lower()
        if sc:
            by_set[sc].append(c)

    data: List[Dict[str, Any]] = []
    for s in sets:
        sc = str(s.get("set_code") or "").strip().lower()
        rows = by_set.get(sc, [])
        top = [_card_row_to_toplist_shape(r) for r in rows]
        row = _merge_set_metadata_for_export(_set_row_to_export_shape(s))
        row["top_25_cards"] = top
        data.append(row)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=4, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(data)} sets, {len(cards)} cards -> {output_path}", flush=True)


def _collect_flat_cards(data: List[Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for s in data:
        if not isinstance(s, dict):
            continue
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue
        for c in top:
            if not isinstance(c, dict):
                continue
            uid = str(c.get("unique_card_id") or "").strip()
            if uid:
                out[uid] = c
    return out


def _wizard_patch_from_flat_card(card: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in card.items() if k.startswith("pokemon_wizard_")}


def apply_wizard_from_json(input_path: Path, *, batch_size: int = 80) -> None:
    raw = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit("pokemon_sets_data.json must be a list of sets")
    flat_by_id = _collect_flat_cards(raw)
    if not flat_by_id:
        print("No cards found under top_25_cards; nothing to apply.", flush=True)
        return

    client = _supabase()
    ids = sorted(flat_by_id.keys())
    print(f"Applying Wizard fields for {len(ids)} cards (batched) ...", flush=True)

    now = datetime.now(timezone.utc).isoformat()
    ok = 0
    err = 0

    for i in range(0, len(ids), batch_size):
        chunk = ids[i : i + batch_size]
        res = (
            client.table("pokemon_cards")
            .select("unique_card_id,metrics,price_history")
            .in_("unique_card_id", chunk)
            .execute()
        )
        rows = {r["unique_card_id"]: r for r in (res.data or []) if r.get("unique_card_id")}

        for uid in chunk:
            flat = flat_by_id.get(uid) or {}
            patch = _wizard_patch_from_flat_card(flat)
            if not patch:
                continue
            cur = rows.get(uid)
            if not cur:
                print(f"  skip: no DB row for {uid!r}", flush=True)
                err += 1
                continue
            old_m = cur.get("metrics") if isinstance(cur.get("metrics"), dict) else {}
            new_m = {**old_m, **patch}
            old_ph = cur.get("price_history") if isinstance(cur.get("price_history"), dict) else {}
            new_ph = dict(old_ph)
            hist = patch.get("pokemon_wizard_price_history")
            if hist is not None:
                new_ph["pokemon_wizard_price_history"] = hist
            try:
                client.table("pokemon_cards").update(
                    {"metrics": new_m, "price_history": new_ph, "last_synced_at": now}
                ).eq("unique_card_id", uid).execute()
                ok += 1
            except Exception as e:
                err += 1
                print(f"  update failed {uid!r}: {e}", flush=True)

        print(f"  progress {min(i + batch_size, len(ids))}/{len(ids)} (ok={ok} err={err})", flush=True)

    print(f"Done. updated_ok={ok} errors={err}", flush=True)


SET_GEMRATE_KEYS = ("gemrate_set_total", "gemrate_id", "gemrate_set_link")


def apply_gemrate_from_json(input_path: Path) -> None:
    raw = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit("pokemon_sets_data.json must be a list of sets")

    client = _supabase()
    now = datetime.now(timezone.utc).isoformat()

    set_ok = 0
    set_skip = 0
    set_err = 0
    for s in raw:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        if not sc:
            continue
        patch = {k: s[k] for k in SET_GEMRATE_KEYS if k in s}
        if not patch:
            set_skip += 1
            continue
        try:
            res = client.table("pokemon_sets").select("metadata").eq("set_code", sc).limit(1).execute()
            rows = res.data or []
            if not rows:
                set_err += 1
                print(f"  skip set: no pokemon_sets row for {sc!r}", flush=True)
                continue
            old_meta = rows[0].get("metadata") if isinstance(rows[0].get("metadata"), dict) else {}
            new_meta = {**old_meta, **patch}
            client.table("pokemon_sets").update({"metadata": new_meta}).eq("set_code", sc).execute()
            set_ok += 1
        except Exception as e:
            set_err += 1
            print(f"  set update failed {sc!r}: {e}", flush=True)

    print(
        f"GemRate sets: updated={set_ok} skipped_no_fields={set_skip} errors={set_err}",
        flush=True,
    )

    flat_by_id = _collect_flat_cards(raw)
    if not flat_by_id:
        print("No cards under top_25_cards; skipping card metrics.", flush=True)
        return

    ids = sorted(flat_by_id.keys())
    batch_size = 80
    ok = 0
    err = 0
    print(f"Applying metrics.gemrate for {len(ids)} cards ...", flush=True)

    for i in range(0, len(ids), batch_size):
        chunk = ids[i : i + batch_size]
        res = (
            client.table("pokemon_cards")
            .select("unique_card_id,metrics")
            .in_("unique_card_id", chunk)
            .execute()
        )
        rows = {r["unique_card_id"]: r for r in (res.data or []) if r.get("unique_card_id")}

        for uid in chunk:
            flat = flat_by_id.get(uid) or {}
            if "gemrate" not in flat:
                continue
            cur = rows.get(uid)
            if not cur:
                err += 1
                print(f"  skip card: no DB row for {uid!r}", flush=True)
                continue
            old_m = cur.get("metrics") if isinstance(cur.get("metrics"), dict) else {}
            new_m = {**old_m, "gemrate": flat.get("gemrate")}
            try:
                client.table("pokemon_cards").update({"metrics": new_m, "last_synced_at": now}).eq(
                    "unique_card_id", uid
                ).execute()
                ok += 1
            except Exception as e:
                err += 1
                print(f"  card update failed {uid!r}: {e}", flush=True)

        print(f"  progress {min(i + batch_size, len(ids))}/{len(ids)} (ok={ok} err={err})", flush=True)

    print(f"GemRate cards: updated_ok={ok} errors={err}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Supabase ↔ pokemon_sets_data.json bridge (Wizard + GemRate)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("export", help="Write pokemon_sets_data.json from Supabase")
    e.add_argument("--output", type=Path, default=ROOT / "pokemon_sets_data.json")

    a = sub.add_parser("apply-wizard", help="Merge pokemon_wizard_* from JSON onto pokemon_cards rows")
    a.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")
    a.add_argument("--batch-size", type=int, default=80)

    g = sub.add_parser("apply-gemrate", help="Merge gemrate card metrics + set GemRate fields from JSON")
    g.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")

    args = ap.parse_args()
    if args.cmd == "export":
        export_json(args.output.resolve())
    elif args.cmd == "apply-wizard":
        apply_wizard_from_json(args.input.resolve(), batch_size=max(1, int(args.batch_size)))
    elif args.cmd == "apply-gemrate":
        apply_gemrate_from_json(args.input.resolve())
    else:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
