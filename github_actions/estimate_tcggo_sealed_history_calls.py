#!/usr/bin/env python3
"""
Query Supabase for saved sealed-product identifiers (pack / box / ETB) and estimate
TCGGO ``GET /history-prices`` API volume.

Uses the same env as ``supabase_wizard_dataset_bridge.py`` (SUPABASE_URL + SUPABASE_KEY
or SUPABASE_SERVICE_ROLE_KEY).

Tracked sets: every row in ``pokemon_sets`` (optionally restrict to set_codes that have
``pokemon_cards`` rows via --sets-with-cards-only).

Products counted:
  - ``metadata.pack_cost_breakdown.tcggo`` (selected + candidates + top-level blocks with
    ``tcggo_product_id``)
  - If ``pokemon_set_pack_pricing`` exists: ``tcgplayer_booster_pack_product_id`` (history
    by ``tcgplayer_id`` counts as one call per id).

Call estimates:
  - **minimal**: one request per product id (matches ``fetch_tcggo_price_history_query`` today,
    which only requests ``page=1``).
  - **full_history**: ``ceil(assumed_result_rows / per_page)`` per product (TCGGO often returns
    ``paging.per_page`` and ``paging.total``; we cannot know ``total`` without a live call, so
    defaults are configurable).
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "github_actions"))

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from supabase import create_client


def _load_env() -> None:
    if not load_dotenv:
        return
    load_dotenv(ROOT / ".env")
    load_dotenv(ROOT / "scrape" / "ebay_listing_checker.env")


def _supabase():
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


def _fetch_paginated(client, table: str, select: str) -> List[Dict[str, Any]]:
    page_size = 1000
    page = 0
    out: List[Dict[str, Any]] = []
    while True:
        start = page * page_size
        end = start + page_size - 1
        res = client.table(table).select(select).range(start, end).execute()
        batch = res.data or []
        out.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
    return out


def _collect_tcggo_internal_ids(meta: Any, *, primary_only: bool) -> Set[int]:
    ids: Set[int] = set()
    if not isinstance(meta, dict):
        return ids
    bd = meta.get("pack_cost_breakdown")
    if not isinstance(bd, dict):
        return ids
    tg = bd.get("tcggo")
    if isinstance(tg, dict):
        sel = tg.get("selected")
        if isinstance(sel, dict):
            v = sel.get("tcggo_product_id")
            if v is not None:
                try:
                    ids.add(int(v))
                except (TypeError, ValueError):
                    pass
        if primary_only:
            return ids
        cands = tg.get("candidates")
        if isinstance(cands, dict):
            for _k, row in cands.items():
                if not isinstance(row, dict):
                    continue
                v = row.get("tcggo_product_id")
                if v is not None:
                    try:
                        ids.add(int(v))
                    except (TypeError, ValueError):
                        pass
    if primary_only:
        return ids
    for blk_key in ("single_booster_pack", "booster_box", "elite_trainer_box"):
        blk = bd.get(blk_key)
        if isinstance(blk, dict):
            v = blk.get("tcggo_product_id")
            if v is not None:
                try:
                    ids.add(int(v))
                except (TypeError, ValueError):
                    pass
    return ids


def _has_tcggo_selected_pack_id(meta: Any) -> bool:
    """True if ``pack_cost_breakdown.tcggo.selected.tcggo_product_id`` is present."""
    return len(_collect_tcggo_internal_ids(meta, primary_only=True)) > 0


def _has_pull_cost_signals(meta: Any) -> bool:
    """Gemrate / EV / pull-rate fields commonly used alongside pack economics."""
    if not isinstance(meta, dict):
        return False
    if meta.get("gemrate_id") or meta.get("gemrate_set_link") or meta.get("gemrate_set_total") is not None:
        return True
    if meta.get("booster_pack_ev") or meta.get("booster_box_ev"):
        return True
    rp = meta.get("rarity_pull_rates")
    if isinstance(rp, dict) and len(rp) > 0:
        return True
    rc = meta.get("rarity_counts")
    return isinstance(rc, dict) and len(rc) > 0


def _has_pack_cost_fields(meta: Any) -> bool:
    if not isinstance(meta, dict):
        return False
    return bool(
        meta.get("pack_cost_primary_usd") is not None
        or meta.get("tcgplayer_pack_price") is not None
        or meta.get("pack_cost_breakdown")
    )


def _has_usd_pack_price_in_metadata(meta: Any) -> bool:
    """True if metadata carries a resolved USD-ish pack price field."""
    if not isinstance(meta, dict):
        return False
    return meta.get("pack_cost_primary_usd") is not None or meta.get("tcgplayer_pack_price") is not None


def _set_codes_with_tcgplayer_pack_pricing(rows: List[Dict[str, Any]]) -> Set[str]:
    """set_codes that have a non-null tcgplayer booster id in ``pokemon_set_pack_pricing``."""
    out: Set[str] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        if r.get("tcgplayer_booster_pack_product_id") is None:
            continue
        sc = str(r.get("set_code") or "").strip().lower()
        if sc:
            out.add(sc)
    return out


def _trainer_gallery_parent_expansion_set_code(set_code: str, known_set_codes: Set[str]) -> Optional[str]:
    """
    Trainer Gallery lists (e.g. swsh10tg) are the same booster product as the main expansion (swsh10).
    Return parent set_code when ``set_code`` ends with ``tg`` and the stripped prefix exists in the DB.
    """
    sc = set_code.strip().lower()
    if len(sc) <= 2 or not sc.endswith("tg"):
        return None
    parent = sc[:-2]
    if parent in known_set_codes:
        return parent
    return None


def _pack_cost_covered_for_set_code(
    set_code: str,
    meta_by_code: Dict[str, Any],
    sets_with_tp_pack_row: Set[str],
    known_set_codes: Set[str],
) -> bool:
    """
    This set has a saved USD pack price or pricing-table row, or (Trainer Gallery adjunct only)
    the parent expansion has USD/table row or any pack_cost_* / breakdown (same booster product).
    """
    m = meta_by_code.get(set_code)
    if _has_usd_pack_price_in_metadata(m) or set_code in sets_with_tp_pack_row:
        return True
    parent = _trainer_gallery_parent_expansion_set_code(set_code, known_set_codes)
    if not parent:
        return False
    pm = meta_by_code.get(parent)
    if _has_usd_pack_price_in_metadata(pm) or parent in sets_with_tp_pack_row:
        return True
    return _has_pack_cost_fields(pm)


def _collect_tcgplayer_pack_ids_from_pricing(rows: List[Dict[str, Any]]) -> Set[int]:
    ids: Set[int] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        v = r.get("tcgplayer_booster_pack_product_id")
        if v is not None:
            try:
                ids.add(int(v))
            except (TypeError, ValueError):
                pass
    return ids


def main() -> int:
    ap = argparse.ArgumentParser(description="Estimate TCGGO /history-prices calls from Supabase")
    ap.add_argument(
        "--sets-with-cards-only",
        action="store_true",
        help="Only count sets that have at least one pokemon_cards row",
    )
    ap.add_argument(
        "--per-page",
        type=int,
        default=30,
        help="Assumed TCGGO history paging size for full_history estimate",
    )
    ap.add_argument(
        "--assume-result-rows",
        type=int,
        default=100,
        help="Assumed number of daily history rows returned per product (for full_history estimate)",
    )
    ap.add_argument(
        "--primary-only",
        action="store_true",
        help="Only count tcggo.selected.tcggo_product_id (one sealed anchor per set), not candidates/blocks",
    )
    ap.add_argument(
        "--list-missing-pack-cost",
        action="store_true",
        help="Print set_code and set_name for sets with no pack price in metadata and no tcgplayer pack row in pokemon_set_pack_pricing (Trainer Gallery *tg sets count as covered if the parent expansion has pack cost)",
    )
    args = ap.parse_args()

    client = _supabase()
    print("Fetching pokemon_sets ...", flush=True)
    sets = _fetch_paginated(client, "pokemon_sets", "set_code,set_name,metadata")
    set_codes_with_cards: Optional[Set[str]] = None
    if args.sets_with_cards_only:
        print("Fetching pokemon_cards set_code list ...", flush=True)
        cards = _fetch_paginated(client, "pokemon_cards", "set_code")
        set_codes_with_cards = {str(c.get("set_code") or "").strip().lower() for c in cards if c.get("set_code")}

    pricing_rows_early: List[Dict[str, Any]] = []
    try:
        pricing_rows_early = _fetch_paginated(
            client, "pokemon_set_pack_pricing", "set_code,tcgplayer_booster_pack_product_id"
        )
        print(f"Fetched pokemon_set_pack_pricing rows: {len(pricing_rows_early)}", flush=True)
    except Exception as e:
        print(f"pokemon_set_pack_pricing: not available ({e!r})", flush=True)
    if set_codes_with_cards is not None:
        pricing_rows_early = [
            r
            for r in pricing_rows_early
            if str(r.get("set_code") or "").strip().lower() in set_codes_with_cards
        ]
    sets_with_tp_pack_row = _set_codes_with_tcgplayer_pack_pricing(pricing_rows_early)

    meta_by_code: Dict[str, Any] = {}
    known_set_codes: Set[str] = set()
    for s in sets:
        sc0 = str(s.get("set_code") or "").strip().lower()
        if not sc0:
            continue
        known_set_codes.add(sc0)
        meta_by_code[sc0] = s.get("metadata")

    tcggo_ids: Set[int] = set()
    tcgplayer_pack_ids: Set[int] = set()
    sets_with_pack_meta = 0
    sets_considered = 0
    with_tcggo_selected = 0
    with_pull_signals = 0
    pull_signals_no_tcggo_selected = 0
    with_pack_cost_any = 0
    pack_cost_no_tcggo_selected = 0
    missing_pack_cost_rows: List[Tuple[str, str]] = []
    for s in sets:
        sc = str(s.get("set_code") or "").strip().lower()
        if not sc:
            continue
        if set_codes_with_cards is not None and sc not in set_codes_with_cards:
            continue
        sets_considered += 1
        meta = s.get("metadata")
        got = _collect_tcggo_internal_ids(meta, primary_only=bool(args.primary_only))
        if got:
            sets_with_pack_meta += 1
        tcggo_ids |= got

        sel = _has_tcggo_selected_pack_id(meta)
        if sel:
            with_tcggo_selected += 1
        ps = _has_pull_cost_signals(meta)
        if ps:
            with_pull_signals += 1
            if not sel:
                pull_signals_no_tcggo_selected += 1
        if _has_pack_cost_fields(meta):
            with_pack_cost_any += 1
            if not sel:
                pack_cost_no_tcggo_selected += 1
        if args.list_missing_pack_cost:
            if not _pack_cost_covered_for_set_code(sc, meta_by_code, sets_with_tp_pack_row, known_set_codes):
                sn = str(s.get("set_name") or "").strip()
                missing_pack_cost_rows.append((sc, sn))

    pricing_rows = pricing_rows_early
    tcgplayer_pack_ids = _collect_tcgplayer_pack_ids_from_pricing(pricing_rows)

    n_tcggo = len(tcggo_ids)
    n_tp = len(tcgplayer_pack_ids)
    # Same physical pack may appear as both tcggo internal id (metadata) and tcgplayer id (pricing table);
    # worst case is n_tcggo + n_tp calls if you query both keys without resolving to one id first.
    unique_history_targets = n_tcggo + n_tp
    pages_per = max(1, math.ceil(max(1, args.assume_result_rows) / max(1, args.per_page)))
    minimal_calls = unique_history_targets
    full_calls = unique_history_targets * pages_per

    print()
    print("=== Coverage vs pull-cost / Gemrate metadata ===")
    print(f"Sets considered: {sets_considered} (filter={'all' if set_codes_with_cards is None else 'with_cards'})")
    print(f"Sets WITH tcggo.selected.tcggo_product_id (one sealed id): {with_tcggo_selected}")
    print(f"Sets WITHOUT that id (no TCGGO sealed anchor saved): {sets_considered - with_tcggo_selected}")
    print(f"Sets WITH Gemrate / EV / pull-rate signals in metadata: {with_pull_signals}")
    print(
        f"  of those, missing tcggo.selected pack id (pull context but no sealed TCGGO product row): "
        f"{pull_signals_no_tcggo_selected}"
    )
    print(f"Sets WITH any pack_cost_* / pack_cost_breakdown in metadata: {with_pack_cost_any}")
    print(f"  of those, missing tcggo.selected id: {pack_cost_no_tcggo_selected}")
    print()
    print("=== Sealed products (saved in DB) ===")
    print(f"Sets with tcggo product ids counted this run (primary_only={args.primary_only}): {sets_with_pack_meta}")
    print(f"Unique TCGGO internal product ids (metadata): {n_tcggo}")
    print(f"Unique tcgplayer booster pack product ids (pricing table): {n_tp}")
    print(
        f"Upper-bound /history-prices targets (tcggo ids + tcgplayer ids; may double-count the same pack): "
        f"{unique_history_targets}"
    )
    print()
    print("=== API call estimates (per full metadata refresh) ===")
    print(f"Minimal (page=1 only, current fetch_tcggo_price_history_query): {minimal_calls} GET /history-prices")
    print(
        f"Full history (assume {args.assume_result_rows} rows, per_page={args.per_page}): "
        f"~{full_calls} GET (~{pages_per} pages x {unique_history_targets} products)"
    )
    print()
    print("Moving forward: run only these calls per scheduled job (plus optional episode index +")
    print("  episode products if you still need to discover ids) instead of full pack-cost runner.")
    if args.list_missing_pack_cost:
        missing_pack_cost_rows.sort(key=lambda t: t[0])
        print()
        print("=== Sets with no pack cost data ===")
        print(
            "Definition: no pack_cost_primary_usd / tcgplayer_pack_price in pokemon_sets.metadata, "
            "and no tcgplayer_booster_pack_product_id row in pokemon_set_pack_pricing; "
            "sets whose code ends with tg (Trainer Gallery) are omitted when the parent expansion "
            "(set_code without the tg suffix) has any pack-cost metadata, USD pack fields, or a pricing-table row."
        )
        print(f"Count: {len(missing_pack_cost_rows)}")
        for code, name in missing_pack_cost_rows:
            print(f"{code}\t{name}" if name else code)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
