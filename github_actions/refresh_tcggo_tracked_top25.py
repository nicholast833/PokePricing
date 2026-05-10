#!/usr/bin/env python3
"""
Assign ``pokemon_cards.tracked_priority`` (1..N) per set from TCGGO ``/episodes/{id}/cards``
sorted by ``price_highest``, within a hard cap on TCGGO HTTP calls (default ~200) per run.

Designed for a ~3-day full rotation over ~200 English sets: schedule every ~3 days with
``TCGGO_TRACKED_TOP25_MAX_CALLS=200``, or raise the cap / run more often. Oldest
``metadata.tcggo_tracked_top25_at`` is refreshed first.

Episode index is cached in ``predictor_analytics_assets`` (``tcggo_episodes_index``) to avoid
re-listing all episodes every run (TTL default 14 days).

Env:
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY)
  TCGPRO_API_KEY | RAPIDAPI_KEY_TCGGO | RAPIDAPI_KEY

  TCGGO_TRACKED_TOP25_MAX_CALLS (default 200)
  TCGGO_EPISODE_LIST_MAX_PAGES (default 22) — pages of GET /episodes (one call each)
  TCGGO_EPISODES_CACHE_DAYS (default 14)
  TCGGO_TRACKED_PER_PAGE (default 25)
  TCGGO_TRACKED_TOP25_SLEEP_S (default 0.2)

Bootstrap (no TCGGO):
  python github_actions/refresh_tcggo_tracked_top25.py --bootstrap-market-price
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "github_actions"))

from supabase_wizard_dataset_bridge import _fetch_paginated, _supabase  # noqa: E402
from tcggo_api_fetcher import (  # noqa: E402
    fetch_all_episodes,
    fetch_episode_cards_top,
    tcggo_product_internal_id,
    tcggo_product_tcgplayer_id,
)

ASSET_EPISODES = "tcggo_episodes_index"


def _norm_str(s: Any) -> str:
    return unicodedata.normalize("NFC", str(s or "").strip()).casefold()


def _episode_index_from_rows(episodes: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for e in episodes:
        if not isinstance(e, dict):
            continue
        nm = e.get("name")
        eid = e.get("id")
        if nm and eid is not None:
            try:
                out[_norm_str(nm)] = int(eid)
            except (TypeError, ValueError):
                continue
    return out


def _resolve_tcggo_episode_id(
    set_row: Dict[str, Any],
    episodes_by_name: Dict[str, int],
) -> Optional[int]:
    set_name = str(set_row.get("set_name") or "")
    set_code = str(set_row.get("set_code") or "").strip().lower()
    norm_set = _norm_str(set_name)
    eid = episodes_by_name.get(norm_set)
    if eid:
        return int(eid)
    if "promo" in norm_set:
        if "wizards" in norm_set or set_code == "basep":
            v = episodes_by_name.get(_norm_str("Wizards Black Star Promos"))
            if v:
                return int(v)
        if "nintendo" in norm_set or set_code == "np":
            v = episodes_by_name.get(_norm_str("Nintendo Black Star Promos"))
            if v:
                return int(v)
        if "ex" in norm_set or set_code == "ex5":
            v = episodes_by_name.get(_norm_str("EX Promos"))
            if v:
                return int(v)
    return None


def _load_episodes_cache(client: Any) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    r = (
        client.table("predictor_analytics_assets")
        .select("payload,updated_at")
        .eq("asset_key", ASSET_EPISODES)
        .limit(1)
        .execute()
    )
    rows = r.data or []
    if not rows:
        return None, None
    row = rows[0]
    p = row.get("payload")
    ts = row.get("updated_at")
    return (p if isinstance(p, dict) else None), (str(ts) if ts else None)


def _upsert_asset(client: Any, key: str, payload: Dict[str, Any]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    client.table("predictor_analytics_assets").upsert(
        [{"asset_key": key, "payload": payload, "updated_at": now}],
        on_conflict="asset_key",
    ).execute()


def _load_or_fetch_episodes(
    client: Any,
    api_key: str,
    *,
    max_episode_pages: int,
    cache_days: int,
) -> Tuple[Dict[str, int], int]:
    """Returns ``(episodes_by_norm_name, tcggo_http_calls)``."""
    now = datetime.now(timezone.utc)
    cached, row_updated_at = _load_episodes_cache(client)
    if cached and row_updated_at:
        try:
            updated = datetime.fromisoformat(str(row_updated_at).replace("Z", "+00:00"))
        except ValueError:
            updated = None
        if updated and (now - updated) < timedelta(days=max(1, cache_days)):
            eps = cached.get("episodes")
            if isinstance(eps, list) and eps:
                return _episode_index_from_rows([e for e in eps if isinstance(e, dict)]), 0

    eps_rows, http_calls = fetch_all_episodes(api_key, max_pages=max(1, int(max_episode_pages)))
    _upsert_asset(
        client,
        ASSET_EPISODES,
        {
            "episodes": eps_rows,
        },
    )
    return _episode_index_from_rows(eps_rows), int(http_calls)


def _num(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    import math

    return v if math.isfinite(v) else None


def _match_card_for_api_row(
    set_cards: List[Dict[str, Any]],
    api_row: Dict[str, Any],
) -> Optional[str]:
    nname = _norm_str(api_row.get("name"))
    num = _norm_str(api_row.get("card_number") or api_row.get("number") or "")
    tid = tcggo_product_internal_id(api_row)
    tpp = tcggo_product_tcgplayer_id(api_row)
    for c in set_cards:
        if _norm_str(c.get("name")) == nname and _norm_str(c.get("number")) == num:
            uid = str(c.get("unique_card_id") or "").strip()
            return uid or None
    for c in set_cards:
        m = c.get("metrics") if isinstance(c.get("metrics"), dict) else {}
        if tid is not None and m.get("tcggo_id") == tid:
            uid = str(c.get("unique_card_id") or "").strip()
            return uid or None
        if tpp is not None:
            for k in ("tcgtracking_product_id", "tcgplayer_product_id", "tcgplayer_id"):
                v = m.get(k)
                try:
                    if v is not None and int(v) == int(tpp):
                        uid = str(c.get("unique_card_id") or "").strip()
                        return uid or None
                except (TypeError, ValueError):
                    continue
    return None


def _merge_set_metadata(client: Any, set_code: str, patch: Dict[str, Any]) -> None:
    sc = str(set_code or "").strip()
    if not sc:
        return
    r = client.table("pokemon_sets").select("metadata").eq("set_code", sc).limit(1).execute()
    rows = r.data or []
    meta: Dict[str, Any] = dict((rows[0] or {}).get("metadata") or {}) if rows else {}
    meta.update(patch)
    client.table("pokemon_sets").update({"metadata": meta}).eq("set_code", sc).execute()


def _apply_tracked_ranks(
    client: Any,
    set_code: str,
    ordered_uids: List[str],
    *,
    source: str,
    extra_meta: Optional[Dict[str, Any]] = None,
) -> int:
    """Clear ranks for set, then set ``tracked_priority`` 1..len(ordered_uids). Returns count applied."""
    sc = str(set_code or "").strip().lower()
    if not sc or not ordered_uids:
        return 0
    client.table("pokemon_cards").update({"tracked_priority": None}).eq("set_code", sc).execute()
    uids = [u for u in ordered_uids if u]
    res = client.table("pokemon_cards").select("*").in_("unique_card_id", uids).execute()
    by_uid = {str(r.get("unique_card_id")): r for r in (res.data or []) if r.get("unique_card_id")}
    rows_out: List[Dict[str, Any]] = []
    for i, uid in enumerate(uids, start=1):
        row = by_uid.get(uid)
        if not row:
            continue
        row = dict(row)
        row["tracked_priority"] = i
        rows_out.append(row)
    if rows_out:
        client.table("pokemon_cards").upsert(rows_out, on_conflict="unique_card_id").execute()
    now = datetime.now(timezone.utc).isoformat()
    meta_patch: Dict[str, Any] = {
        "tcggo_tracked_top25_at": now,
        "tcggo_tracked_top25_source": source,
    }
    if extra_meta:
        meta_patch.update(extra_meta)
    _merge_set_metadata(client, sc, meta_patch)
    return len(rows_out)


def bootstrap_market_price_rank(client: Any) -> int:
    """Rank by ``market_price`` desc per set (no TCGGO). Returns number of cards assigned."""
    cards = _fetch_paginated(
        client,
        "pokemon_cards",
        "unique_card_id,set_code,name,number,market_price,metrics",
        order="set_code",
    )
    by_set: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for c in cards:
        sc = str(c.get("set_code") or "").strip().lower()
        if sc:
            by_set[sc].append(c)
    n_assigned = 0
    for sc, rows in by_set.items():
        ranked = sorted(
            rows,
            key=lambda r: (_num(r.get("market_price")) or 0.0),
            reverse=True,
        )[:25]
        uids = [str(c.get("unique_card_id") or "").strip() for c in ranked if c.get("unique_card_id")]
        n_assigned += _apply_tracked_ranks(
            client,
            sc,
            uids,
            source="market_price_bootstrap",
        )
    return n_assigned


def run_tcggo_refresh(client: Any, api_key: str) -> int:
    max_calls = max(10, int(os.environ.get("TCGGO_TRACKED_TOP25_MAX_CALLS", "200")))
    ep_pages = max(1, int(os.environ.get("TCGGO_EPISODE_LIST_MAX_PAGES", "22")))
    cache_days = max(1, int(os.environ.get("TCGGO_EPISODES_CACHE_DAYS", "14")))
    per_page = max(1, min(100, int(os.environ.get("TCGGO_TRACKED_PER_PAGE", "25"))))
    sleep_s = max(0.0, float(os.environ.get("TCGGO_TRACKED_TOP25_SLEEP_S", "0.2") or "0.2"))

    episodes_by_name, ep_calls = _load_or_fetch_episodes(
        client,
        api_key,
        max_episode_pages=ep_pages,
        cache_days=cache_days,
    )
    calls_used = ep_calls

    set_rows = _fetch_paginated(
        client,
        "pokemon_sets",
        "set_code,set_name,metadata",
        order="set_code",
    )
    cards = _fetch_paginated(
        client,
        "pokemon_cards",
        "unique_card_id,set_code,name,number,market_price,metrics",
        order="unique_card_id",
    )
    by_set_cards: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for c in cards:
        sc = str(c.get("set_code") or "").strip().lower()
        if sc:
            by_set_cards[sc].append(c)

    def sort_key(row: Dict[str, Any]) -> Tuple[float, str]:
        meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        at = str(meta.get("tcggo_tracked_top25_at") or "").strip()
        try:
            ts = datetime.fromisoformat(at.replace("Z", "+00:00")).timestamp()
        except ValueError:
            ts = 0.0
        return (ts, str(row.get("set_code") or ""))

    set_rows_sorted = sorted(set_rows, key=sort_key)

    refreshed = 0
    for srow in set_rows_sorted:
        if calls_used + 1 > max_calls:
            break
        sc = str(srow.get("set_code") or "").strip().lower()
        if not sc:
            continue
        eid = _resolve_tcggo_episode_id(srow, episodes_by_name)
        if not eid:
            continue

        api_cards = fetch_episode_cards_top(api_key, eid, per_page=per_page)
        calls_used += 1
        time.sleep(sleep_s)

        set_cards = by_set_cards.get(sc, [])
        if not set_cards:
            continue

        ordered_uids: List[str] = []
        for api_row in api_cards:
            if len(ordered_uids) >= per_page:
                break
            uid = _match_card_for_api_row(set_cards, api_row)
            if uid and uid not in ordered_uids:
                ordered_uids.append(uid)

        n = _apply_tracked_ranks(
            client,
            sc,
            ordered_uids,
            source="tcggo_price_highest",
            extra_meta={
                "tcggo_tracked_top25_episode_id": int(eid),
                "tcggo_tracked_top25_calls_budget": max_calls,
            },
        )
        refreshed += 1
        print(
            f"{sc}: ranked {n} cards (TCGGO episode {eid}), tcggo_calls {calls_used}/{max_calls}",
            flush=True,
        )

    print(f"Done. Sets refreshed: {refreshed}, tcggo_calls≈{calls_used}", flush=True)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Refresh tracked top cards per set (TCGGO or bootstrap)")
    ap.add_argument(
        "--bootstrap-market-price",
        action="store_true",
        help="Assign tracked_priority from market_price (no TCGGO); run once after migration.",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    api_key = (
        (os.environ.get("TCGPRO_API_KEY") or "").strip()
        or (os.environ.get("RAPIDAPI_KEY_TCGGO") or "").strip()
        or (os.environ.get("RAPIDAPI_KEY") or "").strip()
    )
    if not args.bootstrap_market_price and not api_key:
        print("Missing TCGPRO_API_KEY / RAPIDAPI_KEY (or use --bootstrap-market-price)", file=sys.stderr)
        return 1

    client = _supabase()
    if args.dry_run:
        print("Dry-run: no writes.", flush=True)
        return 0

    if args.bootstrap_market_price:
        n = bootstrap_market_price_rank(client)
        print(f"Bootstrap assigned ranks on {n} card rows.", flush=True)
        return 0

    return run_tcggo_refresh(client, api_key)


if __name__ == "__main__":
    raise SystemExit(main())
