"""
Merge append-only price history segments for long-running workflows.

Used by ``run_daily_api_queue`` (TCGGO + eBay), ``sync_pokemon_wizard`` (Wizard chart rows),
and ``supabase_wizard_dataset_bridge.apply_wizard`` (Supabase JSONB).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Set


def merge_tcggo_market_history_by_date(
    existing: Any,
    incoming: Any,
    *,
    max_points: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Union by ``date`` (YYYY-MM-DD); incoming row wins on duplicate day. Sorted ascending; tail-capped."""
    cap = max_points if max_points is not None else max(500, int(os.environ.get("TCGGO_MERGED_MAX_POINTS", "5000")))
    old_l = existing if isinstance(existing, list) else []
    new_l = incoming if isinstance(incoming, list) else []
    by_date: Dict[str, Dict[str, Any]] = {}
    for row in old_l + new_l:
        if not isinstance(row, dict):
            continue
        d = str(row.get("date") or "")[:10]
        if len(d) < 10:
            continue
        cur = dict(by_date.get(d) or {})
        cur.update(row)
        cur["date"] = d
        by_date[d] = cur
    out = sorted(by_date.values(), key=lambda r: str(r.get("date") or ""))
    if len(out) > cap:
        out = out[-cap:]
    return out


def _wizard_row_merge_key(row: Dict[str, Any]) -> str:
    sk = str(row.get("sort_key") or "").strip()
    if len(sk) >= 10:
        head = sk[:10].replace("/", "-")
        if head[:4].isdigit() and head[5:7].isdigit() and head[8:10].isdigit():
            return head
    return sk[:200] if sk else "__empty__"


def merge_wizard_price_history_rows(
    existing: Any,
    incoming: Any,
    *,
    max_rows: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Union Wizard table/chart rows by stable key (calendar prefix of ``sort_key`` when present)."""
    cap = max_rows if max_rows is not None else max(400, int(os.environ.get("WIZARD_MERGED_MAX_ROWS", "3000")))
    old_l = existing if isinstance(existing, list) else []
    new_l = incoming if isinstance(incoming, list) else []
    by_k: Dict[str, Dict[str, Any]] = {}
    for row in old_l:
        if not isinstance(row, dict):
            continue
        by_k[_wizard_row_merge_key(row)] = dict(row)
    for row in new_l:
        if not isinstance(row, dict):
            continue
        by_k[_wizard_row_merge_key(row)] = dict(row)
    out = sorted(by_k.values(), key=lambda r: str(r.get("sort_key") or ""))
    if len(out) > cap:
        out = out[-cap:]
    return out


def _yesterday_utc(d_key: str) -> Optional[str]:
    try:
        d0 = datetime.strptime(d_key[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return (d0 - timedelta(days=1)).strftime("%Y-%m-%d")


def append_ebay_anonymous_cohort_daily(
    price_history: Dict[str, Any],
    *,
    today_d: str,
    total_api: Any,
    cohort: Sequence[Dict[str, Any]],
) -> None:
    """
    Append one day to ``price_history['ebay_active_anonymous_cohort_history']``.

    Each entry: date, total_api, n_listings, listing_sigs (sorted SHA-256 hex, no item ids),
    optional sig_ended_proxy / sig_new_proxy vs previous stored day.
    """
    maxh = max(60, int(os.environ.get("EBAY_ANONYMOUS_HISTORY_MAX", "730")))

    hist = list(price_history.get("ebay_active_anonymous_cohort_history") or [])
    hist = [x for x in hist if not (isinstance(x, dict) and str(x.get("date") or "")[:10] == today_d)]

    sigs: List[str] = []
    for row in cohort:
        if not isinstance(row, dict):
            continue
        s = row.get("sig")
        if isinstance(s, str) and len(s) == 64:
            sigs.append(s)
    sigs_sorted = sorted(set(sigs))
    now_set: Set[str] = set(sigs_sorted)

    prev_entry: Optional[Dict[str, Any]] = None
    yd = _yesterday_utc(today_d)
    if yd:
        for x in reversed(hist):
            if isinstance(x, dict) and str(x.get("date") or "")[:10] == yd:
                prev_entry = x
                break

    tot_out: Any = total_api
    if tot_out is not None:
        try:
            tot_out = int(tot_out)
        except (TypeError, ValueError):
            pass
    entry: Dict[str, Any] = {
        "date": today_d,
        "total_api": tot_out,
        "n_listings": len(sigs_sorted),
        "listing_sigs": sigs_sorted,
    }
    if prev_entry and isinstance(prev_entry.get("listing_sigs"), list):
        prev_set = {str(s) for s in prev_entry["listing_sigs"] if isinstance(s, str)}
        entry["sig_ended_proxy"] = len(prev_set - now_set)
        entry["sig_new_proxy"] = len(now_set - prev_set)

    hist.append(entry)
    hist.sort(key=lambda r: str(r.get("date") or ""))
    if len(hist) > maxh:
        hist = hist[-maxh:]
    price_history["ebay_active_anonymous_cohort_history"] = hist
