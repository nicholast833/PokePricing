#!/usr/bin/env python3
"""
Build Explorer “trending” leaderboards from tracked ``pokemon_cards`` rows and upsert
``predictor_analytics_assets`` key ``explorer_trending_daily`` (JSON consumed by index.html).

Inspired by Collectrics-style boards (prior-day movers, graded vs raw lift):
  - prior_day_dollar_movers — last two Collectrics JustTCG daily points (``j_raw_price``)
  - week_pct_movers — % change vs best JustTCG point on or before ~7d before latest
  - psa10_vs_raw_leaders — PSA 10 vs raw/ungraded median from ``tcggo_ebay_sold_prices`` (TCGGO)

Env: ``SUPABASE_URL`` + ``SUPABASE_SERVICE_ROLE_KEY`` or ``SUPABASE_KEY``.

  python github_actions/build_explorer_trending_from_supabase.py
  python github_actions/build_explorer_trending_from_supabase.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "github_actions"))

from supabase_wizard_dataset_bridge import _card_row_to_toplist_shape, _supabase  # noqa: E402

_TOP_N = 20
_TRACKED_PAGE = 800


def _num(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _median_from_tcggo_sold_row(row: Dict[str, Any]) -> Optional[float]:
    for k, v in row.items():
        lk = str(k).lower()
        if not any(t in lk for t in ("median", "mean", "average", "avg")):
            continue
        n = _num(v)
        if n is not None and n > 0:
            return n
    for k in ("price", "sold_price", "ebay_price", "amount"):
        n = _num(row.get(k))
        if n is not None and n > 0:
            return n
    return None


def _tcggo_sold_label(row: Dict[str, Any]) -> str:
    g = str(
        row.get("grader")
        or row.get("grading_company")
        or row.get("company")
        or row.get("psa")
        or row.get("label")
        or row.get("name")
        or ""
    ).strip()
    gr = str(row.get("grade") or row.get("grade_label") or row.get("condition") or "").strip()
    return f"{g} {gr}".strip().lower()


def _justtcg_sorted(flat: Dict[str, Any]) -> List[Tuple[str, float]]:
    h = flat.get("collectrics_history_justtcg")
    if not isinstance(h, list):
        return []
    out: List[Tuple[str, float]] = []
    for r in h:
        if not isinstance(r, dict):
            continue
        dk = str(r.get("date") or "")[:10]
        if len(dk) < 10:
            continue
        p = _num(r.get("j_raw_price"))
        if p is None or p <= 0:
            continue
        out.append((dk, p))
    out.sort(key=lambda t: t[0])
    return out


def _prior_day_move(pts: List[Tuple[str, float]]) -> Optional[Dict[str, Any]]:
    if len(pts) < 2:
        return None
    prev_d, prev_p = pts[-2]
    last_d, last_p = pts[-1]
    delta = last_p - prev_p
    pct = (delta / prev_p) * 100.0 if prev_p > 0 else None
    return {
        "prior_date": last_d,
        "ref_date": prev_d,
        "delta_usd": round(delta, 4),
        "delta_pct": round(pct, 3) if pct is not None else None,
        "last_price_usd": round(last_p, 4),
    }


def _week_pct_move(pts: List[Tuple[str, float]]) -> Optional[Dict[str, Any]]:
    if len(pts) < 2:
        return None
    last_d, last_p = pts[-1]
    try:
        last_dt = datetime.strptime(last_d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    cut = last_dt - timedelta(days=7)
    ref_p = None
    ref_d = None
    for d, p in pts:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if dt <= cut and p > 0:
            ref_p, ref_d = p, d
    if ref_p is None or ref_p <= 0:
        return None
    pct = ((last_p - ref_p) / ref_p) * 100.0
    return {
        "from_date": ref_d,
        "to_date": last_d,
        "pct_change": round(pct, 3),
        "from_price_usd": round(ref_p, 4),
        "to_price_usd": round(last_p, 4),
    }


def _psa10_vs_raw(flat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rows = flat.get("tcggo_ebay_sold_prices")
    if not isinstance(rows, list) or len(rows) < 1:
        return None
    parsed: List[Tuple[str, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        m = _median_from_tcggo_sold_row(row)
        if m is None:
            continue
        parsed.append((_tcggo_sold_label(row), m))
    if len(parsed) < 1:
        return None
    psa10 = None
    for lbl, m in parsed:
        if "psa" in lbl and re.search(r"\b10\b", lbl):
            psa10 = m if psa10 is None else max(psa10, m)
    raw_m = None
    for lbl, m in parsed:
        if "raw" in lbl or "ungraded" in lbl:
            raw_m = m if raw_m is None else min(raw_m, m)
    if raw_m is None:
        for lbl, m in parsed:
            if "psa" in lbl or "bgs" in lbl or "cgc" in lbl or "sgc" in lbl:
                continue
            raw_m = m if raw_m is None else min(raw_m, m)
    if raw_m is None:
        mp = _num(flat.get("market_price"))
        if mp is not None and mp > 0:
            raw_m = mp
    if psa10 is None or raw_m is None or raw_m <= 0:
        return None
    ratio = psa10 / raw_m
    return {
        "psa10_median_usd": round(psa10, 4),
        "raw_median_usd": round(raw_m, 4),
        "psa10_vs_raw_ratio": round(ratio, 4),
        "psa10_vs_raw_pct": round((ratio - 1.0) * 100.0, 2),
    }


def _card_summary(flat: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "unique_card_id": str(flat.get("unique_card_id") or ""),
        "set_code": str(flat.get("set_code") or ""),
        "name": str(flat.get("name") or ""),
        "number": flat.get("number"),
        "image_url": flat.get("image_url"),
        "rarity": flat.get("rarity"),
    }


def _fetch_tracked_cards(client: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    start = 0
    while True:
        q = (
            client.table("pokemon_cards")
            .select("unique_card_id,set_code,name,number,image_url,rarity,market_price,metrics,tracked_priority")
            .gte("tracked_priority", 1)
            .order("tracked_priority")
            .range(start, start + _TRACKED_PAGE - 1)
        )
        res = q.execute()
        batch = res.data or []
        out.extend(batch)
        if len(batch) < _TRACKED_PAGE:
            break
        start += _TRACKED_PAGE
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    client = _supabase()
    rows = _fetch_tracked_cards(client)
    now_iso = datetime.now(timezone.utc).isoformat()

    prior: List[Dict[str, Any]] = []
    week: List[Dict[str, Any]] = []
    grade: List[Dict[str, Any]] = []

    for raw in rows:
        flat = _card_row_to_toplist_shape(raw)
        base = _card_summary(flat)
        if not base["unique_card_id"]:
            continue
        pts = _justtcg_sorted(flat)
        pm = _prior_day_move(pts)
        if pm is not None and abs(pm["delta_usd"]) >= 0.01:
            prior.append({**base, **pm})
        wm = _week_pct_move(pts)
        if wm is not None and abs(wm["pct_change"]) >= 0.5:
            week.append({**base, **wm})
        gm = _psa10_vs_raw(flat)
        if gm is not None and gm["psa10_vs_raw_ratio"] >= 1.15:
            grade.append({**base, **gm})

    prior.sort(key=lambda r: abs(float(r.get("delta_usd") or 0)), reverse=True)
    week.sort(key=lambda r: abs(float(r.get("pct_change") or 0)), reverse=True)
    grade.sort(key=lambda r: float(r.get("psa10_vs_raw_ratio") or 0), reverse=True)

    payload = {
        "computed_at": now_iso,
        "prior_day_dollar_movers": prior[:_TOP_N],
        "week_pct_movers": week[:_TOP_N],
        "psa10_vs_raw_leaders": grade[:_TOP_N],
    }

    if args.dry_run:
        summary = {
            "computed_at": payload.get("computed_at"),
            "prior_day_dollar_movers": len(payload.get("prior_day_dollar_movers") or []),
            "week_pct_movers": len(payload.get("week_pct_movers") or []),
            "psa10_vs_raw_leaders": len(payload.get("psa10_vs_raw_leaders") or []),
        }
        print(json.dumps(summary, indent=2))
        return 0

    client.table("predictor_analytics_assets").upsert(
        {
            "asset_key": "explorer_trending_daily",
            "payload": payload,
            "updated_at": now_iso,
        },
        on_conflict="asset_key",
    ).execute()
    print(f"Upserted explorer_trending_daily ({len(rows)} tracked cards scanned).", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
