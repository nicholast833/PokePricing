#!/usr/bin/env python3
"""
Build Explorer “trending” leaderboards from tracked ``pokemon_cards`` rows and upsert
``predictor_analytics_assets`` key ``explorer_trending_daily`` (JSON consumed by index.html).

Inspired by Collectrics-style boards (prior-day movers, graded vs raw lift):
  - prior_day_dollar_movers — last two daily closes: Collectrics ``j_raw_price`` when present,
    else merged ``price_history.tcggo_market_history`` ``price_usd`` (TCGGO daily sync).
  - week_pct_movers — same blended series vs best point on or before ~7d before latest
  - psa10_vs_raw_leaders — PSA 10 vs raw from ``tcggo_ebay_sold_prices`` with robust medians
    (no ``min()`` over mislabeled grades; ratio sanity cap via ``EXPLORER_TRENDING_MAX_PSA_RAW_RATIO``).
  - set_chase_by_tracked — per-set sum of top 10 ``market_price`` among tracked cards (chase proxy).
  - set_pack_trend_pct — largest |%| pack moves from ``pokemon_set_pack_pricing.pack_cost_price_history``.
  - set_most_tracked — tracked card counts per set.

Env: ``SUPABASE_URL`` + ``SUPABASE_SERVICE_ROLE_KEY`` or ``SUPABASE_KEY``.
Optional: ``EXPLORER_TRENDING_MAX_PSA_RAW_RATIO`` (default ``75``) drops PSA10/raw rows above this ratio
from ``psa10_vs_raw_leaders`` (guards mislabeled TCGGO grade rows).
Optional: ``EXPLORER_PACK_TREND_DAYS`` (default ``14``) lookback for pack % trend.

  python github_actions/build_explorer_trending_from_supabase.py
  python github_actions/build_explorer_trending_from_supabase.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from collections import defaultdict
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


def _median_simple(vals: List[float]) -> Optional[float]:
    pos = [float(x) for x in vals if isinstance(x, (int, float)) and math.isfinite(float(x)) and float(x) > 0]
    if not pos:
        return None
    pos.sort()
    n = len(pos)
    mid = n // 2
    if n % 2:
        return pos[mid]
    return (pos[mid - 1] + pos[mid]) / 2.0


_MEDIAN_KEY_BAD = ("volume", "count", "sample", "sales", "listing", "observation", "num_")


def _median_from_tcggo_sold_row(row: Dict[str, Any]) -> Optional[float]:
    """Pick a single positive USD-like median from a TCGGO /ebay-sold-prices row (avoid count/volume fields)."""
    preferred = (
        "median_sold_price",
        "median_price",
        "ebay_median_price",
        "sold_median",
        "median_usd",
        "median",
    )
    lk_to_orig = {str(k).lower(): k for k in row}
    for pk in preferred:
        orig = lk_to_orig.get(pk.lower())
        if orig is not None:
            n = _num(row.get(orig))
            if n is not None and n > 0:
                return n
    best_key: Optional[Tuple[int, str]] = None
    picked_val: Optional[float] = None
    for k, v in row.items():
        lk = str(k).lower()
        if "median" not in lk and "mean" not in lk and "average" not in lk and lk not in ("avg",):
            continue
        if any(b in lk for b in _MEDIAN_KEY_BAD):
            continue
        n = _num(v)
        if n is None or n <= 0:
            continue
        pri = 0 if "median" in lk else 1
        cand = (pri, str(k))
        if best_key is None or cand < best_key:
            best_key = cand
            picked_val = n
    if picked_val is not None:
        return picked_val
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


def _tcggo_market_sorted(flat: Dict[str, Any]) -> List[Tuple[str, float]]:
    """Daily TCGPlayer market USD from ``price_history.tcggo_market_history`` (daily queue)."""
    ph = flat.get("price_history")
    if not isinstance(ph, dict):
        return []
    hist = ph.get("tcggo_market_history")
    if not isinstance(hist, list):
        return []
    out: List[Tuple[str, float]] = []
    for r in hist:
        if not isinstance(r, dict):
            continue
        dk = str(r.get("date") or "")[:10]
        if len(dk) < 10:
            continue
        p = _num(r.get("price_usd"))
        if p is None or p <= 0:
            p = _num(r.get("tcg_player_market"))
        if p is None or p <= 0:
            continue
        out.append((dk, p))
    out.sort(key=lambda t: t[0])
    return out


def _merged_daily_close_sorted(flat: Dict[str, Any]) -> List[Tuple[str, float]]:
    """
    One price per calendar day for movers: Collectrics JustTCG wins on overlap,
    else TCGGO market history fills gaps (and supplies series when Collectrics is empty).
    """
    by_day: Dict[str, float] = {}
    for dk, p in _tcggo_market_sorted(flat):
        by_day[dk] = p
    for dk, p in _justtcg_sorted(flat):
        by_day[dk] = p
    out = [(d, by_day[d]) for d in sorted(by_day)]
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


def _pct_move_days(pts: List[Tuple[str, float]], days: int) -> Optional[Dict[str, Any]]:
    if len(pts) < 2:
        return None
    last_d, last_p = pts[-1]
    try:
        last_dt = datetime.strptime(last_d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    cut = last_dt - timedelta(days=int(days))
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


def _week_pct_move(pts: List[Tuple[str, float]]) -> Optional[Dict[str, Any]]:
    return _pct_move_days(pts, 7)


_SLAB_GRADER_RE = re.compile(
    r"\b(psa|bgs|beckett|cgc|sgc|tag)\b",
    re.I,
)


def _label_looks_graded_slab(lbl: str) -> bool:
    s = str(lbl or "").strip().lower()
    if not s:
        return False
    if _SLAB_GRADER_RE.search(s):
        return True
    if "slab" in s or "subgrade" in s:
        return True
    return False


def _label_looks_explicit_raw(lbl: str) -> bool:
    s = str(lbl or "").strip().lower()
    if not s:
        return False
    if "raw" in s or "ungraded" in s or "not graded" in s:
        return True
    if re.search(
        r"\b(nm|near mint|near-mint|lightly played|moderately played|heavily played|damaged)\b",
        s,
    ):
        return True
    return False


def _psa10_vs_raw(flat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    max_ratio = float(os.environ.get("EXPLORER_TRENDING_MAX_PSA_RAW_RATIO", "75"))

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

    psa10_vals = [
        m
        for lbl, m in parsed
        if re.search(r"\bpsa\b", str(lbl).lower()) and re.search(r"\b10\b", str(lbl).lower())
    ]
    psa10 = _median_simple(psa10_vals) if psa10_vals else None

    raw_explicit = [m for lbl, m in parsed if _label_looks_explicit_raw(lbl)]
    if raw_explicit:
        raw_m = _median_simple(raw_explicit)
    else:
        floor_usd = 0.5
        if psa10 is not None and psa10 > 0 and max_ratio > 0:
            floor_usd = max(floor_usd, psa10 / max_ratio)
        loose = [
            m
            for lbl, m in parsed
            if not _label_looks_graded_slab(lbl) and m is not None and m >= floor_usd
        ]
        raw_m = _median_simple(loose) if loose else None

    if raw_m is None:
        mp = _num(flat.get("market_price"))
        if mp is not None and mp > 0:
            raw_m = mp

    if psa10 is None or raw_m is None or raw_m <= 0:
        return None
    ratio = psa10 / raw_m

    if ratio > max_ratio or not math.isfinite(ratio):
        return None
    if raw_m < 1.0 and ratio > 30:
        return None

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


def _fetch_set_name_map(client: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    page = 800
    start = 0
    while True:
        res = (
            client.table("pokemon_sets")
            .select("set_code,name,metadata")
            .order("set_code")
            .range(start, start + page - 1)
            .execute()
        )
        batch = res.data or []
        for r in batch:
            if not isinstance(r, dict):
                continue
            sc = str(r.get("set_code") or "").strip().lower()
            if not sc:
                continue
            nm = str(r.get("name") or r.get("set_name") or "").strip()
            if not nm:
                meta = r.get("metadata") if isinstance(r.get("metadata"), dict) else {}
                nm = str(meta.get("set_name") or meta.get("title") or "").strip()
            out[sc] = nm or sc
        if len(batch) < page:
            break
        start += page
    return out


def _pack_price_pts_from_row(row: Dict[str, Any]) -> List[Tuple[str, float]]:
    hist = row.get("pack_cost_price_history")
    if not isinstance(hist, list):
        return []
    out: List[Tuple[str, float]] = []
    for r in hist:
        if not isinstance(r, dict):
            continue
        dk = str(r.get("date") or "")[:10]
        if len(dk) < 10:
            continue
        p = _num(r.get("price_usd"))
        if p is None or p <= 0:
            p = _num(r.get("tcg_player_market"))
        if p is None or p <= 0:
            continue
        out.append((dk, p))
    out.sort(key=lambda t: t[0])
    return out


def _build_set_chase_from_tracked(raw_rows: List[Dict[str, Any]], names: Dict[str, str]) -> List[Dict[str, Any]]:
    by_prices: Dict[str, List[float]] = defaultdict(list)
    for raw in raw_rows:
        flat = _card_row_to_toplist_shape(raw)
        sc = str(flat.get("set_code") or "").strip().lower()
        p = _num(flat.get("market_price"))
        if not sc or p is None or p <= 0:
            continue
        by_prices[sc].append(float(p))
    rows: List[Dict[str, Any]] = []
    for sc, prices in by_prices.items():
        prices.sort(reverse=True)
        n_take = min(10, len(prices))
        chase = sum(prices[:n_take])
        rows.append(
            {
                "set_code": sc,
                "set_name": names.get(sc, sc),
                "chase_sum_usd": round(chase, 2),
                "n_tracked": len(prices),
            }
        )
    rows.sort(key=lambda r: float(r.get("chase_sum_usd") or 0), reverse=True)
    return rows[:_TOP_N]


def _build_set_most_tracked(raw_rows: List[Dict[str, Any]], names: Dict[str, str]) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = defaultdict(int)
    for raw in raw_rows:
        flat = _card_row_to_toplist_shape(raw)
        sc = str(flat.get("set_code") or "").strip().lower()
        if not sc:
            continue
        counts[sc] += 1
    rows = [
        {"set_code": sc, "set_name": names.get(sc, sc), "n_tracked": n}
        for sc, n in counts.items()
    ]
    rows.sort(key=lambda r: int(r.get("n_tracked") or 0), reverse=True)
    return rows[:_TOP_N]


def _build_set_pack_trends(names: Dict[str, str], pack_rows: List[Dict[str, Any]], days: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in pack_rows:
        if not isinstance(row, dict):
            continue
        sc = str(row.get("set_code") or "").strip().lower()
        if not sc:
            continue
        pts = _pack_price_pts_from_row(row)
        mv = _pct_move_days(pts, days)
        if mv is None or abs(float(mv.get("pct_change") or 0)) < 0.25:
            continue
        out.append(
            {
                "set_code": sc,
                "set_name": names.get(sc, sc),
                "pct_change": mv["pct_change"],
                "from_date": mv.get("from_date"),
                "to_date": mv.get("to_date"),
                "from_price_usd": mv.get("from_price_usd"),
                "to_price_usd": mv.get("to_price_usd"),
            }
        )
    out.sort(key=lambda r: abs(float(r.get("pct_change") or 0)), reverse=True)
    return out[:_TOP_N]


def _fetch_pack_pricing_rows(client: Any) -> List[Dict[str, Any]]:
    try:
        res = (
            client.table("pokemon_set_pack_pricing")
            .select("set_code,pack_cost_price_history,pack_cost_primary_usd")
            .execute()
        )
        data = res.data or []
        return [x for x in data if isinstance(x, dict)]
    except Exception:
        return []


def _fetch_tracked_cards(client: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    start = 0
    while True:
        q = (
            client.table("pokemon_cards")
            .select(
                "unique_card_id,set_code,name,number,image_url,rarity,market_price,metrics,price_history,tracked_priority"
            )
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
    pack_days = max(3, min(int(os.environ.get("EXPLORER_PACK_TREND_DAYS", "14")), 120))
    set_names = _fetch_set_name_map(client)
    pack_rows = _fetch_pack_pricing_rows(client)
    set_chase = _build_set_chase_from_tracked(rows, set_names)
    set_tracked = _build_set_most_tracked(rows, set_names)
    set_pack = _build_set_pack_trends(set_names, pack_rows, pack_days)

    prior: List[Dict[str, Any]] = []
    week: List[Dict[str, Any]] = []
    grade: List[Dict[str, Any]] = []

    for raw in rows:
        flat = _card_row_to_toplist_shape(raw)
        base = _card_summary(flat)
        if not base["unique_card_id"]:
            continue
        pts = _merged_daily_close_sorted(flat)
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
        "pack_trend_days": pack_days,
        "set_chase_by_tracked": set_chase,
        "set_most_tracked": set_tracked,
        "set_pack_trend_pct": set_pack,
        "prior_day_dollar_movers": prior[:_TOP_N],
        "week_pct_movers": week[:_TOP_N],
        "psa10_vs_raw_leaders": grade[:_TOP_N],
    }

    if args.dry_run:
        summary = {
            "computed_at": payload.get("computed_at"),
            "pack_trend_days": payload.get("pack_trend_days"),
            "set_chase_by_tracked": len(payload.get("set_chase_by_tracked") or []),
            "set_most_tracked": len(payload.get("set_most_tracked") or []),
            "set_pack_trend_pct": len(payload.get("set_pack_trend_pct") or []),
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
