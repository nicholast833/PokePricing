#!/usr/bin/env python3
"""
Poll Google Trends (unofficial pytrends) for a **macro** hobby query, aggregate
weekly points to **calendar-year means**, and write `tcg_macro_interest_by_year.json`.

Google often returns sparse pre-2004 data; missing years stay null in the output.

Install: pip install pytrends

Example:
  python scrape/fetch_tcg_macro_trends_pytrends.py --keyword "pokemon tcg" --geo US
  python scrape/fetch_tcg_macro_trends_pytrends.py --keyword "pokemon cards" --geo ""
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "tcg_macro_interest_by_year.json"


def _yearly_means_from_interest_df(df, col: str) -> Dict[int, float]:
    sums: Dict[int, float] = defaultdict(float)
    counts: Dict[int, int] = defaultdict(int)
    if df is None or df.empty or col not in df.columns:
        return {}
    for ts, row in df.iterrows():
        try:
            y = int(ts.year)  # type: ignore[attr-defined]
        except Exception:
            continue
        v = row[col]
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(fv):
            continue
        sums[y] += fv
        counts[y] += 1
    out: Dict[int, float] = {}
    for y, s in sums.items():
        c = counts[y]
        if c > 0:
            out[y] = round(s / c, 3)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch macro Google Trends → tcg_macro_interest_by_year.json")
    ap.add_argument("--keyword", default="pokemon tcg", help="Single Trends search term (pytrends batch of 1)")
    ap.add_argument(
        "--geo",
        default="US",
        help="Trends region, e.g. US, GB, or empty string for worldwide aggregate",
    )
    ap.add_argument(
        "--timeframe",
        default="",
        help="Custom 'YYYY-MM-DD YYYY-MM-DD' or preset e.g. 'today 5-y', 'all'. Default: 2004-01-01 → today",
    )
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output JSON path")
    ap.add_argument("--sleep", type=float, default=2.0, help="Seconds after request (rate courtesy)")
    args = ap.parse_args()

    try:
        from pytrends.request import TrendReq  # type: ignore
    except ImportError:
        print("Install pytrends: pip install pytrends", file=sys.stderr)
        return 2

    kw = str(args.keyword).strip()
    if not kw:
        print("Empty --keyword", file=sys.stderr)
        return 1

    geo = str(args.geo).strip() if args.geo is not None else ""

    timeframe = str(args.timeframe).strip() if args.timeframe else f"2004-01-01 {date.today().isoformat()}"

    pytrends = TrendReq(hl="en-US", tz=360)
    print(f"Building payload: {kw!r} geo={geo!r} timeframe={timeframe!r}", file=sys.stderr)
    pytrends.build_payload([kw], cat=0, timeframe=timeframe, geo=geo, gprop="")
    time.sleep(max(0.0, float(args.sleep)))
    df = pytrends.interest_over_time()
    if df is None or df.empty:
        print("interest_over_time() returned no rows (blocked, geo, or query).", file=sys.stderr)
        return 3

    if "isPartial" in df.columns:
        df = df.drop(columns=["isPartial"], errors="ignore")

    col = kw if kw in df.columns else (df.columns[0] if len(df.columns) else None)
    if col is None:
        print("Unexpected dataframe columns", list(df.columns), file=sys.stderr)
        return 3

    yearly = _yearly_means_from_interest_df(df, str(col))
    print(f"Aggregated {len(yearly)} calendar years from {len(df)} weekly rows.", file=sys.stderr)

    prior: Dict[str, Any] = {}
    if args.out.is_file():
        try:
            prior = json.loads(args.out.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            prior = {}

    by_year: Dict[str, Any] = {}
    if isinstance(prior.get("by_year"), dict):
        for k, v in prior["by_year"].items():
            by_year[str(k)] = v

    if not by_year:
        sets_path = ROOT / "pokemon_sets_data.json"
        if sets_path.is_file():
            import re

            sets = json.loads(sets_path.read_text(encoding="utf-8"))
            ys = set()
            if isinstance(sets, list):
                for s in sets:
                    if not isinstance(s, dict):
                        continue
                    rd = str(s.get("release_date") or "").strip()
                    m = re.match(r"^(\d{4})\b", rd)
                    if m:
                        y = int(m.group(1))
                        if 1980 <= y <= 2100:
                            ys.add(y)
            for y in sorted(ys):
                by_year[str(y)] = None

    for y_str, val in yearly.items():
        by_year[str(y_str)] = val

    built = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    doc = {
        "schema": "tcg_macro_interest_by_year",
        "schema_version": 1,
        "series_label": f"Google Trends (pytrends): {kw!r} · geo={geo or 'world'} · timeframe={timeframe!r} · mean weekly index by calendar year",
        "source_note": f"Polled via pytrends at {built}. Unofficial API; respect Google ToS and rate limits.",
        "fetched_at_utc": built,
        "keyword": kw,
        "geo": geo,
        "timeframe": timeframe,
        "by_year": dict(sorted(by_year.items(), key=lambda kv: int(kv[0]) if str(kv[0]).isdigit() else 0)),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    filled = sum(1 for v in by_year.values() if isinstance(v, (int, float)) and not isinstance(v, bool))
    print(f"Wrote {args.out} ({filled} numeric years).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
