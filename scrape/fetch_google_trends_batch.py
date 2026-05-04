#!/usr/bin/env python3
"""
Optional: batch-refresh Google Trends per base species using pytrends (unofficial).

This repo does **not** run this in CI. It exists so you can periodically rebuild
`Trend_Index_Average` before running `build_species_popularity_index.py`.

Install:
  pip install pytrends

Example (US, last 12 months — adjust geo / timeframe to match your prior export):
  python scrape/fetch_google_trends_batch.py --geo US --timeframe today 12-m \\
      --names-file species_names.txt --out trends_raw.json

Then merge into `google_trends_momentum.json` manually or extend this script.

Note: respect Google rate limits; sleep between requests.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import List, Optional


def _interest_over_time_with_retries(pytrends, batch: List[str], timeframe: str, geo: str, chunk_sleep: float) -> object:
    from pytrends import exceptions as pexc  # type: ignore

    tmre = getattr(pexc, "TooManyRequestsError", None)
    delays = (15.0, 45.0, 90.0, 150.0)
    last_err: Optional[Exception] = None
    for attempt, wait in enumerate([0.0] + list(delays)):
        if wait > 0:
            print(f"Rate limit / error — sleeping {wait:.0f}s then retry chunk (attempt {attempt + 1})…", file=sys.stderr)
            time.sleep(wait)
        try:
            pytrends.build_payload(batch, timeframe=timeframe, geo=geo)
            time.sleep(max(1.5, chunk_sleep * 0.15))
            return pytrends.interest_over_time()
        except Exception as e:
            is_429 = False
            if tmre is not None and isinstance(e, tmre):
                is_429 = True
            resp = getattr(e, "response", None)
            if resp is not None and getattr(resp, "status_code", None) == 429:
                is_429 = True
            if is_429:
                last_err = e
                continue
            raise
    if last_err:
        raise last_err
    raise RuntimeError("interest_over_time failed")


def _merge_rows_into_momentum(path: Path, rows: List[dict]) -> int:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("momentum JSON must be a list")
    by_char = {
        str(r.get("Character") or "").strip(): float(r["Trend_Index_Average"])
        for r in rows
        if isinstance(r, dict) and r.get("Character") and r.get("Trend_Index_Average") is not None
    }
    n = 0
    for row in data:
        if not isinstance(row, dict):
            continue
        ch = str(row.get("Character") or "").strip()
        if ch in by_char:
            row["Trend_Index_Average"] = round(by_char[ch], 3)
            n += 1
    path.write_text(json.dumps(data, indent=4, ensure_ascii=False) + "\n", encoding="utf-8")
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description="Stub / optional pytrends batch fetch")
    ap.add_argument("--names-file", type=Path, help="One species name per line")
    ap.add_argument("--geo", default="US")
    ap.add_argument("--timeframe", default="today 12-m")
    ap.add_argument("--out", type=Path, default=Path("trends_pytrends_raw.json"))
    ap.add_argument(
        "--momentum-json",
        type=Path,
        default=None,
        help="If set, merge Trend_Index_Average from fetched rows into this google_trends_momentum.json (by Character)",
    )
    ap.add_argument(
        "--chunk-sleep",
        type=float,
        default=28.0,
        help="Seconds to wait after each successful 5-term batch (rate limiting)",
    )
    ap.add_argument(
        "--resume",
        action="store_true",
        help="If --out exists, load prior rows and skip Character names already present",
    )
    args = ap.parse_args()

    try:
        from pytrends.request import TrendReq  # type: ignore
    except ImportError:
        print(
            "pytrends is not installed. Run: pip install pytrends\n"
            "Then re-run with --names-file listing base species (e.g. Charizard).",
            file=sys.stderr,
        )
        return 2

    if not args.names_file:
        print("Provide --names-file with one Pokémon name per line.", file=sys.stderr)
        return 1

    names = [ln.strip() for ln in args.names_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if not names:
        print("No names in file.", file=sys.stderr)
        return 1

    rows: List[dict] = []
    done: set[str] = set()
    if args.resume and args.out.is_file():
        try:
            prev = json.loads(args.out.read_text(encoding="utf-8"))
            if isinstance(prev, list):
                rows = [r for r in prev if isinstance(r, dict) and r.get("Character")]
                done = {str(r["Character"]).strip() for r in rows}
                print(f"Resume: loaded {len(rows)} rows from {args.out}", file=sys.stderr)
        except json.JSONDecodeError:
            pass

    names = [n for n in names if n not in done]
    if not names:
        print("Nothing left to fetch (all names already in --out).", file=sys.stderr)
        if args.momentum_json and args.momentum_json.is_file() and rows:
            n = _merge_rows_into_momentum(args.momentum_json, rows)
            print(f"Re-merged {n} momentum rows.", file=sys.stderr)
        return 0

    pytrends = TrendReq(hl="en-US", tz=360)
    chunk = 5  # pytrends batch limit
    chunk_sleep = max(8.0, float(args.chunk_sleep))
    print(f"Batches of {chunk}, {chunk_sleep:.0f}s pause between batches, timeframe={args.timeframe!r} geo={args.geo!r}", file=sys.stderr)
    for i in range(0, len(names), chunk):
        batch = names[i : i + chunk]
        print(f"  Batch {i // chunk + 1}: {batch!r}", file=sys.stderr)
        try:
            df = _interest_over_time_with_retries(pytrends, batch, str(args.timeframe), str(args.geo), chunk_sleep)
        except Exception as e:
            print(f"    Batch failed ({e!r}); saving partial progress.", file=sys.stderr)
            args.out.write_text(json.dumps(rows, indent=2), encoding="utf-8")
            if args.momentum_json and args.momentum_json.is_file() and rows:
                n = _merge_rows_into_momentum(args.momentum_json, rows)
                print(f"    Checkpoint: {args.out} ({len(rows)} rows), merged {n} into momentum.", file=sys.stderr)
            return 5
        if df is None or df.empty:
            print("    (empty response, skipping batch)", file=sys.stderr)
            time.sleep(chunk_sleep)
            continue
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"], errors="ignore")
        for name in batch:
            if name not in df.columns:
                continue
            mean_v = float(df[name].mean())
            rows.append({"Character": name, "Trend_Index_Average": round(mean_v, 3)})
        args.out.write_text(json.dumps(rows, indent=2), encoding="utf-8")
        if args.momentum_json and args.momentum_json.is_file():
            n = _merge_rows_into_momentum(args.momentum_json, rows)
            print(f"    checkpoint -> {len(rows)} raw rows, {n} momentum matches", file=sys.stderr)
        time.sleep(chunk_sleep)

    args.out.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"Wrote {len(rows)} rows to {args.out}", file=sys.stderr)

    if args.momentum_json:
        path = args.momentum_json
        if not path.is_file():
            print(f"--momentum-json not found: {path}", file=sys.stderr)
            return 4
        n = _merge_rows_into_momentum(path, rows)
        print(f"Updated Trend_Index_Average for {n} rows in {path}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
