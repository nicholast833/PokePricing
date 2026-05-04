#!/usr/bin/env python3
"""
Summarize PriceCharting coverage after sync_pricecharting.py:

- Explorer sets with no segment in pricecharting_set_paths.json
- top_25_cards missing pricecharting_url (never merged or slug miss)
- Merged cards missing pricecharting_chart_data (VGPC history blob) or grade table

  python scrape/report_pricecharting_gaps.py
  python scrape/report_pricecharting_gaps.py --output tmp/pricecharting_remaining_gaps.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
PATHS = ROOT / "pricecharting_set_paths.json"
DATA = ROOT / "pokemon_sets_data.json"
SKIPS = ROOT / "pricecharting_sync_skips.json"


def load_paths() -> Dict[str, str]:
    if not PATHS.is_file():
        return {}
    raw = json.loads(PATHS.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in raw.items():
        sk = str(k).strip()
        if sk.startswith("_") or not isinstance(v, str):
            continue
        t = v.strip().strip("/")
        if sk.lower() and t:
            out[sk.lower()] = t
    return out


def latest_skips() -> List[Dict[str, Any]]:
    if not SKIPS.is_file():
        return []
    doc = json.loads(SKIPS.read_text(encoding="utf-8"))
    runs = doc.get("runs") if isinstance(doc, dict) else None
    if not runs or not isinstance(runs, list):
        return []
    last = runs[-1]
    if not isinstance(last, dict):
        return []
    sk = last.get("skips")
    return sk if isinstance(sk, list) else []


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--output", type=Path, default=ROOT / "tmp" / "pricecharting_remaining_gaps.json")
    args = ap.parse_args()

    paths = load_paths()
    data = json.loads(DATA.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        print("Expected pokemon_sets_data.json array", file=sys.stderr)
        return 2

    explorer_missing_path: List[Dict[str, str]] = []
    cards_no_url: List[Dict[str, Any]] = []
    cards_no_chart: List[Dict[str, Any]] = []
    cards_no_grades: List[Dict[str, Any]] = []

    for s in data:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        if not sc:
            continue
        sn = str(s.get("set_name") or "")
        seg = paths.get(sc)
        if not seg:
            explorer_missing_path.append({"set_code": sc, "set_name": sn})
            continue
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue
        for c in top:
            if not isinstance(c, dict):
                continue
            nm = c.get("name")
            num = c.get("number")
            row = {"set_code": sc, "set_name": sn, "pc_segment": seg, "name": nm, "number": num}
            url = c.get("pricecharting_url")
            if not url:
                cards_no_url.append(dict(row))
                continue
            if not c.get("pricecharting_chart_data"):
                cards_no_chart.append(dict(row))
            if not c.get("pricecharting_grade_prices"):
                cards_no_grades.append(dict(row))

    out = {
        "mapped_set_codes": len(paths),
        "explorer_sets_without_path": len(explorer_missing_path),
        "top25_cards_missing_pricecharting_url": len(cards_no_url),
        "merged_cards_missing_chart_data": len(cards_no_chart),
        "merged_cards_missing_grade_prices_table": len(cards_no_grades),
        "explorer_sets_without_path_detail": explorer_missing_path,
        "cards_missing_url_detail": cards_no_url,
        "cards_missing_chart_data_detail": cards_no_chart[:200],
        "cards_missing_chart_data_truncated": len(cards_no_chart) > 200,
        "cards_missing_grade_prices_detail": cards_no_grades[:200],
        "cards_missing_grade_prices_truncated": len(cards_no_grades) > 200,
        "latest_sync_skip_rows": latest_skips(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {args.output}")
    print(
        f"No path: {len(explorer_missing_path)} sets | "
        f"No card URL: {len(cards_no_url)} | "
        f"No chart_data: {len(cards_no_chart)} | "
        f"No grade_prices: {len(cards_no_grades)} | "
        f"Skip log rows (last run): {len(out['latest_sync_skip_rows'])}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
