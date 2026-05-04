#!/usr/bin/env python3
"""
Create / refresh tcg_macro_interest_by_year.json with one key per distinct
calendar release year in pokemon_sets_data.json (values null — replace with
numeric indices from Google Trends or another macro hobby series).

After running, edit by_year in the JSON (or merge a CSV) so each year has a
number; the analytics chart plots that X vs card price for every listing whose
set released in that year (e.g. Skyridge → 2003).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
SETS_PATH = ROOT / "pokemon_sets_data.json"
OUT_PATH = ROOT / "tcg_macro_interest_by_year.json"


def release_year(s: str) -> Optional[int]:
    if not s or not isinstance(s, str):
        return None
    m = re.match(r"^\s*(\d{4})\b", s.strip())
    if not m:
        return None
    y = int(m.group(1))
    return y if 1980 <= y <= 2100 else None


def main() -> int:
    if not SETS_PATH.is_file():
        print(f"Missing {SETS_PATH}", file=sys.stderr)
        return 1
    sets = json.loads(SETS_PATH.read_text(encoding="utf-8"))
    if not isinstance(sets, list):
        print("pokemon_sets_data.json must be a list", file=sys.stderr)
        return 1
    years: set[int] = set()
    for s in sets:
        if not isinstance(s, dict):
            continue
        y = release_year(str(s.get("release_date") or ""))
        if y is not None:
            years.add(y)
    by_year = {str(y): None for y in sorted(years)}
    doc = {
        "schema": "tcg_macro_interest_by_year",
        "schema_version": 1,
        "series_label": "Hobby-wide interest index (set release calendar year)",
        "source_note": "Replace null values with numeric indices (e.g. Google Trends).",
        "by_year": by_year,
    }
    prior = {}
    if OUT_PATH.is_file():
        try:
            prior = json.loads(OUT_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            prior = {}
        if isinstance(prior.get("by_year"), dict):
            for k, v in prior["by_year"].items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    if k in by_year:
                        by_year[k] = float(v) if isinstance(v, float) else int(v)
        for keep in ("series_label", "source_note"):
            if keep in prior and isinstance(prior[keep], str) and prior[keep].strip():
                doc[keep] = prior[keep].strip()
    OUT_PATH.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {OUT_PATH} with {len(by_year)} year keys ({sum(1 for v in by_year.values() if v is not None)} numeric).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
