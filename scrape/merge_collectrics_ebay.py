#!/usr/bin/env python3
"""
Merge optional MyCollectrics-style **eBay liquidity** fields onto cards in
`pokemon_sets_data.json` from a small sidecar (manual overrides).

Prefer the API sync for bulk data + price histories:
  python scrape/sync_collectrics_data.py --backup

Sidecar: `collectrics_ebay_liquidity.json` — JSON array of rows:
  {
    "set_code": "sv4",
    "number": "121",
    "name": "Meowth ex",
    "collectrics_ebay_listings": 142,
    "collectrics_ebay_sold_volume": 89
  }

At least one of collectrics_ebay_listings / collectrics_ebay_sold_volume should
be set (integers). Matching uses the same normalisation as TCGTracking merge
(`set_code` lowercased, card number / name norms).

Run:
  python scrape/merge_collectrics_ebay.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIDECAR = ROOT / "collectrics_ebay_liquidity.json"
SETS = ROOT / "pokemon_sets_data.json"


def main() -> int:
    if not SETS.is_file():
        print(f"Missing {SETS}", file=sys.stderr)
        return 1
    if not SIDECAR.is_file():
        print(f"No {SIDECAR.name} — create with [] or row objects.", file=sys.stderr)
        return 0

    sys.path.insert(0, str(ROOT))
    from tcgtracking_merge import norm_card_name, norm_card_number

    def row_key(sc: str, num: object, nm: object) -> str:
        return f"{str(sc).strip().lower()}|{norm_card_number(num)}|{norm_card_name(nm)}"

    rows = json.loads(SIDECAR.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or len(rows) == 0:
        print("Sidecar empty; nothing to merge.")
        return 0

    idx: dict[str, dict] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        sc = r.get("set_code")
        if sc is None or str(sc).strip() == "":
            continue
        idx[row_key(str(sc), r.get("number"), r.get("name"))] = r

    data = json.loads(SETS.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        print("pokemon_sets_data.json must be a list", file=sys.stderr)
        return 1

    hits = 0
    for s in data:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        for c in s.get("top_25_cards") or []:
            if not isinstance(c, dict):
                continue
            k = row_key(sc, c.get("number"), c.get("name"))
            row = idx.get(k)
            if not row:
                continue
            li = row.get("collectrics_ebay_listings")
            sv = row.get("collectrics_ebay_sold_volume")
            if li is not None:
                try:
                    c["collectrics_ebay_listings"] = int(li)
                except (TypeError, ValueError):
                    pass
            if sv is not None:
                try:
                    c["collectrics_ebay_sold_volume"] = int(sv)
                except (TypeError, ValueError):
                    pass
            hits += 1

    SETS.write_text(json.dumps(data, indent=4, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Merged Collectrics eBay fields onto {hits} top-list card(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
