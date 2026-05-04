#!/usr/bin/env python3
"""
Merge missing card-level fields from ``pokemon_sets_data.json.bak`` into the current dataset.

Preserves all keys on the current file; fills in gemrate, Pokémon Wizard, TCGTracking,
Collectrics, eBay, PriceCharting, etc. when absent on the current card.

Does not remove or overwrite ``tcggo`` subtrees from current (TCGGO API merges stay authoritative).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CUR = ROOT / "pokemon_sets_data.json"
DEFAULT_BAK = ROOT / "pokemon_sets_data.json.bak"


def norm_name(s: str) -> str:
    return " ".join(str(s or "").strip().lower().split())


def norm_num(s: Any) -> str:
    return str(s if s is not None else "").strip()


def card_key(name: Any, num: Any) -> tuple[str, str]:
    return (norm_name(name), norm_num(num))


def is_empty(val: Any) -> bool:
    if val is None:
        return True
    if val == "":
        return True
    if isinstance(val, list) and len(val) == 0:
        return True
    if isinstance(val, dict) and len(val) == 0:
        return True
    return False


def should_fill(current: Any) -> bool:
    return is_empty(current)


def merge_card(current: dict[str, Any], backup: dict[str, Any]) -> tuple[dict[str, Any], int]:
    filled = 0
    out = dict(current)
    cur_tcggo = current.get("tcggo")
    for k, v in backup.items():
        if k == "tcggo":
            continue
        if k not in out or should_fill(out.get(k)):
            if not is_empty(v):
                out[k] = v
                filled += 1
    if isinstance(cur_tcggo, dict) and cur_tcggo:
        out["tcggo"] = cur_tcggo
    return out, filled


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--current", type=Path, default=DEFAULT_CUR)
    ap.add_argument("--backup", type=Path, default=DEFAULT_BAK)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cur = json.loads(args.current.read_text(encoding="utf-8"))
    bak = json.loads(args.backup.read_text(encoding="utf-8"))
    if not isinstance(cur, list) or not isinstance(bak, list):
        raise SystemExit("Expected JSON arrays")

    bak_sets: dict[str, dict[str, Any]] = {}
    for s in bak:
        if isinstance(s, dict) and s.get("set_code"):
            bak_sets[str(s["set_code"])] = s

    total_fill = 0
    cards_touched = 0
    missing_set = 0

    for s in cur:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "")
        if not sc or sc not in bak_sets:
            if sc:
                missing_set += 1
            continue
        bs = bak_sets[sc]
        bmap: dict[tuple[str, str], dict[str, Any]] = {}
        for c in bs.get("top_25_cards") or []:
            if not isinstance(c, dict):
                continue
            bmap[card_key(c.get("name"), c.get("number"))] = c

        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue
        new_top = []
        for c in top:
            if not isinstance(c, dict):
                new_top.append(c)
                continue
            bk = card_key(c.get("name"), c.get("number"))
            bc = bmap.get(bk)
            if not bc:
                new_top.append(c)
                continue
            merged, n = merge_card(c, bc)
            total_fill += n
            if n:
                cards_touched += 1
            new_top.append(merged)
        s["top_25_cards"] = new_top

    rep = {
        "backup_file": str(args.backup),
        "current_file": str(args.current),
        "fields_filled_total": total_fill,
        "cards_with_any_fill": cards_touched,
        "sets_in_current_missing_from_backup": missing_set,
    }
    rep_path = args.current.with_suffix(".json.restore_from_bak_report.json")
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")

    if args.dry_run:
        print(json.dumps(rep, indent=2))
        return 0

    args.current.write_text(json.dumps(cur, indent=4, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Restored fields: {total_fill} on {cards_touched} cards. Report: {rep_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
