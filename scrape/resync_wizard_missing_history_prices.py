#!/usr/bin/env python3
"""
Find top_25_cards rows that have pokemon_wizard_url + price_history but at least one history row
with a missing price_usd (e.g. old sync when $1,995.52 did not match \\$(\\d+\\.\\d+)).

Then run sync_pokemon_wizard.py for the affected set_codes only (no --resume-skip-has-url).

Usage:
  python scrape/resync_wizard_missing_history_prices.py --dry-run
  python scrape/resync_wizard_missing_history_prices.py --sleep 0.15
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Set

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "pokemon_sets_data.json"

HEADER_LABELS = frozenset(
    {"date", "price", "trend", "when", "label", "sort_key", ""},
)


def row_missing_price_usd(row: Any) -> bool:
    if not isinstance(row, dict):
        return False
    label = str(row.get("label") or row.get("sort_key") or "").strip().lower()
    if label in HEADER_LABELS:
        return False
    pu = row.get("price_usd")
    if pu is None:
        return True
    if isinstance(pu, str) and not pu.strip():
        return True
    return False


def card_needs_wizard_price_resync(card: Dict[str, Any]) -> bool:
    if not card.get("pokemon_wizard_url"):
        return False
    ph = card.get("pokemon_wizard_price_history")
    if not isinstance(ph, list):
        return False
    for row in ph:
        if row_missing_price_usd(row):
            return True
    return False


def affected_set_codes(data: List[Any]) -> Set[str]:
    out: Set[str] = set()
    for s in data:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        if not sc:
            continue
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue
        for c in top:
            if isinstance(c, dict) and card_needs_wizard_price_resync(c):
                out.add(sc)
                break
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Resync Wizard for sets with null price_usd in history rows")
    ap.add_argument("--input", type=Path, default=DATA)
    ap.add_argument("--dry-run", action="store_true", help="Only print set_codes and counts")
    ap.add_argument("--sleep", type=float, default=0.15, help="Passed to sync_pokemon_wizard.py")
    ap.add_argument("--max-sets-per-batch", type=int, default=60, help="Chunk --only-set-codes to avoid huge argv")
    args = ap.parse_args()

    raw = json.loads(args.input.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        print("Expected JSON array", file=sys.stderr)
        return 2

    codes = sorted(affected_set_codes(raw))
    n_cards = 0
    for s in raw:
        if not isinstance(s, dict):
            continue
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue
        for c in top:
            if isinstance(c, dict) and card_needs_wizard_price_resync(c):
                n_cards += 1

    print(f"Affected sets: {len(codes)}", flush=True)
    print(f"Affected top-list card rows: {n_cards}", flush=True)
    if not codes:
        print("Nothing to do.", flush=True)
        return 0
    print("set_codes:", ",".join(codes), flush=True)

    if args.dry_run:
        return 0

    py = sys.executable
    wiz = ROOT / "scrape" / "sync_pokemon_wizard.py"
    env = {**__import__("os").environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
    chunk = max(1, int(args.max_sets_per_batch))
    for i in range(0, len(codes), chunk):
        batch = codes[i : i + chunk]
        only = ",".join(batch)
        cmd = [py, str(wiz), "--sleep", str(args.sleep), "--only-set-codes", only]
        print(f"\n>>> batch {i // chunk + 1} ({len(batch)} sets): {only[:120]}{'...' if len(only) > 120 else ''}", flush=True)
        r = subprocess.run(cmd, cwd=str(ROOT), env=env)
        if r.returncode != 0:
            print(f"FAILED exit {r.returncode}", flush=True)
            return r.returncode

    print("\nDone.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
