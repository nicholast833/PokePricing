#!/usr/bin/env python3
"""
Refresh Pokémon Wizard chart price history for every set's top_25_cards in pokemon_sets_data.json.

Runs scrape/sync_pokemon_wizard.py in full mode (not --only-missing-price-history), so each tracked
card's Wizard card page is re-fetched and ``pokemon_wizard_price_history`` is **merged** with the
latest chart/table rows (newer scrape wins on duplicate dates) so long-running schedules accumulate
history beyond a single 1Y chart window where rows overlap.

Designed for GitHub Actions; no API secrets required (public Wizard HTML + TCGPlayer mp-search-api).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run(cmd: list[str]) -> int:
    print("==>", " ".join(cmd), flush=True)
    return subprocess.call(cmd, cwd=str(ROOT))


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Poll Pokémon Wizard 1Y chart history for all top_25_cards across sets",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Delay between HTTP calls (default 0.2; raise if you see 429s)",
    )
    ap.add_argument("--max-sets", type=int, default=0, help="0 = all sets after filters")
    ap.add_argument(
        "--max-cards",
        type=int,
        default=0,
        help="Stop after N card rows (0 = no cap; useful for smoke tests)",
    )
    ap.add_argument("--only-set-codes", default="", help="Comma-separated set_code values")
    ap.add_argument("--no-backup", action="store_true", help="Skip .bak copy of pokemon_sets_data.json")
    ap.add_argument(
        "--no-checkpoint-every-set",
        action="store_true",
        help="Write pokemon_sets_data.json only once at end (faster; riskier on timeout)",
    )
    args, passthrough = ap.parse_known_args()

    data = ROOT / "pokemon_sets_data.json"
    if not data.is_file():
        print(f"Missing {data}", file=sys.stderr)
        return 1

    py = sys.executable
    sync = ROOT / "scrape" / "sync_pokemon_wizard.py"
    if not sync.is_file():
        print(f"Missing {sync}", file=sys.stderr)
        return 1

    cmd: list[str] = [
        py,
        str(sync),
        "--sleep",
        str(max(0.0, args.sleep)),
    ]
    if not args.no_backup:
        cmd.append("--backup")
    if args.max_sets > 0:
        cmd.extend(["--max-sets", str(args.max_sets)])
    if args.max_cards > 0:
        cmd.extend(["--max-cards", str(args.max_cards)])
    oc = (args.only_set_codes or "").strip()
    if oc:
        cmd.extend(["--only-set-codes", oc])
    if args.no_checkpoint_every_set:
        cmd.append("--no-checkpoint-every-set")
    if passthrough:
        cmd.extend(passthrough)

    return _run(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
