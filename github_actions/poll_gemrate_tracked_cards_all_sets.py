#!/usr/bin/env python3
"""
Run scrape/gemrate_scraper.py over pokemon_sets_data.json (all sets' top_25_cards).

Designed for GitHub Actions after supabase_wizard_dataset_bridge.py export.
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
    ap = argparse.ArgumentParser(description="Poll GemRate for all tracked cards in pokemon_sets_data.json")
    ap.add_argument(
        "--data",
        type=Path,
        default=ROOT / "pokemon_sets_data.json",
        help="Path to pokemon_sets_data.json",
    )
    ap.add_argument(
        "--sleep-sets",
        type=float,
        default=2.0,
        help="Seconds between set scrapes (passed to gemrate_scraper.py --sleep-sets)",
    )
    args, passthrough = ap.parse_known_args()
    data = args.data.resolve()
    if not data.is_file():
        print(f"Missing {data}", file=sys.stderr)
        return 1

    script = ROOT / "scrape" / "gemrate_scraper.py"
    if not script.is_file():
        print(f"Missing {script}", file=sys.stderr)
        return 1

    cmd: list[str] = [
        sys.executable,
        str(script),
        "--data",
        str(data),
        "--sleep-sets",
        str(max(0.0, args.sleep_sets)),
    ]
    if passthrough:
        cmd.extend(passthrough)
    return _run(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
