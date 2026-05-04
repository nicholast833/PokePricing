#!/usr/bin/env python3
"""
Run timestamped backup, then Pokémon Wizard sync (all sets), then PriceCharting for every set listed in
pricecharting_set_paths.json. Each child uses per-set checkpoints by default.

  python scrape/run_bulk_wizard_pricecharting.py --sleep-wizard 0.12 --sleep-pc 0.18
  python scrape/run_bulk_wizard_pricecharting.py --pricecharting-only --sleep-pc 0.2
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    ap = argparse.ArgumentParser(description="Backup + Wizard + PriceCharting bulk sync")
    ap.add_argument("--wizard-only", action="store_true")
    ap.add_argument("--pricecharting-only", action="store_true")
    ap.add_argument("--sleep-wizard", type=float, default=0.12)
    ap.add_argument("--sleep-pc", type=float, default=0.18)
    ap.add_argument("--skip-backup", action="store_true")
    args = ap.parse_args()
    if args.wizard_only and args.pricecharting_only:
        ap.error("use at most one of --wizard-only / --pricecharting-only")

    py = sys.executable
    if not args.skip_backup:
        r = subprocess.run([py, str(ROOT / "scripts" / "backup_pokemon_sets_data.py")], cwd=str(ROOT))
        if r.returncode != 0:
            return r.returncode

    env = {**__import__("os").environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
    if not args.pricecharting_only:
        cmd = [py, str(ROOT / "scrape" / "sync_pokemon_wizard.py"), "--sleep", str(args.sleep_wizard)]
        print(">>>", " ".join(cmd), flush=True)
        r = subprocess.run(cmd, cwd=str(ROOT), env=env)
        if r.returncode != 0:
            return r.returncode
    if not args.wizard_only:
        cmd = [py, str(ROOT / "scrape" / "sync_pricecharting.py"), "--sleep", str(args.sleep_pc)]
        print(">>>", " ".join(cmd), flush=True)
        r = subprocess.run(cmd, cwd=str(ROOT), env=env)
        if r.returncode != 0:
            return r.returncode
    print("Done.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
