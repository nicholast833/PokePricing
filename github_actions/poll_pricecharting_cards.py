#!/usr/bin/env python3
"""
PriceCharting poll: **Supabase by default** (query → scrape → write metrics). No ``pokemon_sets_data.json`` required.

Legacy JSON path (export → scrape file → apply):

  python github_actions/poll_pricecharting_cards.py --use-json-file [--skip-export] ...

Passthrough flags go to ``scrape/sync_pricecharting.py`` (e.g. ``--tracked-only``, ``--only-set-codes``, ``--sleep``).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BRIDGE = ROOT / "github_actions" / "supabase_wizard_dataset_bridge.py"
SYNC = ROOT / "scrape" / "sync_pricecharting.py"


def _run(cmd: list[str]) -> int:
    print("==>", " ".join(cmd), flush=True)
    return subprocess.call(cmd, cwd=str(ROOT))


def main() -> int:
    ap = argparse.ArgumentParser(description="Poll PriceCharting (Supabase-first or JSON round-trip)")
    ap.add_argument(
        "--use-json-file",
        action="store_true",
        help="Use pokemon_sets_data.json: export, scrape --data, apply-pricecharting",
    )
    ap.add_argument(
        "--data",
        type=Path,
        default=ROOT / "pokemon_sets_data.json",
        help="With --use-json-file: JSON path",
    )
    ap.add_argument(
        "--skip-export",
        action="store_true",
        help="With --use-json-file: do not re-export from Supabase",
    )
    ap.add_argument(
        "--skip-apply",
        action="store_true",
        help="Scrape only: with default Supabase mode use sync's --skip-supabase-apply; with --use-json-file omit apply-pricecharting",
    )
    args, passthrough = ap.parse_known_args()

    if not SYNC.is_file():
        print(f"Missing {SYNC}", file=sys.stderr)
        return 1

    if args.use_json_file:
        data = args.data.resolve()
        if not args.skip_export:
            if not BRIDGE.is_file():
                print(f"Missing {BRIDGE}", file=sys.stderr)
                return 1
            code = _run([sys.executable, str(BRIDGE), "export", "--output", str(data)])
            if code != 0:
                return code
        if not data.is_file():
            print(f"Missing {data}", file=sys.stderr)
            return 1
        cmd: list[str] = [sys.executable, str(SYNC), "--data", str(data)]
        if passthrough:
            cmd.extend(passthrough)
        code = _run(cmd)
        if code != 0:
            return code
        if args.skip_apply:
            return 0
        if not BRIDGE.is_file():
            print(f"Missing {BRIDGE}", file=sys.stderr)
            return 1
        return _run([sys.executable, str(BRIDGE), "apply-pricecharting", "--input", str(data)])

    cmd = [sys.executable, str(SYNC), "--from-supabase"]
    if args.skip_apply:
        cmd.append("--skip-supabase-apply")
    if passthrough:
        cmd.extend(passthrough)
    return _run(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
