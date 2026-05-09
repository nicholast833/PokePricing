#!/usr/bin/env python3
"""
CI / local orchestrator: Supabase export → sync_pack_costs → apply to DB (metadata + pokemon_set_pack_pricing).

Does not rely on workflow artifacts; the database is the source of truth after apply.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run(cmd: list[str]) -> int:
    print("==>", " ".join(cmd), flush=True)
    return subprocess.call(cmd, cwd=str(ROOT))


def main() -> int:
    ap = argparse.ArgumentParser(description="Export, compute pack costs, push to Supabase")
    ap.add_argument("--cache", type=Path, default=ROOT / "tcg_cache")
    ap.add_argument("--sleep", type=float, default=0.12)
    ap.add_argument(
        "--prefer",
        default="auto",
        help="auto | single_pack | box_implied | etb_implied | tcggo",
    )
    ap.add_argument("--tcggo-history-days", type=int, default=180)
    ap.add_argument(
        "--skip-supabase",
        action="store_true",
        help="Run export + sync only (no apply-pack-costs)",
    )
    ap.add_argument(
        "--no-pricing-table",
        action="store_true",
        help="Only merge pokemon_sets.metadata; skip upsert to pokemon_set_pack_pricing (table not migrated yet)",
    )
    args, rest = ap.parse_known_args()

    data = ROOT / "pokemon_sets_data.json"
    py = sys.executable
    bridge = ROOT / "github_actions" / "supabase_wizard_dataset_bridge.py"
    sync = ROOT / "github_actions" / "sync_pack_costs.py"
    if not bridge.is_file() or not sync.is_file():
        print("Missing bridge or sync_pack_costs.py", file=sys.stderr)
        return 1

    code = _run([py, str(bridge), "export", "--output", str(data)])
    if code != 0:
        return code

    cmd = [
        py,
        str(sync),
        "--all-sets",
        "--cache",
        str(args.cache.resolve()),
        "--sleep",
        str(max(0.0, args.sleep)),
        "--prefer",
        str(args.prefer),
        "--tcggo-history-days",
        str(max(7, int(args.tcggo_history_days))),
    ]
    if rest:
        cmd.extend(rest)
    code = _run(cmd)
    if code != 0:
        return code

    if args.skip_supabase:
        print("==> skip-supabase: not running apply-pack-costs", flush=True)
        return 0

    apply_cmd = [py, str(bridge), "apply-pack-costs", "--input", str(data)]
    if args.no_pricing_table:
        apply_cmd.append("--no-pricing-table")
    return _run(apply_cmd)


if __name__ == "__main__":
    raise SystemExit(main())
