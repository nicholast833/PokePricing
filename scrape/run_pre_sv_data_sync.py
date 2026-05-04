#!/usr/bin/env python3
"""
Run Collectrics + Pokémon Wizard syncs for Explorer sets released before Scarlet & Violet base
(release_date < 2023/03/31). Collectrics often returns no cards for these sets; Wizard still fills
price history / trends where set paths exist.

Optional: pass --with-pricecharting to also run scrape/sync_pricecharting.py for the same
set_codes (each set must be listed in pricecharting_set_paths.json).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "pokemon_sets_data.json"
CUTOFF = "2023/03/31"


def pre_sv_set_codes() -> str:
    data = json.loads(DATA.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("pokemon_sets_data.json must be a list")
    seen: set[str] = set()
    ordered: list[str] = []
    for s in data:
        if not isinstance(s, dict):
            continue
        code = str(s.get("set_code") or "").strip()
        if not code:
            continue
        rd = str(s.get("release_date") or "").replace("-", "/")
        if rd and rd < CUTOFF:
            cl = code.lower()
            if cl not in seen:
                seen.add(cl)
                ordered.append(code)
    return ",".join(ordered)


def main() -> int:
    ap = argparse.ArgumentParser(description="Sync Collectrics + Wizard for pre–Scarlet & Violet sets")
    ap.add_argument(
        "--collectrics-only",
        action="store_true",
        help="Only run sync_collectrics_data.py",
    )
    ap.add_argument(
        "--wizard-only",
        action="store_true",
        help="Only run sync_pokemon_wizard.py (resume after Collectrics or network errors)",
    )
    ap.add_argument(
        "--with-pricecharting",
        action="store_true",
        help="After Collectrics + Wizard, run sync_pricecharting.py for the same set_codes (needs pricecharting_set_paths.json entries).",
    )
    args = ap.parse_args()
    if args.collectrics_only and args.wizard_only:
        ap.error("use at most one of --collectrics-only / --wizard-only")
    if args.with_pricecharting and (args.collectrics_only or args.wizard_only):
        ap.error("--with-pricecharting needs the default Collectrics + Wizard flow (omit --collectrics-only / --wizard-only)")

    only = pre_sv_set_codes()
    n = len(only.split(",")) if only else 0
    print(f"Pre-SV sets (release < {CUTOFF}): {n} set_codes (progress prints from each child script)", flush=True)
    if not only:
        return 2

    env_py = sys.executable
    steps: list[tuple[str, list[str]]] = []
    if not args.wizard_only:
        steps.append(("sync_collectrics_data.py", ["--backup", "--sleep", "0.12"]))
    if not args.collectrics_only:
        steps.append(("sync_pokemon_wizard.py", ["--backup", "--sleep", "0.15"]))
    if args.with_pricecharting:
        steps.append(("sync_pricecharting.py", ["--backup", "--sleep", "0.18"]))

    child_env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
    for script, extra in steps:
        cmd = [env_py, str(ROOT / "scrape" / script), *extra, "--only-set-codes", only]
        print("\n>>>", " ".join(cmd[:6]), f"... --only-set-codes ({n} sets)", flush=True)
        r = subprocess.run(cmd, cwd=str(ROOT), env=child_env)
        if r.returncode != 0:
            print(f"FAILED: {script} exit {r.returncode}", flush=True)
            return r.returncode
    print("\nDone.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
