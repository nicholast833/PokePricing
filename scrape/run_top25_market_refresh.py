#!/usr/bin/env python3
"""
Run market/scrape syncs for every set's ``top_25_cards`` (and related set rows), in a safe order:

  1. Timestamped JSON backup
  2. TCGTracking merge (cache + ids; helps TCGPlayer / Wizard resolution)
  3. Pokémon Wizard (all sets; long)
  4. PriceCharting
  5. Collectrics
  6. TCGPlayer mp-search-api snapshots + history append
  7–9. eBay Browse / Finding sold / sold listings (best-effort if env creds missing)

  python scrape/run_top25_market_refresh.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run_required(argv: list[str]) -> None:
    print("\n>>>", " ".join(argv), flush=True)
    r = subprocess.run(
        [sys.executable, *argv],
        cwd=str(ROOT),
        env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
    )
    if r.returncode != 0:
        raise SystemExit(r.returncode)


def run_optional(argv: list[str]) -> None:
    print("\n>>> (optional)", " ".join(argv), flush=True)
    subprocess.run(
        [sys.executable, *argv],
        cwd=str(ROOT),
        env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
    )


def main() -> int:
    run_required([str(ROOT / "scripts" / "backup_pokemon_sets_data.py")])
    run_required(
        [
            str(ROOT / "scrape" / "tcgtracking_merge.py"),
            "--backup",
            "--sleep",
            "0.2",
            "--input",
            "pokemon_sets_data.json",
            "--output",
            "pokemon_sets_data.json",
            "--cache",
            "tcg_cache",
        ]
    )
    run_required([str(ROOT / "scrape" / "sync_pokemon_wizard.py"), "--sleep", "0.12"])
    run_required(
        [
            str(ROOT / "scrape" / "sync_pricecharting.py"),
            "--backup",
            "--sleep",
            "0.18",
        ]
    )
    run_required(
        [
            str(ROOT / "scrape" / "sync_collectrics_data.py"),
            "--backup",
            "--sleep",
            "0.12",
        ]
    )
    run_required(
        [
            str(ROOT / "scrape" / "sync_tcgplayer_mpapi.py"),
            "--all-sets",
            "--backup",
            "--sleep",
            "0.2",
        ]
    )
    for extra in (
        [str(ROOT / "scrape" / "sync_ebay_browse_listings.py"), "--all-sets", "--backup", "--sleep", "0.55"],
        [
            str(ROOT / "scrape" / "sync_ebay_sales_finding_api.py"),
            "--all-sets",
            "--backup",
            "--sleep",
            "0.55",
            "--days",
            "90",
        ],
        [str(ROOT / "scrape" / "sync_ebay_sold_listings.py"), "--all-sets", "--backup", "--sleep", "0.5"],
    ):
        run_optional(extra)
    print("\nDone run_top25_market_refresh.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
