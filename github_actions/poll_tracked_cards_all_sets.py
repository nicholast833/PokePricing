#!/usr/bin/env python3
"""
Orchestrate marketplace polling for every set's top_25_cards in pokemon_sets_data.json.

Runs (in order):
  1. sync_tcgplayer_mpapi.py --all-sets  (public mp-search-api)
  2. sync_ebay_browse_listings.py --all-sets  (OAuth; needs EBAY_APP_ID + EBAY_CERT_ID)
  3. sync_ebay_sold_listings.py --all-sets  (optional; slow HTML scrape via curl_cffi)

Designed for GitHub Actions; can also be run locally with the same env vars as the eBay scripts.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GA = Path(__file__).resolve().parent


def _run(cmd: list[str]) -> int:
    print("==>", " ".join(cmd), flush=True)
    return subprocess.call(cmd, cwd=str(ROOT))


def main() -> int:
    ap = argparse.ArgumentParser(description="Poll top_25_cards across all sets in pokemon_sets_data.json")
    ap.add_argument("--no-tcgplayer", action="store_true", help="Skip TCGPlayer mp-search-api refresh")
    ap.add_argument("--no-ebay-browse", action="store_true", help="Skip eBay Buy Browse active listings")
    ap.add_argument(
        "--ebay-sold",
        action="store_true",
        help="Include eBay sold HTML scrape (very slow at scale)",
    )
    ap.add_argument("--tcgplayer-sleep", type=float, default=0.35, help="Delay between TCGPlayer API calls")
    ap.add_argument("--ebay-browse-sleep", type=float, default=-1.0, help="eBay browse delay (-1: env default)")
    ap.add_argument("--ebay-sold-sleep", type=float, default=2.0, help="Delay between eBay sold page fetches")
    args = ap.parse_args()

    data = ROOT / "pokemon_sets_data.json"
    if not data.is_file():
        print(f"Missing {data}", file=sys.stderr)
        return 1

    if args.no_tcgplayer and args.no_ebay_browse and not args.ebay_sold:
        print("Nothing to run: all steps disabled.", file=sys.stderr)
        return 1

    py = sys.executable

    if not args.no_tcgplayer:
        code = _run(
            [
                py,
                str(GA / "sync_tcgplayer_mpapi.py"),
                "--all-sets",
                "--backup",
                "--sleep",
                str(max(0.0, args.tcgplayer_sleep)),
            ]
        )
        if code != 0:
            return code

    if not args.no_ebay_browse:
        browse_cmd = [
            py,
            str(GA / "sync_ebay_browse_listings.py"),
            "--all-sets",
            "--backup",
        ]
        if args.ebay_browse_sleep >= 0:
            browse_cmd.extend(["--sleep", str(args.ebay_browse_sleep)])
        code = _run(browse_cmd)
        if code != 0:
            return code

    if args.ebay_sold:
        code = _run(
            [
                py,
                str(GA / "sync_ebay_sold_listings.py"),
                "--all-sets",
                "--backup",
                "--sleep",
                str(max(0.0, args.ebay_sold_sleep)),
            ]
        )
        if code != 0:
            return code

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
