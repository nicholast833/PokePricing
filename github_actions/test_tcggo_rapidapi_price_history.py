#!/usr/bin/env python3
"""
One-shot smoke test: TCGGO Pokemon API (RapidAPI) price history.

Loads ``RAPIDAPI_KEY_TCGGO`` or ``RAPIDAPI_KEY`` from ``ebay_listing_checker.env``.

Default example: Victini 171/086 — SV: Black Bolt — TCGPlayer product 642546.
The public docs list ``history-prices`` by internal ``id``, ``cardmarket_id``, or ``tcgid``.
This card’s Cardmarket id (from TCGTracking cache) is 836245; use ``--tcgplayer-id-query`` to
try ``tcgplayer_id=642546`` instead (support may vary).

Docs: https://www.pokemon-api.com/docs/  |  RapidAPI: https://rapidapi.com/tcggopro/api/pokemon-tcg-api
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

from fetch_tcggo_top_card_price_history import rapidapi_tcggo_key, tcggo_gateway_headers_query

DEFAULT_ENV = Path(__file__).resolve().parent / "ebay_listing_checker.env"
RAPIDAPI_HOST = "pokemon-tcg-api.p.rapidapi.com"
# Live gateway uses ``/history-prices``. Marketing examples often show ``/pokemon/history-prices`` (404).
HISTORY_PATH = "/history-prices"


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    raw = path.read_text(encoding="utf-8", errors="replace")
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            continue
        key, _, val = s.partition("=")
        key, val = key.strip(), val.strip().strip('"').strip("'")
        if not key or key in os.environ:
            continue
        os.environ[key] = val


def main() -> int:
    ap = argparse.ArgumentParser(description="TCGGO RapidAPI history-prices (single GET).")
    ap.add_argument("--env-file", type=Path, default=DEFAULT_ENV, help="Path to ebay_listing_checker.env")
    ap.add_argument("--tcgplayer-id", type=int, default=642546, help="TCGPlayer product id (metadata only unless --tcgplayer-only)")
    ap.add_argument("--cardmarket-id", type=int, default=836245, help="Cardmarket id query param (same card as default TCGPlayer product)")
    ap.add_argument("--tcgplayer-id-query", action="store_true", help="Use tcgplayer_id=... instead of cardmarket_id (if API supports it)")
    ap.add_argument("--days", type=int, default=31, help="Approximate calendar window ending today (UTC date)")
    args = ap.parse_args()

    load_env_file(args.env_file)
    key = rapidapi_tcggo_key()
    if not key:
        print("Missing RAPIDAPI_KEY / RAPIDAPI_KEY_TCGGO in environment or", args.env_file, file=sys.stderr)
        return 1

    end = date.today()
    start = end - timedelta(days=max(1, args.days))
    q: dict[str, str] = {
        "date_from": start.isoformat(),
        "date_to": end.isoformat(),
        "page": "1",
        "sort": "desc",
    }
    if args.tcgplayer_id_query:
        q["tcgplayer_id"] = str(args.tcgplayer_id)
    else:
        q["cardmarket_id"] = str(args.cardmarket_id)

    hdrs, q2 = tcggo_gateway_headers_query(key, RAPIDAPI_HOST, q)
    url = f"https://{RAPIDAPI_HOST}{HISTORY_PATH}?{urllib.parse.urlencode(q2)}"
    req = urllib.request.Request(url, headers=hdrs, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            status = resp.getcode()
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        print(f"HTTP {e.code}: {err_body[:4000]}", file=sys.stderr)
        return 1

    print(f"GET {url}")
    print(f"HTTP {status}")
    try:
        data = json.loads(body)
        print(json.dumps(data, indent=2)[:24000])
    except json.JSONDecodeError:
        print(body[:8000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
