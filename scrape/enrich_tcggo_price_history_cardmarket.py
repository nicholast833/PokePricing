#!/usr/bin/env python3
"""
Re-fetch ``/history-prices`` for cards that already have ``tcggo.price_history_en`` and merge
``cm_low`` (EUR) into each day's object as ``cardmarket_low_eur`` alongside ``tcg_player_market``.

One GET per card with existing price_history_en (default: 25 cards from prior scan).

Reads ``RAPIDAPI_KEY_TCGGO`` (preferred) or ``RAPIDAPI_KEY`` from ``ebay_listing_checker.env``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fetch_tcggo_top_card_price_history import rapidapi_tcggo_key, tcggo_gateway_headers_query

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
DEFAULT_ENV = SCRIPT_DIR / "ebay_listing_checker.env"
DEFAULT_DATA = ROOT / "pokemon_sets_data.json"

HOST = "pokemon-tcg-api.p.rapidapi.com"
HISTORY_PATH = "/history-prices"


def load_env(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, _, v = s.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def fetch_history(key: str, item_id: int, date_from: str, date_to: str) -> dict[str, Any]:
    hdrs, params = tcggo_gateway_headers_query(
        key,
        HOST,
        {"id": str(int(item_id)), "date_from": date_from, "date_to": date_to, "page": "1", "sort": "desc"},
    )
    q = urllib.parse.urlencode(params)
    url = f"https://{HOST}{HISTORY_PATH}?{q}"
    req = urllib.request.Request(url, headers=hdrs, method="GET")
    with urllib.request.urlopen(req, timeout=75.0) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    ap.add_argument("--data", type=Path, default=DEFAULT_DATA)
    ap.add_argument("--sleep", type=float, default=0.06)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    load_env(args.env_file)
    key = rapidapi_tcggo_key()
    if not key:
        print("Set RAPIDAPI_KEY or RAPIDAPI_KEY_TCGGO in", args.env_file, file=sys.stderr)
        return 1

    data = json.loads(args.data.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("Expected array")

    calls = 0
    for s in data:
        if not isinstance(s, dict):
            continue
        for c in s.get("top_25_cards") or []:
            if not isinstance(c, dict):
                continue
            tg = c.get("tcggo")
            if not isinstance(tg, dict):
                continue
            ph = tg.get("price_history_en")
            if not isinstance(ph, dict):
                continue
            cid = tg.get("id")
            if cid is None:
                continue
            try:
                iid = int(cid)
            except (TypeError, ValueError):
                continue
            df = str(ph.get("date_from") or "").strip()
            dt = str(ph.get("date_to") or "").strip()
            if not df or not dt:
                continue

            try:
                raw = fetch_history(key, iid, df, dt)
            except urllib.error.HTTPError as e:
                print(f"HTTP {e.code} id={iid}: {e.read()[:600]!r}", file=sys.stderr)
                return 1

            calls += 1
            api_data = raw.get("data")
            daily = ph.get("daily")
            if not isinstance(daily, dict):
                daily = {}
            if isinstance(api_data, dict):
                for day, row in api_data.items():
                    if not isinstance(row, dict):
                        continue
                    dkey = str(day)
                    slot = daily.get(dkey)
                    if not isinstance(slot, dict):
                        slot = {}
                    if row.get("tcg_player_market") is not None:
                        slot["tcg_player_market"] = row["tcg_player_market"]
                    if row.get("cm_low") is not None:
                        slot["cardmarket_low_eur"] = row["cm_low"]
                    daily[dkey] = slot
            ph["daily"] = daily
            ph["cardmarket_enriched_iso"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

            if args.sleep > 0:
                time.sleep(args.sleep)

    rep = {"history_enrich_calls": calls}
    rep_path = args.data.with_suffix(".json.tcggo_cm_enrich_report.json")
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")

    if args.dry_run:
        print(json.dumps(rep, indent=2))
        return 0

    args.data.write_text(json.dumps(data, indent=4, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Enriched {calls} cards with Cardmarket EUR per day. Report: {rep_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
