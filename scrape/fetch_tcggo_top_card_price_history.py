#!/usr/bin/env python3
"""
Fetch TCGGO ``GET /history-prices`` for the top 5 cards (by list order) in each of the 5
most recent sets that already have ``tcggo.id`` on cards — **25 API calls** total.

Starts at **Perfect Order** (``me3``) and walks **older** by ``release_date``.

``date_from`` / ``date_to``: last ~365 calendar days ending today, but **not before** the
set's ``release_date`` (new sets only get partial history).

Stores **English-relevant** data only: per-day ``tcggo_player_market`` (USD-style TCGPlayer
field from API) under ``card["tcggo"]["price_history_en"]``. Raw Cardmarket keys are not kept.

Writes ``pokemon_sets_data.json.tcggo_price_history_sample.json`` with the **full raw**
API JSON for **one** card (first call) for inspection.
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
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

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


def rapidapi_tcggo_key() -> str:
    """RapidAPI credential for pokemon-tcg-api. ``RAPIDAPI_KEY_TCGGO`` wins when non-empty; else ``RAPIDAPI_KEY``."""
    inv = (os.environ.get("RAPIDAPI_KEY_TCGGO") or "").strip()
    if inv:
        return inv
    return (os.environ.get("RAPIDAPI_KEY") or "").strip()


def tcggo_gateway_headers_query(key: str, host: str, query: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    """``tcggo_*`` keys: ``rapidapi-key`` query only (TCGGO). Otherwise ``X-RapidAPI-Key`` (RapidAPI app key)."""
    params = dict(query)
    headers: dict[str, str] = {"X-RapidAPI-Host": host, "Accept": "application/json"}
    if key.startswith("tcggo_"):
        params["rapidapi-key"] = key
    else:
        headers["X-RapidAPI-Key"] = key
    return headers, params


def parse_set_release(s: str) -> date | None:
    s = (s or "").strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def date_window_for_set(set_release: date | None, end: date) -> tuple[date, date]:
    """Up to one year ending ``end``, never starting before set release."""
    start = end - timedelta(days=365)
    if set_release is not None and start < set_release:
        start = set_release
    if start > end:
        start = end
    return start, end


def fetch_history(
    key: str,
    *,
    item_id: int,
    date_from: date,
    date_to: date,
    timeout: float,
) -> dict[str, Any]:
    """TCGGO ``tcggo_inv_*`` keys authenticate via ``rapidapi-key`` query (see pokemon-api.com docs)."""
    headers, params = tcggo_gateway_headers_query(
        key,
        HOST,
        {
            "id": str(int(item_id)),
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "page": "1",
            "sort": "desc",
        },
    )
    q = urllib.parse.urlencode(params)
    url = f"https://{HOST}{HISTORY_PATH}?{q}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def slim_en_daily(raw: dict[str, Any]) -> dict[str, Any]:
    data = raw.get("data")
    if not isinstance(data, dict):
        return {}
    out: dict[str, Any] = {}
    for day_key, row in data.items():
        if not isinstance(row, dict):
            continue
        m = row.get("tcg_player_market")
        if m is None:
            continue
        out[str(day_key)] = {"tcg_player_market": m}
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    ap.add_argument("--data", type=Path, default=DEFAULT_DATA)
    ap.add_argument("--sets", type=int, default=5)
    ap.add_argument("--cards-per-set", type=int, default=5)
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
        raise SystemExit("pokemon_sets_data.json must be a JSON array")

    end = date.today()
    candidates: list[tuple[date, str, dict[str, Any]]] = []
    for s in data:
        if not isinstance(s, dict):
            continue
        rd = parse_set_release(str(s.get("release_date") or ""))
        if rd is None:
            continue
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue
        ok = 0
        for c in top:
            if not isinstance(c, dict):
                continue
            tg = c.get("tcggo")
            if isinstance(tg, dict) and tg.get("id") is not None:
                try:
                    int(tg["id"])
                    ok += 1
                except (TypeError, ValueError):
                    continue
        if ok < 1:
            continue
        sc = str(s.get("set_code") or "")
        candidates.append((rd, sc, s))

    # Newest first; tie-breaker: set_code so order is stable (me3 before me2pt5 etc. by date anyway)
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)

    try:
        anchor_idx = next(i for i, (_, sc, _) in enumerate(candidates) if sc == "me3")
    except StopIteration:
        anchor_idx = 0
    reordered = candidates[anchor_idx : anchor_idx + args.sets]
    if len(reordered) < args.sets:
        reordered = candidates[: args.sets]

    now = datetime.now(timezone.utc).isoformat()
    report: dict[str, Any] = {
        "sync_iso": now,
        "end_date": end.isoformat(),
        "calls": [],
        "sample_raw_response": None,
    }

    calls = 0
    max_calls = args.sets * args.cards_per_set

    stop_all = False
    for rd, set_code, s in reordered:
        if stop_all:
            break
        set_name = str(s.get("set_name") or "")
        date_from, date_to = date_window_for_set(rd, end)
        top = s.get("top_25_cards")
        assert isinstance(top, list)
        picked = 0
        for card in top:
            if calls >= max_calls or picked >= args.cards_per_set:
                stop_all = calls >= max_calls
                break
            if not isinstance(card, dict):
                continue
            tg = card.get("tcggo")
            if not isinstance(tg, dict) or tg.get("id") is None:
                continue
            try:
                iid = int(tg["id"])
            except (TypeError, ValueError):
                continue

            try:
                raw = fetch_history(key, item_id=iid, date_from=date_from, date_to=date_to, timeout=75.0)
            except urllib.error.HTTPError as e:
                print(f"HTTP {e.code} id={iid} ({set_name}): {e.read()[:800]!r}", file=sys.stderr)
                return 1

            calls += 1
            daily_en = slim_en_daily(raw)
            tg["price_history_en"] = {
                "currency": "USD",
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "sync_iso": now,
                "daily": daily_en,
                "paging": raw.get("paging"),
                "results": raw.get("results"),
            }

            entry = {
                "set_code": set_code,
                "set_name": set_name,
                "card": str(card.get("name")),
                "number": str(card.get("number")),
                "tcggo_id": iid,
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "days_with_tcg_player_market": len(daily_en),
                "api_results": raw.get("results"),
            }
            report["calls"].append(entry)

            if report["sample_raw_response"] is None:
                report["sample_raw_response"] = {
                    "set_code": set_code,
                    "set_name": set_name,
                    "card_name": card.get("name"),
                    "card_number": card.get("number"),
                    "tcggo_id": iid,
                    "date_from": date_from.isoformat(),
                    "date_to": date_to.isoformat(),
                    "api": raw,
                }

            picked += 1
            if args.sleep > 0:
                time.sleep(args.sleep)

    report["total_calls"] = calls
    sample_path = args.data.with_suffix(".json.tcggo_price_history_sample.json")
    if report["sample_raw_response"] is not None:
        sample_path.write_text(json.dumps(report["sample_raw_response"], indent=2, ensure_ascii=False), encoding="utf-8")

    rep_path = args.data.with_suffix(".json.tcggo_price_history_run_report.json")
    # omit huge duplicate from run report
    slim_report = {k: v for k, v in report.items() if k != "sample_raw_response"}
    slim_report["sample_file"] = str(sample_path)
    rep_path.write_text(json.dumps(slim_report, indent=2, ensure_ascii=False), encoding="utf-8")

    if args.dry_run:
        print(json.dumps(slim_report, indent=2))
        return 0

    args.data.write_text(json.dumps(data, indent=4, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Calls: {calls}; sample raw -> {sample_path}; summary -> {rep_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
