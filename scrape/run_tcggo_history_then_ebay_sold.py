#!/usr/bin/env python3
"""
For each top-list card (with ``tcggo.id``), in **set recency** order:

  1. One RapidAPI ``GET /history-prices`` (pokemon-tcg-api.p.rapidapi.com) — counts toward daily cap (~100/day free tier).
  2. eBay sold HTML scrape for the **same** card (no RapidAPI).

Writes ``pokemon_sets_data.json`` after **each** card via ``write_json_atomic``.

Examples:

  # Default: 100 − 29 = 71 RapidAPI calls; ~2.1s between calls (30/min provider cap)
  python scrape/run_tcggo_history_then_ebay_sold.py --sleep-ebay 0.55

  # Only RapidAPI (e.g. eBay HTML blocked on this machine)
  python scrape/run_tcggo_history_then_ebay_sold.py --skip-ebay

  # Resume after a prior run hit budget (skip cards that already have ``price_history_en``)
  python scrape/run_tcggo_history_then_ebay_sold.py --skip-ebay --skip-existing-history --max-rapidapi-calls 50 --rapidapi-used-today 0

  # Dashboard shows **29 calls remaining** (not 29 used): cap explicitly
  python scrape/run_tcggo_history_then_ebay_sold.py --max-rapidapi-calls 29 --sleep-ebay 0.55

Order: sets sorted by ``release_date`` descending (newest first), then ``top_25_cards`` list order.
Stops when RapidAPI budget is exhausted or a 429/limit-style HTTP error occurs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

from fetch_tcggo_top_card_price_history import (  # noqa: E402
    DEFAULT_DATA,
    DEFAULT_ENV,
    date_window_for_set,
    fetch_history,
    load_env,
    parse_set_release,
    rapidapi_tcggo_key,
    slim_en_daily,
)
from json_atomic_util import write_json_atomic  # noqa: E402
from sync_ebay_browse_listings import build_search_query, load_ebay_env  # noqa: E402
from sync_ebay_sold_listings import _is_graded, scrape_sold  # noqa: E402


def _iter_cards_by_set_recency(data: list[Any]) -> list[tuple[str, str, dict[str, Any], dict[str, Any]]]:
    """Yield (set_code, set_name, set_dict, card_dict) newest sets first."""
    candidates: list[tuple[date, str, dict[str, Any]]] = []
    for s in data:
        if not isinstance(s, dict):
            continue
        rd = parse_set_release(str(s.get("release_date") or ""))
        if rd is None:
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        if not sc:
            continue
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue
        candidates.append((rd, sc, s))

    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    out: list[tuple[str, str, dict[str, Any], dict[str, Any]]] = []
    for _rd, _sc, s in candidates:
        sc = str(s.get("set_code") or "").strip().lower()
        set_name = str(s.get("set_name") or sc)
        top = s.get("top_25_cards")
        assert isinstance(top, list)
        for card in top:
            if not isinstance(card, dict):
                continue
            tg = card.get("tcggo")
            if not isinstance(tg, dict) or tg.get("id") is None:
                continue
            try:
                int(tg["id"])
            except (TypeError, ValueError):
                continue
            out.append((sc, set_name, s, card))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="TCGGO history-prices (RapidAPI) then eBay sold per card; set recency order.")
    ap.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    ap.add_argument("--data", type=Path, default=DEFAULT_DATA)
    ap.add_argument(
        "--max-rapidapi-calls",
        type=int,
        default=None,
        help="Max RapidAPI /history-prices GETs this run. If omitted, use 100 − --rapidapi-used-today.",
    )
    ap.add_argument(
        "--rapidapi-used-today",
        type=int,
        default=29,
        help="Calls already consumed today on the free tier (default 29 → budget 71).",
    )
    ap.add_argument(
        "--rapidapi-daily-limit",
        type=int,
        default=100,
        help="Free-tier daily cap for subtraction (default 100).",
    )
    ap.add_argument(
        "--sleep-api",
        type=float,
        default=2.1,
        help="Pause after each RapidAPI call (free tier ~30/min → keep ≥2s).",
    )
    ap.add_argument("--sleep-ebay", type=float, default=0.55, help="Pause after each eBay scrape (and between retries inside scraper).")
    ap.add_argument(
        "--skip-ebay",
        action="store_true",
        help="Only fetch TCGGO history (RapidAPI). Use when eBay HTML is blocked or you only need price_history_en.",
    )
    ap.add_argument(
        "--skip-existing-history",
        action="store_true",
        help="Skip cards that already have tcggo.price_history_en (resume after a prior run stopped on budget).",
    )
    ap.add_argument("--dry-run", action="store_true", help="Do not write pokemon_sets_data.json (still calls APIs unless --skip-api).")
    ap.add_argument("--skip-api", action="store_true", help="Dry path: only list planned order, no network.")
    args = ap.parse_args()

    load_env(args.env_file)
    load_ebay_env(args.env_file.resolve())

    key = rapidapi_tcggo_key()
    if not key and not args.skip_api:
        print("Set RAPIDAPI_KEY or RAPIDAPI_KEY_TCGGO in", args.env_file, file=sys.stderr)
        return 1

    data_path = args.data.resolve()
    data = json.loads(data_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        print("Expected JSON array", file=sys.stderr)
        return 1

    plan = _iter_cards_by_set_recency(data)
    end = date.today()
    now = datetime.now(timezone.utc).isoformat()
    if args.max_rapidapi_calls is not None:
        budget = max(0, int(args.max_rapidapi_calls))
    else:
        budget = max(0, int(args.rapidapi_daily_limit) - max(0, int(args.rapidapi_used_today)))
    api_used = 0

    report: dict[str, Any] = {
        "sync_iso": now,
        "max_rapidapi_calls": budget,
        "rapidapi_calls": 0,
        "cards_ebay_ok": 0,
        "cards": [],
        "stopped_reason": None,
    }

    if args.skip_api:
        for i, (sc, sn, _s, c) in enumerate(plan[:50]):
            print(f"  [{i}] {sc} {c.get('name')!r} #{c.get('number')}")
        print(f"... planned chainable cards (tcggo.id): {len(plan)} total (showing first 50)")
        return 0

    for sc, set_name, _s, card in plan:
        if api_used >= budget:
            report["stopped_reason"] = "rapidapi_budget"
            break

        tg = card.get("tcggo")
        assert isinstance(tg, dict)
        if args.skip_existing_history:
            ph0 = tg.get("price_history_en")
            if isinstance(ph0, dict) and (ph0.get("sync_iso") or ph0.get("daily") is not None):
                continue
        try:
            iid = int(tg["id"])
        except (TypeError, ValueError):
            continue

        rd = parse_set_release(str(_s.get("release_date") or ""))
        date_from, date_to = date_window_for_set(rd, end)
        nm = str(card.get("name") or "")
        num = card.get("number")

        entry: dict[str, Any] = {
            "set_code": sc,
            "set_name": set_name,
            "card": nm,
            "number": num,
            "tcggo_id": iid,
        }

        try:
            raw = fetch_history(key, item_id=iid, date_from=date_from, date_to=date_to, timeout=75.0)
        except urllib.error.HTTPError as e:
            body = e.read()[:1200] if hasattr(e, "read") else b""
            text = body.decode("utf-8", errors="replace") if isinstance(body, (bytes, bytearray)) else str(body)
            entry["rapidapi_error"] = f"HTTP {e.code}"
            entry["rapidapi_body"] = text[:800]
            report["cards"].append(entry)
            if e.code == 429 or "limit" in text.lower() or "exceeded" in text.lower():
                report["stopped_reason"] = f"rapidapi_http_{e.code}"
                print(f"RapidAPI stop: HTTP {e.code} id={iid} {text[:200]!r}", file=sys.stderr)
                break
            print(f"HTTP {e.code} id={iid} ({set_name}): {text[:400]!r}", file=sys.stderr)
            return 1

        api_used += 1
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
        entry["days_with_tcg_player_market"] = len(daily_en)
        if args.sleep_api > 0:
            time.sleep(args.sleep_api)

        if args.skip_ebay:
            entry["ebay_ungraded"] = 0
            entry["ebay_graded"] = 0
            entry["ebay_skipped"] = True
        else:
            q = build_search_query(set_name, nm, num)
            sales = scrape_sold(q, retries=3, sleep_between=max(0.5, args.sleep_ebay))
            graded = [x for x in sales if _is_graded(x["title"])]
            ungraded = [x for x in sales if not _is_graded(x["title"])]
            graded.sort(key=lambda x: x["date"])
            ungraded.sort(key=lambda x: x["date"])
            card["ebay_sold_history_graded"] = graded
            card["ebay_sold_history_ungraded"] = ungraded
            card["ebay_sold_sync_iso"] = datetime.now(timezone.utc).isoformat()
            entry["ebay_ungraded"] = len(ungraded)
            entry["ebay_graded"] = len(graded)
            report["cards_ebay_ok"] += 1
        report["cards"].append(entry)
        report["rapidapi_calls"] = api_used

        if not args.skip_ebay and args.sleep_ebay > 0:
            time.sleep(max(0.0, args.sleep_ebay))

        if not args.dry_run:
            write_json_atomic(data_path, data)

        if args.skip_ebay:
            ebay_note = "eBay skipped"
        else:
            ebay_note = f"eBay ug={len(ungraded)} gr={len(graded)}"
        print(
            f"[{api_used}/{budget}] {sc} {nm[:36]!r} #{num}  TCGGO days={len(daily_en)}  {ebay_note}",
            flush=True,
        )

    rep_path = data_path.with_suffix(".json.tcggo_ebay_sold_orchestrator_report.json")
    report["rapidapi_calls"] = api_used
    rep_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Report -> {rep_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
