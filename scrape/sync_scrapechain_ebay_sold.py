#!/usr/bin/env python3
"""
Merge ScrapeChain eBay completed (sold) listings into pokemon_sets_data.json.

POST https://ebay-api.scrapechain.com/findCompletedItems (see
https://github.com/colindaniels/eBay-sold-items-documentation )

Per matched top_25_cards row writes:
  ebay_sold_observations          — list[{date, price, title}]  (date = YYYY-MM-DD)
  ebay_sold_scrapechain_sync_iso  — UTC ISO timestamp

Existing ebay_sold_history_* from Finding API or HTML scrape are left intact;
the Explorer merges observations in shared.js (ebaySoldRowsDeduped).

Example:
  python scrape/sync_scrapechain_ebay_sold.py --only-set-codes me3 --sleep 2.5
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

from json_atomic_util import write_json_atomic  # noqa: E402

from sync_ebay_browse_listings import build_search_query  # noqa: E402

URL = "https://ebay-api.scrapechain.com/findCompletedItems"
DEFAULT_EXCLUDED = "lot bulk proxy custom"


def parse_date_sold(raw: str) -> Optional[str]:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%b %d, %Y").strftime("%Y-%m-%d")
    except ValueError:
        pass
    try:
        dt = datetime.strptime(s, "%b %d")
        dt = dt.replace(year=datetime.now(timezone.utc).year)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def post_find_completed(payload: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        text = resp.read().decode("utf-8", errors="replace")
    return json.loads(text)


def products_to_observations(products: List[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for p in products:
        if not isinstance(p, dict):
            continue
        ds = parse_date_sold(str(p.get("date_sold") or ""))
        if not ds:
            continue
        try:
            price = float(p.get("sale_price"))
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        title = str(p.get("title") or "").strip()
        if not title:
            continue
        out.append({"date": ds, "price": price, "title": title})
    out.sort(key=lambda r: (r["date"], r["price"]))
    return out


def fetch_observations_for_card(
    *,
    set_name: str,
    card_name: str,
    card_num: Any,
    browse_query: str,
    category_id: Optional[str],
    excluded: str,
) -> List[Dict[str, Any]]:
    qb = (browse_query or "").strip()
    q = qb if len(qb) >= 10 else build_search_query(set_name, str(card_name or ""), card_num)
    q = q[:350]

    def run(with_cat: bool) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {
            "keywords": q,
            "excluded_keywords": excluded,
            "max_search_results": 240,
            "remove_outliers": False,
            "site_id": "0",
        }
        if with_cat and category_id:
            payload["category_id"] = category_id
        try:
            data = post_find_completed(payload)
        except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError, TimeoutError):
            return []
        if not data.get("success"):
            return []
        prods = data.get("products") or []
        return products_to_observations(prods if isinstance(prods, list) else [])

    if category_id:
        obs = run(True)
        if not obs:
            obs = run(False)
    else:
        obs = run(False)
    return obs


def main() -> int:
    ap = argparse.ArgumentParser(description="Merge ScrapeChain sold listings into pokemon_sets_data.json")
    ap.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--output", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--only-set-codes", default="", help="Comma-separated set_code values (e.g. me3)")
    ap.add_argument("--sleep", type=float, default=2.0, help="Seconds between API calls")
    ap.add_argument("--excluded-keywords", default=DEFAULT_EXCLUDED)
    ap.add_argument(
        "--category-id",
        default="183454",
        help="eBay category id (default 183454 CCG singles); pass empty to skip category on first try",
    )
    ap.add_argument("--backup", action="store_true")
    args = ap.parse_args()

    only = {x.strip().lower() for x in args.only_set_codes.split(",") if x.strip()}
    if not only:
        raise SystemExit("Pass --only-set-codes me3 (or comma-separated list).")

    inp = args.input.resolve()
    out = args.output.resolve()
    if args.backup and inp == out and inp.is_file():
        bak = inp.with_suffix(inp.suffix + ".scrapechain_bak")
        bak.write_bytes(inp.read_bytes())
        print("Wrote backup ->", bak, flush=True)

    cat = (args.category_id or "").strip() or None

    data = json.loads(inp.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("Expected pokemon_sets_data.json as a JSON array")

    rep = {
        "sync_iso": datetime.now(timezone.utc).isoformat(),
        "sets": 0,
        "cards": 0,
        "cards_with_observations": 0,
        "observation_rows": 0,
    }

    sleep_s = max(0.5, float(args.sleep))
    excluded = str(args.excluded_keywords or DEFAULT_EXCLUDED).strip() or DEFAULT_EXCLUDED

    for s in data:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        if sc not in only:
            continue
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue
        set_name = str(s.get("set_name") or sc)
        rep["sets"] += 1
        print(f"[ScrapeChain sold] set={sc!r} cards={len(top)}", flush=True)

        for idx, card in enumerate(top, start=1):
            if not isinstance(card, dict):
                continue
            nm = str(card.get("name") or "")
            num = card.get("number")
            qb = str(card.get("ebay_browse_query") or "").strip()
            print(f"  [{idx}] {nm[:42]!r} #{num} …", flush=True)
            obs = fetch_observations_for_card(
                set_name=set_name,
                card_name=nm,
                card_num=num,
                browse_query=qb,
                category_id=cat,
                excluded=excluded,
            )
            rep["cards"] += 1
            now_iso = datetime.now(timezone.utc).isoformat()
            card["ebay_sold_observations"] = obs
            card["ebay_sold_scrapechain_sync_iso"] = now_iso
            if obs:
                rep["cards_with_observations"] += 1
                rep["observation_rows"] += len(obs)
                print(f"    -> {len(obs)} observation rows", flush=True)
            else:
                print("    -> 0 rows", flush=True)
            time.sleep(sleep_s)

        write_json_atomic(out, data)
        print(f"  checkpoint saved -> {out}", flush=True)

    rep_path = out.parent / (out.name + ".scrapechain_ebay_sold_report.json")
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(json.dumps(rep, indent=2), flush=True)
    print("Wrote report ->", rep_path, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
