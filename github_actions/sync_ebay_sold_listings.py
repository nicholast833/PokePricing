#!/usr/bin/env python3
"""
Sync eBay sold listing history into pokemon_sets_data.json top_25_cards.

Extends sync_ebay_browse_listings.py infrastructure: same env file, same
build_search_query, same JSON data format, same --only-set-codes / --all-sets
flags, same --backup flag.

Since api.ebay.com is DNS-blocked in some environments, this script uses the
public eBay Sold search page (www.ebay.com/sch/i.html?LH_Sold=1) via curl_cffi
to bypass TLS fingerprint bot-protection.  It does NOT scrape seller identities,
feedback, item IDs, or buyer information — only sold price, sold date, listing
title (for grading classification), and condition keywords.

For **official API** sold data (last N days via Finding ``findCompletedItems``),
see ``sync_ebay_sales_finding_api.py`` (same ``EBAY_APP_ID``; no HTML).

Stored fields per card:
  ebay_sold_history_ungraded  – list[{date, price, title}] sorted oldest->newest
  ebay_sold_history_graded    – list[{date, price, title}] sorted oldest->newest
  ebay_sold_sync_iso          – ISO timestamp of last successful sync

Example (Perfect Order only):
  python scrape/sync_ebay_sold_listings.py --only-set-codes me3 --backup

Env file: scrape/ebay_listing_checker.env
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(ROOT / "scrape"))

from dataset_report_paths import dataset_sidecar_report_path  # noqa: E402
from json_atomic_util import write_json_atomic  # noqa: E402

try:
    from curl_cffi import requests as cffi_requests  # type: ignore
    from bs4 import BeautifulSoup  # type: ignore
except ImportError:
    sys.exit(
        "Missing dependencies. Install with:\n"
        "  pip install curl_cffi beautifulsoup4\n"
    )

# Re-use env loading from sibling script
from sync_ebay_browse_listings import (  # noqa: E402
    load_ebay_env,
    build_search_query,
    DEFAULT_ENV,
)

GRADED_KEYWORDS = frozenset(["psa", "cgc", "bgs", "beckett", "ace", "pca", "graded", "gem mint"])


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _is_graded(title: str) -> bool:
    lower = title.lower()
    return any(kw in lower for kw in GRADED_KEYWORDS)


def _parse_price(text: str) -> Optional[float]:
    """Extract USD price from eBay price cell text."""
    if " to " in text:
        text = text.split(" to ")[0]
    m = re.search(r"[\d,]+\.\d{2}", text)
    if m:
        try:
            return float(m.group(0).replace(",", ""))
        except ValueError:
            pass
    return None


def _parse_sold_date(text: str) -> Optional[str]:
    """Return YYYY-MM-DD or None."""
    text = text.replace("Sold", "").strip()
    for fmt in ("%b %d, %Y", "%b %d"):
        try:
            dt = datetime.strptime(text, fmt)
            if fmt == "%b %d":
                dt = dt.replace(year=datetime.now().year)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# eBay scraper
# ---------------------------------------------------------------------------

_SESSION: Optional[Any] = None


def _get_session() -> Any:
    global _SESSION
    if _SESSION is None:
        _SESSION = cffi_requests.Session(impersonate="chrome110")
    return _SESSION


def scrape_sold(query: str, retries: int = 3, sleep_between: float = 2.0) -> List[Dict[str, Any]]:
    """
    Fetch sold/completed eBay listings for *query* via public HTML search.
    Returns list of {date, price, title} dicts (unsorted).
    """
    url = (
        "https://www.ebay.com/sch/i.html?"
        + "LH_Sold=1&LH_Complete=1&_ipg=240&_nkw="
        + quote_plus(query)
    )
    session = _get_session()
    last_err: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=20)
        except Exception as exc:
            last_err = exc
            print(f"    [retry {attempt}/{retries}] fetch error: {exc}", flush=True)
            time.sleep(sleep_between)
            continue

        if r.status_code != 200:
            last_err = Exception(f"HTTP {r.status_code}")
            print(f"    [retry {attempt}/{retries}] HTTP {r.status_code}", flush=True)
            time.sleep(sleep_between)
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        # eBay renders either classic (.s-item li) or new card-style (li[data-view])
        items = soup.find_all("li", class_="s-item")
        if not items or len(items) <= 1:
            items = soup.select("li[data-view]")

        if not items:
            last_err = Exception("0 items parsed (possible bot block)")
            print(f"    [retry {attempt}/{retries}] 0 items – possible bot block", flush=True)
            time.sleep(sleep_between * 2)
            continue

        # success – parse results
        sales: List[Dict[str, Any]] = []
        for item in items:
            title_el = item.select_one(".s-item__title") or item.select_one(".s-card__title")
            price_el = item.select_one(".s-item__price") or item.select_one(".s-card__price")

            date_el = item.select_one(".s-item__title--tag")
            date_str = date_el.text if date_el else None
            if not date_str:
                node = item.find(string=re.compile(r"Sold\s+.*20\d\d"))
                if node:
                    date_str = str(node)

            if not title_el or not price_el or not date_str:
                continue

            title = (
                title_el.text
                .replace("New Listing", "")
                .replace("Opens in a new window or tab", "")
                .strip()
            )
            if "Shop on eBay" in title:
                continue

            price = _parse_price(price_el.text)
            date = _parse_sold_date(date_str)
            if price is None or date is None:
                continue

            sales.append({"date": date, "price": price, "title": title})

        return sales

    # all retries exhausted
    print(f"    WARN: scrape_sold failed after {retries} retries: {last_err}", flush=True)
    return []


# ---------------------------------------------------------------------------
# Main sync loop
# ---------------------------------------------------------------------------

def run(
    *,
    input_path: Path,
    output_path: Path,
    only_set_codes: Optional[set],
    sleep_s: float,
    force_empty: bool = False,
) -> Dict[str, Any]:
    data = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("Expected pokemon_sets_data.json as a JSON array")

    rep: Dict[str, Any] = {
        "sync_iso": datetime.now(timezone.utc).isoformat(),
        "sets_processed": 0,
        "cards_processed": 0,
        "cards_with_data": 0,
        "total_ungraded_sales": 0,
        "total_graded_sales": 0,
        "cards_preserved_empty_scrape": 0,
    }

    for s in data:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        if only_set_codes is not None and sc not in only_set_codes:
            continue
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue

        set_name = str(s.get("set_name") or sc)
        rep["sets_processed"] += 1
        rows = [x for x in top if isinstance(x, dict)]
        nrows = len(rows)
        print(f"[eBay Sold] set={sc!r} name={set_name[:48]!r} cards={nrows}", flush=True)

        for idx, card in enumerate(rows, start=1):
            nm = str(card.get("name") or "")
            num = card.get("number")
            q = build_search_query(set_name, nm, num)
            print(f"  [{idx}/{nrows}] {nm[:40]!r} #{num} …", flush=True)

            sales = scrape_sold(q, retries=3, sleep_between=sleep_s)
            rep["cards_processed"] += 1

            graded = [s for s in sales if _is_graded(s["title"])]
            ungraded = [s for s in sales if not _is_graded(s["title"])]

            # Sort oldest->newest
            graded.sort(key=lambda x: x["date"])
            ungraded.sort(key=lambda x: x["date"])

            card["ebay_sold_history_graded"] = graded
            card["ebay_sold_history_ungraded"] = ungraded
            card["ebay_sold_sync_iso"] = datetime.now(timezone.utc).isoformat()

            rep["total_graded_sales"] += len(graded)
            rep["total_ungraded_sales"] += len(ungraded)
            if graded or ungraded:
                rep["cards_with_data"] += 1

            print(
                f"    -> {len(ungraded)} ungraded, {len(graded)} graded sales",
                flush=True,
            )
            time.sleep(max(0.0, sleep_s))

        write_json_atomic(output_path, data)
        print(f"  checkpoint saved -> {output_path}", flush=True)

    return rep


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Sync eBay sold listing history into pokemon_sets_data.json"
    )
    ap.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--output", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    ap.add_argument("--only-set-codes", default="", help="Comma-separated set_code values (e.g. me3)")
    ap.add_argument("--sleep", type=float, default=2.0, help="Seconds between searches (default 2.0)")
    ap.add_argument("--backup", action="store_true")
    ap.add_argument("--all-sets", action="store_true", help="Sync every set's top_25_cards")
    args = ap.parse_args()

    load_ebay_env(args.env_file.resolve())

    only: Optional[set] = None
    if args.only_set_codes.strip():
        only = {x.strip().lower() for x in args.only_set_codes.split(",") if x.strip()}
    if not only and not args.all_sets:
        raise SystemExit(
            "Refusing a full-catalog run: pass --only-set-codes me3 (or comma list), "
            "or --all-sets to sync every set."
        )

    inp = args.input.resolve()
    out = args.output.resolve()

    if args.backup and inp == out and inp.is_file():
        bak = inp.with_suffix(inp.suffix + ".sold_bak")
        shutil.copy2(inp, bak)
        print("Wrote backup ->", bak, flush=True)

    rep = run(
        input_path=inp,
        output_path=out,
        only_set_codes=only,
        sleep_s=max(0.5, args.sleep),
        force_empty=bool(args.force_empty),
    )

    rep_path = dataset_sidecar_report_path(out, ".ebay_sold_sync_report.json")
    rep_path.parent.mkdir(parents=True, exist_ok=True)
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(json.dumps(rep, indent=2), flush=True)
    print("Wrote report ->", rep_path, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
