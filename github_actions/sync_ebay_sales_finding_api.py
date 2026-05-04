#!/usr/bin/env python3
"""
Fetch recent *sold* (completed) eBay listings for every top_25_cards row via the
**eBay Finding API** `findCompletedItems` call.

Why not Buy Browse?
  The Buy Browse `item_summary/search` endpoint only covers **active** listings.
  Completed/sold research for third-party apps is exposed historically through the
  **Finding API** (application id only; no user OAuth).

This script:
  - Uses `EBAY_APP_ID` as `SECURITY-APPNAME` (same App Id as Browse OAuth client_id).
  - Restricts listing **end times** to the last ``--days`` calendar days (default 90 ≈ 3 months).
  - Optional ``--category-id`` (default **183454**, CCG Individual Cards) plus keyword variants
    (with/without ``#``) to improve hit rate — still **Finding API only**.
  - Uses ``ebay_browse_query`` on the card when present (same string as Buy Browse), else
    ``build_search_query`` from ``sync_ebay_browse_listings``.
  - Paginates up to ``--max-pages`` per card (default 5 × 100 rows).
  - Writes the same card fields as ``sync_ebay_sold_listings.py`` (HTML scrape):
      ebay_sold_history_ungraded, ebay_sold_history_graded, ebay_sold_sync_iso
    plus ``ebay_sold_source`` = ``finding_api``.

eBay may rate-limit or retire Finding; if calls fail, use ``sync_ebay_sold_listings.py``
(HTML sold search) as a fallback.

Example:
  python scrape/sync_ebay_sales_finding_api.py --all-sets --backup --days 90

  python scrape/sync_ebay_sales_finding_api.py --only-set-codes sv4,me2 --days 90

Env: scrape/ebay_listing_checker.env — requires EBAY_APP_ID (see ebay_listing_checker.env.example).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

from json_atomic_util import write_json_atomic  # noqa: E402

from sync_ebay_browse_listings import (  # noqa: E402
    DEFAULT_ENV,
    build_search_query,
    load_ebay_env,
)

GRADED_KEYWORDS = frozenset(["psa", "cgc", "bgs", "beckett", "ace", "pca", "graded", "gem mint"])

FINDING_BASE = "https://svcs.ebay.com/services/search/FindingService/v1"


def _env(name: str, default: Optional[str] = None) -> str:
    return (os.environ.get(name, default) or "").strip()


def _is_graded(title: str) -> bool:
    lower = title.lower()
    return any(kw in lower for kw in GRADED_KEYWORDS)


def _unwrap(v: Any) -> Any:
    while isinstance(v, list) and len(v) == 1:
        v = v[0]
    return v


def _as_str(v: Any) -> str:
    v = _unwrap(v)
    if v is None:
        return ""
    if isinstance(v, dict):
        if "__value__" in v:
            return str(v["__value__"])
        return str(v)
    return str(v)


def _parse_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    title = _as_str(item.get("title")).strip()
    if not title or "shop on ebay" in title.lower():
        return None

    selling = item.get("sellingStatus")
    selling = _unwrap(selling)
    if not isinstance(selling, dict):
        return None
    price_raw = _unwrap(selling.get("currentPrice"))
    if not isinstance(price_raw, dict):
        return None
    val = price_raw.get("__value__")
    try:
        price = float(val)
    except (TypeError, ValueError):
        return None

    listing_info = item.get("listingInfo")
    listing_info = _unwrap(listing_info)
    if not isinstance(listing_info, dict):
        return None
    end_s = _as_str(listing_info.get("endTime")).strip()
    if not end_s:
        return None
    # e.g. 2025-12-01T15:04:02.000Z
    try:
        if end_s.endswith("Z"):
            dt = datetime.fromisoformat(end_s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(end_s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        date_only = dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
    except ValueError:
        return None

    return {"date": date_only, "price": price, "title": title}


def finding_find_completed_page(
    *,
    keywords: str,
    app_id: str,
    end_from_utc: datetime,
    end_to_utc: datetime,
    page: int,
    per_page: int,
    global_id: str,
    category_id: str = "",
) -> Tuple[str, List[Dict[str, Any]], int]:
    """
    Returns (ack, items, total_entries_estimate).
    ack is 'Success', 'Warning', or 'Failure'.
    """
    def _iso_z(dt: datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # itemFilter: SoldItemsOnly, EndTimeFrom, EndTimeTo
    q = [
        ("OPERATION-NAME", "findCompletedItems"),
        ("SERVICE-VERSION", "1.13.0"),
        ("SECURITY-APPNAME", app_id),
        ("GLOBAL-ID", global_id),
        ("RESPONSE-DATA-FORMAT", "JSON"),
        ("REST-PAYLOAD", ""),
        ("keywords", keywords[:350]),
        ("paginationInput.entriesPerPage", str(max(1, min(per_page, 100)))),
        ("paginationInput.pageNumber", str(max(1, page))),
        ("sortOrder", "EndTimeNewest"),
        ("itemFilter(0).name", "SoldItemsOnly"),
        ("itemFilter(0).value", "true"),
        ("itemFilter(1).name", "EndTimeFrom"),
        ("itemFilter(1).value", _iso_z(end_from_utc)),
        ("itemFilter(2).name", "EndTimeTo"),
        ("itemFilter(2).value", _iso_z(end_to_utc)),
    ]
    cid = (category_id or "").strip()
    if cid:
        q.append(("categoryId", cid))
    url = FINDING_BASE + "?" + urllib.parse.urlencode(q)
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "PokemonTCG-Explorer/sync_ebay_sales_finding_api (Finding API)"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        return "Failure", [], 0
    except Exception:
        return "Failure", [], 0

    try:
        root = json.loads(raw)
    except json.JSONDecodeError:
        return "Failure", [], 0

    resp_block = root.get("findCompletedItemsResponse")
    resp_block = _unwrap(resp_block)
    if not isinstance(resp_block, dict):
        return "Failure", [], 0

    ack = _as_str(resp_block.get("ack")).lower()
    if "success" in ack:
        ack_norm = "Success"
    elif "warning" in ack:
        ack_norm = "Warning"
    else:
        ack_norm = "Failure"

    if ack_norm == "Failure":
        return ack_norm, [], 0

    sr = resp_block.get("searchResult")
    sr = _unwrap(sr)
    if not isinstance(sr, dict):
        return ack_norm, [], 0

    total = 0
    try:
        tc = sr.get("@count")
        if tc is not None:
            total = int(_unwrap(tc))
    except (TypeError, ValueError):
        total = 0

    raw_items = sr.get("item")
    if raw_items is None:
        return ack_norm, [], total
    if isinstance(raw_items, dict):
        raw_items = [raw_items]
    if not isinstance(raw_items, list):
        return ack_norm, [], total

    out: List[Dict[str, Any]] = []
    for it in raw_items:
        parsed = _parse_item(it) if isinstance(it, dict) else None
        if parsed:
            out.append(parsed)
    return ack_norm, out, total


def _paginate_find_completed(
    *,
    keywords: str,
    app_id: str,
    end_from: datetime,
    end_to: datetime,
    max_pages: int,
    per_page: int,
    sleep_s: float,
    global_id: str,
    category_id: str,
    seen: set[Tuple[str, float, str]],
    merged: List[Dict[str, Any]],
) -> bool:
    """Append sold rows into *merged*; return True if any item was returned."""
    got_any = False
    for page in range(1, max_pages + 1):
        ack, items, total = finding_find_completed_page(
            keywords=keywords,
            app_id=app_id,
            end_from_utc=end_from,
            end_to_utc=end_to,
            page=page,
            per_page=per_page,
            global_id=global_id,
            category_id=category_id,
        )
        if ack == "Failure" and not items:
            break
        for it in items:
            got_any = True
            key = (it["date"], round(float(it["price"]), 2), it["title"][:96])
            if key in seen:
                continue
            seen.add(key)
            merged.append(it)
        if len(items) < per_page:
            break
        if total and page * per_page >= total:
            break
        time.sleep(max(0.0, sleep_s))
    return got_any


def fetch_sales_for_query(
    *,
    keywords: str,
    app_id: str,
    days: int,
    max_pages: int,
    per_page: int,
    sleep_s: float,
    global_id: str,
    category_id: str = "",
) -> List[Dict[str, Any]]:
    """
    Try a few Finding keyword/category combinations (still API-only).
    eBay's index is sparse for some queries; ``#`` in keywords can reduce hits.
    """
    now = datetime.now(timezone.utc)
    end_from = now - timedelta(days=max(1, days))
    seen: set[Tuple[str, float, str]] = set()
    merged: List[Dict[str, Any]] = []

    alt_kw = re.sub(r"\s*#\s*", " ", keywords)
    alt_kw = re.sub(r"\s+", " ", alt_kw).strip()

    cid = (category_id or "").strip()
    strategies: List[Tuple[str, str]] = []
    if cid:
        strategies.append((keywords, cid))
    strategies.append((keywords, ""))
    if alt_kw != keywords:
        if cid:
            strategies.append((alt_kw, cid))
        strategies.append((alt_kw, ""))

    for kw, cat in strategies:
        got = _paginate_find_completed(
            keywords=kw,
            app_id=app_id,
            end_from=end_from,
            end_to=now,
            max_pages=max_pages,
            per_page=per_page,
            sleep_s=sleep_s,
            global_id=global_id,
            category_id=cat,
            seen=seen,
            merged=merged,
        )
        if got:
            break

    # Strict window on parsed dates (API can drift)
    cutoff = end_from.date().isoformat()
    merged = [x for x in merged if x.get("date") and x["date"] >= cutoff]
    merged.sort(key=lambda x: x["date"])
    return merged


def run(
    *,
    input_path: Path,
    output_path: Path,
    only_set_codes: Optional[set[str]],
    sleep_s: float,
    days: int,
    max_pages: int,
    per_page: int,
    global_id: str,
    finding_category_id: str = "",
) -> Dict[str, Any]:
    app_id = _env("EBAY_APP_ID")
    if not app_id:
        raise SystemExit("EBAY_APP_ID is required (used as Finding API SECURITY-APPNAME).")

    data = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("Expected pokemon_sets_data.json as a JSON array")

    rep: Dict[str, Any] = {
        "sync_iso": datetime.now(timezone.utc).isoformat(),
        "source": "finding_api_findCompletedItems",
        "days_window": days,
        "global_id": global_id,
        "finding_category_id": (finding_category_id or "").strip() or None,
        "sets_processed": 0,
        "cards_processed": 0,
        "cards_with_data": 0,
        "total_ungraded_sales": 0,
        "total_graded_sales": 0,
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
        print(f"[eBay Finding sold] set={sc!r} name={set_name[:48]!r} cards={nrows}", flush=True)

        for idx, card in enumerate(rows, start=1):
            nm = str(card.get("name") or "")
            num = card.get("number")
            qb = str(card.get("ebay_browse_query") or "").strip()
            q = qb if len(qb) >= 10 else build_search_query(set_name, nm, num)
            q = q[:350]
            print(f"  [{idx}/{nrows}] {nm[:40]!r} #{num} …", flush=True)

            sales = fetch_sales_for_query(
                keywords=q,
                app_id=app_id,
                days=days,
                max_pages=max_pages,
                per_page=per_page,
                sleep_s=sleep_s,
                global_id=global_id,
                category_id=finding_category_id,
            )

            graded = [x for x in sales if _is_graded(x["title"])]
            ungraded = [x for x in sales if not _is_graded(x["title"])]

            prev_ug = card.get("ebay_sold_history_ungraded")
            prev_g = card.get("ebay_sold_history_graded")
            prev_n = (
                (len(prev_ug) if isinstance(prev_ug, list) else 0)
                + (len(prev_g) if isinstance(prev_g, list) else 0)
            )
            if not graded and not ungraded and prev_n > 0:
                print(
                    f"    -> Finding returned 0; keeping existing sold history ({prev_n} rows)",
                    flush=True,
                )
            else:
                card["ebay_sold_history_graded"] = graded
                card["ebay_sold_history_ungraded"] = ungraded
                card["ebay_sold_sync_iso"] = datetime.now(timezone.utc).isoformat()
                card["ebay_sold_source"] = "finding_api"
                rep["total_graded_sales"] += len(graded)
                rep["total_ungraded_sales"] += len(ungraded)
                if graded or ungraded:
                    rep["cards_with_data"] += 1
                print(
                    f"    -> {len(ungraded)} ungraded, {len(graded)} graded (last {days}d window)",
                    flush=True,
                )

            rep["cards_processed"] += 1
            time.sleep(max(0.0, sleep_s))

        write_json_atomic(output_path, data)
        print(f"  checkpoint saved -> {output_path}", flush=True)

    return rep


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Sync sold eBay listings (Finding API findCompletedItems) into pokemon_sets_data.json"
    )
    ap.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--output", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    ap.add_argument("--only-set-codes", default="", help="Comma-separated set_code values (e.g. me3)")
    ap.add_argument("--sleep", type=float, default=-1.0, help="Sleep between API calls (default from env or 0.55)")
    ap.add_argument("--days", type=int, default=90, help="Only sales with listing end time in this many days (default 90)")
    ap.add_argument("--max-pages", type=int, default=5, help="Max Finding pages per card (100 items/page)")
    ap.add_argument("--per-page", type=int, default=100, help="Items per page (max 100)")
    ap.add_argument(
        "--global-id",
        default="",
        help="Finding GLOBAL-ID (default EBAY-US or EBAY_FINDING_GLOBAL_ID env)",
    )
    ap.add_argument(
        "--category-id",
        default="183454",
        help=(
            "eBay categoryId for findCompletedItems (default 183454 = CCG Individual Cards). "
            "Pass empty or 'none' to search all categories."
        ),
    )
    ap.add_argument("--backup", action="store_true")
    ap.add_argument("--all-sets", action="store_true", help="Process every set's top_25_cards")
    args = ap.parse_args()

    load_ebay_env(args.env_file.resolve())

    only: Optional[set[str]] = None
    if args.only_set_codes.strip():
        only = {x.strip().lower() for x in args.only_set_codes.split(",") if x.strip()}
    if not only and not args.all_sets:
        raise SystemExit(
            "Refusing a full-catalog run: pass --only-set-codes me3 (or comma list), "
            "or --all-sets to sync every set."
        )

    sleep_s = args.sleep
    if sleep_s < 0:
        try:
            sleep_s = float(_env("EBAY_BROWSE_SLEEP_SECONDS", "0.55") or "0.55")
        except ValueError:
            sleep_s = 0.55

    gid = (args.global_id.strip() or _env("EBAY_FINDING_GLOBAL_ID", "EBAY-US") or "EBAY-US").strip()
    cat_raw = (args.category_id or "").strip()
    finding_cat = "" if cat_raw.lower() in ("", "none", "off", "0") else cat_raw

    inp = args.input.resolve()
    out = args.output.resolve()

    if args.backup and inp == out and inp.is_file():
        bak = inp.with_suffix(inp.suffix + ".finding_sold_bak")
        shutil.copy2(inp, bak)
        print("Wrote backup ->", bak, flush=True)

    rep = run(
        input_path=inp,
        output_path=out,
        only_set_codes=only,
        sleep_s=max(0.0, sleep_s),
        days=max(1, int(args.days)),
        max_pages=max(1, int(args.max_pages)),
        per_page=max(1, min(100, int(args.per_page))),
        global_id=gid,
        finding_category_id=finding_cat,
    )

    rep_path = out.parent / (out.name + ".ebay_finding_sold_sync_report.json")
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(json.dumps(rep, indent=2), flush=True)
    print("Wrote report ->", rep_path, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
