#!/usr/bin/env python3
"""
Fetch TCGPlayer marketplace snapshot data via the public **mp-search-api** product
details endpoint (same family as ``sync_pokemon_wizard.fetch_tcgplayer_product_name``).

Per card (top_25_cards):
  - Resolves **TCGPlayer** ``productId`` for mp-search-api from (in order):
    ``collectrics_tcg_player_id``, ``tcgtracking_product_id`` (same id space as cache),
    then TCGTracking cache ``products.json`` via ``find_product_for_card``.
    **Not** used: ``pricecharting_product_id`` (PriceCharting's own ids) or Wizard
    ``/cards/{id}/`` (site-internal ids).
  - GET ``https://mp-search-api.tcgplayer.com/v1/product/{id}/details``
  - Writes:
        tcgplayer_product_id, tcgplayer_market_price_usd, tcgplayer_lowest_price_usd,
        tcgplayer_lowest_price_with_shipping_usd, tcgplayer_listings_count,
        tcgplayer_sellers_count, tcgplayer_score, tcgplayer_sync_iso
    and appends one row to **tcgplayer_market_history** (UTC date deduped) for LSRL /
    predictor time series. Re-run the script on different days to grow ~1Y of points.

**Sales volume:** TCGPlayer does not expose per-interval *sold* counts on this endpoint;
``listings`` / ``sellers`` are **live listing depth** (liquidity proxy), stored explicitly.

Per set (set row):
  - Finds **English booster pack** SKU in TCG cache (name ~= ``{Set} Booster Pack`` without
    ``Sleeved``, ``Bundle``, ``Code``) and merges the same snapshot fields under
    ``tcgplayer_booster_pack_*`` plus ``tcgplayer_booster_pack_market_history``.

Example:
  python scrape/sync_tcgplayer_mpapi.py --only-set-codes me3 --backup --sleep 0.35
  python scrape/sync_tcgplayer_mpapi.py --all-sets --backup --sleep 0.2

Requires ``tcg_cache/{set_id}/products.json`` from a prior ``tcgtracking_merge.py`` run
(or any populated cache dir whose ``set_name`` matches the set after ``norm_set_key``).
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(ROOT / "scrape"))

from dataset_report_paths import dataset_sidecar_report_path  # noqa: E402
from json_atomic_util import write_json_atomic  # noqa: E402

from tcgtracking_merge import (  # noqa: E402
    find_product_for_card,
    index_products_by_number,
    norm_card_name,
    norm_set_key,
)

MP_BASE = "https://mp-search-api.tcgplayer.com/v1/product"
HEADERS = {"User-Agent": "Mozilla/5.0 PokemonTCG-Explorer/sync_tcgplayer_mpapi (hobbyist)"}
HIST_MAX = 400


def _f(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _i(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


def http_json(url: str, *, timeout: int = 45) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_product_details(product_id: int) -> Optional[Dict[str, Any]]:
    url = f"{MP_BASE}/{int(product_id)}/details"
    try:
        d = http_json(url)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError):
        return None
    return d if isinstance(d, dict) else None


def details_to_snapshot(d: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "market_usd": _f(d.get("marketPrice")),
        "lowest_usd": _f(d.get("lowestPrice")),
        "lowest_with_shipping_usd": _f(d.get("lowestPriceWithShipping")),
        "listings": _i(d.get("listings")),
        "sellers": _i(d.get("sellers")),
        "score": _f(d.get("score")),
    }


def merge_snapshot_into_card(
    target: Dict[str, Any],
    *,
    prefix: str,
    product_id: int,
    d: Dict[str, Any],
    snap: Dict[str, Any],
    sync_iso: str,
) -> None:
    target[f"{prefix}product_id"] = int(product_id)
    if snap.get("market_usd") is not None:
        target[f"{prefix}market_price_usd"] = round(float(snap["market_usd"]), 2)
    if snap.get("lowest_usd") is not None:
        target[f"{prefix}lowest_price_usd"] = round(float(snap["lowest_usd"]), 2)
    if snap.get("lowest_with_shipping_usd") is not None:
        target[f"{prefix}lowest_price_with_shipping_usd"] = round(float(snap["lowest_with_shipping_usd"]), 2)
    if snap.get("listings") is not None:
        target[f"{prefix}listings_count"] = int(snap["listings"])
    if snap.get("sellers") is not None:
        target[f"{prefix}sellers_count"] = int(snap["sellers"])
    if snap.get("score") is not None:
        target[f"{prefix}score"] = round(float(snap["score"]), 5)
    target[f"{prefix}sync_iso"] = sync_iso
    pname = d.get("productName")
    if pname:
        target[f"{prefix}product_name"] = str(pname).strip()

    day = sync_iso[:10]
    hist_key = f"{prefix}market_history"
    row = {
        "date": day,
        "sync_iso": sync_iso,
        **{k: v for k, v in snap.items() if v is not None},
    }
    prev = target.get(hist_key)
    hist: List[Dict[str, Any]] = list(prev) if isinstance(prev, list) else []
    hist = [x for x in hist if isinstance(x, dict) and x.get("date") != day]
    hist.append(row)
    hist.sort(key=lambda x: str(x.get("date") or ""))
    if len(hist) > HIST_MAX:
        hist = hist[-HIST_MAX:]
    target[hist_key] = hist


def tcgplayer_product_id_from_card(card: Dict[str, Any]) -> Optional[int]:
    """IDs known to be TCGPlayer marketplace product ids (6–7 digit typical)."""
    for k in ("collectrics_tcg_player_id", "tcgtracking_product_id"):
        v = card.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s.isdigit():
            return int(s)
    return None


def find_tcg_cache_products_path(cache_dir: Path, set_name: str) -> Optional[Path]:
    """Pick tcg_cache/{{set_id}}/products.json whose set_name matches *set_name* (norm_set_key)."""
    want = norm_set_key(set_name)
    if not want:
        return None
    if not cache_dir.is_dir():
        return None
    best: Optional[Tuple[int, Path]] = None
    for sub in cache_dir.iterdir():
        if not sub.is_dir():
            continue
        pj = sub / "products.json"
        if not pj.is_file():
            continue
        try:
            raw = json.loads(pj.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        sn = norm_set_key(str(raw.get("set_name") or ""))
        if sn != want:
            continue
        try:
            sid = int(sub.name)
        except ValueError:
            sid = 0
        if best is None or sid >= best[0]:
            best = (sid, pj)
    return best[1] if best else None


def pick_booster_pack_product(products: List[Dict[str, Any]], set_display_name: str) -> Optional[Dict[str, Any]]:
    """
    English-style single booster pack: name contains 'Booster Pack' but not Sleeved / Bundle / Code / Case.
    Prefer name that starts with or closely matches the set name.
    """
    short = norm_set_key(set_display_name).replace("pokemon", "").strip()
    cands: List[Dict[str, Any]] = []
    for p in products:
        if not isinstance(p, dict):
            continue
        name = str(p.get("name") or "")
        low = name.lower()
        if "booster pack" not in low:
            continue
        if any(x in low for x in ("sleeved", "bundle", "code card", " case", "3-pack", "6-pack", "blister")):
            continue
        cands.append(p)
    if not cands:
        return None
    # Prefer shortest clean name (plain pack vs long promos).
    cands.sort(key=lambda x: len(str(x.get("name") or "")))
    for p in cands:
        if short and short in norm_set_key(str(p.get("name") or "")):
            return p
    return cands[0]


def run_set(
    set_row: Dict[str, Any],
    *,
    cache_dir: Path,
    sleep_s: float,
    rep: Dict[str, Any],
) -> None:
    set_name = str(set_row.get("set_name") or "")
    products_path = find_tcg_cache_products_path(cache_dir, set_name)
    products: List[Dict[str, Any]] = []
    if products_path and products_path.is_file():
        try:
            pdata = json.loads(products_path.read_text(encoding="utf-8"))
            products = pdata.get("products") or []
        except (json.JSONDecodeError, OSError):
            products = []
    by_num = index_products_by_number(products) if products else {}

    sync_iso = datetime.now(timezone.utc).isoformat()

    pack_p = pick_booster_pack_product(products, set_name) if products else None
    if pack_p and pack_p.get("id"):
        pid = int(pack_p["id"])
        d = fetch_product_details(pid)
        time.sleep(max(0.0, sleep_s))
        if d:
            snap = details_to_snapshot(d)
            merge_snapshot_into_card(
                set_row,
                prefix="tcgplayer_booster_pack_",
                product_id=pid,
                d=d,
                snap=snap,
                sync_iso=sync_iso,
            )
            rep["pack_merged"] = 1
            print(
                f"  [pack] {pack_p.get('name')!r} id={pid} mkt={snap.get('market_usd')} "
                f"listings={snap.get('listings')} sellers={snap.get('sellers')}",
                flush=True,
            )
        else:
            rep["pack_fetch_fail"] += 1
            print(f"  [pack] WARN no mp-api data for product_id={pid}", flush=True)
    else:
        rep["pack_skipped"] += 1
        print("  [pack] SKIP no booster pack row in tcg_cache products", flush=True)

    top = set_row.get("top_25_cards") or []
    if not isinstance(top, list):
        return

    for card in top:
        if not isinstance(card, dict):
            continue
        rep["cards_considered"] += 1
        nm = card.get("name")
        num = card.get("number")
        pid = tcgplayer_product_id_from_card(card)
        if pid is None and by_num:
            p = find_product_for_card(card, by_num)
            if p and p.get("id"):
                pid = int(p["id"])
        if pid is None:
            rep["cards_skipped_no_id"] += 1
            print(f"  [card] SKIP no product id  #{num} {str(nm)[:40]!r}", flush=True)
            continue

        d = fetch_product_details(pid)
        time.sleep(max(0.0, sleep_s))
        if not d:
            rep["cards_fetch_fail"] += 1
            print(f"  [card] FAIL mp-api  #{num} {str(nm)[:40]!r} pid={pid}", flush=True)
            continue
        snap = details_to_snapshot(d)
        merge_snapshot_into_card(
            card,
            prefix="tcgplayer_",
            product_id=pid,
            d=d,
            snap=snap,
            sync_iso=sync_iso,
        )
        rep["cards_merged"] += 1
        print(
            f"  [card] OK #{num} {str(nm)[:32]!r} pid={pid} mkt={snap.get('market_usd')} "
            f"listings={snap.get('listings')} sellers={snap.get('sellers')}",
            flush=True,
        )


def main() -> int:
    ap = argparse.ArgumentParser(description="Merge TCGPlayer mp-search-api snapshots into pokemon_sets_data.json")
    ap.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--output", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--cache", type=Path, default=ROOT / "tcg_cache", help="TCGTracking cache dir (per-set products.json)")
    ap.add_argument("--only-set-codes", default="", help="Comma-separated set_code (e.g. me3)")
    ap.add_argument(
        "--all-sets",
        action="store_true",
        help="Refresh every set's booster pack + top_25_cards (many mp-search-api calls).",
    )
    ap.add_argument("--sleep", type=float, default=0.35, help="Delay between mp-search-api calls")
    ap.add_argument("--backup", action="store_true")
    args = ap.parse_args()

    only = {x.strip().lower() for x in args.only_set_codes.split(",") if x.strip()}
    if args.all_sets and only:
        raise SystemExit("Pass either --all-sets or --only-set-codes, not both.")
    if not only and not args.all_sets:
        raise SystemExit(
            "Pass --only-set-codes me3 (or comma list), or --all-sets for every set. Refusing ambiguous run."
        )

    inp = args.input.resolve()
    out = args.output.resolve()
    if args.backup and inp == out and inp.is_file():
        bak = inp.with_suffix(inp.suffix + ".tcgplayer_mpapi_bak")
        shutil.copy2(inp, bak)
        print("Wrote backup ->", bak, flush=True)

    data = json.loads(inp.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("Expected pokemon_sets_data.json as a JSON array")

    rep: Dict[str, Any] = {
        "sync_iso": datetime.now(timezone.utc).isoformat(),
        "source": "mp_search_api_v1_product_details",
        "only_set_codes": sorted(only) if only else ["__ALL_SETS__"],
        "sets_matched": 0,
        "cards_considered": 0,
        "cards_merged": 0,
        "cards_skipped_no_id": 0,
        "cards_fetch_fail": 0,
        "pack_merged": 0,
        "pack_skipped": 0,
        "pack_fetch_fail": 0,
    }

    for set_row in data:
        if not isinstance(set_row, dict):
            continue
        sc = str(set_row.get("set_code") or "").strip().lower()
        if not args.all_sets and sc not in only:
            continue
        set_name = str(set_row.get("set_name") or sc)
        print(f"[TCGPlayer mpapi] set={sc!r} name={set_name[:56]!r}", flush=True)
        rep["sets_matched"] += 1
        run_set(set_row, cache_dir=args.cache.resolve(), sleep_s=max(0.0, float(args.sleep)), rep=rep)

    write_json_atomic(out, data)
    rep_path = dataset_sidecar_report_path(out, ".tcgplayer_mpapi_sync_report.json")
    rep_path.parent.mkdir(parents=True, exist_ok=True)
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(json.dumps(rep, indent=2), flush=True)
    print("Wrote report ->", rep_path, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
