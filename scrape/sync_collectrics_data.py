#!/usr/bin/env python3
"""
Fetch Collectrics eBay liquidity + price histories for cards in pokemon_sets_data.json
(top_25_cards per set).

Uses MyCollectrics HTTP API (same source as card pages, e.g.):
  GET https://mycollectrics.com/api/set/{COLLECTRICS_SET}/cards
  GET https://mycollectrics.com/api/card/{id}

1) Bulk set/cards provides per-card id, raw-price, raw-history (near-mint proxy history).
2) Per-card payload adds history-*, collectrics meta, and history-ebay-market for listing counts.

Set-code resolution (first hit):
  - collectrics_set_code_aliases.json: { "<our set_code lower>": "COLLECTRICS" }
  - else: set_code.upper()  (works for Base, Neo, many older sets)

Run:
  python scrape/sync_collectrics_data.py
  python scrape/sync_collectrics_data.py --sleep 0.12 --backup
  python scrape/sync_collectrics_data.py --max-sets 20 --no-card-detail   # bulk only, fast
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tcgtracking_merge import norm_card_name, norm_card_number  # noqa: E402


def write_json_atomic(path: Path, data: Any, *, indent: int = 4) -> None:
    path = path.resolve()
    text = json.dumps(data, indent=indent, ensure_ascii=False) + "\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)

HEADERS = {"User-Agent": "Mozilla/5.0 PokemonTCG-Explorer/sync_collectrics_data"}
ALIASES_PATH = ROOT / "collectrics_set_code_aliases.json"

# Prefix on our merged card fields (matches app.js / analytics: collectrics_ebay_* )
PREF = "collectrics_"

# API key -> merged field suffix (under collectrics_*)
DETAIL_FIELD_MAP = {
    "history": f"{PREF}price_history",
    "history-psa": f"{PREF}history_psa",
    "history-ebay": f"{PREF}history_ebay",
    "history-ebay-market": f"{PREF}history_ebay_market",
    "history-ebay-derived": f"{PREF}history_ebay_derived",
    "history-justtcg": f"{PREF}history_justtcg",
    "history-collectrics": f"{PREF}history_collectrics",
    "collectrics": f"{PREF}modeled_meta",
}

_PRODUCT_RE = re.compile(r"^(.+?)\s*#\s*(\d+)\s*$")


def load_aliases() -> Dict[str, str]:
    out: Dict[str, str] = {}
    if ALIASES_PATH.is_file():
        raw = json.loads(ALIASES_PATH.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            for k, v in raw.items():
                sk = str(k).strip()
                if sk.startswith("_") or sk == "comment" or not isinstance(v, str):
                    continue
                ks, vs = sk.lower(), str(v).strip()
                if ks and vs:
                    out[ks] = vs
    return out


def http_json(url: str, timeout: int = 90) -> Any:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def normalize_keys(obj: Any) -> Any:
    if isinstance(obj, dict):
        fixed: Dict[str, Any] = {}
        for k, v in obj.items():
            nk = str(k).replace("-", "_")
            fixed[nk] = normalize_keys(v)
        return fixed
    if isinstance(obj, list):
        return [normalize_keys(x) for x in obj]
    return obj


def parse_product_name(product_name: str) -> Optional[Tuple[str, str]]:
    if not product_name:
        return None
    m = _PRODUCT_RE.match(product_name.strip())
    if not m:
        return None
    name = m.group(1).strip()
    try:
        num = str(int(m.group(2)))
    except ValueError:
        num = m.group(2).strip()
    return name, num


def build_ce_row_index(cards: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for c in cards:
        if not isinstance(c, dict):
            continue
        pn = c.get("product-name") or c.get("product_name")
        parsed = parse_product_name(str(pn or ""))
        if parsed:
            nm, num = parsed
            key = f"{norm_card_number(num)}|{norm_card_name(nm)}"
        else:
            cn = c.get("card-number")
            key = f"{norm_card_number(cn)}|{norm_card_name(str(pn or ''))}"
        idx[key] = c
    return idx


def clear_collectrics_fields(card: Dict[str, Any]) -> None:
    for k in list(card.keys()):
        if k.startswith(PREF):
            del card[k]


def _raw_price_from_history_row(row: Dict[str, Any]) -> Optional[float]:
    for k in ("raw-price", "raw_price"):
        if k in row and row[k] is not None:
            try:
                return float(row[k])
            except (TypeError, ValueError):
                return None
    return None


def history_raw_price_deltas_pct(history: Optional[List[Dict[str, Any]]]) -> Dict[str, Optional[float]]:
    """Approximate Collectrics-style % change vs 30d / 60d using blended raw-price series."""
    out: Dict[str, Optional[float]] = {"raw_price_change_30d_pct": None, "raw_price_change_60d_pct": None}
    if not history or not isinstance(history, list):
        return out
    rows = [r for r in history if isinstance(r, dict) and r.get("date")]
    if len(rows) < 2:
        return out
    from datetime import datetime, timedelta

    def parse_d(s: Any) -> Optional[datetime]:
        try:
            return datetime.strptime(str(s)[:10], "%Y-%m-%d")
        except (TypeError, ValueError):
            return None

    last = rows[-1]
    last_dt = parse_d(last.get("date"))
    last_px = _raw_price_from_history_row(last)
    if last_dt is None or last_px is None or last_px <= 0:
        return out

    def px_near(target: datetime) -> Optional[float]:
        best: Optional[Tuple[float, int]] = None
        for r in rows:
            dt = parse_d(r.get("date"))
            if dt is None:
                continue
            px = _raw_price_from_history_row(r)
            if px is None:
                continue
            delta = abs((dt - target).days)
            if best is None or delta < best[1]:
                best = (px, delta)
        return best[0] if best else None

    for key, days in (("raw_price_change_30d_pct", 30), ("raw_price_change_60d_pct", 60)):
        old_px = px_near(last_dt - timedelta(days=days))
        if old_px is not None and old_px > 0:
            out[key] = round((last_px - old_px) / old_px * 100.0, 4)
    return out


def summary_ebay_market(
    rows: Optional[List[Dict[str, Any]]],
) -> Tuple[Optional[int], Optional[int]]:
    if not rows:
        return None, None
    last = rows[-1]
    listings: Optional[int] = None
    li = last.get("active-to")
    if li is None:
        li = last.get("active_to")
    try:
        if li is not None:
            listings = int(li)
    except (TypeError, ValueError):
        pass

    from datetime import datetime, timedelta

    try:
        latest_dt = datetime.strptime(str(last.get("date")), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return listings, None

    cutoff = latest_dt - timedelta(days=6)
    ended_sum = 0
    for r in rows:
        try:
            dt = datetime.strptime(str(r.get("date")), "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue
        if dt >= cutoff:
            try:
                ended_sum += int(r.get("ended") or 0)
            except (TypeError, ValueError):
                pass
    return listings, ended_sum if ended_sum else None


def merge_card_payload(
    dest: Dict[str, Any],
    bulk: Dict[str, Any],
    detail: Optional[Dict[str, Any]],
    include_detail: bool,
) -> None:
    cid = bulk.get("id")
    if cid is not None:
        dest[f"{PREF}card_id"] = str(cid)

    rh = bulk.get("raw-history") or bulk.get("raw_history")
    if isinstance(rh, list) and rh:
        dest[f"{PREF}raw_price_history"] = normalize_keys(rh)

    lp = bulk.get("latest-date") or bulk.get("latest_date")
    rp = bulk.get("raw-price")
    if lp is not None or rp is not None:
        snap: Dict[str, Any] = {}
        if lp is not None:
            snap["date"] = lp
        if rp is not None:
            try:
                snap["raw_price"] = float(rp)
            except (TypeError, ValueError):
                snap["raw_price"] = rp
        dest[f"{PREF}latest_listing"] = snap

    if not include_detail or not detail:
        return

    for api_key, dest_key in DETAIL_FIELD_MAP.items():
        block = detail.get(api_key)
        if block is None:
            continue
        dest[dest_key] = normalize_keys(block)

    hem = detail.get("history-ebay-market")
    if isinstance(hem, list) and hem:
        listings, ended7 = summary_ebay_market(hem)
        if listings is not None:
            dest[f"{PREF}ebay_listings"] = listings
        if ended7 is not None:
            dest[f"{PREF}ebay_sold_volume"] = ended7
        last_em = hem[-1]
        if isinstance(last_em, dict):
            dest[f"{PREF}ebay_market_snapshot"] = normalize_keys(last_em)

    tid = detail.get("tcg-id") or detail.get("tcg_id")
    if tid is not None and str(tid).strip():
        dest[f"{PREF}tcg_player_id"] = str(tid).strip()

    hist = detail.get("history")
    if isinstance(hist, list) and hist:
        for k, v in history_raw_price_deltas_pct(hist).items():
            if v is not None:
                dest[f"{PREF}{k}"] = v


def run(
    input_path: Path,
    output_path: Path,
    sleep_s: float,
    max_sets: int,
    no_card_detail: bool,
    max_detail_fetches: int,
    only_set_codes: Optional[Any] = None,
) -> Dict[str, Any]:
    aliases = load_aliases()
    data = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("pokemon_sets_data.json must be a list")

    filter_codes: Optional[set] = None
    if only_set_codes:
        if isinstance(only_set_codes, (set, frozenset)):
            filter_codes = {str(x).strip().lower() for x in only_set_codes if str(x).strip()}
        else:
            filter_codes = {x.strip().lower() for x in str(only_set_codes).split(",") if x.strip()}

    report: Dict[str, Any] = {
        "aliases_loaded": len(aliases),
        "sets_total": len(data),
        "only_set_codes": sorted(filter_codes) if filter_codes else None,
        "sets_with_bulk_match": 0,
        "cards_matched_bulk": 0,
        "cards_detail_fetched": 0,
        "detail_fetch_cap": max_detail_fetches if max_detail_fetches > 0 else None,
        "skipped_sets": [],
    }

    detail_budget = max_detail_fetches if max_detail_fetches > 0 else 10**9

    planned = 0
    for s in data:
        if not isinstance(s, dict):
            continue
        oc = str(s.get("set_code") or "").strip().lower()
        if filter_codes is not None and oc not in filter_codes:
            continue
        if not oc:
            continue
        planned += 1
        if max_sets > 0 and planned >= max_sets:
            break
    print(f"Collectrics sync: {planned} set(s) queued (detail cap={detail_budget if detail_budget < 10**8 else 'none'})", flush=True)

    set_attempt = 0
    for s in data:
        if not isinstance(s, dict):
            continue
        our_code = str(s.get("set_code") or "").strip().lower()
        if filter_codes is not None and our_code not in filter_codes:
            continue
        if not our_code:
            continue
        if max_sets > 0 and set_attempt >= max_sets:
            break
        set_attempt += 1

        ce = aliases.get(our_code) or our_code.upper()
        bulk_url = f"https://mycollectrics.com/api/set/{ce}/cards"
        print(f"  [{set_attempt}/{planned}] set_code={our_code!r} collectrics={ce!r} …", flush=True)

        try:
            time.sleep(sleep_s)
            bulk_payload = http_json(bulk_url)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            report["skipped_sets"].append(
                {"set_code": our_code, "collectrics_try": ce, "error": str(e)}
            )
            continue

        cards = bulk_payload.get("cards")
        if not isinstance(cards, list) or len(cards) == 0:
            report["skipped_sets"].append(
                {
                    "set_code": our_code,
                    "collectrics_try": ce,
                    "error": "empty_or_missing_cards",
                }
            )
            continue

        index = build_ce_row_index(cards)
        report["sets_with_bulk_match"] += 1

        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue

        for c in top:
            if not isinstance(c, dict):
                continue
            k = f"{norm_card_number(c.get('number'))}|{norm_card_name(c.get('name'))}"
            b = index.get(k)
            if not b:
                continue

            clear_collectrics_fields(c)
            detail: Optional[Dict[str, Any]] = None
            fetch_detail = not no_card_detail and detail_budget > 0
            if fetch_detail:
                cid = b.get("id")
                if cid is not None:
                    url = f"https://mycollectrics.com/api/card/{cid}"
                    try:
                        time.sleep(sleep_s)
                        detail = http_json(url)
                        report["cards_detail_fetched"] += 1
                        detail_budget -= 1
                    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError):
                        detail = None

            merge_card_payload(c, b, detail, include_detail=not no_card_detail)
            report["cards_matched_bulk"] += 1

    write_json_atomic(output_path, data)

    skipped = report.pop("skipped_sets", [])
    report["skipped_sets_count"] = len(skipped)
    report["skipped_sets_sample"] = skipped[:25]

    rep_path = output_path.parent / (output_path.name + ".collectrics_sync_report.json")
    rep_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description="Sync Collectrics liquidity + price history into pokemon_sets_data.json")
    ap.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--output", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--sleep", type=float, default=0.12, help="Delay between HTTP requests")
    ap.add_argument("--max-sets", type=int, default=0, help="0 = all sets")
    ap.add_argument(
        "--no-card-detail",
        action="store_true",
        help="Only use /api/set/{code}/cards (raw-price history); skip per-card /api/card/{id}",
    )
    ap.add_argument(
        "--max-detail-fetches",
        type=int,
        default=0,
        help="Cap full card requests (0 = no cap). Useful for smoke tests.",
    )
    ap.add_argument(
        "--backup",
        action="store_true",
        help="If output == input, copy input to .bak before writing",
    )
    ap.add_argument(
        "--only-set-codes",
        default="",
        help="Comma-separated set_code values to process (e.g. sv1,zsv10pt5). Default: all sets.",
    )
    args = ap.parse_args()

    inp = args.input.resolve()
    out = args.output.resolve()
    if args.backup and inp == out and inp.is_file():
        bak = inp.with_suffix(inp.suffix + ".bak")
        shutil.copy2(inp, bak)
        print("Wrote backup", bak, flush=True)

    max_sets = args.max_sets if args.max_sets and args.max_sets > 0 else 0
    cap = args.max_detail_fetches if args.max_detail_fetches > 0 else 0
    only = args.only_set_codes.strip() or None

    run(
        inp,
        out,
        sleep_s=max(0.0, args.sleep),
        max_sets=max_sets,
        no_card_detail=args.no_card_detail,
        max_detail_fetches=cap,
        only_set_codes=only,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
