"""
Merge US market data from TCGTracking Open TCG API into pokemon_sets_data.json.

Docs: https://tcgtracking.com/tcgapi/
Pokemon (English) category: 3  ->  /tcgapi/v1/3/sets/{set_id}  + /pricing + /skus

Preserves ThePriceDex-sourced fields (pull rates, rarity_pull_rates, etc.); adds
tcgtracking_* fields (including tcgtracking_low_usd from the chosen NM/EN SKU row),
and optionally replaces market_price when --prefer-tcgtracking
(stashing the pre-overwrite value as pricedex_market_usd for analytics blending).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

BASE = "https://tcgtracking.com/tcgapi/v1/3"
HEADERS = {"User-Agent": "Mozilla/5.0 PokemonTCG-Explorer/1.0 (tcgtracking_merge)"}


def fetch_json(url: str, timeout: int = 60) -> Any:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_json_cached(url: str, cache_path: Optional[str]) -> Any:
    if cache_path and os.path.isfile(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    data = fetch_json(url)
    if cache_path:
        os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    return data


def norm_set_key(name: Optional[str]) -> str:
    if not name:
        return ""
    s = name.strip().lower()
    s = s.replace("pokémon", "pokemon").replace("poké", "poke")
    s = s.replace(" & ", " and ").replace("&", " and ")
    s = re.sub(r"^ex\s+", "", s, flags=re.I)
    s = re.sub(r"^[\s]*", "", s)
    # Strip common TCGTracking prefixes: SWSH10:, SV01:, ME:, etc.
    s = re.sub(
        r"^(swsh|sv|smp|sm|xy|bw|ex|mep|me|ssb|cel|tk|dv|np|si|fb|fbl|rc|clb|swsh|tg|svp)\d*[:\s]+",
        "",
        s,
        flags=re.I,
    )
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_card_number(num: Any) -> str:
    if num is None:
        return ""
    s = str(num).strip()
    if "/" in s:
        s = s.split("/")[0].strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return s.lower()
    try:
        return str(int(digits))
    except ValueError:
        return s.lower()


def norm_card_name(name: Optional[str]) -> str:
    if not name:
        return ""
    return re.sub(r"\s+", " ", name.lower().strip())


def tcg_variant_letter(rarity: Optional[str]) -> str:
    r = (rarity or "").lower()
    if "reverse" in r:
        return "RH"
    if any(
        x in r
        for x in (
            "holo",
            "v ",
            " vmax",
            " vstar",
            " ex",
            " gx",
            " ultra",
            " secret",
            " illustration",
            " special",
            " radiant",
            " amazing",
            " prime",
            " break",
            " lv.",
            " ace spec",
        )
    ):
        return "H"
    return "N"


def pick_tcg_market(
    tcg_prices: Optional[Dict[str, Any]], rarity: Optional[str]
) -> Tuple[Optional[float], Optional[str]]:
    if not tcg_prices:
        return None, None
    block = tcg_prices.get("tcg") or {}
    if not isinstance(block, dict) or not block:
        return None, None
    if len(block) == 1:
        k = next(iter(block))
        m = block[k].get("market")
        return (float(m) if m is not None else None), k
    r = (rarity or "").lower()
    if "reverse" in r and "Reverse Holofoil" in block:
        m = block["Reverse Holofoil"].get("market")
        return (float(m) if m is not None else None), "Reverse Holofoil"
    if any(
        x in r
        for x in (
            "holo",
            "v",
            "vmax",
            "vstar",
            "ex",
            "gx",
            "ultra",
            "secret",
            "illustration",
            "special",
            "radiant",
            "amazing",
        )
    ):
        for prefer in ("Holofoil", "Holo", "Reverse Holofoil"):
            if prefer in block:
                m = block[prefer].get("market")
                return (float(m) if m is not None else None), prefer
    if "Normal" in block:
        m = block["Normal"].get("market")
        return (float(m) if m is not None else None), "Normal"
    best_k, best_m = None, None
    for k, v in block.items():
        if not isinstance(v, dict):
            continue
        m = v.get("market")
        if m is None:
            continue
        if best_m is None or float(m) > float(best_m):
            best_k, best_m = k, m
    return (float(best_m) if best_m is not None else None), best_k


def sku_var_from_subtype(subtype: Optional[str]) -> Optional[str]:
    if not subtype:
        return None
    s = subtype.lower()
    if "reverse" in s:
        return "RH"
    if "holo" in s or "foil" in s:
        return "H"
    if "normal" in s:
        return "N"
    return None


def find_standard_booster_box_product(
    products: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Main English-style booster box SKU (excludes code cards, cases, half boxes)."""
    best: Optional[Dict[str, Any]] = None
    for p in products:
        name = (p.get("name") or "").lower()
        if "code card" in name:
            continue
        if "booster box" not in name:
            continue
        if any(x in name for x in (" case", "half booster", "mini ", "mini booster")):
            continue
        # Prefer the shortest name (often the plain set booster box vs. long promos).
        if best is None or len(name) < len((best.get("name") or "").lower()):
            best = p
    return best


def sku_nm_en_normal_best(skdata: Optional[Dict[str, Any]], product_id: str) -> Optional[Dict[str, Any]]:
    """Pick NM / EN / Normal (`var` N) row; if multiple SKUs, prefer higher listing count."""
    if not skdata or "products" not in skdata:
        return None
    prods = skdata["products"].get(str(product_id)) or skdata["products"].get(product_id)
    if not isinstance(prods, dict):
        return None
    best: Optional[Dict[str, Any]] = None
    best_cnt = -1
    for _, row in prods.items():
        if not isinstance(row, dict):
            continue
        if row.get("cnd") != "NM" or row.get("lng") != "EN" or row.get("var") != "N":
            continue
        cnt = row.get("cnt")
        try:
            c = int(cnt) if cnt is not None else 0
        except (TypeError, ValueError):
            c = 0
        if best is None or c > best_cnt:
            best, best_cnt = row, c
    return best


def attach_sealed_booster_box_implied_pack(
    set_row: Dict[str, Any],
    products: List[Dict[str, Any]],
    skdata: Optional[Dict[str, Any]],
) -> None:
    """
    Live sealed booster box from TCGTracking skus → implied $/pack (÷ packs_per_box from Dex, else 36).
    Avoids scraping TCGPlayer directly (see DATA_SOURCE_OVERHAUL_PROPOSAL §11).
    """
    for k in (
        "tcgtracking_sealed_booster_box_product_id",
        "tcgtracking_sealed_booster_box_mkt_usd",
        "tcgtracking_sealed_booster_box_low_usd",
        "tcgtracking_sealed_booster_box_listings_nm_en",
        "tcgtracking_implied_pack_usd_sealed_mkt",
        "tcgtracking_implied_pack_usd_sealed_low",
    ):
        set_row.pop(k, None)

    box_p = find_standard_booster_box_product(products)
    if not box_p or not skdata:
        return
    pid = str(box_p.get("id") or "")
    if not pid:
        return
    row = sku_nm_en_normal_best(skdata, pid)
    if not row:
        return
    raw_ppb = set_row.get("packs_per_box")
    try:
        ppb = int(raw_ppb) if raw_ppb is not None else 36
    except (TypeError, ValueError):
        ppb = 36
    if ppb <= 0:
        ppb = 36

    def _f(x: Any) -> Optional[float]:
        if x is None:
            return None
        try:
            v = float(x)
        except (TypeError, ValueError):
            return None
        return v if v > 0 else None

    mkt = _f(row.get("mkt"))
    low = _f(row.get("low"))
    cnt = row.get("cnt")
    try:
        cnt_i = int(cnt) if cnt is not None else None
    except (TypeError, ValueError):
        cnt_i = None

    set_row["tcgtracking_sealed_booster_box_product_id"] = int(box_p["id"])
    if mkt is not None:
        set_row["tcgtracking_sealed_booster_box_mkt_usd"] = round(mkt, 2)
        set_row["tcgtracking_implied_pack_usd_sealed_mkt"] = round(mkt / ppb, 4)
    if low is not None:
        set_row["tcgtracking_sealed_booster_box_low_usd"] = round(low, 2)
        set_row["tcgtracking_implied_pack_usd_sealed_low"] = round(low / ppb, 4)
    if cnt_i is not None:
        set_row["tcgtracking_sealed_booster_box_listings_nm_en"] = cnt_i


def _sku_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        x = float(val)
    except (TypeError, ValueError):
        return None
    return x if x > 0 else None


def pick_nm_en_sku_fields(
    skus_root: Optional[Dict[str, Any]],
    product_id: str,
    subtype_used: Optional[str],
    rarity: Optional[str],
) -> Tuple[Optional[int], Optional[str], Optional[float], Optional[float]]:
    """Best NM/EN SKU row: cnt, sku_id, low, mkt. Upstream ``cnt`` may cap at 25."""
    if not skus_root or "products" not in skus_root:
        return None, None, None, None
    prods = skus_root["products"].get(str(product_id)) or skus_root["products"].get(product_id)
    if not prods:
        return None, None, None, None
    var = sku_var_from_subtype(subtype_used) or tcg_variant_letter(rarity)
    best_cnt: Optional[int] = None
    best_sid: Optional[str] = None
    best_row: Optional[Dict[str, Any]] = None
    for sid, row in prods.items():
        if not isinstance(row, dict):
            continue
        if row.get("cnd") != "NM" or row.get("lng") != "EN":
            continue
        if row.get("var") != var:
            continue
        cnt = row.get("cnt")
        if cnt is None:
            continue
        try:
            c = int(cnt)
        except (TypeError, ValueError):
            continue
        if best_cnt is None or c > best_cnt:
            best_cnt, best_sid, best_row = c, sid, row
    if best_cnt is not None and best_row is not None:
        return best_cnt, best_sid, _sku_float(best_row.get("low")), _sku_float(best_row.get("mkt"))
    for sid, row in prods.items():
        if not isinstance(row, dict):
            continue
        if row.get("cnd") == "NM" and row.get("lng") == "EN" and row.get("cnt") is not None:
            try:
                c = int(row["cnt"])
            except (TypeError, ValueError):
                continue
            return c, sid, _sku_float(row.get("low")), _sku_float(row.get("mkt"))
    return None, None, None, None


def fuzz_set_key(name: Optional[str]) -> str:
    """Alphanumeric-only bag; strips `and` so HeartGold & SoulSilver ~ HeartGold SoulSilver."""
    k = norm_set_key(name or "")
    k = k.replace(" and ", "")
    return re.sub(r"[^a-z0-9]+", "", k)


def build_tcg_set_index(sets_payload: Dict[str, Any]) -> Tuple[Dict[str, int], Dict[str, int]]:
    """(norm_set_key -> id, fuzz_set_key -> id). Last wins on collision."""
    out: Dict[str, int] = {}
    fuzz: Dict[str, int] = {}
    for s in sets_payload.get("sets") or []:
        sid = s.get("id")
        name = s.get("name")
        if sid is None or not name:
            continue
        sid_i = int(sid)
        k = norm_set_key(name)
        if k:
            out[k] = sid_i
        fz = fuzz_set_key(name)
        if fz:
            fuzz[fz] = sid_i
    return out, fuzz


def resolve_set_id(set_name: str, index: Dict[str, int], fuzz_index: Dict[str, int]) -> Optional[int]:
    k = norm_set_key(set_name)
    if k in index:
        return index[k]
    # e.g. JSON "Base" vs TCGTracking "Base Set"
    if f"{k} set" in index:
        return index[f"{k} set"]
    if f"{k} base set" in index:
        return index[f"{k} base set"]
    # Strip trailing " base set" / " expansion" style mismatches
    for suffix in (" base set", " expansion"):
        if k.endswith(suffix):
            short = k[: -len(suffix)].strip()
            if short in index:
                return index[short]
            if f"{short} set" in index:
                return index[f"{short} set"]
    fz = fuzz_set_key(set_name)
    if fz in fuzz_index:
        return fuzz_index[fz]
    return None


def index_products_by_number(products: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    by_num: Dict[str, List[Dict[str, Any]]] = {}
    for p in products:
        num = norm_card_number(p.get("number"))
        if not num:
            continue
        by_num.setdefault(num, []).append(p)
    return by_num


def find_product_for_card(
    card: Dict[str, Any], by_num: Dict[str, List[Dict[str, Any]]]
) -> Optional[Dict[str, Any]]:
    num = norm_card_number(card.get("number"))
    if not num or num not in by_num:
        return None
    candidates = by_num[num]
    if len(candidates) == 1:
        return candidates[0]
    cname = norm_card_name(card.get("name"))
    for p in candidates:
        if norm_card_name(p.get("name")) == cname:
            return p
    # Fuzzy: card name contained
    for p in candidates:
        pn = norm_card_name(p.get("name"))
        if cname and (cname in pn or pn in cname):
            return p
    return candidates[0]


def merge_set(
    set_row: Dict[str, Any],
    set_id: int,
    cache_dir: str,
    prefer_tcg: bool,
    sleep_s: float,
) -> Dict[str, Any]:
    base = os.path.join(cache_dir, str(set_id))
    os.makedirs(base, exist_ok=True)

    products_url = f"{BASE}/sets/{set_id}"
    pricing_url = f"{BASE}/sets/{set_id}/pricing"
    skus_url = f"{BASE}/sets/{set_id}/skus"

    prod_path = os.path.join(base, "products.json")
    price_path = os.path.join(base, "pricing.json")
    skus_path = os.path.join(base, "skus.json")

    pdata = fetch_json_cached(products_url, prod_path)
    time.sleep(sleep_s)
    priced = fetch_json_cached(pricing_url, price_path)
    time.sleep(sleep_s)
    try:
        skdata = fetch_json_cached(skus_url, skus_path)
    except urllib.error.HTTPError:
        skdata = None

    products = pdata.get("products") or []
    prices = priced.get("prices") or {}
    sku_updated = (skdata or {}).get("updated") if skdata else priced.get("updated")

    attach_sealed_booster_box_implied_pack(set_row, products, skdata)

    by_num = index_products_by_number(products)

    set_row["tcgtracking_set_id"] = set_id
    set_row["tcgtracking_price_updated"] = sku_updated

    matched = 0
    for card in set_row.get("top_25_cards") or []:
        p = find_product_for_card(card, by_num)
        if not p:
            card["tcgtracking_match"] = "none"
            continue
        pid = str(p["id"])
        mkt, subtype = pick_tcg_market(prices.get(pid), card.get("rarity"))
        cnt, sku_id, low_sku, mkt_sku = pick_nm_en_sku_fields(skdata, pid, subtype, card.get("rarity"))

        card["tcgtracking_product_id"] = int(p["id"])
        if mkt is not None:
            card["tcgtracking_market_usd"] = round(mkt, 2)
        else:
            card["tcgtracking_market_usd"] = None
        card["tcgtracking_price_subtype"] = subtype
        card["tcgtracking_listings_nm_en"] = cnt
        card["tcgtracking_sku_id"] = sku_id
        card["tcgtracking_low_usd"] = round(low_sku, 2) if low_sku is not None else None
        if prefer_tcg and mkt is not None:
            mp = card.get("market_price")
            if mp is not None:
                try:
                    card["pricedex_market_usd"] = round(float(mp), 2)
                except (TypeError, ValueError):
                    pass
            card["market_price"] = round(mkt, 2)
        card["tcgtracking_match"] = "ok"
        matched += 1

    set_row["tcgtracking_cards_matched"] = matched
    set_row["tcgtracking_cards_total_top"] = len(set_row.get("top_25_cards") or [])
    return set_row


def load_set_overrides(path: str) -> Dict[str, int]:
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    out: Dict[str, int] = {}
    for k, v in raw.items():
        try:
            out[str(k)] = int(v)
        except (TypeError, ValueError):
            continue
    return out


def run(
    input_path: str,
    output_path: str,
    cache_dir: str,
    max_sets: int,
    prefer_tcg: bool,
    sleep_s: float,
    overrides_path: str,
) -> None:
    with open(input_path, "r", encoding="utf-8") as f:
        data: List[Dict[str, Any]] = json.load(f)

    sets_index_path = os.path.join(cache_dir, "_index_sets.json")
    sets_payload = fetch_json_cached(f"{BASE}/sets", sets_index_path)
    index, fuzz_index = build_tcg_set_index(sets_payload)
    time.sleep(sleep_s)

    overrides = load_set_overrides(overrides_path)

    unmatched: List[str] = []
    for i, set_row in enumerate(data):
        if max_sets and i >= max_sets:
            break
        name = set_row.get("set_name") or ""
        sid = overrides.get(name)
        if sid is None:
            sid = resolve_set_id(name, index, fuzz_index)
        if sid is None:
            unmatched.append(name)
            set_row["tcgtracking_set_id"] = None
            set_row["tcgtracking_match"] = "unresolved_set"
            time.sleep(sleep_s)
            continue
        try:
            merge_set(set_row, sid, cache_dir, prefer_tcg, sleep_s)
        except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError, OSError) as e:
            set_row["tcgtracking_error"] = str(e)
            unmatched.append(f"{name} (http:{e})")
        time.sleep(sleep_s)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    report = {
        "output": output_path,
        "sets_processed_cap": max_sets or len(data),
        "unresolved_set_names": unmatched,
        "unresolved_count": len(unmatched),
    }
    rep_path = output_path + ".merge_report.json"
    with open(rep_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge TCGTracking prices into pokemon_sets_data.json")
    ap.add_argument("--input", default="pokemon_sets_data.json")
    ap.add_argument("--output", default="pokemon_sets_data.json")
    ap.add_argument("--cache", default="tcg_cache", help="Directory for per-set and index cache")
    ap.add_argument("--max-sets", type=int, default=0, help="0 = all sets (slow)")
    ap.add_argument(
        "--prefer-tcgtracking",
        action="store_true",
        help="Overwrite market_price on top_25_cards when TCG price is found",
    )
    ap.add_argument("--sleep", type=float, default=0.25, help="Delay between HTTP calls (seconds)")
    ap.add_argument(
        "--backup",
        action="store_true",
        help="If output path equals input, copy input to .bak before write",
    )
    ap.add_argument(
        "--overrides",
        default="tcgtracking_set_overrides.json",
        help="Optional JSON object: {\"Set Name As In pokemon_sets_data\": tcg_numeric_set_id, ...}",
    )
    args = ap.parse_args()

    os.makedirs(args.cache, exist_ok=True)
    in_p = args.input
    out_p = args.output
    if os.path.abspath(in_p) == os.path.abspath(out_p) and args.backup:
        bak = in_p + ".bak"
        shutil.copy2(in_p, bak)
        print("Wrote backup", bak)

    run(in_p, out_p, args.cache, args.max_sets, args.prefer_tcgtracking, args.sleep, args.overrides)


if __name__ == "__main__":
    main()
