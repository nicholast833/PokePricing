#!/usr/bin/env python3
"""
Estimate English **per-booster-pack USD** for each set in pokemon_sets_data.json using:

1. **TCGTracking Open TCG API** (no key): ``/tcgapi/v1/3/sets/{id}`` + ``/pricing``, or local
   ``tcg_cache/{id}/products.json`` + ``pricing.json`` when present.
2. **TCGCSV** (no key, daily export): ``/tcgplayer/3/groups`` then ``/tcgplayer/3/{groupId}/products`` + ``prices``
   when no cache match — see https://tcgcsv.com/docs
3. **TCGGO** (RapidAPI / direct key, optional — same host as card history): list products with
   ``GET /episodes/{episodeId}/products`` (or ``GET /pokemon/products``), then
   ``GET /history-prices`` with ``tcgplayer_id`` or internal product ``id`` per
   https://www.tcggo.com/api-docs/v1/ — pairs with the TCGPlayer product id from (1)/(2).

Per-set **pack cost history** (``pack_cost_price_history`` + ``pack_cost_price_history_en``) mirrors
card ``tcggo_market_history`` / ``price_history_en.daily``: dated ``price_usd`` (TCGPlayer market),
``cm_low`` (USD-normalized), and derived ``high`` / ``low`` / ``mid`` when both sources exist for a day.

Selection priority (``--prefer auto`` when ``--tcggo-key`` or ``TCGPRO_API_KEY`` is set):
  - **TCGGO** episode primary SKUs: ``/history-prices`` on internal product id (``tcg_player_market`` as USD, else
    ``cm_low`` converted from EUR via ``TCGGO_CARDMARKET_EUR_TO_USD``). Metadata stores ``history_usd`` (up to
    ``TCGGO_HISTORY_USD_MAX_POINTS`` newest days) with ``tcg_player_market_usd`` / ``cm_low_usd`` only — no EUR
    amounts in exported breakdown fields. Cardmarket ``lowest`` on the episode list payload is not used as USD.
  - TCGCSV catalog is ignored when the chosen booster product name does not match ``set_name`` tokens.
  - Else single-pack / ETB / booster-box logic from Tracking + validated TCGCSV (unchanged).

Example:
  python github_actions/sync_pack_costs.py --all-sets --cache tcg_cache --sleep 0.15
  python github_actions/sync_pack_costs.py --all-sets --cache tcg_cache --tcggo-key "$env:TCGPRO_API_KEY"
  python github_actions/sync_pack_costs.py --only-set-codes sv7 --prefer tcggo
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import statistics
import sys
import unicodedata
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
from tcgtracking_merge import find_standard_booster_box_product, norm_set_key  # noqa: E402

from sync_tcgplayer_mpapi import find_tcg_cache_products_path, pick_booster_pack_product  # noqa: E402

from tcggo_api_fetcher import (  # noqa: E402
    extract_latest_cm_low,
    extract_latest_market_price,
    fetch_all_episodes,
    fetch_episode_products_all,
    fetch_tcggo_price_history_query,
    find_tcggo_product_row_for_tcgplayer_id,
    tcggo_product_internal_id,
)

TCGTRACK_BASE = "https://tcgtracking.com/tcgapi/v1/3"
TCGCSV_GROUPS = "https://tcgcsv.com/tcgplayer/3/groups"
HEAD = {"User-Agent": "Mozilla/5.0 PokemonTCG-Explorer/sync_pack_costs (hobbyist)"}

# Approximate EUR→USD for TCGGO Cardmarket ``cm_low`` when ``tcg_player_market`` is null (public constant, not a secret).
TCGGO_CARDMARKET_EUR_TO_USD = 1.09

# Most recent daily points to keep in pack_cost_breakdown.tcggo (avoids huge JSON in Supabase metadata).
TCGGO_HISTORY_USD_MAX_POINTS = 150


def _interchange_pack_history_from_history_usd(
    history_usd: List[Dict[str, Any]],
    *,
    sync_iso: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Turn TCGGO ``history_usd`` rows (``date``, ``tcg_player_market_usd``, ``cm_low_usd``) into:

    1. ``pack_cost_price_history`` — ascending by date, same spirit as ``tcggo_market_history`` on cards
       (``date``, ``price_usd``, ``cm_low``, optional ``high_usd`` / ``low_usd`` / ``mid_usd`` per day).
    2. ``pack_cost_price_history_en`` — same envelope as ``card.tcggo.price_history_en``:
       ``{ currency, daily: { 'YYYY-MM-DD': { tcg_player_market, cm_low, high, low, mid } }, sync_iso, source }``.
    """
    if not isinstance(history_usd, list) or not history_usd:
        return [], {}
    asc = list(reversed(history_usd))
    points: List[Dict[str, Any]] = []
    daily: Dict[str, Any] = {}
    for r in asc:
        if not isinstance(r, dict):
            continue
        d = str(r.get("date") or "").strip()[:10]
        if len(d) < 10:
            continue
        pt: Dict[str, Any] = {"date": d}
        vals: List[float] = []
        tpm = r.get("tcg_player_market_usd")
        if tpm is not None:
            try:
                v = float(tpm)
                if v > 0:
                    pt["price_usd"] = round(v, 4)
                    vals.append(v)
            except (TypeError, ValueError):
                pass
        cm = r.get("cm_low_usd")
        if cm is not None:
            try:
                v = float(cm)
                if v > 0:
                    pt["cm_low_usd"] = round(v, 4)
                    pt["cm_low"] = round(v, 4)
                    vals.append(v)
            except (TypeError, ValueError):
                pass
        for rk, pk in (
            ("tcg_player_high_usd", "high_usd"),
            ("tcg_player_low_usd", "low_usd"),
            ("tcg_player_mid_usd", "mid_usd"),
        ):
            v = r.get(rk)
            if v is None:
                continue
            try:
                fv = float(v)
                if fv > 0:
                    pt[pk] = round(fv, 4)
            except (TypeError, ValueError):
                pass
        if "high_usd" not in pt and len(vals) >= 2:
            pt["high_usd"] = round(max(vals), 4)
            pt["low_usd"] = round(min(vals), 4)
            pt["mid_usd"] = round(statistics.mean(vals), 4)
        elif "high_usd" not in pt and len(vals) == 1:
            v = vals[0]
            pt["high_usd"] = pt["low_usd"] = pt["mid_usd"] = round(v, 4)
        if len(pt) <= 1:
            continue
        points.append(pt)
        slot: Dict[str, Any] = {}
        if "price_usd" in pt:
            slot["tcg_player_market"] = pt["price_usd"]
        if "cm_low" in pt:
            slot["cm_low"] = pt["cm_low"]
        if "high_usd" in pt:
            slot["high"] = pt["high_usd"]
            slot["low"] = pt["low_usd"]
            slot["mid"] = pt["mid_usd"]
        if slot:
            daily[d] = slot
    if not points:
        return [], {}
    en: Dict[str, Any] = {
        "currency": "USD",
        "daily": daily,
        "sync_iso": sync_iso,
        "source": "tcggo_pack_history",
    }
    return points, en


def _collect_history_usd_lists_from_tcggo_block(tg: Any) -> List[List[Dict[str, Any]]]:
    out: List[List[Dict[str, Any]]] = []
    if not isinstance(tg, dict):
        return out
    sel = tg.get("selected")
    if isinstance(sel, dict):
        hu = sel.get("history_usd")
        if isinstance(hu, list) and hu:
            out.append(hu)
    leg = tg.get("legacy")
    if isinstance(leg, dict):
        hu = leg.get("history_usd")
        if isinstance(hu, list) and hu:
            out.append(hu)
        sel2 = leg.get("selected")
        if isinstance(sel2, dict):
            hu2 = sel2.get("history_usd")
            if isinstance(hu2, list) and hu2:
                out.append(hu2)
    po = tg.get("primary_only")
    if isinstance(po, dict):
        out.extend(_collect_history_usd_lists_from_tcggo_block(po))
    return out


def _best_pack_history_usd_from_breakdown(breakdown: Any) -> Optional[List[Dict[str, Any]]]:
    """Longest ``history_usd`` list found under ``pack_cost_breakdown.tcggo``."""
    bd = breakdown if isinstance(breakdown, dict) else {}
    tg = bd.get("tcggo")
    lists = _collect_history_usd_lists_from_tcggo_block(tg)
    if not lists:
        return None
    lists.sort(key=len, reverse=True)
    return lists[0]


def _tcggo_history_series_usd(hist: Dict[str, Any], *, cm_low_is_eur: bool) -> List[Dict[str, Any]]:
    """
    Full /history-prices ``data`` map as a list of USD-normalized rows (newest dates first).
    ``tcg_player_market`` is treated as USD. ``cm_low`` is multiplied by TCGGO_CARDMARKET_EUR_TO_USD when *cm_low_is_eur*.
    """
    data = hist.get("data") if isinstance(hist, dict) else None
    if not isinstance(data, dict) or not data:
        return []
    rate = float(TCGGO_CARDMARKET_EUR_TO_USD)
    keys = sorted(data.keys(), reverse=True)[:TCGGO_HISTORY_USD_MAX_POINTS]
    out: List[Dict[str, Any]] = []
    for dk in keys:
        row = data.get(dk)
        if not isinstance(row, dict):
            continue
        ent: Dict[str, Any] = {"date": str(dk)}
        tpm = row.get("tcg_player_market")
        if tpm is not None:
            try:
                v = float(tpm)
                if v > 0:
                    ent["tcg_player_market_usd"] = round(v, 4)
            except (TypeError, ValueError):
                pass
        cm = row.get("cm_low")
        if cm is not None:
            try:
                v = float(cm)
                if v > 0:
                    ent["cm_low_usd"] = round(v * rate, 4) if cm_low_is_eur else round(v, 4)
            except (TypeError, ValueError):
                pass
        for src, dst in (
            ("tcg_player_high", "tcg_player_high_usd"),
            ("tcg_player_low", "tcg_player_low_usd"),
            ("tcg_player_mid", "tcg_player_mid_usd"),
        ):
            v = row.get(src)
            if v is None:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            if fv > 0:
                ent[dst] = round(fv, 4)
        if len(ent) > 1:
            out.append(ent)
    return out

_PACKS_IN_NAME_RE = re.compile(r"(\d+)\s*(?:booster\s+packs?|packs?\s*&\s*booster)", re.I)

# norm_set_key turns "&" into "and"; do not require these as product-name tokens.
_SET_NAME_STOPWORDS = frozenset(
    {"and", "or", "the", "of", "in", "a", "an", "to", "for", "on", "set", "pokemon", "tcg"}
)


def _norm_str(s: Any) -> str:
    return unicodedata.normalize("NFC", str(s).strip()).casefold()


def _episode_index_from_rows(episodes: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for e in episodes:
        if not isinstance(e, dict):
            continue
        nm = e.get("name")
        eid = e.get("id")
        if nm and eid is not None:
            try:
                out[_norm_str(nm)] = int(eid)
            except (TypeError, ValueError):
                continue
    return out


def _resolve_tcggo_episode_id(
    set_row: Dict[str, Any],
    episodes_by_name: Dict[str, int],
) -> Optional[int]:
    set_name = str(set_row.get("set_name") or "")
    set_code = str(set_row.get("set_code") or "").strip().lower()
    norm_set = _norm_str(set_name)
    eid = episodes_by_name.get(norm_set)
    if eid:
        return int(eid)
    if "promo" in norm_set:
        if "wizards" in norm_set or set_code == "basep":
            v = episodes_by_name.get(_norm_str("Wizards Black Star Promos"))
            if v:
                return int(v)
        if "nintendo" in norm_set or set_code == "np":
            v = episodes_by_name.get(_norm_str("Nintendo Black Star Promos"))
            if v:
                return int(v)
        if "ex" in norm_set or set_code == "ex5":
            v = episodes_by_name.get(_norm_str("EX Promos"))
            if v:
                return int(v)
    return None


def _fetch_tcggo_pack_market_usd(
    api_key: str,
    *,
    pack_tcgplayer_pid: int,
    set_row: Dict[str, Any],
    episodes_by_name: Dict[str, int],
    episode_products_cache: Dict[int, List[Dict[str, Any]]],
    sleep_s: float,
    history_days: int,
) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    Resolve sealed booster-pack USD via TCGGO history: try ``tcgplayer_id`` query first, then
    episode product row → internal ``id`` → history (matches official product docs).
    """
    meta: Dict[str, Any] = {"tcgplayer_pack_product_id": int(pack_tcgplayer_pid)}
    # 1) Direct history by TCGPlayer product id (documented alternate key for /history-prices).
    try:
        hist = fetch_tcggo_price_history_query(
            api_key, days=history_days, tcgplayer_id=int(pack_tcgplayer_pid)
        )
        if isinstance(hist, dict):
            ser = _tcggo_history_series_usd(hist, cm_low_is_eur=True)
            if ser:
                meta["history_usd"] = ser
        v = extract_latest_market_price(hist) if hist else None
        if v is not None:
            meta["path"] = "history_prices.tcgplayer_id"
            meta["values_are_usd"] = True
            return float(v), meta
    except Exception as ex:
        meta["tcgplayer_id_error"] = str(ex)[:240]

    # 2) Episode catalog → match tcgplayer id → internal id → history
    ep_id = _resolve_tcggo_episode_id(set_row, episodes_by_name)
    if not ep_id:
        meta["path"] = "episode_unresolved"
        return None, meta
    meta["episode_id"] = int(ep_id)
    if ep_id not in episode_products_cache:
        episode_products_cache[ep_id] = fetch_episode_products_all(
            int(ep_id), api_key, sleep_s=max(0.0, sleep_s)
        )
        time.sleep(max(0.0, sleep_s))
    rows = episode_products_cache.get(ep_id) or []
    meta["episode_product_rows"] = len(rows)
    row = find_tcggo_product_row_for_tcgplayer_id(rows, int(pack_tcgplayer_pid))
    if not row:
        meta["path"] = "episode_products.no_row"
        return None, meta
    internal = tcggo_product_internal_id(row)
    if not internal:
        meta["path"] = "episode_products.no_internal_id"
        return None, meta
    meta["tcggo_product_id"] = int(internal)
    try:
        hist2 = fetch_tcggo_price_history_query(api_key, days=history_days, tcggo_id=int(internal))
        if isinstance(hist2, dict):
            ser2 = _tcggo_history_series_usd(hist2, cm_low_is_eur=True)
            if ser2:
                meta["history_usd"] = ser2
        v2 = extract_latest_market_price(hist2) if hist2 else None
        if v2 is not None:
            meta["path"] = "history_prices.tcggo_internal_id"
            meta["values_are_usd"] = True
            return float(v2), meta
    except Exception as ex:
        meta["internal_id_error"] = str(ex)[:240]
    meta["path"] = "history_failed"
    return None, meta


def _f(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _packs_per_box(set_row: Dict[str, Any]) -> int:
    raw = set_row.get("packs_per_box")
    try:
        n = int(raw) if raw is not None else 36
    except (TypeError, ValueError):
        n = 36
    return n if n > 0 else 36


def _infer_packs_in_sealed(name: str) -> Optional[int]:
    m = _PACKS_IN_NAME_RE.search(name or "")
    if m:
        return int(m.group(1))
    low = (name or "").lower()
    if "elite trainer box" in low:
        if re.search(r"\b8\b", low) and "pack" in low:
            return 8
        if re.search(r"\b10\b", low) and "pack" in low:
            return 10
        return 9
    if "super premium collection" in low or "upc" in low:
        m2 = re.search(r"(\d+)\s*booster", low)
        if m2:
            return int(m2.group(1))
    return None


def _product_name_matches_set(set_row: Dict[str, Any], product_name: str) -> bool:
    """Episode / TCGCSV product name should mention the set (prevents Base + Scarlet & Violet mixups)."""
    sn = norm_set_key(str(set_row.get("set_name") or "")).replace("pokemon tcg", "").strip()
    pn = norm_set_key(product_name)
    if not sn:
        return True
    parts = [t for t in sn.split() if len(t) >= 2 and t not in _SET_NAME_STOPWORDS]
    if not parts:
        return sn in pn if len(sn) >= 2 else True
    return all(p in pn for p in parts)


def _tcgcsv_products_align_with_set(set_row: Dict[str, Any], products: List[Dict[str, Any]]) -> bool:
    pack = pick_booster_pack_product(products, str(set_row.get("set_name") or ""))
    if not pack:
        return False
    return _product_name_matches_set(set_row, str(pack.get("name") or ""))


def _sanitize_pack_cost_breakdown(set_row: Dict[str, Any], breakdown: Any) -> Dict[str, Any]:
    """Drop single/box/ETB blocks whose product names do not match this set (stale TCGCSV or bad tcg_cache)."""
    if not isinstance(breakdown, dict):
        return {"packs_per_box": _packs_per_box(set_row)}
    bd = dict(breakdown)
    for k in ("single_booster_pack", "booster_box", "elite_trainer_box"):
        blk = bd.get(k)
        if isinstance(blk, dict) and blk.get("name"):
            if not _product_name_matches_set(set_row, str(blk.get("name") or "")):
                bd.pop(k, None)
    return bd


def _primary_from_breakdown_after_sanitize(
    set_row: Dict[str, Any], breakdown: Dict[str, Any]
) -> Tuple[Optional[float], str]:
    """Re-derive primary from whatever tier blocks survived sanitization (non-TCGGO paths)."""
    sp = breakdown.get("single_booster_pack") if isinstance(breakdown.get("single_booster_pack"), dict) else {}
    single_usd = _f(sp.get("market_usd")) if sp else None
    box = breakdown.get("booster_box") if isinstance(breakdown.get("booster_box"), dict) else {}
    box_implied = _f(box.get("implied_pack_usd")) if box else None
    etb = breakdown.get("elite_trainer_box") if isinstance(breakdown.get("elite_trainer_box"), dict) else {}
    etb_implied = _f(etb.get("implied_pack_usd")) if etb else None
    if single_usd is not None:
        return single_usd, "single_booster_pack"
    if etb_implied is not None:
        return etb_implied, "etb_implied"
    if box_implied is not None:
        return box_implied, "booster_box_implied"
    return None, "none"


def _row_cardmarket_currency(row: Dict[str, Any]) -> str:
    prices = row.get("prices") if isinstance(row.get("prices"), dict) else {}
    cm = prices.get("cardmarket") if isinstance(prices.get("cardmarket"), dict) else {}
    c = str(cm.get("currency") or "EUR").strip().upper()
    return c or "EUR"


def _tcggo_is_excluded_product_name(name: str) -> bool:
    low = (name or "").lower()
    return any(
        k in low
        for k in (
            " case",
            "pokemon center",
            "checklane",
            "blister",
            "bundle",
            "fun pack",
            "collection",
            "tin",
            "build & battle",
        )
    )


def _tcggo_history_usd_for_internal_id(
    api_key: str,
    internal_id: int,
    *,
    history_days: int,
    sleep_s: float,
    currency_hint: str,
) -> Tuple[Optional[float], Dict[str, Any]]:
    """USD from /history-prices: prefer tcg_player_market; else cm_low (EUR → USD via TCGGO_CARDMARKET_EUR_TO_USD)."""
    meta: Dict[str, Any] = {"tcggo_product_id": int(internal_id)}
    try:
        hist = fetch_tcggo_price_history_query(api_key, days=history_days, tcggo_id=int(internal_id))
        time.sleep(max(0.0, sleep_s))
    except Exception as ex:
        meta["history_error"] = str(ex)[:220]
        return None, meta
    if not isinstance(hist, dict):
        return None, meta
    cm_low_is_eur = (currency_hint or "EUR").strip().upper() == "EUR"
    series = _tcggo_history_series_usd(hist, cm_low_is_eur=cm_low_is_eur)
    if series:
        meta["history_usd"] = series
    usd = extract_latest_market_price(hist)
    if usd is not None:
        meta["price_source"] = "tcg_player_market"
        meta["values_are_usd"] = True
        return float(usd), meta
    cm = extract_latest_cm_low(hist)
    if cm is None:
        meta["price_source"] = "none"
        return None, meta
    cur = (currency_hint or "EUR").strip().upper()
    meta["price_source"] = "cm_low"
    meta["values_are_usd"] = True
    if cur == "EUR":
        out = round(float(cm) * float(TCGGO_CARDMARKET_EUR_TO_USD), 4)
        meta["cm_low_usd"] = out
        return out, meta
    meta["note"] = "cm_low_treated_as_usd"
    meta["cm_low_usd"] = round(float(cm), 4)
    return float(cm), meta


def _tcggo_primary_pack_usd_from_episode(
    api_key: str,
    set_row: Dict[str, Any],
    rows: List[Dict[str, Any]],
    *,
    history_days: int,
    sleep_s: float,
) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    Primary sealed SKUs for the episode: resolve USD via /history-prices on TCGGO internal product ids.
    Ignores live Cardmarket ``lowest`` on the list payload (EUR) so we never store EUR as USD.
    """
    ppb = _packs_per_box(set_row)
    cands: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        name = str(r.get("name") or "")
        if not name:
            continue
        low = name.lower()
        if _tcggo_is_excluded_product_name(name):
            continue
        if not _product_name_matches_set(set_row, name):
            continue
        internal = tcggo_product_internal_id(r)
        if not internal:
            continue
        cur_hint = _row_cardmarket_currency(r)
        kind: Optional[str] = None
        if "sleeved booster" in low:
            kind = "sleeved_booster"
        elif "booster box" in low:
            kind = "booster_box"
        elif "elite trainer box" in low:
            kind = "elite_trainer_box"
        elif "booster" in low:
            kind = "single_booster_pack"
        if not kind:
            continue
        prev = cands.get(kind)
        cand = {"name": name, "tcggo_product_id": int(internal), "currency_hint": cur_hint}
        if prev is None or len(name) < len(str(prev.get("name") or "")):
            cands[kind] = cand

    cands_public = {
        k: {"name": str(v.get("name") or ""), "tcggo_product_id": int(v["tcggo_product_id"])}
        for k, v in cands.items()
        if isinstance(v, dict)
    }

    priority = ("single_booster_pack", "sleeved_booster", "elite_trainer_box", "booster_box")
    for k in priority:
        cand = cands.get(k)
        if not cand:
            continue
        tid = int(cand["tcggo_product_id"])
        usd, hmeta = _tcggo_history_usd_for_internal_id(
            api_key, tid, history_days=history_days, sleep_s=sleep_s, currency_hint=str(cand.get("currency_hint") or "EUR")
        )
        if usd is None:
            continue
        name = str(cand.get("name") or "")
        low = name.lower()
        implied = float(usd)
        if "booster box" in low:
            packs = _infer_packs_in_sealed(name) or ppb
            implied = round(usd / float(packs if packs > 0 else ppb), 4)
        elif "elite trainer box" in low:
            packs = _infer_packs_in_sealed(name) or 9
            implied = round(usd / float(packs if packs > 0 else 9), 4)
        sel = {
            "name": name,
            "tcggo_product_id": tid,
            "implied_pack_usd": implied,
            **hmeta,
        }
        br: Dict[str, Any] = {
            "path": "episode_products.primary_history",
            "candidates": cands_public,
            "selected_kind": k,
            "selected": sel,
        }
        return implied, br
    return None, {"path": "episode_products.no_primary_match", "candidates": cands_public}


def _http_json(url: str, *, timeout: int = 90) -> Any:
    req = urllib.request.Request(url, headers=HEAD)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def first_tcg_market_usd(price_entry: Any) -> Optional[float]:
    if not isinstance(price_entry, dict):
        return None
    tcg = price_entry.get("tcg")
    if not isinstance(tcg, dict):
        return None
    for prefer in ("Normal", "Holofoil", "Reverse Holofoil"):
        row = tcg.get(prefer)
        if isinstance(row, dict):
            m = _f(row.get("market"))
            if m is not None:
                return m
    for row in tcg.values():
        if isinstance(row, dict):
            m = _f(row.get("market"))
            if m is not None:
                return m
    return None


def _load_local_cache_bundle(cache_dir: Path, set_name: str) -> Optional[Tuple[int, List[Dict[str, Any]], Dict[str, Any]]]:
    if not cache_dir.is_dir():
        return None
    pj = find_tcg_cache_products_path(cache_dir, set_name)
    if not pj or not pj.is_file():
        return None
    try:
        sid = int(pj.parent.name)
    except ValueError:
        return None
    try:
        pdata = json.loads(pj.read_text(encoding="utf-8"))
        products = pdata.get("products") or []
    except (json.JSONDecodeError, OSError):
        products = []
    prices: Dict[str, Any] = {}
    prj = pj.parent / "pricing.json"
    if prj.is_file():
        try:
            prices = json.loads(prj.read_text(encoding="utf-8")).get("prices") or {}
        except (json.JSONDecodeError, OSError):
            prices = {}
    return sid, products, prices


def _fetch_live_tracking(sid: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    products = _http_json(f"{TCGTRACK_BASE}/sets/{sid}", timeout=120)
    plist = products.get("products") if isinstance(products, dict) else []
    if not isinstance(plist, list):
        plist = []
    try:
        pr = _http_json(f"{TCGTRACK_BASE}/sets/{sid}/pricing", timeout=120)
        prices = pr.get("prices") if isinstance(pr, dict) else {}
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError):
        prices = {}
    if not isinstance(prices, dict):
        prices = {}
    return plist, prices


def _pick_etb_product(products: List[Dict[str, Any]], set_display_name: str) -> Optional[Dict[str, Any]]:
    short = norm_set_key(set_display_name).replace("pokemon", "").strip()
    cands: List[Dict[str, Any]] = []
    for p in products:
        if not isinstance(p, dict):
            continue
        name = str(p.get("name") or "")
        low = name.lower()
        if "elite trainer box" not in low:
            continue
        if "case" in low:
            continue
        cands.append(p)
    if not cands:
        return None
    cands.sort(key=lambda x: len(str(x.get("name") or "")))
    for p in cands:
        if short and short in norm_set_key(str(p.get("name") or "")):
            return p
    return cands[0]


def _clear_pack_cost_fields(set_row: Dict[str, Any]) -> None:
    for k in (
        "pack_cost_primary_usd",
        "pack_cost_method",
        "pack_cost_sync_iso",
        "pack_cost_breakdown",
        "pack_cost_price_history",
        "pack_cost_price_history_en",
    ):
        set_row.pop(k, None)


def _tcgcsv_match_group_id(set_row: Dict[str, Any], groups: List[Dict[str, Any]]) -> Optional[int]:
    want_name = norm_set_key(str(set_row.get("set_name") or ""))
    want_abbr = str(set_row.get("set_code") or "").strip().upper()
    best: Tuple[int, int, str] = (-1, 0, "")  # score, groupId, label
    for g in groups:
        if not isinstance(g, dict):
            continue
        gid = g.get("groupId")
        try:
            gid_i = int(gid)
        except (TypeError, ValueError):
            continue
        nm = norm_set_key(str(g.get("name") or ""))
        abbr = str(g.get("abbreviation") or "").strip().upper()
        score = 0
        if want_abbr and abbr == want_abbr:
            score += 120
        if want_name and nm:
            if nm == want_name:
                score += 200
            elif want_name in nm:
                # Require a reasonably strong containment signal (prefix/word-ish boundary),
                # otherwise short set names can over-match unrelated groups.
                if nm.startswith(want_name) or f" {want_name} " in f" {nm} ":
                    score += 90
            elif len(nm) >= 6 and nm in want_name:
                # Only allow reverse containment for non-trivial group names.
                score += 40
        if score > best[0]:
            best = (score, gid_i, str(g.get("name") or ""))
    return best[1] if best[0] >= 90 else None


def _tcgcsv_products_prices(group_id: int) -> Tuple[List[Dict[str, Any]], Dict[int, float]]:
    proot = f"https://tcgcsv.com/tcgplayer/3/{group_id}"
    try:
        pj = _http_json(f"{proot}/products", timeout=120)
        pr = _http_json(f"{proot}/prices", timeout=120)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError):
        return [], {}
    products = pj.get("results") if isinstance(pj, dict) else []
    if not isinstance(products, list):
        products = []
    rows = pr.get("results") if isinstance(pr, dict) else []
    market_by_pid: Dict[int, float] = {}
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            pid = row.get("productId")
            m = _f(row.get("marketPrice"))
            if m is None:
                continue
            try:
                pid_i = int(pid)
            except (TypeError, ValueError):
                continue
            prev = market_by_pid.get(pid_i)
            if prev is None or m > prev:
                market_by_pid[pid_i] = m
    return products, market_by_pid


def _price_for_product(
    pid: int,
    prices: Dict[str, Any],
    tcgcsv_markets: Optional[Dict[int, float]],
) -> Optional[float]:
    pe = prices.get(str(pid)) or prices.get(pid)
    m = first_tcg_market_usd(pe)
    if m is not None:
        return m
    if tcgcsv_markets and pid in tcgcsv_markets:
        return tcgcsv_markets[pid]
    return None


def compute_pack_costs(
    set_row: Dict[str, Any],
    products: List[Dict[str, Any]],
    prices: Dict[str, Any],
    *,
    prefer: str,
    tcgcsv_markets: Optional[Dict[int, float]] = None,
) -> Tuple[Optional[float], str, Dict[str, Any]]:
    set_name = str(set_row.get("set_name") or "")
    ppb = _packs_per_box(set_row)
    breakdown: Dict[str, Any] = {"packs_per_box": ppb}

    pack_p = pick_booster_pack_product(products, set_name)
    single_usd: Optional[float] = None
    if pack_p and pack_p.get("id"):
        try:
            pid = int(pack_p["id"])
        except (TypeError, ValueError):
            pid = 0
        if pid:
            single_usd = _price_for_product(pid, prices, tcgcsv_markets)
            breakdown["single_booster_pack"] = {
                "product_id": pid,
                "name": pack_p.get("name"),
                "market_usd": single_usd,
            }

    box_p = find_standard_booster_box_product(products)
    box_usd: Optional[float] = None
    box_implied: Optional[float] = None
    if box_p and box_p.get("id"):
        try:
            bid = int(box_p["id"])
        except (TypeError, ValueError):
            bid = 0
        if bid:
            box_usd = _price_for_product(bid, prices, tcgcsv_markets)
            if box_usd is not None:
                box_implied = round(box_usd / ppb, 4)
            breakdown["booster_box"] = {
                "product_id": bid,
                "name": box_p.get("name"),
                "market_usd": box_usd,
                "implied_pack_usd": box_implied,
            }

    etb_p = _pick_etb_product(products, set_name)
    etb_usd: Optional[float] = None
    etb_implied: Optional[float] = None
    etb_packs: Optional[int] = None
    if etb_p and etb_p.get("id"):
        try:
            eid = int(etb_p["id"])
        except (TypeError, ValueError):
            eid = 0
        if eid:
            etb_usd = _price_for_product(eid, prices, tcgcsv_markets)
            etb_packs = _infer_packs_in_sealed(str(etb_p.get("name") or ""))
            if etb_usd is not None and etb_packs and etb_packs > 0:
                etb_implied = round(etb_usd / etb_packs, 4)
            breakdown["elite_trainer_box"] = {
                "product_id": eid,
                "name": etb_p.get("name"),
                "market_usd": etb_usd,
                "inferred_packs": etb_packs,
                "implied_pack_usd": etb_implied,
            }

    if prefer == "single_pack" and single_usd is not None:
        return single_usd, "single_booster_pack", breakdown
    if prefer == "box_implied" and box_implied is not None:
        return box_implied, "booster_box_implied", breakdown
    if prefer == "etb_implied" and etb_implied is not None:
        return etb_implied, "etb_implied", breakdown

    # auto
    if single_usd is not None:
        return single_usd, "single_booster_pack", breakdown
    if etb_implied is not None:
        return etb_implied, "etb_implied", breakdown
    if box_implied is not None:
        return box_implied, "booster_box_implied", breakdown
    return None, "none", breakdown


def run_one_set(
    set_row: Dict[str, Any],
    *,
    cache_dir: Path,
    prefer: str,
    sleep_s: float,
    groups_store: Dict[str, Any],
    tcggo_state: Dict[str, Any],
    rep: Dict[str, Any],
) -> None:
    set_name = str(set_row.get("set_name") or "")
    sc = str(set_row.get("set_code") or "").strip().lower()
    _clear_pack_cost_fields(set_row)
    api_key = str(tcggo_state.get("api_key") or "").strip()

    bundle = _load_local_cache_bundle(cache_dir, set_name)
    tcgcsv_markets: Optional[Dict[int, float]] = None
    products: List[Dict[str, Any]] = []
    prices: Dict[str, Any] = {}

    if bundle:
        sid, products, prices = bundle
        if not prices and sid:
            try:
                _, prices = _fetch_live_tracking(sid)
                rep["live_pricing_fetches"] = rep.get("live_pricing_fetches", 0) + 1
                time.sleep(max(0.0, sleep_s))
            except Exception as e:
                rep["pricing_fetch_errors"] = rep.get("pricing_fetch_errors", 0) + 1
                print(f"  [{sc}] WARN live pricing {sid}: {e!r}", flush=True)
        if products and not _tcgcsv_products_align_with_set(set_row, products):
            print(
                f"  [{sc}] WARN tcg_cache catalog booster does not match set {set_name!r}; ignoring cache",
                flush=True,
            )
            products, prices = [], {}
            bundle = None  # type: ignore[assignment]
    if not bundle:
        rep["sets_no_cache"] = rep.get("sets_no_cache", 0) + 1
        if groups_store.get("groups") is None:
            try:
                print("  [tcgcsv] loading groups catalog ...", flush=True)
                raw = _http_json(TCGCSV_GROUPS, timeout=180)
                groups_store["groups"] = list(raw.get("results") or [])
                time.sleep(0.12)
            except Exception as e:
                print(f"  [{sc}] SKIP no tcg_cache match and TCGCSV groups failed: {e!r}", flush=True)
                rep["sets_skipped"] += 1
                return
        gid = _tcgcsv_match_group_id(set_row, groups_store.get("groups") or [])
        if not gid:
            print(f"  [{sc}] SKIP no tcg_cache and no TCGCSV group match for {set_name!r}", flush=True)
            rep["sets_skipped"] += 1
            return
        try:
            products, tcgcsv_markets = _tcgcsv_products_prices(gid)
            norm_prods: List[Dict[str, Any]] = []
            for p in products:
                if not isinstance(p, dict):
                    continue
                q = dict(p)
                if "id" not in q and q.get("productId") is not None:
                    try:
                        q["id"] = int(q["productId"])
                    except (TypeError, ValueError):
                        pass
                norm_prods.append(q)
            products = norm_prods
            time.sleep(0.12)
            rep["tcgcsv_group_hits"] = rep.get("tcgcsv_group_hits", 0) + 1
            print(f"  [{sc}] TCGCSV groupId={gid} products={len(products)}", flush=True)
            if not _tcgcsv_products_align_with_set(set_row, products):
                print(
                    f"  [{sc}] WARN TCGCSV booster name does not match set {set_name!r}; ignoring TCGCSV catalog",
                    flush=True,
                )
                products, tcgcsv_markets, prices = [], None, {}
        except Exception as e:
            print(f"  [{sc}] SKIP TCGCSV fetch failed: {e!r}", flush=True)
            rep["sets_skipped"] += 1
            return

    if not products:
        if not api_key:
            print(f"  [{sc}] SKIP empty product list", flush=True)
            rep["sets_skipped"] += 1
            return
        primary, method, breakdown = None, "none", {"packs_per_box": _packs_per_box(set_row)}
    else:
        track_prefer = "auto" if prefer == "tcggo" else prefer
        primary, method, breakdown = compute_pack_costs(
            set_row, products, prices, prefer=track_prefer, tcgcsv_markets=tcgcsv_markets
        )

    tcggo_usd: Optional[float] = None
    tcggo_br: Dict[str, Any] = {}
    if api_key:
        ep_id = _resolve_tcggo_episode_id(set_row, tcggo_state.get("episodes_by_name") or {})
        if ep_id is not None:
            ecache = tcggo_state.setdefault("episode_products", {})
            if ep_id not in ecache:
                ecache[ep_id] = fetch_episode_products_all(int(ep_id), api_key, sleep_s=max(0.0, sleep_s))
                time.sleep(max(0.0, sleep_s))
            tcggo_usd, tcggo_br = _tcggo_primary_pack_usd_from_episode(
                api_key,
                set_row,
                ecache.get(ep_id) or [],
                history_days=int(tcggo_state.get("history_days") or 180),
                sleep_s=sleep_s,
            )
            tcggo_br["episode_id"] = int(ep_id)
        else:
            tcggo_br = {"path": "episode_unresolved"}

        # Fallback to legacy history-by-product-id path only when primary-only scan fails.
        if tcggo_usd is None:
            sp = breakdown.get("single_booster_pack") if isinstance(breakdown.get("single_booster_pack"), dict) else {}
            tid = sp.get("product_id")
            if tid and _product_name_matches_set(set_row, str(sp.get("name") or "")):
                try:
                    tcggo_usd, tcggo_br_legacy = _fetch_tcggo_pack_market_usd(
                        api_key,
                        pack_tcgplayer_pid=int(tid),
                        set_row=set_row,
                        episodes_by_name=tcggo_state.get("episodes_by_name") or {},
                        episode_products_cache=tcggo_state.setdefault("episode_products", {}),
                        sleep_s=sleep_s,
                        history_days=int(tcggo_state.get("history_days") or 180),
                    )
                    tcggo_br = {"path": "legacy_history_fallback", "primary_only": tcggo_br, "legacy": tcggo_br_legacy}
                except Exception as ex:
                    tcggo_br = {
                        "path": "legacy_history_fallback.error",
                        "primary_only": tcggo_br,
                        "error": str(ex)[:240],
                    }
                    rep["tcggo_errors"] = rep.get("tcggo_errors", 0) + 1

        if tcggo_usd is not None:
            rep["tcggo_ok"] = rep.get("tcggo_ok", 0) + 1
        else:
            rep["tcggo_miss"] = rep.get("tcggo_miss", 0) + 1
    breakdown["tcggo"] = tcggo_br

    def _tcggo_method_label() -> str:
        if not isinstance(tcggo_br, dict):
            return "tcggo_pack_history"
        if tcggo_br.get("path") == "episode_products.primary_history":
            return "tcggo_episode_primary"
        return "tcggo_pack_history"

    mlabel = _tcggo_method_label()
    if prefer == "tcggo":
        if tcggo_usd is not None:
            primary, method = float(tcggo_usd), mlabel
        else:
            primary, method = None, "none"
    elif prefer == "auto" and api_key and tcggo_usd is not None:
        primary, method = float(tcggo_usd), mlabel

    if method == "tcggo_episode_primary" and isinstance(tcggo_br, dict):
        ppb = breakdown.get("packs_per_box") if isinstance(breakdown.get("packs_per_box"), int) else _packs_per_box(
            set_row
        )
        sel = tcggo_br.get("selected") if isinstance(tcggo_br.get("selected"), dict) else {}
        kind = str(tcggo_br.get("selected_kind") or "")
        clean: Dict[str, Any] = {"packs_per_box": ppb, "tcggo": tcggo_br}
        if sel and primary is not None:
            if kind in ("single_booster_pack", "sleeved_booster"):
                clean["single_booster_pack"] = {
                    "name": sel.get("name"),
                    "market_usd": round(float(primary), 4),
                    "tcggo_product_id": sel.get("tcggo_product_id"),
                }
            elif kind == "booster_box":
                clean["booster_box"] = {
                    "name": sel.get("name"),
                    "implied_pack_usd": round(float(primary), 4),
                    "tcggo_product_id": sel.get("tcggo_product_id"),
                    "price_source": sel.get("price_source"),
                }
            elif kind == "elite_trainer_box":
                clean["elite_trainer_box"] = {
                    "name": sel.get("name"),
                    "implied_pack_usd": round(float(primary), 4),
                    "tcggo_product_id": sel.get("tcggo_product_id"),
                    "price_source": sel.get("price_source"),
                }
        breakdown = clean

    breakdown = _sanitize_pack_cost_breakdown(set_row, breakdown)
    if method not in ("tcggo_episode_primary", "tcggo_pack_history"):
        rp, rm = _primary_from_breakdown_after_sanitize(set_row, breakdown)
        primary, method = rp, rm

    iso = datetime.now(timezone.utc).isoformat()
    set_row["pack_cost_sync_iso"] = iso
    set_row["pack_cost_breakdown"] = breakdown
    set_row["pack_cost_method"] = method
    hist_usd = _best_pack_history_usd_from_breakdown(breakdown)
    if hist_usd:
        pts, en = _interchange_pack_history_from_history_usd(hist_usd, sync_iso=iso)
        if pts:
            set_row["pack_cost_price_history"] = pts
            set_row["pack_cost_price_history_en"] = en
        else:
            set_row["pack_cost_price_history"] = []
            set_row["pack_cost_price_history_en"] = {}
    else:
        set_row["pack_cost_price_history"] = []
        set_row["pack_cost_price_history_en"] = {}
    if primary is not None:
        set_row["pack_cost_primary_usd"] = round(float(primary), 4)
        set_row["tcgplayer_pack_price"] = round(float(primary), 2)
        rep["sets_updated"] += 1
        print(
            f"  [{sc}] OK method={method} pack_usd={set_row['tcgplayer_pack_price']!r} ({set_name[:40]!r})",
            flush=True,
        )
    else:
        set_row["pack_cost_primary_usd"] = None
        rep["sets_no_price"] += 1
        print(f"  [{sc}] WARN no pack price derived", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Sync per-pack USD estimates (TCGTracking + TCGCSV)")
    ap.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--output", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--cache", type=Path, default=ROOT / "tcg_cache")
    ap.add_argument("--only-set-codes", default="", help="Comma-separated set_code")
    ap.add_argument("--all-sets", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.12, help="Delay after network calls")
    ap.add_argument(
        "--prefer",
        choices=("auto", "single_pack", "box_implied", "etb_implied", "tcggo"),
        default="auto",
        help="Pricing tier: auto prefers TCGGO pack history when --tcggo-key is set, else Tracking/TCGCSV",
    )
    ap.add_argument(
        "--tcggo-key",
        default="",
        help="RapidAPI key or tcggo_* key (else env TCGPRO_API_KEY / RAPIDAPI_KEY_TCGGO / RAPIDAPI_KEY)",
    )
    ap.add_argument(
        "--tcggo-history-days",
        type=int,
        default=180,
        help="Date window for GET /history-prices on the booster-pack product",
    )
    ap.add_argument("--backup", action="store_true")
    args = ap.parse_args()

    only = {x.strip().lower() for x in args.only_set_codes.split(",") if x.strip()}
    if args.all_sets and only:
        raise SystemExit("Pass either --all-sets or --only-set-codes, not both.")
    if not only and not args.all_sets:
        raise SystemExit("Pass --all-sets or --only-set-codes.")

    inp = args.input.resolve()
    out = args.output.resolve()
    if args.backup and inp == out and inp.is_file():
        bak = inp.with_suffix(inp.suffix + ".pack_costs_bak")
        shutil.copy2(inp, bak)
        print("Wrote backup ->", bak, flush=True)

    data = json.loads(inp.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("Expected pokemon_sets_data.json as a JSON array")

    rep: Dict[str, Any] = {
        "sync_iso": datetime.now(timezone.utc).isoformat(),
        "source": "tcgtracking_tcgcsv_tcggo_optional",
        "prefer": args.prefer,
        "only_set_codes": sorted(only) if only else ["__ALL_SETS__"],
        "sets_updated": 0,
        "sets_skipped": 0,
        "sets_no_price": 0,
        "sets_no_cache": 0,
        "tcgcsv_group_hits": 0,
        "live_pricing_fetches": 0,
        "pricing_fetch_errors": 0,
        "tcggo_ok": 0,
        "tcggo_miss": 0,
        "tcggo_errors": 0,
    }

    key = (
        str(args.tcggo_key or "").strip()
        or os.environ.get("TCGPRO_API_KEY", "").strip()
        or os.environ.get("RAPIDAPI_KEY_TCGGO", "").strip()
        or os.environ.get("RAPIDAPI_KEY", "").strip()
    )
    tcggo_state: Dict[str, Any] = {
        "api_key": key,
        "episodes_by_name": {},
        "episode_products": {},
        "history_days": max(7, int(args.tcggo_history_days or 180)),
    }
    if key:
        print("[tcggo] loading /episodes index ...", flush=True)
        try:
            ep_rows = fetch_all_episodes(key, sleep_s=max(0.0, float(args.sleep)))
            idx = _episode_index_from_rows(ep_rows)
            idx[_norm_str("Wizards Black Star Promos")] = idx.get(_norm_str("Wizards Black Star Promos"), 125)
            idx[_norm_str("Nintendo Black Star Promos")] = idx.get(_norm_str("Nintendo Black Star Promos"), 113)
            tcggo_state["episodes_by_name"] = idx
            print(f"[tcggo] episodes indexed: {len(idx)}", flush=True)
        except Exception as e:
            print(f"[tcggo] WARN episode index failed ({e!r}); TCGGO pack prices disabled.", flush=True)
            tcggo_state["api_key"] = ""

    groups_store: Dict[str, Any] = {"groups": None}
    cache_dir = args.cache.resolve()

    for set_row in data:
        if not isinstance(set_row, dict):
            continue
        sc = str(set_row.get("set_code") or "").strip().lower()
        if not args.all_sets and sc not in only:
            continue
        set_name = str(set_row.get("set_name") or sc)
        print(f"[pack-costs] set={sc!r} name={set_name[:56]!r}", flush=True)
        run_one_set(
            set_row,
            cache_dir=cache_dir,
            prefer=str(args.prefer),
            sleep_s=max(0.0, float(args.sleep)),
            groups_store=groups_store,
            tcggo_state=tcggo_state,
            rep=rep,
        )

    write_json_atomic(out, data)
    rep_path = dataset_sidecar_report_path(out, ".pack_costs_sync_report.json")
    rep_path.parent.mkdir(parents=True, exist_ok=True)
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(json.dumps(rep, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
