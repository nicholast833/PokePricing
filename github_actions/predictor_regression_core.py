"""
Mirror ``regression.js`` + predictor price path from ``shared.js`` / ``predictor.js``.

Used by ``precompute_predictor_from_supabase.py`` so the hosted engine matches the browser.
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from statistics import median
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

COMPOSITE_KEYS: List[str] = [
    "pullCost",
    "charVol",
    "trends",
    "rarityTier",
    "setAge",
    "tcgMacro",
    "gradedPop",
    "hypeScarcity",
    "hypePullRatio",
    "artistChase",
    "pcGradedUsedRatio",
    "pcChaseSlabPremium",
]

_VARIANT_RE = re.compile(
    r"\s+(VMAX|VSTAR|V|ex|EX|GX|LV\.X|MEGA|BREAK|δ)\b.*$",
    re.IGNORECASE,
)


def species_key_from_card_name(name: Any) -> str:
    if not name or not isinstance(name, str):
        return ""
    s = _VARIANT_RE.sub("", name).strip()
    if not s:
        return ""
    tok = (s.split() or [""])[0]
    return tok.lower() if len(tok) >= 2 else ""


def _num(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def price_dedup_for_median(values: List[float], rel_eps: float = 0.006) -> List[float]:
    ok = sorted(v for v in values if v is not None and math.isfinite(v) and v > 0)
    out: List[float] = []
    for v in ok:
        if not any(abs(u - v) <= rel_eps * max(u, v, 1) for u in out):
            out.append(v)
    return out


def median_array(arr: List[float]) -> Optional[float]:
    if not arr:
        return None
    return float(median(arr))


def wizard_history_positive_usd_median(card: Mapping[str, Any]) -> Optional[float]:
    hist = card.get("pokemon_wizard_price_history")
    if not isinstance(hist, list) or not hist:
        return None
    vals: List[float] = []
    for row in hist:
        if not isinstance(row, dict):
            continue
        n = _num(row.get("price_usd"))
        if n is not None and n > 0:
            vals.append(n)
    return median_array(vals) if vals else None


def pricecharting_cents_to_usd(cents: Any) -> Optional[float]:
    n = _num(cents)
    if n is None or n <= 0:
        return None
    return n / 100.0


def pricecharting_history_positive_usd_median(card: Mapping[str, Any]) -> Optional[float]:
    d = card.get("pricecharting_chart_data")
    if not isinstance(d, dict):
        return None
    used = d.get("used")
    if not isinstance(used, list):
        return None
    vals: List[float] = []
    for pt in used:
        if not isinstance(pt, (list, tuple)) or len(pt) < 2:
            continue
        u = pricecharting_cents_to_usd(pt[1])
        if u is not None:
            vals.append(u)
    return median_array(vals) if vals else None


def collect_deduped_positive_usd_prices(card: Mapping[str, Any]) -> List[float]:
    vals: List[float] = []

    def push(v: Any) -> None:
        n = _num(v)
        if n is not None and n > 0:
            vals.append(n)

    push(card.get("market_price"))
    push(card.get("pricedex_market_usd"))
    push(card.get("tcgtracking_market_usd"))
    push(card.get("tcgapi_market_usd"))
    push(card.get("pokemon_wizard_current_price_usd"))
    wh = wizard_history_positive_usd_median(card)
    if wh is not None:
        push(wh)
    push(card.get("pricecharting_used_price_usd"))
    push(card.get("pricecharting_graded_price_usd"))
    pch = pricecharting_history_positive_usd_median(card)
    if pch is not None:
        push(pch)
    return price_dedup_for_median(vals)


def resolve_explorer_chart_usd(card: Mapping[str, Any]) -> Optional[float]:
    dedup = collect_deduped_positive_usd_prices(card)
    if len(dedup) >= 2:
        return median_array(dedup)
    if len(dedup) == 1:
        return dedup[0]
    mp = _num(card.get("market_price"))
    return mp if mp is not None and mp > 0 else None


def get_card_graded_pop_total(card: Mapping[str, Any]) -> Optional[float]:
    if not card:
        return None
    g = card.get("gemrate")
    if isinstance(g, dict) and g.get("total") is not None:
        t = _num(g.get("total"))
        if t is not None and t >= 0:
            return t
    psa = _num(card.get("psa_graded_pop_total"))
    if psa is not None and psa >= 0:
        return psa
    return None


def pricecharting_chase_grade_usd(card: Mapping[str, Any]) -> Optional[float]:
    gp = card.get("pricecharting_grade_prices")
    if not isinstance(gp, dict):
        return None
    best: Optional[float] = None
    for label, raw in gp.items():
        lab = str(label or "").strip()
        if not lab:
            continue
        low = lab.lower()
        if re.search(r"\bblack\b", low):
            continue
        n = _num(raw)
        if n is None or n <= 0:
            continue
        ok = (
            re.match(r"^psa\s*10$", lab, re.I)
            or re.match(r"^bgs\s*10$", lab, re.I)
            or re.match(r"^cgc\s*10$", lab, re.I)
            or re.match(r"^tag\s*10$", lab, re.I)
            or re.match(r"^grade\s*9\.5$", lab, re.I)
            or re.match(r"^ace\s*10$", lab, re.I)
            or re.match(r"^sgc\s*10$", lab, re.I)
            or (re.search(r"cgc", lab, re.I) and re.search(r"pristine", lab, re.I))
        )
        if not ok:
            continue
        best = n if best is None else max(best, n)
    return best


def predictor_pc_anchor_usd(card: Mapping[str, Any]) -> Optional[float]:
    used = _num(card.get("pricecharting_used_price_usd"))
    pch = pricecharting_history_positive_usd_median(card)
    nm_parts: List[float] = []
    if used is not None and used > 0:
        nm_parts.append(used)
    if pch is not None and pch > 0:
        nm_parts.append(pch)
    pc_nm = (
        float(median(price_dedup_for_median(nm_parts)))
        if nm_parts
        else None
    )

    blend_hint = resolve_explorer_chart_usd(card)
    base = pc_nm
    if blend_hint is not None and blend_hint > 0:
        if base is None or base <= 0:
            base = blend_hint
        else:
            lo = min(base, blend_hint)
            hi = max(base, blend_hint)
            if hi / lo > 4:
                base = blend_hint
            else:
                base = float(median(price_dedup_for_median([base, blend_hint])))

    if base is None or base <= 0 or not math.isfinite(base):
        return None

    chase = pricecharting_chase_grade_usd(card)
    from_slabs = chase / 2.05 if chase is not None and chase > 0 else None
    if from_slabs is None or not math.isfinite(from_slabs) or from_slabs <= 0:
        return base

    max_slab_lift = base * 2.25
    if from_slabs <= max_slab_lift:
        return max(base, from_slabs)
    return base


def predictor_calibrate_usd(
    card: Mapping[str, Any], raw_model_usd: float
) -> Dict[str, Any]:
    anchor = predictor_pc_anchor_usd(card)
    if anchor is None or not math.isfinite(raw_model_usd) or raw_model_usd <= 0:
        return {"final": raw_model_usd, "raw": raw_model_usd, "blended": False}
    r = anchor / raw_model_usd
    if r < 1.55:
        return {"final": raw_model_usd, "raw": raw_model_usd, "blended": False, "anchor": anchor}
    t = min(0.94, math.log10(r) / 1.48)
    lf = math.log10(raw_model_usd) * (1 - t) + math.log10(anchor) * t
    return {
        "final": 10**lf,
        "raw": raw_model_usd,
        "blended": True,
        "anchor": anchor,
        "t": t,
    }


def weighted_mean_std(xs: List[float], ws: List[float]) -> Optional[Tuple[float, float]]:
    if not xs:
        return None
    sum_w = sum(ws)
    sum_wx = sum(ws[i] * xs[i] for i in range(len(xs)))
    if sum_w <= 0:
        return None
    mean = sum_wx / sum_w
    sum_var = sum(ws[i] * (xs[i] - mean) ** 2 for i in range(len(xs)))
    std = math.sqrt(sum_var / sum_w)
    return mean, std


def weighted_pearson_r(xs: List[float], ys: List[float], ws: List[float]) -> float:
    if len(xs) < 2:
        return 0.0
    ms_x = weighted_mean_std(xs, ws)
    ms_y = weighted_mean_std(ys, ws)
    if not ms_x or not ms_y or ms_x[1] == 0 or ms_y[1] == 0:
        return 0.0
    sum_cov = sum(ws[i] * (xs[i] - ms_x[0]) * (ys[i] - ms_y[0]) for i in range(len(xs)))
    sum_w = sum(ws)
    return sum_cov / (sum_w * ms_x[1] * ms_y[1])


def fit_weighted_linear_y_on_x(
    xs: List[float], ys: List[float], ws: List[float]
) -> Optional[Dict[str, float]]:
    if len(xs) < 2:
        return None
    ms_x = weighted_mean_std(xs, ws)
    ms_y = weighted_mean_std(ys, ws)
    if not ms_x or not ms_y:
        return None
    r = weighted_pearson_r(xs, ys, ws)
    if ms_x[1] == 0:
        return {"b0": ms_y[0], "b1": 0.0, "r": r, "r2": r * r}
    b1 = r * (ms_y[1] / ms_x[1])
    b0 = ms_y[0] - b1 * ms_x[0]
    return {"b0": b0, "b1": b1, "r": r, "r2": r * r}


def composite_score_from_row(
    features: Mapping[str, Any], model: Mapping[str, Any]
) -> Optional[float]:
    keys = model.get("keys")
    if not keys:
        return None
    means: Mapping[str, Any] = model.get("means") or {}
    stds: Mapping[str, Any] = model.get("stds") or {}
    r_by: Mapping[str, Any] = model.get("r") or {}
    sum_z = 0.0
    sum_w = 0.0
    for k in keys:
        kk = str(k)
        v = features.get(kk)
        if v is None:
            continue
        fv = float(v)
        if not math.isfinite(fv):
            continue
        mean = means.get(kk)
        std = stds.get(kk)
        rr = r_by.get(kk)
        if mean is None or std is None or rr is None:
            continue
        fm = float(mean)
        fs = float(std)
        fr = float(rr)
        if fs == 0:
            continue
        z = (fv - fm) / fs
        sum_z += z * fr
        sum_w += abs(fr)
    if sum_w == 0:
        return None
    return sum_z / sum_w


def extract_features(
    card: MutableMapping[str, Any],
    set_row: Mapping[str, Any],
    analytics_state: Mapping[str, Any],
) -> Dict[str, float]:
    feat: Dict[str, float] = {}

    rate: Any = card.get("card_pull_rate")
    if isinstance(rate, str):
        m = re.search(r"1 in ([\d,.]+)", rate)
        if m:
            try:
                n = float(m.group(1).replace(",", ""))
                rate = 1.0 / n if n > 0 else None
            except ValueError:
                rate = None
        else:
            try:
                rate = float(rate)
            except ValueError:
                rate = None
    if _num(rate) is not None and float(rate) > 0:
        feat["pullCost"] = -math.log(float(rate))

    species = str(card.get("species") or species_key_from_card_name(card.get("name")) or "")
    char_data = analytics_state.get("characterData") or []
    char_hit = next(
        (c for c in char_data if isinstance(c, dict) and c.get("species") == species),
        None,
    )
    feat["charVol"] = float(char_hit["volume_score"]) if char_hit else 0.0

    trends = analytics_state.get("trendsData") or []
    t_hit = next(
        (t for t in trends if isinstance(t, dict) and t.get("species") == species),
        None,
    )
    if t_hit and t_hit.get("trends_score") is not None:
        feat["trends"] = float(t_hit["trends_score"])
    else:
        feat["trends"] = 0.0

    ro = card.get("rarity_ordinal")
    feat["rarityTier"] = float(ro) if _num(ro) is not None else 0.0

    rd = set_row.get("release_date")
    rel_utc: Optional[datetime] = None
    if rd:
        s = str(rd).strip()[:10]
        try:
            rel_utc = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        except ValueError:
            rel_utc = None
    if rel_utc:
        now = datetime.now(timezone.utc)
        years = (now - rel_utc).total_seconds() / (365.25 * 24 * 3600)
        feat["setAge"] = math.sqrt(max(0.0, years))

    macro_raw = analytics_state.get("tcgMacroInterest") or {}
    by_year = (
        macro_raw.get("by_year")
        if isinstance(macro_raw.get("by_year"), dict)
        else macro_raw
    )
    if rel_utc and isinstance(by_year, dict):
        yr = rel_utc.year
        v = by_year.get(yr, by_year.get(str(yr)))
        feat["tcgMacro"] = float(v) if v is not None and _num(v) is not None else 0.0

    pop_total = get_card_graded_pop_total(card)
    if pop_total is not None:
        feat["gradedPop"] = math.log10(1.0 + float(pop_total))

    if "trends" in feat and "pullCost" in feat:
        feat["hypeScarcity"] = feat["trends"] * feat["pullCost"]
        feat["hypePullRatio"] = feat["trends"] / (feat["pullCost"] + 1.0)

    artists = analytics_state.get("artistChaseLookup") or {}
    art = card.get("artist")
    if art and isinstance(artists, dict):
        feat["artistChase"] = float(artists.get(str(art), 0.0))
    else:
        feat["artistChase"] = 0.0

    pc_used = _num(card.get("pricecharting_used_price_usd"))
    pc_graded_agg = _num(card.get("pricecharting_graded_price_usd"))
    if (
        pc_used is not None
        and pc_used > 0
        and pc_graded_agg is not None
        and pc_graded_agg > 0
    ):
        feat["pcGradedUsedRatio"] = math.log10(max(1.001, pc_graded_agg / pc_used))
    chase_gem = pricecharting_chase_grade_usd(card)
    if chase_gem is not None and pc_used is not None and pc_used > 0:
        feat["pcChaseSlabPremium"] = math.log10(max(1.001, chase_gem / pc_used))

    return feat


def build_global_model(
    rows: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Optional[Dict[str, float]]]:
    keys = COMPOSITE_KEYS
    means: Dict[str, float] = {}
    stds: Dict[str, float] = {}
    r_by: Dict[str, float] = {}

    for k in keys:
        xs: List[float] = []
        ys: List[float] = []
        ws: List[float] = []
        for r in rows:
            feat = r.get("feat") or {}
            if feat.get(k) is None:
                continue
            fv = float(feat[k])
            pr = r.get("price")
            if not math.isfinite(fv) or pr is None or float(pr) <= 0:
                continue
            xs.append(fv)
            ys.append(math.log10(float(pr)))
            ws.append(1.0)
        if len(xs) < 5:
            continue
        ms = weighted_mean_std(xs, ws)
        if not ms:
            continue
        means[k] = ms[0]
        stds[k] = ms[1]
        r_by[k] = weighted_pearson_r(xs, ys, ws)

    final_keys = [k for k in keys if k in means and abs(r_by.get(k, 0.0)) > 0.1]
    global_model: Dict[str, Any] = {
        "keys": final_keys,
        "means": {k: means[k] for k in final_keys},
        "stds": {k: stds[k] for k in final_keys},
        "r": {k: r_by[k] for k in final_keys},
    }

    xs2: List[float] = []
    ys2: List[float] = []
    ws2: List[float] = []
    for r in rows:
        feat = r.get("feat") or {}
        pr = r.get("price")
        if pr is None or float(pr) <= 0:
            continue
        cx = composite_score_from_row(feat, global_model)
        if cx is None:
            continue
        xs2.append(cx)
        ys2.append(math.log10(float(pr)))
        ws2.append(1.0)

    reg = fit_weighted_linear_y_on_x(xs2, ys2, ws2)
    return global_model, reg


def build_analytics_state_from_asset_payloads(
    characters: Any,
    trends: Any,
    artists: Any,
    tcg_macro: Any,
) -> Dict[str, Any]:
    character_data = characters if isinstance(characters, list) else []
    trends_data = trends if isinstance(trends, list) else []
    artist_list = artists if isinstance(artists, list) else []
    tcg_doc: Dict[str, Any] = (
        dict(tcg_macro) if isinstance(tcg_macro, dict) and not isinstance(tcg_macro, list) else {}
    )

    artist_chase_lookup: Dict[str, float] = {}
    for a in artist_list:
        if not isinstance(a, dict):
            continue
        name = str(a.get("artist") or a.get("Artist") or "").strip()
        med = a.get("chase_median")
        if med is None:
            med = a.get("Median_Market_Price")
        fn = _num(med)
        if name and fn is not None and fn > 0:
            artist_chase_lookup[name] = float(fn)

    return {
        "characterData": character_data,
        "trendsData": trends_data,
        "artistChaseLookup": artist_chase_lookup,
        "tcgMacroInterest": tcg_doc,
    }
