#!/usr/bin/env python3
"""
Rebuild ``predictor_analytics_assets`` from live ``pokemon_cards`` + ``pokemon_sets``.

These are **proxies** derived from chart prices and Pokémon Wizard short-trend fields
already stored on card rows — **not** literal Google Trends time series. Use the same
``asset_key`` names as the static JSON sidecars so the Explorer / Predictor / Analytics
frontends keep working.

Species keys match ``SHARED_UTILS.deriveExplorerSpeciesKeyFromCardName`` (first token
after stripping common Pokémon-card suffixes), lowercased for ``regression.js``.

Env: ``SUPABASE_URL`` and ``SUPABASE_SERVICE_ROLE_KEY`` or ``SUPABASE_KEY``.

  python github_actions/build_predictor_analytics_from_supabase.py
  python github_actions/build_predictor_analytics_from_supabase.py --dry-run
"""

from __future__ import annotations

import argparse
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Mapping, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "github_actions"))

from supabase_wizard_dataset_bridge import (  # noqa: E402
    _card_row_to_toplist_shape,
    _fetch_paginated,
    _supabase,
)

# Mirror predictor.js / shared.js variant strip, then first word.
_VARIANT_RE = re.compile(
    r"\s+(VMAX|VSTAR|V|ex|EX|GX|LV\.X|MEGA|BREAK|δ)\b.*$",
    re.IGNORECASE,
)

_CHASE_RARE_RE = re.compile(
    r"illustration|special\s+illustration|secret|ultra\s+rare|hyper\s+rare|"
    r"amazing\s+rare|rainbow|shiny\s+rare|gold|ace\s+spec|double\s+rare|"
    r"ir\b|sir\b|csr\b|sar\b|hr\b|ur\b",
    re.IGNORECASE,
)


def species_key_from_card_name(name: Optional[str]) -> str:
    if not name or not isinstance(name, str):
        return ""
    s = _VARIANT_RE.sub("", name).strip()
    if not s:
        return ""
    return s.split()[0].lower()


def base_display_token(name: Optional[str]) -> str:
    if not name or not isinstance(name, str):
        return ""
    s = _VARIANT_RE.sub("", name).strip()
    if not s:
        return ""
    return s.split()[0]


def _num(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def is_chase_slot(rarity: Optional[str]) -> bool:
    if not rarity or not isinstance(rarity, str):
        return False
    return bool(_CHASE_RARE_RE.search(rarity))


def _release_year(rd: Any) -> Optional[int]:
    if rd is None:
        return None
    s = str(rd).strip()
    if len(s) < 4:
        return None
    try:
        y = int(s[:4])
    except ValueError:
        return None
    return y if 1970 <= y <= 2100 else None


def _minmax_scale(m: Mapping[str, float]) -> Dict[str, float]:
    if not m:
        return {}
    xs = list(m.values())
    lo, hi = min(xs), max(xs)
    if hi <= lo:
        return {k: 50.0 for k in m}
    return {k: (v - lo) / (hi - lo) * 99.0 + 1.0 for k, v in m.items()}


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build predictor analytics asset payloads from Supabase card/set rows"
    )
    ap.add_argument("--dry-run", action="store_true", help="Print sizes only; no upsert")
    args = ap.parse_args()

    client = _supabase()

    set_rows = _fetch_paginated(
        client, "pokemon_sets", "set_code,release_date", order="set_code"
    )
    release_by_code: Dict[str, Any] = {}
    for s in set_rows:
        sc = str(s.get("set_code") or "").strip().lower()
        if sc:
            release_by_code[sc] = s.get("release_date")

    raw_cards = _fetch_paginated(
        client,
        "pokemon_cards",
        "set_code,name,artist,rarity,market_price,metrics",
        order="unique_card_id",
    )
    cards = [_card_row_to_toplist_shape(r) for r in raw_cards]

    # --- Per-species aggregates ---
    label_votes: Dict[str, Counter] = defaultdict(Counter)
    ht_counts: Dict[str, int] = defaultdict(int)
    price_lists: Dict[str, List[float]] = defaultdict(list)
    trend_lists: Dict[str, List[float]] = defaultdict(list)

    # --- Macro: year -> list of card prices ---
    year_prices: Dict[int, List[float]] = defaultdict(list)

    # --- Artist chase ---
    artist_chase_prices: Dict[str, List[float]] = defaultdict(list)
    artist_display: Dict[str, str] = {}

    used = 0
    for c in cards:
        price = _num(c.get("market_price"))
        if price is None or price <= 0:
            continue
        sc = str(c.get("set_code") or "").strip().lower()
        y = _release_year(release_by_code.get(sc))
        if y is not None:
            year_prices[y].append(price)

        sk = species_key_from_card_name(c.get("name"))
        if not sk or len(sk) < 2:
            continue

        tok = base_display_token(c.get("name"))
        if tok:
            label_votes[sk][tok] += 1

        rarity = c.get("rarity")
        chase = is_chase_slot(rarity) or price >= 35.0
        if chase:
            ht_counts[sk] += 1

        price_lists[sk].append(price)
        tr = _num(c.get("pokemon_wizard_current_trend_pct"))
        if tr is not None:
            trend_lists[sk].append(tr)

        if chase:
            art = c.get("artist")
            if art and isinstance(art, str):
                t = art.strip()
                if t and not re.match(r"^unknown\b", t, re.I):
                    key = t.lower()
                    if key not in artist_display:
                        artist_display[key] = t
                    artist_chase_prices[key].append(price)
        used += 1

    if used < 30:
        print(f"Too few priced cards ({used}); refusing to overwrite assets.", file=sys.stderr)
        return 1

    # Character + trends species scores
    trend_raw: Dict[str, float] = {}
    for sk, plist in price_lists.items():
        med_p = median(plist) if plist else 0.0
        tlist = trend_lists.get(sk) or []
        mean_abs = sum(abs(x) for x in tlist) / len(tlist) if tlist else 0.0
        trend_raw[sk] = math.log1p(med_p) * (1.0 + mean_abs * 0.02)

    trend_scaled = _minmax_scale(trend_raw)

    character_payload: List[Dict[str, Any]] = []
    trends_payload: List[Dict[str, Any]] = []

    for sk in sorted(price_lists.keys()):
        char_label, _votes = label_votes[sk].most_common(1)[0] if label_votes[sk] else (sk.title(), 0)
        vol = float(ht_counts.get(sk, 0))
        tr_score = trend_scaled.get(sk, 1.0)
        tlist = trend_lists.get(sk) or []
        mean_abs = sum(abs(x) for x in tlist) / len(tlist) if tlist else 0.0

        character_payload.append(
            {
                "Character": char_label,
                "High_Tier_Print_Volume": vol,
                "Is_Human": False,
                "Trainer_Archetype": "N/A",
                "species": sk,
                "volume_score": vol,
            }
        )
        trends_payload.append(
            {
                "Character": char_label,
                "species": sk,
                "trends_score": tr_score,
                "Popularity_Index": tr_score,
                "Trend_Index_Average": min(100.0, max(1.0, mean_abs * 3.0 + 1.0)),
            }
        )

    # Artist rows (analytics + predictor shapes)
    artist_payload: List[Dict[str, Any]] = []
    for key, plist in sorted(
        artist_chase_prices.items(), key=lambda kv: (-len(kv[1]), kv[0])
    ):
        if len(plist) < 2:
            continue
        med_a = median(plist)
        if med_a <= 0:
            continue
        disp = artist_display.get(key, key)
        artist_payload.append(
            {
                "Artist": disp,
                "Median_Market_Price": med_a,
                "Total_Chase_Cards": len(plist),
                "artist": disp,
                "chase_median": med_a,
            }
        )

    # Macro doc: median price by English set release year, scaled 1–100
    year_medians: Dict[str, float] = {}
    for y, plist in year_prices.items():
        if len(plist) < 3:
            continue
        year_medians[str(y)] = float(median(plist))

    by_year_scaled = _minmax_scale({k: v for k, v in year_medians.items()})

    tcg_doc = {
        "series_label": "Median chart price by set release year (Supabase cards)",
        "label": "Proxy macro index from synced Explorer card prices (not Google Trends)",
        "query": "internal:median_market_price_by_release_year",
        "by_year": {k: float(v) for k, v in by_year_scaled.items()},
    }

    now = datetime.now(timezone.utc).isoformat()
    rows = [
        {"asset_key": "character_premium_scores", "payload": character_payload, "updated_at": now},
        {"asset_key": "google_trends_momentum", "payload": trends_payload, "updated_at": now},
        {"asset_key": "artist_scores", "payload": artist_payload, "updated_at": now},
        {"asset_key": "tcg_macro_interest_by_year", "payload": tcg_doc, "updated_at": now},
    ]

    print(
        f"Built from {used} priced cards / {len(price_lists)} species / "
        f"{len(artist_payload)} artists / {len(by_year_scaled)} macro years.",
        flush=True,
    )

    if args.dry_run:
        print("Dry-run: no upsert.", flush=True)
        return 0

    client.table("predictor_analytics_assets").upsert(rows, on_conflict="asset_key").execute()
    print("Upserted 4 rows into predictor_analytics_assets.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
