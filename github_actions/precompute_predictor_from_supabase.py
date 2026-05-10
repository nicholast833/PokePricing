#!/usr/bin/env python3
"""
Fit the Predictor global composite LSRL from Supabase cards + ``predictor_analytics_assets``,
then upsert per-card precompute rows and a ``predictor_engine_snapshot`` asset.

Env: ``SUPABASE_URL`` and ``SUPABASE_SERVICE_ROLE_KEY`` or ``SUPABASE_KEY`` (service role recommended).

  python github_actions/precompute_predictor_from_supabase.py
  python github_actions/precompute_predictor_from_supabase.py --dry-run
"""

from __future__ import annotations

import argparse
import math
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "github_actions"))

from predictor_regression_core import (  # noqa: E402
    build_analytics_state_from_asset_payloads,
    build_global_model,
    composite_score_from_row,
    extract_features,
    predictor_calibrate_usd,
    resolve_explorer_chart_usd,
    species_key_from_card_name,
)
from supabase_wizard_dataset_bridge import (  # noqa: E402
    _card_row_to_toplist_shape,
    _fetch_paginated,
    _merge_set_metadata_for_export,
    _set_row_to_export_shape,
    _supabase,
)

ENGINE_V = 1
MAX_SCATTER = 4000
PRECOMPUTE_UPSERT_CHUNK = 400


def _merge_set_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return _merge_set_metadata_for_export(_set_row_to_export_shape(row))


def _flatten_card(raw: Dict[str, Any]) -> MutableMapping[str, Any]:
    c = _card_row_to_toplist_shape(raw)
    sk = species_key_from_card_name(c.get("name"))
    if sk:
        c["species"] = sk
    return c


def _load_predictor_assets(client: Any) -> Dict[str, Any]:
    res = client.table("predictor_analytics_assets").select("asset_key,payload").execute()
    rows = res.data or []
    out: Dict[str, Any] = {}
    for row in rows:
        if row and row.get("asset_key"):
            out[str(row["asset_key"])] = row.get("payload")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Precompute predictor engine + per-card payloads")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    client = _supabase()
    assets = _load_predictor_assets(client)
    need = (
        "character_premium_scores",
        "google_trends_momentum",
        "artist_scores",
        "tcg_macro_interest_by_year",
    )
    missing = [k for k in need if k not in assets]
    if missing:
        print(
            f"Missing predictor_analytics_assets keys: {missing}. Run build_predictor_analytics_from_supabase.py first.",
            file=sys.stderr,
        )
        return 1

    analytics_state = build_analytics_state_from_asset_payloads(
        assets["character_premium_scores"],
        assets["google_trends_momentum"],
        assets["artist_scores"],
        assets["tcg_macro_interest_by_year"],
    )

    print("Fetching pokemon_sets ...", flush=True)
    set_rows = _fetch_paginated(
        client,
        "pokemon_sets",
        "set_code,set_name,release_date,metadata",
        order="set_code",
    )
    set_by_code: Dict[str, Dict[str, Any]] = {}
    for s in set_rows:
        sc = str(s.get("set_code") or "").strip().lower()
        if sc:
            set_by_code[sc] = _merge_set_row(s)

    card_select = (
        "unique_card_id,set_code,name,number,rarity,market_price,artist,image_url,"
        "metrics,card_pull_rate,rarity_ordinal,psa_graded_pop_total,tracked_priority"
    )
    print("Fetching pokemon_cards ...", flush=True)
    raw_cards = _fetch_paginated(
        client,
        "pokemon_cards",
        card_select,
        order="unique_card_id",
    )

    training_rows: List[Dict[str, Any]] = []
    for raw in raw_cards:
        card = _flatten_card(raw)
        uid = str(card.get("unique_card_id") or "").strip()
        if not uid:
            continue
        sc = str(card.get("set_code") or "").strip().lower()
        set_row = set_by_code.get(sc) or {}
        price = resolve_explorer_chart_usd(card)
        if price is None or price <= 0:
            continue
        feat = extract_features(card, set_row, analytics_state)
        training_rows.append(
            {"unique_card_id": uid, "card": dict(card), "set": set_row, "feat": feat, "price": price}
        )

    if len(training_rows) < 30:
        print(
            f"Too few tracked training rows ({len(training_rows)}); need pokemon_cards.tracked_priority. "
            "Run: python github_actions/refresh_tcggo_tracked_top25.py --bootstrap-market-price",
            file=sys.stderr,
        )
        return 1

    global_model, global_regression = build_global_model(training_rows)
    if not global_regression:
        print("Global regression fit failed.", file=sys.stderr)
        return 1

    scatter_pool: List[Dict[str, float]] = []
    precompute_rows: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()

    for tr in training_rows:
        uid = tr["unique_card_id"]
        feat = tr["feat"]
        price = float(tr["price"])
        cx = composite_score_from_row(feat, global_model)
        log_chart = math.log10(price)
        raw_usd = 0.0
        if cx is not None and global_regression:
            log_p = global_regression["b0"] + global_regression["b1"] * cx
            raw_usd = 10**log_p
        cal = predictor_calibrate_usd(tr["card"], raw_usd)
        if cx is not None:
            scatter_pool.append({"x": float(cx), "y": log_chart})
        precompute_rows.append(
            {
                "unique_card_id": uid,
                "payload": {
                    "engine_v": ENGINE_V,
                    "composite_x": cx,
                    "log10_chart_usd": log_chart,
                    "predicted_raw_usd": cal.get("raw", raw_usd),
                    "predicted_final_usd": cal["final"],
                    "cal": {
                        "final": cal["final"],
                        "raw": cal.get("raw", raw_usd),
                        "blended": bool(cal.get("blended")),
                        **({"anchor": cal["anchor"]} if cal.get("anchor") is not None else {}),
                        **({"t": cal["t"]} if cal.get("t") is not None else {}),
                    },
                    "trained_at": now,
                },
                "updated_at": now,
            }
        )

    rng = random.Random(42)
    if len(scatter_pool) > MAX_SCATTER:
        scatter_training = rng.sample(scatter_pool, MAX_SCATTER)
    else:
        scatter_training = scatter_pool

    snapshot = {
        "v": ENGINE_V,
        "trained_at": now,
        "training_n": len(training_rows),
        "global_model": global_model,
        "global_regression": global_regression,
        "scatter_training": scatter_training,
    }

    print(
        f"Engine: keys={len(global_model.get('keys') or [])} n={len(training_rows)} "
        f"b0={global_regression['b0']:.4f} b1={global_regression['b1']:.4f}",
        flush=True,
    )
    print(f"Precompute rows: {len(precompute_rows)}", flush=True)

    if args.dry_run:
        print("Dry-run: no upsert.", flush=True)
        return 0

    client.table("predictor_analytics_assets").upsert(
        [
            {
                "asset_key": "predictor_engine_snapshot",
                "payload": snapshot,
                "updated_at": now,
            },
            {
                "asset_key": "analytics_tracked_default",
                "payload": {
                    "engine_v": ENGINE_V,
                    "tracked_priced_cards": len(training_rows),
                    "trained_at": now,
                    "aligned_with_predictor_snapshot": True,
                },
                "updated_at": now,
            },
        ],
        on_conflict="asset_key",
    ).execute()
    print("Upserted predictor_engine_snapshot + analytics_tracked_default meta.", flush=True)

    for i in range(0, len(precompute_rows), PRECOMPUTE_UPSERT_CHUNK):
        chunk = precompute_rows[i : i + PRECOMPUTE_UPSERT_CHUNK]
        client.table("predictor_card_precompute").upsert(
            chunk, on_conflict="unique_card_id"
        ).execute()
        print(f"Upserted precompute {i + len(chunk)}/{len(precompute_rows)}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
