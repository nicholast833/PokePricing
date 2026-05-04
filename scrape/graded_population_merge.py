"""
Merge optional graded (PSA-style) population counts into pokemon_sets_data.json.

Prefer **`pokemetrics_pop_merge.py`** for PSA-style totals from **api.pokemetrics.org**
(PokéMetrics static JSON). Use this file only for **manual overrides** or non-PokéMetrics
sources. The official PSA Public API (bearer token) only supports **per-certificate** lookups —
not bulk population-by-card. For hand-curated pops, use this **sidecar file**:

  graded_population.json   (array of rows; see schema below)

Populate that file from PSA website exports, a paid aggregator, Apify actors,
or manual research. Then run:

  python scrape/graded_population_merge.py
  python scrape/graded_population_merge.py --log-first-cards 100   # per-card lines for first 100 top-list rows

Environment (optional, for one-off cert checks — not used by this merge):
  PSA_BEARER_TOKEN   see psa_publicapi.py
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from typing import Any, Dict, List, Optional, Tuple

from tcgtracking_merge import norm_card_name, norm_card_number


def _row_pop_fields(row: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[str], str]:
    total = row.get("psa_graded_pop_total")
    if total is None:
        total = row.get("psa_pop_total")
    gem = row.get("psa_graded_pop_gem10")
    if gem is None:
        gem = row.get("psa_gem10")
    as_of = row.get("psa_graded_pop_as_of") or row.get("as_of")
    src = row.get("psa_graded_pop_source") or row.get("source") or "graded_population.json"
    try:
        ti = int(total) if total is not None else None
    except (TypeError, ValueError):
        ti = None
    try:
        gi = int(gem) if gem is not None else None
    except (TypeError, ValueError):
        gi = None
    so = str(as_of).strip() if as_of else None
    return ti, gi, so, str(src)[:120]


def build_graded_index(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Key: set_code|num|name_norm -> first row wins."""
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        sc = row.get("set_code")
        if sc is None or str(sc).strip() == "":
            continue
        num = norm_card_number(row.get("number"))
        nm = norm_card_name(row.get("name"))
        key = f"{str(sc).strip().lower()}|{num}|{nm}"
        if key not in out:
            out[key] = row
    return out


def merge_graded_into_sets(
    sets: List[Dict[str, Any]],
    graded_rows: List[Dict[str, Any]],
    *,
    log_first_cards: int = 0,
    log_every: int = 2500,
) -> Tuple[int, int]:
    idx = build_graded_index(graded_rows)
    hits = 0
    cards = 0
    n_sets = len(sets)
    for si, s in enumerate(sets):
        sc = str(s.get("set_code") or "").strip().lower()
        if not sc:
            continue
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue
        set_name = str(s.get("set_name") or sc)[:60]
        for card in top:
            if not isinstance(card, dict):
                continue
            cards += 1
            num = norm_card_number(card.get("number"))
            nm = norm_card_name(card.get("name"))
            key = f"{sc}|{num}|{nm}"
            row = idx.get(key)
            matched = bool(row)
            if row:
                total, gem, as_of, src = _row_pop_fields(row)
                if total is not None:
                    card["psa_graded_pop_total"] = total
                if gem is not None:
                    card["psa_graded_pop_gem10"] = gem
                if as_of:
                    card["psa_graded_pop_as_of"] = as_of
                card["psa_graded_pop_source"] = src
                hits += 1

            if log_first_cards and cards <= log_first_cards:
                name_disp = str(card.get("name") or "")[:44]
                num_disp = str(card.get("number") or "")[:12]
                st = "MATCH" if matched else "no row"
                print(
                    f"[graded_pop] {cards}/{log_first_cards} {st}  "
                    f"set {si + 1}/{n_sets} {set_name!r} ({sc})  #{num_disp} {name_disp}",
                    flush=True,
                )
            elif log_every > 0 and cards > log_first_cards and cards % log_every == 0:
                print(
                    f"[graded_pop] ... scanned {cards} top-list cards, {hits} graded row matches so far",
                    flush=True,
                )
    return hits, cards


def run(
    input_path: str,
    graded_path: str,
    output_path: str,
    backup: bool,
    *,
    log_first_cards: int = 0,
    log_every: int = 2500,
) -> Dict[str, Any]:
    if not os.path.isfile(input_path):
        raise FileNotFoundError(input_path)
    print(f"[graded_pop] loading {input_path!r} ...", flush=True)
    with open(input_path, "r", encoding="utf-8") as f:
        sets = json.load(f)
    if not isinstance(sets, list):
        raise ValueError("pokemon_sets_data.json must be a JSON array of sets")
    print(f"[graded_pop] loaded {len(sets)} set(s)", flush=True)

    graded_rows: List[Dict[str, Any]] = []
    if os.path.isfile(graded_path):
        print(f"[graded_pop] loading sidecar {graded_path!r} ...", flush=True)
        with open(graded_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, list):
            graded_rows = raw
        elif isinstance(raw, dict) and isinstance(raw.get("cards"), list):
            graded_rows = raw["cards"]
    else:
        print(f"[graded_pop] no sidecar at {graded_path!r} (merge will match zero rows)", flush=True)

    print(f"[graded_pop] graded index rows: {len(graded_rows)}", flush=True)
    if log_first_cards:
        print(
            f"[graded_pop] per-card console lines for the first {log_first_cards} top-list cards; "
            f"then every {log_every} cards until done.",
            flush=True,
        )

    if backup and os.path.isfile(output_path):
        bak = output_path + ".bak"
        print(f"[graded_pop] backup → {bak!r}", flush=True)
        shutil.copy2(output_path, bak)

    print("[graded_pop] merging into top_25_cards ...", flush=True)
    hits, n_cards = merge_graded_into_sets(
        sets, graded_rows, log_first_cards=log_first_cards, log_every=log_every
    )
    print(f"[graded_pop] merge pass done: {n_cards} card(s) seen, {hits} sidecar match(es)", flush=True)

    print(f"[graded_pop] writing {output_path!r} ...", flush=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(sets, f, indent=4)
    print("[graded_pop] write complete.", flush=True)

    return {
        "output": output_path,
        "graded_rows": len(graded_rows),
        "cards_seen": n_cards,
        "cards_matched": hits,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge graded_population.json into pokemon_sets_data.json")
    ap.add_argument("--input", default="pokemon_sets_data.json")
    ap.add_argument("--graded", default="graded_population.json")
    ap.add_argument("--output", default="pokemon_sets_data.json")
    ap.add_argument("--no-backup", action="store_true")
    ap.add_argument(
        "--log-first-cards",
        type=int,
        default=0,
        metavar="N",
        help="Print one progress line per top-list card for the first N cards (0 = off).",
    )
    ap.add_argument(
        "--log-every",
        type=int,
        default=2500,
        metavar="M",
        help="After the first --log-first-cards cards, print a summary every M cards (0 = off).",
    )
    args = ap.parse_args()
    info = run(
        args.input,
        args.graded,
        args.output,
        backup=not args.no_backup,
        log_first_cards=max(0, args.log_first_cards),
        log_every=max(0, args.log_every),
    )
    print(json.dumps(info, indent=2), flush=True)


if __name__ == "__main__":
    main()
