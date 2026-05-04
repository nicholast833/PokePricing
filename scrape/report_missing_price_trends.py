#!/usr/bin/env python3
"""
Scan pokemon_sets_data.json top_25_cards for missing price-trend chart inputs.

A card is "missing trend charts" when BOTH:
  - Collectrics: fewer than 1 usable history row (Explorer shows chart when >= 1)
  - Pokémon Wizard: fewer than 1 row after the same header-row filter as app.js

Also reports whether the *set* has any card with pokemon_wizard_url (proxy for
Wizard coverage in this export) and counts cards with a Wizard URL.

Usage:
  python scrape/report_missing_price_trends.py
  python scrape/report_missing_price_trends.py --top 25 --json-out missing_trends_report.json
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple


def filter_wizard_rows(ph: Any) -> List[dict]:
    if not isinstance(ph, list):
        return []
    out: List[dict] = []
    for row in ph:
        if not isinstance(row, dict):
            continue
        l = str(row.get("label") or "").strip().lower()
        if l in ("date", "price", "trend", "when", "label", "sort_key"):
            continue
        sk = str(row.get("sort_key") or "").strip().lower()
        if sk in ("date", "price", "trend"):
            continue
        out.append(row)
    return out


def has_trend_charts(card: dict) -> Tuple[bool, int, int]:
    ce = card.get("collectrics_price_history") or []
    ce_n = len(ce) if isinstance(ce, list) else 0
    wiz = filter_wizard_rows(card.get("pokemon_wizard_price_history"))
    wiz_n = len(wiz)
    ok = ce_n >= 1 or wiz_n >= 1
    return ok, ce_n, wiz_n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=Path("pokemon_sets_data.json"))
    ap.add_argument("--top", type=int, default=25, help="Max cards per set list (top_N)")
    ap.add_argument("--json-out", type=Path, default=None)
    args = ap.parse_args()

    data = json.loads(args.input.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        print("Expected array of sets", file=__import__("sys").stderr)
        return 1

    per_set: List[dict] = []
    all_missing: List[dict] = []

    for s in data:
        if not isinstance(s, dict):
            continue
        code = str(s.get("set_code") or "").strip()
        name = str(s.get("set_name") or "").strip()
        cards = s.get("top_25_cards") or s.get(f"top_{args.top}_cards") or []
        if not isinstance(cards, list):
            cards = []
        n = len(cards)
        missing = 0
        set_has_any_wiz_url = False
        cards_with_wiz_url = 0
        for c in cards:
            if not isinstance(c, dict):
                continue
            if c.get("pokemon_wizard_url"):
                set_has_any_wiz_url = True
                cards_with_wiz_url += 1
            ok, ce_n, wiz_n = has_trend_charts(c)
            if not ok:
                missing += 1
                all_missing.append(
                    {
                        "set_code": code,
                        "set_name": name,
                        "card_name": c.get("name"),
                        "number": c.get("number"),
                        "collectrics_rows": ce_n,
                        "wizard_rows_after_filter": wiz_n,
                        "pokemon_wizard_url": bool(c.get("pokemon_wizard_url")),
                    }
                )
        if n > 0:
            per_set.append(
                {
                    "set_code": code,
                    "set_name": name,
                    "cards_in_list": n,
                    "missing_trend_charts": missing,
                    "missing_frac": round(missing / n, 4),
                    "set_has_any_pokemon_wizard_url": set_has_any_wiz_url,
                    "cards_with_pokemon_wizard_url": cards_with_wiz_url,
                }
            )

    per_set.sort(key=lambda r: (-r["missing_trend_charts"], -r["missing_frac"], r["set_code"]))

    print("=== Sets with the most top-list cards lacking Collectrics AND Wizard chart rows ===\n")
    for r in per_set[:40]:
        if r["missing_trend_charts"] == 0:
            continue
        wiz_flag = "set_wizard_urls=yes" if r["set_has_any_pokemon_wizard_url"] else "set_wizard_urls=no"
        print(
            f"{r['missing_trend_charts']:3d} / {r['cards_in_list']:3d} missing  |  {r['set_code']!r:12}  |  {wiz_flag}  |  {r['set_name'][:60]}"
        )

    print(f"\nTotal cards missing both trend sources (in scanned lists): {len(all_missing)}")

    # XY Blastoise-EX style check
    print("\n=== Sample: XY base Blastoise-EX (xy1) ===")
    for s in data:
        if str(s.get("set_code") or "").strip().lower() != "xy1":
            continue
        for c in s.get("top_25_cards") or []:
            if not isinstance(c, dict):
                continue
            if re.search(r"blastoise", str(c.get("name") or ""), re.I):
                raw_w = c.get("pokemon_wizard_price_history") or []
                ok, ce_n, wn = has_trend_charts(c)
                print(
                    json.dumps(
                        {
                            "name": c.get("name"),
                            "number": c.get("number"),
                            "pokemon_wizard_url": c.get("pokemon_wizard_url"),
                            "raw_wizard_history_len": len(raw_w) if isinstance(raw_w, list) else 0,
                            "filtered_wizard_len": wn,
                            "collectrics_len": ce_n,
                            "charts_ok": ok,
                        },
                        indent=2,
                    )
                )
        break

    if args.json_out:
        out = {"sets_ranked": per_set, "missing_cards": all_missing}
        args.json_out.write_text(json.dumps(out, indent=2), encoding="utf-8")
        print(f"\nWrote {args.json_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
