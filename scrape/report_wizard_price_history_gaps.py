#!/usr/bin/env python3
"""
Report Pokémon (creature) cards in pokemon_sets_data.json top lists that lack usable
Pokémon Wizard *price history* rows, and whether the set is mapped to a Wizard set page.

Uses the same filtered row rules as app.js / report_missing_price_trends.py (drops
header-like label/sort_key rows).

A set listed in pokemon_wizard_set_paths.json can usually be synced via
scrape/sync_pokemon_wizard.py (set listing resolves product ids when TCG ids are missing).

When --summary-out is set, a trailing subsection is appended from pokemon_wizard_sync_skips.json
(written by sync_pokemon_wizard.py) so skipped cards can be found for manual follow-up.

Usage:
  python scrape/report_wizard_price_history_gaps.py
  python scrape/report_wizard_price_history_gaps.py --json-out tmp/wizard_hist_gaps.json
  python scrape/report_wizard_price_history_gaps.py --csv-out tmp/wizard_hist_gaps_sets.csv
  python scrape/report_wizard_price_history_gaps.py --supertype any --summary-out sets_missing_wizard_price_history.txt
  python scrape/report_wizard_price_history_gaps.py --supertype any --csv-out sets.csv --csv-only-gaps \\
      --set-codes-out sets_missing_codes.txt
  python scrape/report_wizard_price_history_gaps.py --supertype any --list-cards
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
WIZARD_SYNC_SKIPS_LOG = ROOT / "pokemon_wizard_sync_skips.json"


def wizard_sync_skips_appendix_lines(*, max_runs: int = 20) -> List[str]:
    """Tab lines for sets_missing_wizard_price_history.txt (latest sync runs first)."""
    lines: List[str] = [
        "",
        "# --- Wizard sync skips (from pokemon_wizard_sync_skips.json; newest run last in file, shown newest first below) ---",
    ]
    if not WIZARD_SYNC_SKIPS_LOG.is_file():
        lines.append("# (no skip log yet — run scrape/sync_pokemon_wizard.py; skips append after each run)")
        return lines
    try:
        raw = json.loads(WIZARD_SYNC_SKIPS_LOG.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        lines.append("# (skip log unreadable or invalid JSON)")
        return lines
    runs = raw.get("runs") if isinstance(raw, dict) else None
    if not isinstance(runs, list) or not runs:
        lines.append("# runs[] empty")
        return lines
    tail = runs[-max_runs:] if len(runs) > max_runs else runs
    lines.append(f"# skip_log_runs_stored={len(runs)}  runs_shown_below={len(tail)}")
    last = runs[-1] if runs else None
    if isinstance(last, dict):
        st = last.get("stats")
        if isinstance(st, dict):
            keys = (
                "cards_considered",
                "cards_merged",
                "cards_skipped_no_tcg_id",
                "cards_skipped_tcg_api",
                "cards_skipped_wizard_fetch",
                "max_cards",
                "only_missing_price_history",
            )
            bits = [f"{k}={st.get(k)}" for k in keys if k in st]
            if bits:
                lines.append("# latest_sync_run_stats: " + " ".join(bits))
        note = last.get("note")
        if not note and isinstance(st, dict):
            note = st.get("note")
        if note:
            lines.append("# latest_sync_run_note: " + str(note).replace("\t", " ")[:240])
    lines.append(
        "# sync_iso\tset_code\tcard_number\tcard_name\treason_code\tdetail"
    )
    for run in reversed(tail):
        if not isinstance(run, dict):
            continue
        iso = str(run.get("sync_iso") or "").replace("\t", " ")
        skips = run.get("skips")
        if not isinstance(skips, list):
            continue
        for s in skips:
            if not isinstance(s, dict):
                continue
            lines.append(
                "\t".join(
                    [
                        iso,
                        str(s.get("set_code") or ""),
                        str(s.get("card_number") or ""),
                        str(s.get("card_name") or "").replace("\t", " "),
                        str(s.get("reason_code") or ""),
                        str(s.get("detail") or "").replace("\t", " "),
                    ]
                )
            )
    return lines


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


def wizard_history_ok(card: dict) -> Tuple[bool, int]:
    wiz = filter_wizard_rows(card.get("pokemon_wizard_price_history"))
    n = len(wiz)
    return n >= 1, n


def load_wizard_paths(path: Path) -> Dict[str, str]:
    if not path.is_file():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in raw.items():
        ks = str(k).strip()
        if ks.startswith("_") or not isinstance(v, str):
            continue
        p = v.strip().strip("/")
        if ks.lower() and p:
            out[ks.lower()] = p
    return out


def card_matches_supertype(card: dict, mode: str) -> bool:
    if mode == "any":
        return True
    st = str(card.get("supertype") or "").strip().lower()
    if mode == "pokemon":
        return st in ("pokémon", "pokemon")
    if mode == "trainer":
        return st == "trainer"
    if mode == "energy":
        return st == "energy"
    return True


def main() -> int:
    ap = argparse.ArgumentParser(
        description="List sets/cards missing Pokémon Wizard price history vs Wizard path coverage."
    )
    ap.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument(
        "--wizard-paths",
        type=Path,
        default=ROOT / "pokemon_wizard_set_paths.json",
        help="Explorer set_code -> Wizard /sets/{id}/{slug} path",
    )
    ap.add_argument("--top", type=int, default=25, help="Key top_N_cards if present else top_25_cards")
    ap.add_argument(
        "--supertype",
        choices=("pokemon", "trainer", "energy", "any"),
        default="pokemon",
        help="Which cards to evaluate (default: Pokémon creatures only)",
    )
    ap.add_argument(
        "--sort",
        choices=("set_code", "missing_desc", "missing_then_code"),
        default="set_code",
        help="set_code: alphabetical; missing_desc: most Pokémon gaps first; missing_then_code: gaps then code",
    )
    ap.add_argument("--json-out", type=Path, default=None, help="Full structured report (sets + cards)")
    ap.add_argument("--csv-out", type=Path, default=None, help="One row per set summary")
    ap.add_argument(
        "--csv-only-gaps",
        action="store_true",
        help="With --csv-out: write only rows where missing_wizard_price_history > 0",
    )
    ap.add_argument(
        "--set-codes-out",
        type=Path,
        default=None,
        help="Write one set_code per line: sets with >=1 gap under the supertype filter, plus any set "
        "with no Wizard path mapping (even if that filter yields 0 gaps, e.g. energy-only tops)",
    )
    ap.add_argument(
        "--summary-out",
        type=Path,
        default=None,
        help="Write a tab-separated summary (# header lines): same inclusion rule as --set-codes-out",
    )
    ap.add_argument(
        "--list-cards",
        action="store_true",
        help="Print every gap card line (can be large)",
    )
    ap.add_argument(
        "--only-actionable",
        action="store_true",
        help="Terminal sections only for sets that have a Wizard path but still have gaps",
    )
    args = ap.parse_args()

    if not args.input.is_file():
        print(f"Missing input: {args.input}", file=sys.stderr)
        return 1

    data = json.loads(args.input.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        print("Expected array of sets", file=sys.stderr)
        return 1

    paths = load_wizard_paths(args.wizard_paths)
    key_top = f"top_{args.top}_cards"
    all_gap_cards: List[dict] = []
    set_rows: List[dict] = []

    for s in data:
        if not isinstance(s, dict):
            continue
        code = str(s.get("set_code") or "").strip()
        code_l = code.lower()
        name = str(s.get("set_name") or "").strip()
        cards = s.get(key_top) or s.get("top_25_cards") or []
        if not isinstance(cards, list):
            cards = []

        wiz_path = paths.get(code_l) or ""
        in_index = bool(wiz_path)

        n_pokemon = 0
        n_trainer = 0
        n_energy = 0
        n_other = 0
        gaps: List[dict] = []

        for c in cards:
            if not isinstance(c, dict):
                continue
            st = str(c.get("supertype") or "").strip().lower()
            if st in ("pokémon", "pokemon"):
                n_pokemon += 1
            elif st == "trainer":
                n_trainer += 1
            elif st == "energy":
                n_energy += 1
            else:
                n_other += 1

            if not card_matches_supertype(c, args.supertype):
                continue

            ok, wn = wizard_history_ok(c)
            if ok:
                continue

            has_url = bool(c.get("pokemon_wizard_url"))
            raw_hist = c.get("pokemon_wizard_price_history")
            raw_n = len(raw_hist) if isinstance(raw_hist, list) else 0
            row = {
                "set_code": code,
                "set_name": name,
                "supertype": c.get("supertype"),
                "card_name": c.get("name"),
                "number": c.get("number"),
                "pokemon_wizard_url": c.get("pokemon_wizard_url"),
                "has_wizard_url": has_url,
                "raw_wizard_history_len": raw_n,
                "filtered_wizard_rows": wn,
                "set_has_wizard_path": in_index,
                "pokemon_wizard_set_path": wiz_path or None,
            }
            gaps.append(row)
            all_gap_cards.append(row)

        considered = sum(1 for c in cards if isinstance(c, dict) and card_matches_supertype(c, args.supertype))
        set_rows.append(
            {
                "set_code": code,
                "set_name": name,
                "set_has_wizard_path": in_index,
                "pokemon_wizard_set_path": wiz_path or None,
                "cards_in_list": len([x for x in cards if isinstance(x, dict)]),
                "considered_supertype_filter": considered,
                "missing_wizard_price_history": len(gaps),
                "missing_with_wizard_path": sum(1 for g in gaps if g["set_has_wizard_path"]),
                "missing_with_wizard_url": sum(1 for g in gaps if g["has_wizard_url"]),
                "theme_counts": {
                    "Pokemon": n_pokemon,
                    "Trainer": n_trainer,
                    "Energy": n_energy,
                    "Other": n_other,
                },
                "gap_cards": gaps,
            }
        )

    if args.sort == "set_code":
        set_rows.sort(key=lambda r: r["set_code"].lower())
    elif args.sort == "missing_desc":
        set_rows.sort(
            key=lambda r: (-r["missing_wizard_price_history"], -r["missing_with_wizard_path"], r["set_code"].lower())
        )
    else:
        set_rows.sort(key=lambda r: (-r["missing_wizard_price_history"], r["set_code"].lower()))

    # --- Terminal output ---
    def print_set_block(title: str, rows: List[dict], *, require_path: Optional[bool] = None) -> None:
        sub = rows
        if require_path is True:
            sub = [r for r in rows if r["set_has_wizard_path"] and r["missing_wizard_price_history"]]
        elif require_path is False:
            sub = [r for r in rows if not r["set_has_wizard_path"] and r["missing_wizard_price_history"]]
        if not sub:
            return
        print(title, flush=True)
        for r in sub:
            if r["missing_wizard_price_history"] == 0:
                continue
            mc = r["theme_counts"]
            th = f"Pkmn={mc['Pokemon']} Tr={mc['Trainer']} En={mc['Energy']}"
            path_s = r["pokemon_wizard_set_path"] or "—"
            print(
                f"  {str(r['set_code']):<10}  gaps={r['missing_wizard_price_history']:3d}/{r['considered_supertype_filter']:3d}  "
                f"path={'Y' if r['set_has_wizard_path'] else 'N'}  {path_s:28}  {th}  |  {r['set_name'][:56]}",
                flush=True,
            )
            if args.list_cards:
                for g in r["gap_cards"]:
                    u = "url=Y" if g["has_wizard_url"] else "url=N"
                    print(
                        f"      · #{g.get('number')} {g.get('card_name')!r}  raw_hist={g['raw_wizard_history_len']}  {u}",
                        flush=True,
                    )
        print(flush=True)

    st_label = {"pokemon": "Pokémon", "trainer": "Trainer", "energy": "Energy", "any": "All supertypes"}[
        args.supertype
    ]
    print(
        f"=== Wizard price history gaps ({st_label} in top list; filtered row count < 1) ===\n"
        f"Input: {args.input}\n"
        f"Wizard paths file: {args.wizard_paths} ({len(paths)} mapped set_codes)\n"
        f"Sort: {args.sort}\n",
        flush=True,
    )

    total_sets_with_gaps = sum(1 for r in set_rows if r["missing_wizard_price_history"])
    actionable = [r for r in set_rows if r["set_has_wizard_path"] and r["missing_wizard_price_history"]]
    no_path = [r for r in set_rows if not r["set_has_wizard_path"] and r["missing_wizard_price_history"]]

    print(
        f"Sets with >=1 gap: {total_sets_with_gaps}  |  "
        f"Mapped on Wizard but gaps (re-sync / matcher): {len(actionable)}  |  "
        f"No Wizard path (add mapping / index): {len(no_path)}\n",
        flush=True,
    )

    if not args.only_actionable:
        print_set_block("--- Sets WITHOUT Wizard path (theme = top-list supertype counts) ---", set_rows, require_path=False)
    print_set_block("--- Sets WITH Wizard path — gaps likely fixable via sync_pokemon_wizard.py ---", set_rows, require_path=True)

    print(f"Total {args.supertype} cards missing Wizard price history: {len(all_gap_cards)}", flush=True)

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        out = {
            "meta": {
                "input": str(args.input),
                "wizard_paths": str(args.wizard_paths),
                "supertype_filter": args.supertype,
                "sort": args.sort,
                "mapped_set_codes": len(paths),
            },
            "sets": [{k: v for k, v in r.items() if k != "gap_cards"} for r in set_rows],
            "gap_cards": all_gap_cards,
        }
        args.json_out.write_text(json.dumps(out, indent=2), encoding="utf-8")
        print(f"\nWrote JSON: {args.json_out}", flush=True)

    if args.csv_out:
        args.csv_out.parent.mkdir(parents=True, exist_ok=True)
        csv_rows = set_rows
        if args.csv_only_gaps:
            csv_rows = [r for r in set_rows if r["missing_wizard_price_history"] > 0]
        fieldnames = [
            "set_code",
            "set_name",
            "set_has_wizard_path",
            "pokemon_wizard_set_path",
            "cards_in_list",
            "considered_supertype_filter",
            "missing_wizard_price_history",
            "missing_with_wizard_url",
            "count_Pokemon",
            "count_Trainer",
            "count_Energy",
            "count_Other",
        ]
        with args.csv_out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in csv_rows:
                mc = r["theme_counts"]
                w.writerow(
                    {
                        "set_code": r["set_code"],
                        "set_name": r["set_name"],
                        "set_has_wizard_path": r["set_has_wizard_path"],
                        "pokemon_wizard_set_path": r["pokemon_wizard_set_path"] or "",
                        "cards_in_list": r["cards_in_list"],
                        "considered_supertype_filter": r["considered_supertype_filter"],
                        "missing_wizard_price_history": r["missing_wizard_price_history"],
                        "missing_with_wizard_url": r["missing_with_wizard_url"],
                        "count_Pokemon": mc["Pokemon"],
                        "count_Trainer": mc["Trainer"],
                        "count_Energy": mc["Energy"],
                        "count_Other": mc["Other"],
                    }
                )
        print(f"Wrote CSV: {args.csv_out} ({len(csv_rows)} rows)", flush=True)

    def include_in_summary_or_codes(r: dict) -> bool:
        """List sets missing Wizard price history under the filter, and any set still unmapped to a Wizard set page."""
        return bool(r["missing_wizard_price_history"] > 0 or not r["set_has_wizard_path"])

    if args.set_codes_out:
        args.set_codes_out.parent.mkdir(parents=True, exist_ok=True)
        listed = [r for r in set_rows if include_in_summary_or_codes(r)]
        lines = [str(r["set_code"]).strip() for r in listed if str(r.get("set_code") or "").strip()]
        args.set_codes_out.write_text("\n".join(sorted(lines, key=str.lower)) + ("\n" if lines else ""), encoding="utf-8")
        print(f"Wrote set codes ({len(lines)}): {args.set_codes_out}", flush=True)

    if args.summary_out:
        args.summary_out.parent.mkdir(parents=True, exist_ok=True)
        gap_sets = [r for r in set_rows if include_in_summary_or_codes(r)]
        gap_sets.sort(key=lambda x: str(x.get("set_code") or "").lower())
        n_gap_only = sum(1 for r in gap_sets if r["missing_wizard_price_history"] > 0)
        n_unmapped = sum(1 for r in gap_sets if not r["set_has_wizard_path"])
        hdr = [
            f"# Pokémon Wizard price history gaps (usable filtered rows < 1 on top list)",
            f"# data_input={args.input}",
            f"# supertype_filter={args.supertype}  top_key=top_{args.top}_cards  summary_sets={len(gap_sets)} "
            f"(sets_with_gaps_under_filter={n_gap_only}  unmapped_set_codes={n_unmapped})",
            "# set_code\tmissing\tconsidered\thas_wizard_path\tpokemon_wizard_set_path\tset_name",
            "# card_lines: one tab indent, then number, name, supertype, has_wizard_url(Y|N), "
            "raw_wizard_history_len, filtered_usable_rows",
        ]
        body: List[str] = []

        def _card_sort_key(g: dict) -> Tuple[str, str]:
            return (str(g.get("number") or "").lower(), str(g.get("card_name") or "").lower())

        for r in gap_sets:
            body.append(
                "\t".join(
                    [
                        str(r.get("set_code") or ""),
                        str(r.get("missing_wizard_price_history", 0)),
                        str(r.get("considered_supertype_filter", 0)),
                        "Y" if r.get("set_has_wizard_path") else "N",
                        str(r.get("pokemon_wizard_set_path") or ""),
                        str(r.get("set_name") or "").replace("\t", " "),
                    ]
                )
            )
            for g in sorted(r.get("gap_cards") or [], key=_card_sort_key):
                nm = str(g.get("card_name") or "").replace("\t", " ")
                st = str(g.get("supertype") or "").replace("\t", " ")
                num = str(g.get("number") or "").replace("\t", " ")
                u = "Y" if g.get("has_wizard_url") else "N"
                raw_n = int(g.get("raw_wizard_history_len") or 0)
                fn = int(g.get("filtered_wizard_rows") or 0)
                body.append("\t" + "\t".join([num, nm, st, u, str(raw_n), str(fn)]))
        body.extend(wizard_sync_skips_appendix_lines())
        args.summary_out.write_text("\n".join(hdr + body) + "\n", encoding="utf-8")
        print(f"Wrote summary: {args.summary_out} ({len(gap_sets)} sets)", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
