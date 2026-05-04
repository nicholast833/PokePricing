#!/usr/bin/env python3
"""
Suggest or merge Pokémon Wizard set paths for Explorer sets whose top-list Pokémon
cards lack usable Wizard price history and are not yet mapped in pokemon_wizard_set_paths.json.

Matching order:
  1) Explicit set_code -> Wizard slug (curated; must exist in pokemon_wizard_sets_index.json)
  2) Substring hints on normalized set_name -> slug
  3) Fuzzy score: max difflib ratio vs Wizard title_hint and slug-as-words, plus token Jaccard

Does not overwrite existing path entries (manual overrides preserved).

Usage:
  python scrape/match_wizard_paths_for_price_gaps.py
  python scrape/match_wizard_paths_for_price_gaps.py --min-fuzzy 0.68 --min-margin 0.08
  python scrape/match_wizard_paths_for_price_gaps.py --apply --dry-run
  python scrape/match_wizard_paths_for_price_gaps.py --apply
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
INDEX_PATH = ROOT / "pokemon_wizard_sets_index.json"
DATA_PATH = ROOT / "pokemon_sets_data.json"
PATHS_PATH = ROOT / "pokemon_wizard_set_paths.json"


def norm_name(s: str) -> str:
    t = str(s or "").lower()
    t = t.replace("&", " and ")
    t = re.sub(r"[:'\"]+", " ", t)
    t = re.sub(r"[^a-z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"\s+and\s+", " ", t)
    return t


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


def pokemon_missing_wizard_history(
    set_row: dict,
    top_key: str,
    *,
    supertype_mode: str,
) -> Tuple[int, int]:
    """Returns (missing_count, pokemon_considered_count) for top list."""
    cards = set_row.get(top_key) or set_row.get("top_25_cards") or []
    if not isinstance(cards, list):
        return 0, 0
    miss = 0
    n = 0
    for c in cards:
        if not isinstance(c, dict):
            continue
        st = str(c.get("supertype") or "").strip().lower()
        if supertype_mode == "pokemon" and st not in ("pokémon", "pokemon"):
            continue
        n += 1
        if len(filter_wizard_rows(c.get("pokemon_wizard_price_history"))) >= 1:
            continue
        miss += 1
    return miss, n


def load_by_slug(path: Path) -> Dict[str, Dict[str, str]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    by_slug = raw.get("by_slug")
    if not isinstance(by_slug, dict):
        raise SystemExit("Index missing by_slug")
    out: Dict[str, Dict[str, str]] = {}
    for slug, meta in by_slug.items():
        if not isinstance(meta, dict):
            continue
        p = str(meta.get("path") or "").strip()
        if not p:
            continue
        out[str(slug).lower()] = meta
    return out


# Explorer set_code -> Wizard index slug (keys of by_slug). Curated for promo / kit naming drift.
EXPLICIT_SLUG_BY_SET_CODE: Dict[str, str] = {
    "swshp": "sword-shield-promo-cards",
    "smp": "sun-moon-promos",
    "xyp": "xy-promos",
    "bwp": "black-and-white-promos",
    "dpp": "diamond-and-pearl-promos",
    "hsp": "hgss-promos",
    "np": "nintendo-promos",
    "basep": "wotc-promo",
    "svp": "scarlet-violet-promo-cards",
    "bp": "best-of-promos",
    "ru1": "rumble",
    "det1": "detective-pikachu",
    "pgo": "pokemon-go-tcg",
    "tk1a": "ex-trainer-kit-1-latias-latios",
    "tk1b": "ex-trainer-kit-1-latias-latios",
    "tk2a": "ex-trainer-kit-2-plusle-minun",
    "tk2b": "ex-trainer-kit-2-plusle-minun",
    "mcd21": "mcdonalds-25th-anniversary-promos",
    "mcd22": "mcdonalds-promos-2022",
    "mcd23": "mcdonalds-promos-2023",
}


# (normalized_substring must appear in norm(set_name), slug)
NAME_HINT_SLUGS: List[Tuple[str, str]] = [
    ("scarlet violet black star", "scarlet-violet-promo-cards"),
    ("sword shield black star", "sword-shield-promo-cards"),
    ("swsh black star", "sword-shield-promo-cards"),
    ("sun moon black star", "sun-moon-promos"),
    ("sm black star", "sun-moon-promos"),
    ("xy black star", "xy-promos"),
    ("black and white black star", "black-and-white-promos"),
    ("bw black star", "black-and-white-promos"),
    ("diamond pearl black star", "diamond-and-pearl-promos"),
    ("dp black star", "diamond-and-pearl-promos"),
    ("hgss black star", "hgss-promos"),
    ("heartgold soulsilver black star", "hgss-promos"),
    ("nintendo black star", "nintendo-promos"),
    ("wizards black star", "wotc-promo"),
    ("wotc black star", "wotc-promo"),
    ("best of game", "best-of-promos"),
    ("pokemon rumble", "rumble"),
    ("detective pikachu", "detective-pikachu"),
]


def token_jaccard(a: str, b: str) -> float:
    sa = set(norm_name(a).split())
    sb = set(norm_name(b).split())
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


def fuzzy_best(
    set_name: str,
    by_slug: Dict[str, Dict[str, str]],
) -> List[Tuple[float, str, str, str]]:
    """Return list of (score, slug, path, label) sorted by score desc."""
    en = norm_name(set_name)
    if not en:
        return []
    scored: List[Tuple[float, str, str, str]] = []
    for slug, meta in by_slug.items():
        th = str(meta.get("title_hint") or "")
        slug_words = slug.replace("-", " ")
        r1 = difflib.SequenceMatcher(None, en, norm_name(th)).ratio() if th else 0.0
        r2 = difflib.SequenceMatcher(None, en, norm_name(slug_words)).ratio()
        j = max(token_jaccard(en, th), token_jaccard(en, slug_words))
        score = max(r1, r2, j * 0.95)
        path = str(meta.get("path") or "").strip()
        if path:
            scored.append((score, slug, path, th or slug))
    scored.sort(key=lambda x: -x[0])
    return scored


def resolve_slug(slug: str, by_slug: Dict[str, Dict[str, str]]) -> Optional[str]:
    meta = by_slug.get(slug.lower())
    if not meta:
        return None
    p = str(meta.get("path") or "").strip()
    return p or None


def main() -> int:
    ap = argparse.ArgumentParser(description="Match Wizard set paths for sets with Pokémon missing Wizard price history")
    ap.add_argument("--data", type=Path, default=DATA_PATH)
    ap.add_argument("--index", type=Path, default=INDEX_PATH)
    ap.add_argument("--paths", type=Path, default=PATHS_PATH)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--min-fuzzy", type=float, default=0.62, help="Minimum fuzzy score to suggest when no explicit/hint rule")
    ap.add_argument("--min-margin", type=float, default=0.06, help="Best minus second-best must exceed this for fuzzy-only apply")
    ap.add_argument("--apply", action="store_true", help="Merge new paths into --paths (never overwrites existing keys)")
    ap.add_argument("--dry-run", action="store_true", help="With --apply, print actions only")
    args = ap.parse_args()

    if not args.data.is_file() or not args.index.is_file():
        print("Need pokemon_sets_data.json and pokemon_wizard_sets_index.json", file=sys.stderr)
        return 2

    by_slug = load_by_slug(args.index)
    data = json.loads(args.data.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        print("Expected array of sets", file=sys.stderr)
        return 1

    prev: Dict[str, str] = {}
    if args.paths.is_file():
        raw_p = json.loads(args.paths.read_text(encoding="utf-8"))
        if isinstance(raw_p, dict):
            for k, v in raw_p.items():
                if str(k).startswith("_") or not isinstance(v, str):
                    continue
                prev[str(k).strip().lower()] = v.strip().strip("/")

    top_key = f"top_{args.top}_cards"
    targets: List[dict] = []
    for s in data:
        if not isinstance(s, dict):
            continue
        code = str(s.get("set_code") or "").strip().lower()
        if not code:
            continue
        if code in prev:
            continue
        miss, n = pokemon_missing_wizard_history(s, top_key, supertype_mode="pokemon")
        if miss <= 0 or n <= 0:
            continue
        targets.append(
            {
                "set_code": code,
                "set_name": str(s.get("set_name") or "").strip(),
                "missing_pokemon": miss,
                "pokemon_in_list": n,
            }
        )

    targets.sort(key=lambda r: (r["set_code"],))

    suggestions: List[dict] = []
    for t in targets:
        code = t["set_code"]
        name = t["set_name"]
        method = ""
        slug = ""
        score = 1.0

        if code in EXPLICIT_SLUG_BY_SET_CODE:
            slug = EXPLICIT_SLUG_BY_SET_CODE[code]
            method = "explicit_set_code"
        else:
            nn = norm_name(name)
            for sub, s_slug in NAME_HINT_SLUGS:
                if sub in nn:
                    slug = s_slug
                    method = "name_hint"
                    score = 0.85
                    break

        path: Optional[str] = None
        if slug:
            path = resolve_slug(slug, by_slug)
            if not path:
                method += "_MISSING_SLUG"
                slug = ""

        if not path:
            ranked = fuzzy_best(name, by_slug)
            if len(ranked) >= 1:
                best_s, best_slug, best_path, best_lab = ranked[0]
                second = ranked[1][0] if len(ranked) > 1 else 0.0
                margin = best_s - second
                if best_s >= args.min_fuzzy and margin >= args.min_margin:
                    path, slug, score = best_path, best_slug, best_s
                    method = "fuzzy"
                else:
                    suggestions.append(
                        {
                            **t,
                            "method": "unmatched",
                            "best_fuzzy": round(best_s, 3),
                            "margin": round(margin, 3),
                            "best_slug": best_slug,
                            "best_path": best_path,
                            "best_label": best_lab,
                        }
                    )
                    continue

        if path:
            suggestions.append(
                {
                    **t,
                    "method": method,
                    "slug": slug,
                    "path": path,
                    "score": round(score, 4) if isinstance(score, float) else score,
                }
            )

    applied = 0
    print(f"Sets needing paths (Pokémon missing Wizard history, no existing map): {len(targets)}", flush=True)
    print(f"Resolvable suggestions: {sum(1 for s in suggestions if s.get('path'))}", flush=True)
    print(f"Still unmatched (review): {sum(1 for s in suggestions if not s.get('path'))}\n", flush=True)

    for row in suggestions:
        if row.get("path"):
            print(
                f"  {row['set_code']:<10}  {row['method']:<18}  score={row.get('score', '—')!s:8}  "
                f"path={row['path']:<32}  {row['set_name'][:52]}",
                flush=True,
            )
        else:
            print(
                f"  {row['set_code']:<10}  UNMATCHED         best={row.get('best_fuzzy')} margin={row.get('margin')}  "
                f"{row.get('best_slug')}  |  {row['set_name'][:40]}",
                flush=True,
            )

    to_write = dict(prev)
    if args.apply:
        for row in suggestions:
            if not row.get("path"):
                continue
            code = row["set_code"]
            if code in to_write:
                continue
            to_write[code] = row["path"]
            applied += 1
            if args.dry_run:
                print(f"[dry-run] would add {code} -> {row['path']}", flush=True)

        if args.dry_run:
            print(f"\nDry-run: would add {applied} path(s).", flush=True)
            return 0

        prev_full: Dict[str, Any] = {}
        if args.paths.is_file():
            prev_full = json.loads(args.paths.read_text(encoding="utf-8"))
            if not isinstance(prev_full, dict):
                prev_full = {}
        out_obj: Dict[str, Any] = {}
        for k, v in prev_full.items():
            if str(k).startswith("_"):
                out_obj[k] = v
        if "_readme" not in out_obj:
            out_obj["_readme"] = (
                "Lowercase Explorer set_code -> Pokémon Wizard set path (setId/slug) for sync when "
                "cards lack TCG product ids. Auto-merged from pokemon_wizard_sets_index.json + set names; "
                "edit this file to override a set_code. Augmented by scrape/match_wizard_paths_for_price_gaps.py."
            )
        gen = out_obj.get("_generated")
        if not isinstance(gen, dict):
            gen = {}
        gen = {
            **gen,
            "wizard_sets_in_index": len(by_slug),
            "paths_written": len(to_write),
            "gap_matcher_last_run": "match_wizard_paths_for_price_gaps.py",
        }
        out_obj["_generated"] = gen
        for code in sorted(to_write.keys()):
            out_obj[code] = to_write[code]
        args.paths.write_text(json.dumps(out_obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"\nWrote {args.paths} (+{applied} new path(s)). Re-run: python scrape/sync_pokemon_wizard.py --only-set-codes ...", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
