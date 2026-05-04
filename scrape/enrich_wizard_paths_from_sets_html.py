#!/usr/bin/env python3
"""
Conservatively add pokemon_wizard_set_paths.json entries using Wizard /sets HTML + index.

1) For each Explorer set missing a path: find Wizard tiles (ordered /sets/{id}/{slug}) with
   nearest pokemontcg.io symbol. If pref_tcg or succ_tcg equals set_code AND the same tile's
   slug matches set_name strongly (>= --min-tcg-name), use that path.

2) Else use merge_pokemon_wizard_set_paths.find_wizard_path when name-vs-slug score >= --min-index.

3) Else optional fuzzy match over index slugs only (not weak tile context), with --min-fuzzy.

Never overwrites existing paths unless --overwrite.

Usage:
  python scrape/enrich_wizard_paths_from_sets_html.py --dry-run
  python scrape/enrich_wizard_paths_from_sets_html.py --apply
  python scrape/enrich_wizard_paths_from_sets_html.py --fetch --apply
"""

from __future__ import annotations

import argparse
import difflib
import importlib.util
import json
import re
import sys
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = Path(__file__).resolve().parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from extract_pokemon_wizard_set_paths import strip_line_gutter  # noqa: E402

PATHS_PATH = ROOT / "pokemon_wizard_set_paths.json"
DATA_PATH = ROOT / "pokemon_sets_data.json"
INDEX_PATH = ROOT / "pokemon_wizard_sets_index.json"
DEFAULT_DL = Path.home() / "Downloads" / "view-source_https___www.pokemonwizard.com_sets.html"


def _load_merge_module() -> Any:
    p = ROOT / "scrape" / "merge_pokemon_wizard_set_paths.py"
    spec = importlib.util.spec_from_file_location("merge_pw_paths", p)
    if spec is None or spec.loader is None:
        raise SystemExit("Cannot load merge_pokemon_wizard_set_paths.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def fetch_sets_html() -> str:
    url = "https://www.pokemonwizard.com/sets"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 PokemonTCG-Explorer/enrich-paths"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read().decode("utf-8", "replace")


def build_tiles(html: str) -> List[Dict[str, Any]]:
    h = strip_line_gutter(html)
    sym_re = re.compile(
        r"(?:https?://)?images\.pokemontcg\.io/([a-z0-9][a-z0-9-]*)/symbol\.png",
        re.I,
    )
    syms: List[Tuple[int, str]] = [(m.start(), m.group(1).lower()) for m in sym_re.finditer(h)]
    href_re = re.compile(
        r'(?:href=["\']|https://(?:www\.)?pokemonwizard\.com)/sets/(\d+)/([a-z0-9-]+)',
        re.I,
    )
    seen: set = set()
    tiles: List[Dict[str, Any]] = []
    for m in href_re.finditer(h):
        sid, slug = m.group(1), m.group(2).lower()
        path = f"{sid}/{slug}"
        if path in seen:
            continue
        seen.add(path)
        start, end = m.start(), m.end()
        pref = None
        for pos, code in reversed(syms):
            if pos < start:
                pref = code
                break
        succ = None
        for pos, code in syms:
            if pos > end:
                succ = code
                break
        tiles.append(
            {
                "path": path,
                "slug": slug,
                "id": sid,
                "pref_tcg": pref,
                "succ_tcg": succ,
            }
        )
    return tiles


def score_slug_name(slug: str, set_name: str, mod: Any) -> float:
    en = mod.norm_name(set_name)
    if not en:
        return 0.0
    sn = mod.norm_name(slug.replace("-", " "))
    if not sn:
        return 0.0
    return difflib.SequenceMatcher(None, en, sn).ratio()


def fuzzy_best_slug(
    set_name: str,
    by_slug: Dict[str, Dict[str, str]],
    mod: Any,
) -> List[Tuple[float, str, str]]:
    en = mod.norm_name(set_name)
    if not en:
        return []
    scored: List[Tuple[float, str, str]] = []
    for slug, meta in by_slug.items():
        th = str(meta.get("title_hint") or "")
        slug_words = slug.replace("-", " ")
        r1 = difflib.SequenceMatcher(None, en, mod.norm_name(th)).ratio() if th else 0.0
        r2 = difflib.SequenceMatcher(None, en, mod.norm_name(slug_words)).ratio()
        path = str(meta.get("path") or "").strip()
        if path:
            scored.append((max(r1, r2), slug, path))
    scored.sort(key=lambda x: -x[0])
    return scored


def resolve_path(
    code: str,
    name: str,
    tiles: List[Dict[str, Any]],
    mod: Any,
    by_slug: Dict[str, Dict[str, str]],
    *,
    min_tcg_name: float,
    min_index: float,
    min_fuzzy: float,
    min_margin: float,
) -> Tuple[str, str, float]:
    """Return (path, method, score). path empty if unmatched."""
    code_l = code.strip().lower()

    best_s = -1.0
    best_p = ""
    best_m = ""
    for i, t in enumerate(tiles):
        if t.get("pref_tcg") != code_l and t.get("succ_tcg") != code_l:
            continue
        for j in (i - 1, i, i + 1):
            if j < 0 or j >= len(tiles):
                continue
            s = score_slug_name(tiles[j]["slug"], name, mod)
            if s >= min_tcg_name and s > best_s:
                best_s, best_p, best_m = s, tiles[j]["path"], f"tcg_window[j={j},anchor={i}]"

    if best_p:
        return best_p, best_m, best_s

    p_name = mod.find_wizard_path(name, by_slug)
    slug_part = p_name.split("/", 1)[-1] if p_name else ""
    s_idx = score_slug_name(slug_part, name, mod) if slug_part else 0.0
    if p_name and s_idx >= min_index:
        return p_name, "index_name", s_idx

    ranked = fuzzy_best_slug(name, by_slug, mod)
    if len(ranked) >= 1:
        s0, _sl, p0 = ranked[0]
        s1 = ranked[1][0] if len(ranked) > 1 else 0.0
        if s0 >= min_fuzzy and (s0 - s1) >= min_margin:
            return p0, "index_fuzzy", s0

    return "", "unmatched", 0.0


def main() -> int:
    ap = argparse.ArgumentParser(description="Safely enrich pokemon_wizard_set_paths from Wizard /sets HTML")
    ap.add_argument("--html", type=Path, default=None)
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--paths", type=Path, default=PATHS_PATH)
    ap.add_argument("--data", type=Path, default=DATA_PATH)
    ap.add_argument("--min-tcg-name", type=float, default=0.66, help="Min name-slug score when using TCG symbol window")
    ap.add_argument("--min-index", type=float, default=0.52, help="Min name score for find_wizard_path result")
    ap.add_argument("--min-fuzzy", type=float, default=0.68, help="Min score for index-only fuzzy")
    ap.add_argument("--min-margin", type=float, default=0.06, help="Fuzzy best minus second-best")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    if args.fetch:
        print("Fetching https://www.pokemonwizard.com/sets ...", flush=True)
        html = fetch_sets_html()
    elif args.html and args.html.is_file():
        html = args.html.read_text(encoding="utf-8", errors="replace")
    elif DEFAULT_DL.is_file():
        print(f"Using default HTML: {DEFAULT_DL}", flush=True)
        html = DEFAULT_DL.read_text(encoding="utf-8", errors="replace")
    else:
        print("Provide --html, --fetch, or save view-source to Downloads default name.", file=sys.stderr)
        return 2

    mod = _load_merge_module()
    by_slug = mod.load_index(INDEX_PATH)
    tiles = build_tiles(html)
    print(f"Parsed {len(tiles)} unique Wizard set paths from HTML", flush=True)

    data = json.loads(args.data.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        return 2

    raw_paths: Dict[str, Any] = {}
    if args.paths.is_file():
        raw_paths = json.loads(args.paths.read_text(encoding="utf-8"))
    if not isinstance(raw_paths, dict):
        raw_paths = {}

    def cur_path(c: str) -> str:
        v = raw_paths.get(c)
        return str(v).strip().strip("/") if isinstance(v, str) else ""

    added: List[Tuple[str, str, float, str]] = []
    skipped = 0
    for s in data:
        if not isinstance(s, dict):
            continue
        code = str(s.get("set_code") or "").strip().lower()
        name = str(s.get("set_name") or "").strip()
        if not code:
            continue
        if cur_path(code) and not args.overwrite:
            skipped += 1
            continue
        path, method, score = resolve_path(
            code,
            name,
            tiles,
            mod,
            by_slug,
            min_tcg_name=args.min_tcg_name,
            min_index=args.min_index,
            min_fuzzy=args.min_fuzzy,
            min_margin=args.min_margin,
        )
        if path:
            added.append((code, path, score, method))

    print(f"Existing paths kept: {skipped}", flush=True)
    print(f"New mappings: {len(added)}", flush=True)
    for code, path, score, method in sorted(added, key=lambda x: x[0]):
        print(f"  {code:<10}  {path:<40}  score={score:.3f}  {method}", flush=True)

    if args.dry_run or not args.apply:
        return 0

    out_obj: Dict[str, Any] = dict(raw_paths)
    gen = out_obj.get("_generated")
    if isinstance(gen, dict):
        gen = dict(gen)
        gen["wizard_page_path_enricher"] = "fetch" if args.fetch else str(args.html or DEFAULT_DL)
        out_obj["_generated"] = gen
    for code, path, _s, _m in added:
        out_obj[code] = path

    text = json.dumps(out_obj, indent=2, ensure_ascii=False) + "\n"
    tmp = args.paths.with_suffix(args.paths.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(args.paths)
    print(f"Wrote {args.paths}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
