#!/usr/bin/env python3
"""
Parse browser "view source" save or raw HTML from pokemonwizard.com/sets
and emit JSON: by_slug (setId/slug paths for each Wizard listing slug).

Handles Chrome view-source tables, relative href="/sets/{id}/{slug}", and absolute wizard URLs.
Note: pokemontcg.io symbol.png thumbnails on that page can belong to a neighboring tile (UI),
so this script does not infer TCG API set_code from symbol images.

Usage:
  python scrape/extract_pokemon_wizard_set_paths.py path/to/view-source....html
  python scrape/extract_pokemon_wizard_set_paths.py --fetch

Default output: pokemon_wizard_sets_index.json (full index by wizard slug)
Optional: merge hints into pokemon_wizard_set_paths.json via separate tooling.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from pathlib import Path
from typing import Dict, List, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "pokemon_wizard_sets_index.json"

# view-source pages wrap HTML; decode minimal entities
_ENTITY_RE = re.compile(r"&lt;|&gt;|&quot;|&amp;|&#39;|&#x27;")


def decode_view_source_line(s: str) -> str:
    s = s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')
    s = s.replace("&amp;", "&").replace("&#39;", "'").replace("&#x27;", "'")
    return s


def strip_line_gutter(html: str) -> str:
    """Remove Chrome view-source table markup if present."""
    if "line-content" not in html[:50000]:
        return html
    parts: List[str] = []
    for m in re.finditer(
        r'<td class="line-content">(?:<span[^>]*>)?(.*?)(?:</span>)?</td>',
        html,
        re.S | re.I,
    ):
        parts.append(decode_view_source_line(m.group(1)))
    # Join without newlines: Chrome splits long lines across table rows; a newline here
    # breaks regexes like <div[^>]*> that must span the split.
    return "".join(parts) if parts else html


def extract_set_paths(html: str) -> List[Tuple[str, str, str]]:
    """
    Return list of (set_id, slug, set_title_guess) from listing hrefs, e.g.
    https://www.pokemonwizard.com/sets/17689/crown-zenith-galarian-gallery
    or relative /sets/605/base-set-2 (common in saved view-source / same-origin HTML).
    """
    html = strip_line_gutter(html)
    pairs: Set[Tuple[str, str]] = set()
    pat = re.compile(
        r'(?:href=["\']|https://(?:www\.)?pokemonwizard\.com)/sets/(\d+)/([a-z0-9-]+)',
        re.I,
    )
    for sid, slug in pat.findall(html):
        pairs.add((sid, slug.lower()))
    out: List[Tuple[str, str, str]] = []
    for sid, slug in sorted(pairs, key=lambda x: (int(x[0]), x[1])):
        title = slug.replace("-", " ").title()
        out.append((sid, slug, title))
    return out


def fetch_sets_html() -> str:
    url = "https://www.pokemonwizard.com/sets"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 PokemonTCG-Explorer/extract"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read().decode("utf-8", "replace")


def main() -> int:
    ap = argparse.ArgumentParser(description="Extract Pokémon Wizard set id/slug index from HTML")
    ap.add_argument(
        "html_file",
        nargs="?",
        type=Path,
        help="Saved view-source or HTML from pokemonwizard.com/sets",
    )
    ap.add_argument(
        "--fetch",
        action="store_true",
        help="Download https://www.pokemonwizard.com/sets instead of reading a file",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Output JSON (default: {DEFAULT_OUT})",
    )
    args = ap.parse_args()

    if args.fetch:
        print("Fetching https://www.pokemonwizard.com/sets ...", flush=True)
        html = fetch_sets_html()
    elif args.html_file and args.html_file.is_file():
        html = args.html_file.read_text(encoding="utf-8", errors="replace")
    else:
        ap.print_help()
        print("Provide html_file or --fetch", file=sys.stderr)
        return 2

    rows = extract_set_paths(html)
    # Index by slug for lookups; include id for path construction
    by_slug: Dict[str, Dict[str, str]] = {}
    by_path: Dict[str, str] = {}
    for sid, slug, title in rows:
        path = f"{sid}/{slug}"
        by_slug[slug] = {"set_id": sid, "slug": slug, "path": path, "title_hint": title}
        by_path[path] = slug

    payload = {
        "_readme": "Auto-generated from Pokémon Wizard /sets listing. "
        "Use path (setId/slug) with sync_pokemon_wizard.py via pokemon_wizard_set_paths.json "
        "(map explorer set_code -> path). Keys starting with _ are ignored.",
        "source": "fetch" if args.fetch else str(args.html_file),
        "set_count": len(rows),
        "by_slug": by_slug,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {args.output} with {len(rows)} sets", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
