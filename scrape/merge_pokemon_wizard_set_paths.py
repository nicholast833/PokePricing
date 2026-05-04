#!/usr/bin/env python3
"""
Build pokemon_wizard_set_paths.json from:
  - pokemon_wizard_sets_index.json (from extract_pokemon_wizard_set_paths.py)
  - pokemon_sets_data.json (Explorer set_code + set_name)

Matching: normalized set name (lowercase, alnum, collapse "and" for &) equals
slug with hyphens replaced by spaces (e.g. crown-zenith-galarian-gallery).

Preserves existing path values from pokemon_wizard_set_paths.json when the
set_code already has an entry (manual override). Updates _readme.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
INDEX_PATH = ROOT / "pokemon_wizard_sets_index.json"
DATA_PATH = ROOT / "pokemon_sets_data.json"
PATHS_PATH = ROOT / "pokemon_wizard_set_paths.json"

# Explorer set_code -> Wizard slug when title match is ambiguous or Wizard uses a different title.
SET_CODE_TO_SLUG: Dict[str, str] = {
    "sv1": "scarlet-violet-base-set",
    "sv3pt5": "scarlet-violet-151",
    "swsh35": "champions-path",
    "pgo": "pokemon-go-tcg",
    "ecard1": "expedition",
    "base1": "base-set",
}


def norm_name(s: str) -> str:
    t = str(s or "").lower()
    t = t.replace("&", " and ")
    t = re.sub(r"[:'\"]+", " ", t)
    t = re.sub(r"[^a-z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"\s+and\s+", " ", t)
    return t


def explorer_compare_keys(set_name: str) -> List[str]:
    """
    Return normalized strings to compare against wizard slug-as-title.
    Wizard often omits leading EX/SM/SV/SWSH that Explorer set titles include.
    """
    full = norm_name(set_name)
    if not full:
        return []
    keys = [full]
    core = full
    for p in ("ex ", "sm ", "sv ", "swsh ", "ss ", "sword shield ", "scarlet violet "):
        if core.startswith(p):
            core = core[len(p) :].strip()
    if core and core not in keys:
        keys.append(core)
    return keys


def load_index(path: Path) -> Dict[str, Dict[str, str]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    by_slug = raw.get("by_slug")
    if not isinstance(by_slug, dict):
        raise SystemExit("Index missing by_slug")
    out: Dict[str, Dict[str, str]] = {}
    for slug, meta in by_slug.items():
        if not isinstance(meta, dict) or "path" not in meta:
            continue
        out[str(slug).lower()] = meta
    return out


def find_wizard_path(set_name: str, by_slug: Dict[str, Dict[str, str]]) -> Optional[str]:
    targets = explorer_compare_keys(set_name)
    if not targets:
        return None
    tgt_set = set(targets)
    best: List[Tuple[int, str]] = []
    for slug, meta in by_slug.items():
        cand = norm_name(slug.replace("-", " "))
        if cand in tgt_set:
            p = str(meta.get("path") or "").strip()
            if p:
                best.append((len(slug), p))
    if not best:
        return None
    best.sort(key=lambda x: -x[0])
    return best[0][1]


def main() -> int:
    ap = argparse.ArgumentParser(description="Merge Wizard set index into pokemon_wizard_set_paths.json")
    ap.add_argument("--index", type=Path, default=INDEX_PATH)
    ap.add_argument("--data", type=Path, default=DATA_PATH)
    ap.add_argument("--output", type=Path, default=PATHS_PATH)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.index.is_file():
        print(f"Missing index: {args.index} — run extract_pokemon_wizard_set_paths.py first", file=sys.stderr)
        return 2
    if not args.data.is_file():
        print(f"Missing {args.data}", file=sys.stderr)
        return 2

    by_slug = load_index(args.index)
    sets_data = json.loads(args.data.read_text(encoding="utf-8"))
    if not isinstance(sets_data, list):
        raise SystemExit("pokemon_sets_data.json must be a list")

    prev: Dict[str, str] = {}
    if args.output.is_file():
        raw_prev = json.loads(args.output.read_text(encoding="utf-8"))
        if isinstance(raw_prev, dict):
            for k, v in raw_prev.items():
                if str(k).startswith("_") or not isinstance(v, str):
                    continue
                prev[str(k).strip().lower()] = v.strip().strip("/")

    merged: Dict[str, str] = {}
    unmatched: List[Tuple[str, str]] = []
    matched = 0
    kept_override = 0

    for s in sets_data:
        if not isinstance(s, dict):
            continue
        code = str(s.get("set_code") or "").strip().lower()
        name = str(s.get("set_name") or "").strip()
        if not code:
            continue
        if code in prev:
            merged[code] = prev[code]
            kept_override += 1
            matched += 1
            continue
        slug_fix = SET_CODE_TO_SLUG.get(code)
        if slug_fix and slug_fix in by_slug:
            p = str(by_slug[slug_fix].get("path") or "").strip()
            if p:
                merged[code] = p
                matched += 1
                continue
        path = find_wizard_path(name, by_slug)
        if path:
            merged[code] = path
            matched += 1
        else:
            unmatched.append((code, name))

    out_obj: Dict[str, Any] = {
        "_readme": "Lowercase Explorer set_code -> Pokémon Wizard set path (setId/slug) for sync when "
        "cards lack TCG product ids. Auto-merged from pokemon_wizard_sets_index.json + set names; "
        "edit this file to override a set_code.",
        "_generated": {
            "index": str(args.index.name),
            "wizard_sets_in_index": len(by_slug),
            "explorer_sets": len([s for s in sets_data if isinstance(s, dict) and s.get("set_code")]),
            "paths_written": len(merged),
            "preserved_manual_paths": kept_override,
        },
    }
    for code in sorted(merged.keys()):
        out_obj[code] = merged[code]

    report = {
        "paths_count": len(merged),
        "unmatched_count": len(unmatched),
        "unmatched_sample": unmatched[:40],
    }
    print(json.dumps(report, indent=2))

    if args.dry_run:
        return 0

    text = json.dumps(out_obj, indent=2, ensure_ascii=False) + "\n"
    args.output.write_text(text, encoding="utf-8")
    print(f"Wrote {args.output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
