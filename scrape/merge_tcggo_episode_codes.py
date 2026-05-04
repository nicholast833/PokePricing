#!/usr/bin/env python3
"""
Merge TCGGO RapidAPI ``/episodes`` payload into ``pokemon_sets_data.json`` set records.

Adds a ``tcggo`` object per matched set: ``episode_id``, ``episode_slug``, ``code`` (may be null),
``released_at``, ``episode_name``.

Default input: ``scrape/tcggo_episodes_all.json`` from ``fetch_tcggo_episodes.py`` (override with ``--episodes-json``).

DB ``set_name`` sometimes differs from TCGGO ``name``; extend ``DB_SET_TO_EPISODE_NAME`` as needed.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
DEFAULT_EPISODES = SCRIPT_DIR / "tcggo_episodes_all.json"
DEFAULT_DATA = ROOT / "pokemon_sets_data.json"

# Explorer ``set_name`` -> TCGGO episode ``name`` (when they differ)
DB_SET_TO_EPISODE_NAME: dict[str, str] = {
    "Mega Evolution Black Star Promos": "MEP Black Star Promos",
    # TCGGO uses an em dash after HS (not in our ``set_name``)
    "Triumphant": "HS\u2014Triumphant",
    "Undaunted": "HS\u2014Undaunted",
    "Unleashed": "HS\u2014Unleashed",
    "Scarlet & Violet Black Star Promos": "SV Black Star Promos",
}


def _relaxed_json_loads(raw: str) -> Any:
    raw = raw.lstrip("\ufeff").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # trailing commas in objects/arrays
        fixed = re.sub(r",(\s*[\]}])", r"\1", raw)
        return json.loads(fixed)


def pick_canonical_episode(group: list[dict[str, Any]]) -> dict[str, Any]:
    """When TCGGO returns duplicate episode names (e.g. two Stellar Crown), keep the primary row."""
    if len(group) == 1:
        return group[0]
    with_logo = [e for e in group if e.get("logo")]
    pool = with_logo if with_logo else group
    # Prefer slug without ``-1`` suffix, then lower numeric id
    def sort_key(e: dict[str, Any]) -> tuple[int, int, str]:
        slug = str(e.get("slug") or "")
        junk = 1 if slug.endswith("-1") or re.search(r"-\d+$", slug) else 0
        return (junk, int(e.get("id") or 0), slug)

    return sorted(pool, key=sort_key)[0]


def load_episodes(path: Path) -> list[dict[str, Any]]:
    raw = path.read_text(encoding="utf-8", errors="replace")
    data = _relaxed_json_loads(raw)
    rows = data.get("data") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        raise SystemExit("Expected top-level { \"data\": [ ... ] } or a JSON array")
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        if (r.get("game") or {}).get("slug") != "pokemon":
            continue
        out.append(r)
    return out


def episode_by_match_name(episodes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    from collections import defaultdict

    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for e in episodes:
        name = str(e.get("name") or "").strip()
        if not name:
            continue
        groups[name].append(e)
    return {name: pick_canonical_episode(g) for name, g in groups.items()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--episodes-json", type=Path, default=DEFAULT_EPISODES)
    ap.add_argument("--data", type=Path, default=DEFAULT_DATA)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    episodes = load_episodes(args.episodes_json)
    by_epi_name = episode_by_match_name(episodes)

    data = json.loads(args.data.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("pokemon_sets_data.json must be a JSON array of sets")

    for s in data:
        if isinstance(s, dict):
            s.pop("tcggo", None)

    matched = 0
    report: dict[str, Any] = {
        "episodes_file": str(args.episodes_json),
        "pokemon_episode_count": len(episodes),
        "unique_episode_names": len(by_epi_name),
        "matched_sets": 0,
        "pairs": [],
    }

    for s in data:
        if not isinstance(s, dict):
            continue
        set_name = str(s.get("set_name") or "").strip()
        if not set_name:
            continue
        epi_name = DB_SET_TO_EPISODE_NAME.get(set_name, set_name)
        ep = by_epi_name.get(epi_name)
        if not ep:
            continue
        code = ep.get("code")
        s["tcggo"] = {
            "episode_id": ep.get("id"),
            "episode_slug": ep.get("slug"),
            "episode_name": ep.get("name"),
            "code": code,
            "released_at": ep.get("released_at"),
        }
        matched += 1
        report["pairs"].append(
            {
                "set_code": s.get("set_code"),
                "set_name": set_name,
                "tcggo_code": code,
                "tcggo_episode_id": ep.get("id"),
            }
        )

    report["matched_sets"] = matched
    used_epi = {p["tcggo_episode_id"] for p in report["pairs"]}
    # Canonical episode names with no matching DB set (e.g. future sets)
    report["episodes_not_in_database"] = [
        {"id": ep.get("id"), "name": name, "code": ep.get("code")}
        for name, ep in by_epi_name.items()
        if ep.get("id") not in used_epi
    ]
    # Raw rows skipped when deduplicating duplicate ``name`` (e.g. second Stellar Crown)
    report["episode_rows_superseded"] = [
        {"id": e.get("id"), "name": e.get("name"), "slug": e.get("slug")}
        for e in episodes
        if e.get("id") not in used_epi
        and by_epi_name.get(str(e.get("name") or ""), {}).get("id") != e.get("id")
    ]

    rep_path = args.data.with_suffix(".json.tcggo_episode_merge_report.json")
    rep_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if args.dry_run:
        print(json.dumps(report, indent=2)[:8000])
        return 0

    args.data.write_text(json.dumps(data, indent=4, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {matched} tcggo blocks into {args.data}")
    print(f"Report: {rep_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
