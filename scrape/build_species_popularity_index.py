#!/usr/bin/env python3
"""
Merge Google Trends (per-species) with:
  1) Multi-poll survey adjusted ranks (gid=1380367315), and
  2) Sparklez Squad 2025 — **ALL POKEMON RANKINGS** block (all variants as separate rows; votes
     are **summed** onto a base-species key so they join `Character` in google_trends_momentum.json).

Default Sparklez file (if present): `data/sparklez_2025_all_pokemon_rankings.csv`
Otherwise fetches the public Google Sheet CSV (same layout).

Outputs an enriched google_trends_momentum.json (array) with:
  - Trend_Index_Average  — Google Trends 1y-style index (preserve from input)
  - Survey_AdjustedRank_Mean — panel polls only (mean adjusted rank), or null
  - Survey_Poll_Count — number of panel poll columns (0–4)
  - Sparklez_MaxVotes — **total** Sparklez votes for that base species (all variant rows summed), or null
  - Sparklez_SyntheticAdj — vote-rank mapped to ~45–99.9 (same scale as panel adj), or null
  - Survey_CombinedAdj_Mean — mean of panel mean and Sparklez synthetic when both exist
  - Popularity_Index — trend + survey blend (single survey_to_index on combined adj)

Re-fetch:
  python scrape/build_species_popularity_index.py

Offline:
  python scrape/build_species_popularity_index.py --panel-csv panel.csv --sparklez-csv path/to/export.csv

Skip Sparklez:
  python scrape/build_species_popularity_index.py --no-sparklez
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import re
import statistics
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PANEL_DEFAULT_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1Md1mJb2UEn4e4yxzjPAW9H8c_oRGebbQzdmTyuvX6xw/export?format=csv&gid=1380367315"
)
SPARKLEZ_DEFAULT_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1bGJqCYBdT8LEaKARb0fCfrVePl90yN77CtJMvIVd0IE/export?format=csv&gid=187132612"
)
SPARKLEZ_DEFAULT_CSV = Path("data/sparklez_2025_all_pokemon_rankings.csv")

# Blend: survey rescaled into ~same upper range as Trend_Index in this file, then weighted sum.
TREND_WEIGHT = 0.42
SURVEY_WEIGHT = 0.58
SURVEY_ADJ_MIN = 48.0
SURVEY_ADJ_MAX = 100.0
TREND_SCALE_MAX = 72.0  # typical Gyarados / Charizard band in existing JSON

OUTPUT_KEYS_STRIP = (
    "Popularity_Index",
    "Survey_AdjustedRank_Mean",
    "Survey_Poll_Count",
    "Sparklez_MaxVotes",
    "Sparklez_SyntheticAdj",
    "Survey_CombinedAdj_Mean",
)


def norm_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


_RANK_ORD_RE = re.compile(r"^\d+(st|nd|rd|th)$", re.I)
_FORM_SUFFIX_RE = re.compile(r"\s*\([^)]*\)\s*$")

# Strip printed prefixes / Rotom etc. so variant rows aggregate onto the same key as `Character` in trends JSON.
_SPARKLEZ_PREFIXES = (
    "mega ",
    "alolan ",
    "galarian ",
    "hisuian ",
    "paldean ",
    "ash-",
    "ash ",
    "shadow ",
    "primal ",
    "ultra ",
    "wash ",
    "fan ",
    "heat ",
    "frost ",
    "mow ",
    "sky ",
)


def sparklez_base_species_key(display_name: str) -> str:
    """Map a survey display name (any variant) to the same normalized key used for species lookups."""
    s = (display_name or "").strip()
    if not s:
        return ""
    s = _FORM_SUFFIX_RE.sub("", s).strip()
    low = s.lower()
    while True:
        hit = False
        for p in _SPARKLEZ_PREFIXES:
            if low.startswith(p):
                s = s[len(p) :].strip()
                low = s.lower()
                hit = True
                break
        if not hit:
            break
    # Mega Charizard X / Y → Charizard (trends `Character` is base species)
    s = re.sub(r"\s+[xy]\s*$", "", s, flags=re.I).strip()
    return norm_name(s)


def fetch_text(url: str, timeout: int = 90) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "PokemonTCG-Explorer-build/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def parse_panel_adjusted_ranks(csv_text: str) -> Dict[str, List[float]]:
    """Parse the wide multi-poll sheet: 4 blocks per row (Japan 2016, Reddit 2019, Intl 2020, Japan 2021)."""
    rows = list(csv.reader(io.StringIO(csv_text)))
    by_key: Dict[str, List[float]] = defaultdict(list)
    for row in rows[2:]:
        if len(row) < 19:
            continue
        blocks: Tuple[Tuple[int, int], ...] = ((2, 3), (6, 8), (11, 13), (16, 18))
        for name_i, adj_i in blocks:
            if name_i >= len(row) or adj_i >= len(row):
                continue
            name = (row[name_i] or "").strip()
            adj_s = (row[adj_i] or "").strip()
            if not name or not adj_s:
                continue
            try:
                adj = float(adj_s)
            except ValueError:
                continue
            if adj < 35.0 or adj > 100.0:
                continue
            by_key[norm_name(name)].append(adj)
    return by_key


def parse_sparklez_all_pokemon_rankings(
    csv_text: str,
) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, str]]:
    """
    Parse the `,,ALL POKEMON RANKINGS,,` section: columns Rank, Dex #, Pokémon Name, Generation, Votes.
    Sums votes for every variant row onto `sparklez_base_species_key(name)` (e.g. Mega + base Charizard).

    Returns (votes_by_base_key, stats, display_name_first) where stats includes:
      ranking_rows, vote_sum_check
    """
    rows = list(csv.reader(io.StringIO(csv_text)))
    votes_sum: Dict[str, int] = defaultdict(int)
    display_first: Dict[str, str] = {}
    in_banner = False
    header_ok = False
    ranking_rows = 0
    vote_sum_check = 0

    for row in rows:
        if not row:
            continue
        joined = ",".join(row).upper()
        if "ALL POKEMON RANKINGS" in joined:
            in_banner = True
            header_ok = False
            votes_sum.clear()
            display_first.clear()
            ranking_rows = 0
            vote_sum_check = 0
            continue
        if not in_banner:
            continue
        r0 = (row[0] or "").strip()
        row2_l = (row[2] or "").strip().lower() if len(row) > 2 else ""
        if "all variants" in row2_l or "all variant" in row2_l:
            continue
        if r0.lower() == "rank" and len(row) >= 5:
            r2 = (row[2] or "").strip().lower()
            if "pok" in r2 and "name" in r2:
                header_ok = True
            continue
        if not header_ok:
            continue
        if len(row) < 5:
            continue
        if not _RANK_ORD_RE.match(r0):
            continue
        name = (row[2] or "").strip()
        votes_s = (row[4] or "").strip().replace(",", "")
        if not name:
            continue
        if not votes_s.lstrip("-").isdigit():
            continue
        v = int(votes_s)
        key = sparklez_base_species_key(name)
        if not key:
            continue
        votes_sum[key] += v
        vote_sum_check += v
        ranking_rows += 1
        if key not in display_first:
            display_first[key] = name

    stats = {"ranking_rows": ranking_rows, "vote_sum_check": vote_sum_check}
    return dict(votes_sum), stats, display_first


def sparklez_votes_to_synthetic_adj(votes_by: Dict[str, int]) -> Dict[str, float]:
    """Map vote totals to synthetic adjusted-rank scores (45 .. 99.9) by global vote rank."""
    items = [(k, v) for k, v in votes_by.items() if v > 0]
    items.sort(key=lambda x: -x[1])
    n = len(items)
    out: Dict[str, float] = {}
    if n == 0:
        return out
    lo, hi = 45.0, 99.9
    if n == 1:
        out[items[0][0]] = hi
        return out
    for i, (k, _v) in enumerate(items):
        adj = hi - (i / (n - 1)) * (hi - lo)
        out[k] = round(adj, 3)
    return out


def survey_to_index(mean_adj: float) -> float:
    """Map mean adjusted rank into ~0..TREND_SCALE_MAX comparable to Trend_Index_Average."""
    t = (mean_adj - SURVEY_ADJ_MIN) / max(1e-6, (SURVEY_ADJ_MAX - SURVEY_ADJ_MIN))
    t = max(0.0, min(1.0, t))
    return t * TREND_SCALE_MAX


def blend(trend: float, combined_adj: Optional[float], has_signal: bool) -> float:
    if not has_signal or combined_adj is None:
        return float(trend)
    s_idx = survey_to_index(float(combined_adj))
    return TREND_WEIGHT * float(trend) + SURVEY_WEIGHT * s_idx


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _display_from_key(key: str) -> str:
    if not key:
        return ""
    return key[0].upper() + key[1:] if len(key) > 1 else key.upper()


def write_species_popularity_list_json(
    path: Path,
    built_utc: str,
    panel_source: str,
    sparklez_source: Optional[str],
    adj_by: Dict[str, List[float]],
    sparklez_by: Dict[str, int],
    sparklez_adj: Dict[str, float],
    sparklez_display: Dict[str, str],
    trends_rows: List[Dict[str, Any]],
) -> int:
    """Full-market species table for analytics UI (union of Sparklez, panel, and TCG trends file)."""
    trends_by_key: Dict[str, Dict[str, Any]] = {}
    for r in trends_rows:
        if not isinstance(r, dict):
            continue
        k = norm_name(str(r.get("Character") or ""))
        if k:
            trends_by_key[k] = r

    trend_vals = [
        float(r["Trend_Index_Average"])
        for r in trends_rows
        if isinstance(r, dict) and _safe_float(r.get("Trend_Index_Average")) is not None
    ]
    trend_median = float(statistics.median(trend_vals)) if trend_vals else 0.0

    all_keys = set(sparklez_by.keys()) | set(adj_by.keys()) | set(trends_by_key.keys())
    rows_out: List[Dict[str, Any]] = []

    for key in all_keys:
        if not key:
            continue
        tr = trends_by_key.get(key)
        display = (
            str(tr.get("Character") or "").strip()
            if tr
            else (sparklez_display.get(key) or _display_from_key(key))
        )
        trend_f = _safe_float(tr.get("Trend_Index_Average")) if tr else None

        adjs = adj_by.get(key) or []
        panel_mean: Optional[float] = sum(adjs) / len(adjs) if adjs else None
        panel_pc = len(adjs)

        sz_votes = int(sparklez_by[key]) if key in sparklez_by and sparklez_by[key] > 0 else None
        sz_adj = sparklez_adj.get(key)

        combined_parts: List[float] = []
        if panel_mean is not None:
            combined_parts.append(float(panel_mean))
        if sz_adj is not None:
            combined_parts.append(float(sz_adj))
        combined_adj: Optional[float] = (
            sum(combined_parts) / len(combined_parts) if combined_parts else None
        )

        has_signal = combined_adj is not None
        trend_for_blend = trend_f if trend_f is not None else trend_median
        pop_sort = (
            float(tr["Popularity_Index"])
            if tr and _safe_float(tr.get("Popularity_Index")) is not None
            else blend(trend_for_blend, combined_adj, has_signal)
        )

        rows_out.append(
            {
                "species_key": key,
                "display_name": display,
                "in_tcg_trends_file": bool(tr),
                "trend_index_average": round(trend_f, 3) if trend_f is not None else None,
                "panel_survey_adj_mean": round(panel_mean, 3) if panel_mean is not None else None,
                "panel_poll_count": int(panel_pc),
                "sparklez_vote_total": int(sz_votes) if sz_votes is not None else None,
                "sparklez_synthetic_adj": round(float(sz_adj), 3) if sz_adj is not None else None,
                "survey_combined_adj_mean": round(float(combined_adj), 3) if combined_adj is not None else None,
                "popularity_index": round(float(tr["Popularity_Index"]), 3)
                if tr and _safe_float(tr.get("Popularity_Index")) is not None
                else round(float(pop_sort), 3),
                "popularity_index_sort": round(float(pop_sort), 3),
            }
        )

    rows_out.sort(key=lambda r: (-(r["popularity_index_sort"] or 0), r["display_name"] or ""))
    for i, r in enumerate(rows_out, start=1):
        r["rank"] = i

    doc: Dict[str, Any] = {
        "schema": "species_popularity_list",
        "schema_version": 1,
        "built_at_utc": built_utc,
        "panel_source": panel_source,
        "sparklez_source": sparklez_source,
        "trend_weight": TREND_WEIGHT,
        "survey_weight": SURVEY_WEIGHT,
        "median_trend_index_for_list_imputation": round(trend_median, 3),
        "species_count": len(rows_out),
        "species": rows_out,
    }
    path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return len(rows_out)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build Popularity_Index into google_trends_momentum.json")
    ap.add_argument("--in-json", type=Path, default=Path("google_trends_momentum.json"))
    ap.add_argument("--out-json", type=Path, default=Path("google_trends_momentum.json"))
    ap.add_argument("--panel-csv", type=Path, default=None, help="Local multi-year panel export")
    ap.add_argument("--panel-url", type=str, default=PANEL_DEFAULT_URL)
    ap.add_argument("--sparklez-csv", type=Path, default=None, help="Local Sparklez export")
    ap.add_argument("--sparklez-url", type=str, default=SPARKLEZ_DEFAULT_URL)
    ap.add_argument("--no-sparklez", action="store_true", help="Do not fetch or merge Sparklez votes")
    ap.add_argument("--dry-run", action="store_true", help="Print summary only; do not write")
    ap.add_argument(
        "--meta-json",
        type=Path,
        default=Path("species_popularity_build_meta.json"),
        help="Write build metadata (weights, source URLs, timestamp)",
    )
    ap.add_argument(
        "--list-json",
        type=Path,
        default=Path("species_popularity_list.json"),
        help="Write full species popularity list for analytics.html (empty path to skip)",
    )
    args = ap.parse_args()

    raw_rows: List[Dict[str, Any]] = json.loads(args.in_json.read_text(encoding="utf-8"))
    if not isinstance(raw_rows, list):
        print("Input must be a JSON array", file=sys.stderr)
        return 1

    if args.panel_csv:
        panel_text = args.panel_csv.read_text(encoding="utf-8", errors="replace")
    else:
        print("Fetching multi-year panel CSV…", file=sys.stderr)
        panel_text = fetch_text(args.panel_url)

    adj_by = parse_panel_adjusted_ranks(panel_text)

    sparklez_by: Dict[str, int] = {}
    sparklez_adj: Dict[str, float] = {}
    sparklez_stats: Dict[str, int] = {}
    sparklez_display: Dict[str, str] = {}
    sparklez_src_meta: Optional[str] = None
    if not args.no_sparklez:
        if args.sparklez_csv:
            sz_src = args.sparklez_csv
            sz_text = sz_src.read_text(encoding="utf-8", errors="replace")
            sparklez_src_meta = str(sz_src)
            print(f"Reading Sparklez from {sz_src} …", file=sys.stderr)
        elif SPARKLEZ_DEFAULT_CSV.exists():
            sz_src = SPARKLEZ_DEFAULT_CSV
            sz_text = sz_src.read_text(encoding="utf-8", errors="replace")
            sparklez_src_meta = str(sz_src)
            print(f"Reading Sparklez from {sz_src} …", file=sys.stderr)
        else:
            print("Fetching Sparklez CSV…", file=sys.stderr)
            sz_text = fetch_text(args.sparklez_url)
            sparklez_src_meta = args.sparklez_url
        sparklez_by, sparklez_stats, sparklez_display = parse_sparklez_all_pokemon_rankings(sz_text)
        sparklez_adj = sparklez_votes_to_synthetic_adj(sparklez_by)
        print(
            f"Sparklez base species (summed variants): {len(sparklez_by)} | "
            f"ranking rows parsed: {sparklez_stats.get('ranking_rows', 0)} | "
            f"vote column sum: {sparklez_stats.get('vote_sum_check', 0)}",
            file=sys.stderr,
        )

    built = datetime.now(timezone.utc).isoformat()
    out_rows: List[Dict[str, Any]] = []
    n_panel = 0
    n_sparklez = 0
    n_both = 0

    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        base = {k: v for k, v in row.items() if k not in OUTPUT_KEYS_STRIP}
        ch = str(base.get("Character") or "").strip()
        trend = base.get("Trend_Index_Average")
        try:
            trend_f = float(trend)
        except (TypeError, ValueError):
            trend_f = 0.0
        key = norm_name(ch)

        adjs = adj_by.get(key) or []
        panel_mean: Optional[float] = None
        panel_pc = 0
        if adjs:
            panel_mean = sum(adjs) / len(adjs)
            panel_pc = len(adjs)
            n_panel += 1

        sz_votes = sparklez_by.get(key)
        sz_adj = sparklez_adj.get(key) if key in sparklez_adj else None
        if sz_votes is not None and sz_votes > 0:
            n_sparklez += 1

        combined_parts: List[float] = []
        if panel_mean is not None:
            combined_parts.append(float(panel_mean))
        if sz_adj is not None:
            combined_parts.append(float(sz_adj))
        if len(combined_parts) == 2:
            n_both += 1

        combined_adj: Optional[float] = None
        if combined_parts:
            combined_adj = sum(combined_parts) / len(combined_parts)

        has_signal = combined_adj is not None
        pop = blend(trend_f, combined_adj, has_signal)

        new_row = dict(base)
        new_row["Trend_Index_Average"] = trend_f
        new_row["Survey_AdjustedRank_Mean"] = round(float(panel_mean), 3) if panel_mean is not None else None
        new_row["Survey_Poll_Count"] = int(panel_pc)
        new_row["Sparklez_MaxVotes"] = int(sz_votes) if sz_votes is not None else None
        new_row["Sparklez_SyntheticAdj"] = round(float(sz_adj), 3) if sz_adj is not None else None
        new_row["Survey_CombinedAdj_Mean"] = round(float(combined_adj), 3) if combined_adj is not None else None
        new_row["Popularity_Index"] = round(pop, 3)
        out_rows.append(new_row)

    print(
        f"Rows: {len(out_rows)} | Panel overlap: {n_panel} | Sparklez overlap: {n_sparklez} | Both: {n_both}",
        file=sys.stderr,
    )
    if args.dry_run:
        return 0
    text = json.dumps(out_rows, indent=4, ensure_ascii=False) + "\n"
    args.out_json.write_text(text, encoding="utf-8")
    meta = {
        "built_at_utc": built,
        "panel_source": str(args.panel_csv) if args.panel_csv else args.panel_url,
        "sparklez_source": None if args.no_sparklez else sparklez_src_meta,
        "sparklez_base_species_count": len(sparklez_by),
        "sparklez_ranking_rows": sparklez_stats.get("ranking_rows", 0),
        "sparklez_vote_column_sum": sparklez_stats.get("vote_sum_check", 0),
        "trend_weight": TREND_WEIGHT,
        "survey_weight": SURVEY_WEIGHT,
        "survey_adj_range": [SURVEY_ADJ_MIN, SURVEY_ADJ_MAX],
        "trend_scale_max": TREND_SCALE_MAX,
        "rows_out": len(out_rows),
        "rows_with_panel": n_panel,
        "rows_with_sparklez": n_sparklez,
        "rows_with_both_surveys": n_both,
    }
    args.meta_json.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"Wrote {args.out_json} and {args.meta_json}", file=sys.stderr)

    list_path = args.list_json
    if list_path and str(list_path).strip():
        panel_src_meta = str(args.panel_csv) if args.panel_csv else args.panel_url
        n_list = write_species_popularity_list_json(
            list_path,
            built,
            panel_src_meta,
            None if args.no_sparklez else sparklez_src_meta,
            adj_by,
            sparklez_by,
            sparklez_adj,
            sparklez_display,
            out_rows,
        )
        print(f"Wrote {list_path} ({n_list} species)", file=sys.stderr)
        meta["species_popularity_list"] = str(list_path)
        meta["species_popularity_list_count"] = n_list
        args.meta_json.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
