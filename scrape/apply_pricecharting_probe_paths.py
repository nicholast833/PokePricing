#!/usr/bin/env python3
"""
Merge PriceCharting console segments from probe_pricecharting_segments.py output
into pricecharting_set_paths.json for sync_pricecharting.py.

  python scrape/apply_pricecharting_probe_paths.py
  python scrape/apply_pricecharting_probe_paths.py --probe tmp/pc_console_probe_full.json --dry-run

Rules:
- Every probe row with a non-empty ``best_segment`` becomes ``set_code -> segment``.
- Existing entries in pricecharting_set_paths.json are applied **after** the probe dict so
  manual overrides win (e.g. Shiny Vault on parent console, wb1 when probe had 404s).
- Keys starting with ``_`` in the paths file (e.g. ``_readme``) are preserved / refreshed.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROBE = ROOT / "tmp" / "pc_console_probe_full.json"
PATHS_FILE = ROOT / "pricecharting_set_paths.json"
REPORT_FILE = ROOT / "tmp" / "pricecharting_path_merge_report.json"

_README = (
    "Lowercase Explorer set_code -> PriceCharting game segment (/console/{segment}, /game/{segment}/…). "
    "Generated from probe JSON plus manual overrides in this file. "
    "Re-run: python scrape/probe_pricecharting_segments.py --no-card-score --output tmp/pc_console_probe_full.json "
    "then python scrape/apply_pricecharting_probe_paths.py"
)


def load_paths_file(path: Path) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Returns (raw dict for _readme preservation), normalized code -> segment."""
    if not path.is_file():
        return {}, {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}, {}
    seg: Dict[str, str] = {}
    for k, v in raw.items():
        sk = str(k).strip()
        if sk.startswith("_") or not isinstance(v, str):
            continue
        t = v.strip().strip("/")
        if sk.lower() and t:
            seg[sk.lower()] = t
    return raw, seg


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--probe", type=Path, default=DEFAULT_PROBE, help="Probe JSON (e.g. tmp/pc_console_probe_full.json)")
    ap.add_argument("--paths-out", type=Path, default=PATHS_FILE)
    ap.add_argument("--report", type=Path, default=REPORT_FILE)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    probe_path = args.probe.resolve()
    if not probe_path.is_file():
        print(f"Missing probe file: {probe_path}", file=sys.stderr)
        print("Run: python scrape/probe_pricecharting_segments.py --no-card-score --output tmp/pc_console_probe_full.json", file=sys.stderr)
        return 2

    doc = json.loads(probe_path.read_text(encoding="utf-8"))
    rows = doc.get("sets")
    if not isinstance(rows, list):
        print("Probe JSON must contain a 'sets' array", file=sys.stderr)
        return 2

    from_probe: Dict[str, str] = {}
    probe_missing: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        sc = str(row.get("set_code") or "").strip().lower()
        if not sc:
            continue
        bs = str(row.get("best_segment") or "").strip().strip("/")
        if bs:
            from_probe[sc] = bs
        else:
            probe_missing.append(
                {
                    "set_code": sc,
                    "set_name": row.get("set_name"),
                    "effective_label": row.get("effective_label"),
                    "grouping": row.get("grouping"),
                }
            )

    _, existing_segs = load_paths_file(args.paths_out.resolve())
    overlaid: Dict[str, str] = dict(from_probe)
    n_overlay = 0
    for k, v in existing_segs.items():
        if overlaid.get(k) != v:
            n_overlay += 1
        overlaid[k] = v

    # Build output object
    out_obj: Dict[str, Any] = {"_readme": _README}
    for k in sorted(overlaid.keys()):
        out_obj[k] = overlaid[k]

    sets_path = ROOT / "pokemon_sets_data.json"
    data_codes: List[str] = []
    if sets_path.is_file():
        data = json.loads(sets_path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            data_codes = sorted(
                {str(s.get("set_code") or "").strip().lower() for s in data if isinstance(s, dict) and s.get("set_code")}
            )

    in_data_no_path = [c for c in data_codes if c not in overlaid]
    report = {
        "probe_file": str(probe_path),
        "paths_out": str(args.paths_out),
        "from_probe_with_segment": len(from_probe),
        "after_overlay_total_keys": len(overlaid),
        "overlay_changed_or_added_from_existing_file": n_overlay,
        "probe_sets_without_best_segment": len(probe_missing),
        "set_codes_in_pokemon_sets_data_without_path": len(in_data_no_path),
        "probe_missing_sets": probe_missing,
        "explorer_set_codes_missing_paths": in_data_no_path,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote report: {args.report}")
    print(
        f"Paths: {len(overlaid)} set_code(s) | probe hits {len(from_probe)} | "
        f"probe no segment {len(probe_missing)} | Explorer sets still unmapped {len(in_data_no_path)}",
        flush=True,
    )

    if args.dry_run:
        print("Dry run: not writing pricecharting_set_paths.json", flush=True)
        return 0

    args.paths_out.write_text(json.dumps(out_obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {args.paths_out} ({len(overlaid)} mappings)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
