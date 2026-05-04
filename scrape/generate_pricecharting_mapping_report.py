#!/usr/bin/env python3
"""
Build a markdown review document from probe_pricecharting_segments.py JSON output.

Optionally verifies prior “missing” sets against a **full** saved PriceCharting
`pokemon-promo` checklist HTML (Save As after scrolling the console so all
`/game/pokemon-promo/…` links exist in the file — live fetch only has ~150 until scroll).

  python scrape/generate_pricecharting_mapping_report.py
  python scrape/generate_pricecharting_mapping_report.py \\
    --promo-html \"C:/Users/you/Downloads/Checklist_ Pokemon Promo Pokemon Cards.html\"
  python scrape/generate_pricecharting_mapping_report.py --skip-promo-verify

By default the markdown **only lists sets that still lack a proper match** (no dedicated console
and not 100% on the scrolled `pokemon-promo` checklist). Use `--include-all-matches` to append the
full dedicated-console and promo-only match tables.

Also copy a full checklist to `tmp/pricecharting_pokemon_promo_checklist.html` to enable
promo verification without passing `--promo-html` each time.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
SCR = ROOT / "scrape"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCR))

from sync_pricecharting import (  # noqa: E402
    build_slug_index,
    load_console_slugs_from_saved_html,
    pc_console_url,
    resolve_slug,
)

PROMO_SEGMENT = "pokemon-promo"
DEFAULT_PROMO_HTML = ROOT / "tmp" / "pricecharting_pokemon_promo_checklist.html"


def esc_cell(s: str) -> str:
    return (s or "").replace("|", "\\|").replace("\n", " ").strip()


def _explorer_sets_by_code(data_path: Path) -> Dict[str, Dict[str, Any]]:
    raw = json.loads(data_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit("pokemon_sets_data.json must be a JSON array")
    out: Dict[str, Dict[str, Any]] = {}
    for s in raw:
        if isinstance(s, dict):
            sc = str(s.get("set_code") or "").strip().lower()
            if sc:
                out[sc] = s
    return out


def promo_match_stats(
    explorer_set: Dict[str, Any],
    promo_slugs: List[str],
    promo_by_num: Dict[str, List[str]],
) -> Tuple[int, int, List[str]]:
    """Returns (matched_count, total_cards_considered, unmatched_summaries)."""
    top = explorer_set.get("top_25_cards")
    if not isinstance(top, list):
        return 0, 0, []
    cards = [c for c in top if isinstance(c, dict)]
    if not cards:
        return 0, 0, []
    unmatched: List[str] = []
    matched = 0
    for c in cards:
        nm = c.get("name")
        num = c.get("number")
        slug = resolve_slug(promo_slugs, promo_by_num, str(nm or ""), num)
        if slug:
            matched += 1
        else:
            unmatched.append(f"{nm!s} #{num}".replace("'", ""))
    return matched, len(cards), unmatched


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=ROOT / "tmp" / "pc_console_probe_full.json")
    ap.add_argument("--pokemon-sets", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--output", type=Path, default=ROOT / "pricecharting_console_mapping_report.md")
    ap.add_argument(
        "--promo-html",
        type=Path,
        default=None,
        help="Saved full-scroll checklist HTML for pokemon-promo. If omitted, uses "
        f"{DEFAULT_PROMO_HTML.relative_to(ROOT)} when that file exists.",
    )
    ap.add_argument(
        "--skip-promo-verify",
        action="store_true",
        help="Do not run pokemon-promo checklist verification (section 1 = probe missing only).",
    )
    ap.add_argument(
        "--include-all-matches",
        action="store_true",
        help="Append full tables for dedicated-console matches and full promo checklist matches.",
    )
    args = ap.parse_args()

    inp = args.input.resolve()
    if not inp.is_file():
        print(f"Missing input: {inp}\nRun: python scrape/probe_pricecharting_segments.py --no-card-score --output {inp}")
        return 2

    sets_path = args.pokemon_sets.resolve()
    if not sets_path.is_file():
        print(f"Missing: {sets_path}")
        return 2

    raw = json.loads(inp.read_text(encoding="utf-8"))
    sets = raw.get("sets")
    if not isinstance(sets, list):
        print("Invalid JSON: expected top-level 'sets' array")
        return 2

    probe_missing = [x for x in sets if isinstance(x, dict) and not x.get("best_segment")]
    found = [x for x in sets if isinstance(x, dict) and x.get("best_segment")]
    probe_missing.sort(key=lambda x: str(x.get("set_code") or "").lower())
    found.sort(key=lambda x: str(x.get("set_code") or "").lower())

    by_code = _explorer_sets_by_code(sets_path)

    promo_html_path: Optional[Path] = None
    promo_slugs: List[str] = []
    promo_by_num: Dict[str, List[str]] = {}
    promo_note = ""

    if not args.skip_promo_verify:
        p = args.promo_html
        if p is None:
            p = DEFAULT_PROMO_HTML if DEFAULT_PROMO_HTML.is_file() else None
        else:
            p = p.resolve()
            if not p.is_file():
                print(f"Warning: --promo-html not found: {p}; promo verification skipped.")
                p = None
        if p is not None and p.is_file():
            promo_html_path = p
            promo_slugs = load_console_slugs_from_saved_html(p, PROMO_SEGMENT)
            promo_by_num = build_slug_index(promo_slugs)
            promo_note = (
                f"Promo checklist file `{p}` — **{len(promo_slugs)}** unique `/game/{PROMO_SEGMENT}/…` slugs "
                f"(saved page after full scroll; initial network HTML has far fewer links)."
            )

    still_missing: List[Dict[str, Any]] = []
    promo_verified: List[Dict[str, Any]] = []

    if promo_html_path and promo_slugs:
        for row in probe_missing:
            sc = str(row.get("set_code") or "").strip().lower()
            ex = by_code.get(sc)
            if not ex:
                still_missing.append(
                    {
                        "set_code": sc,
                        "set_name": row.get("set_name"),
                        "promo_matched": None,
                        "promo_total": None,
                        "unmatched": ["(set_code not in pokemon_sets_data.json)"],
                    }
                )
                continue
            m, t, bad = promo_match_stats(ex, promo_slugs, promo_by_num)
            if t == 0:
                still_missing.append(
                    {
                        "set_code": sc,
                        "set_name": row.get("set_name"),
                        "promo_matched": 0,
                        "promo_total": 0,
                        "unmatched": ["(no top_25_cards rows)"],
                    }
                )
            elif m == t:
                promo_verified.append(
                    {
                        "set_code": sc,
                        "set_name": row.get("set_name"),
                        "promo_slugs": len(promo_slugs),
                    }
                )
            else:
                still_missing.append(
                    {
                        "set_code": sc,
                        "set_name": row.get("set_name"),
                        "promo_matched": m,
                        "promo_total": t,
                        "unmatched": bad,
                    }
                )
        promo_verified.sort(key=lambda x: str(x.get("set_code") or "").lower())
        still_missing.sort(key=lambda x: str(x.get("set_code") or "").lower())
    else:
        still_missing = [
            {
                "set_code": x.get("set_code"),
                "set_name": x.get("set_name"),
                "promo_matched": None,
                "promo_total": None,
                "unmatched": [],
            }
            for x in probe_missing
        ]

    try:
        inp_rel = inp.relative_to(ROOT)
    except ValueError:
        inp_rel = inp
    iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines: list[str] = [
        "# PriceCharting — sets without a proper match",
        "",
        f"Generated **{iso}** from probe output: `{inp_rel}`.",
        "",
        "**Proper match** here means: a dedicated PriceCharting console segment from the name probe, "
        f"**or** (when promo verification runs) every Explorer `top_25_cards` row resolves to a slug on "
        f"`{PROMO_SEGMENT}` using the scrolled checklist HTML.",
        "",
        "Console index pattern: `https://www.pricecharting.com/console/{segment}` (public HTML; used by "
        "`sync_pricecharting.py` to discover per-card `/game/{segment}/…` links).",
        "",
    ]
    if promo_note:
        lines.append(promo_note)
        lines.append("")
    elif not args.skip_promo_verify:
        lines.append(
            f"*Promo verification skipped:* no checklist file at `{DEFAULT_PROMO_HTML.relative_to(ROOT)}` and "
            "no `--promo-html` path given. Save the fully scrolled Pokemon Promo console page from "
            "`https://www.pricecharting.com/console/pokemon-promo` and pass `--promo-html`, or copy it to "
            f"`{DEFAULT_PROMO_HTML.relative_to(ROOT)}`."
        )
        lines.append("")
    lines.extend(["---", "", "## Summary", "",])
    lines.append(
        f"- Dedicated console match (probe): **{len(found)}** sets  \n"
        f"- Full `pokemon-promo` checklist match (all top-25 cards): **{len(promo_verified)}** sets  \n"
        f"- **Still need mapping:** **{len(still_missing)}** set(s) below  \n"
    )
    lines.append("")
    lines.append(
        "Run `python scrape/generate_pricecharting_mapping_report.py --include-all-matches` to append the full "
        "matched-set reference tables to this file."
    )
    lines.extend(["", "---", ""])

    n_miss = len(still_missing)
    lines.extend([f"## Sets without a proper match ({n_miss})", "",])
    if promo_html_path:
        lines.append(
            "Listed here: no dedicated console from the probe **and** not every `top_25_cards` row appears in the "
            "scrolled `pokemon-promo` slug list (`sync_pricecharting.load_console_slugs_from_saved_html`)."
        )
    else:
        lines.append(
            "Listed here: no dedicated PriceCharting console from the name probe. "
            "Promo verification was skipped — add `tmp/pricecharting_pokemon_promo_checklist.html` or `--promo-html` "
            "to test `pokemon-promo`."
        )
    lines.append("")

    if n_miss == 0:
        lines.append("*None — every Explorer set has either a dedicated console segment or a full promo checklist match.*")
    elif n_miss <= 15:
        for x in still_missing:
            sc = str(x.get("set_code") or "")
            sn = str(x.get("set_name") or "")
            pm, pt = x.get("promo_matched"), x.get("promo_total")
            lines.append(f"### `{esc_cell(sc)}` — {esc_cell(sn)}")
            lines.append("")
            if pm is not None and pt is not None and pt > 0:
                lines.append(f"- **Promo checklist:** {pm}/{pt} top-list cards matched on `{PROMO_SEGMENT}`")
            elif pt == 0:
                lines.append("- **Promo checklist:** no `top_25_cards` rows to test")
            else:
                lines.append("- **Promo checklist:** not run")
            bad = x.get("unmatched") or []
            if bad:
                lines.append("- **Unmatched cards** (need manual PC slug / numbering alignment):")
                for b in bad:
                    lines.append(f"  - {esc_cell(b)}")
            lines.append("")
    else:
        lines.extend(
            [
                "| Set ID (`set_code`) | Full set name (`set_name`) | Promo match (in checklist) | Notes |",
                "| --- | --- | --- | --- |",
            ]
        )
        for x in still_missing:
            sc = esc_cell(str(x.get("set_code") or ""))
            sn = esc_cell(str(x.get("set_name") or ""))
            pm = x.get("promo_matched")
            pt = x.get("promo_total")
            if pm is None and pt is None:
                pm_s = "—"
            elif pt == 0:
                pm_s = "0/0"
            else:
                pm_s = f"{pm}/{pt}"
            bad = x.get("unmatched") or []
            note = esc_cell("; ".join(bad[:6]) + ("; …" if len(bad) > 6 else ""))
            lines.append(f"| `{sc}` | {sn} | {pm_s} | {note} |")

    if not args.include_all_matches:
        out = args.output.resolve()
        out.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print("Wrote", out, "(unmatched only; use --include-all-matches for full tables)")
        if promo_html_path:
            print(f"Promo slugs from file: {len(promo_slugs)}; still missing: {n_miss}; promo-only matches: {len(promo_verified)}")
        return 0

    lines.extend(["", "---", "", f"## Reference: dedicated console matches ({len(found)})", "",])
    lines.append(
        "Each row is the **first** console segment that returned HTTP 200 and yielded parseable game links "
        "(from `probe_pricecharting_segments.py`). Confirm before bulk-adding to `pricecharting_set_paths.json`."
    )
    lines.extend(
        [
            "",
            "| Set ID (`set_code`) | Full set name (`set_name`) | Grouping note | PriceCharting `segment` | Console URL |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for x in found:
        sc = esc_cell(str(x.get("set_code") or ""))
        sn = esc_cell(str(x.get("set_name") or ""))
        grp = esc_cell(str(x.get("grouping") or "—"))
        seg = esc_cell(str(x.get("best_segment") or ""))
        url = esc_cell(str(x.get("best_console_url") or ""))
        lines.append(f"| `{sc}` | {sn} | {grp} | `{seg}` | {url} |")

    if promo_verified:
        lines.extend(
            [
                "",
                "---",
                "",
                f"## Reference: matched via `{PROMO_SEGMENT}` full checklist ({len(promo_verified)})",
                "",
                "These sets had **no** dedicated console hit in the probe, but **every** Explorer `top_25_cards` row "
                f"matched a `/game/{PROMO_SEGMENT}/…` slug using the same `resolve_slug` logic as `sync_pricecharting.py` "
                "against the scrolled/saved promo HTML.",
                "",
                "| Set ID (`set_code`) | Full set name (`set_name`) | Grouping note | PriceCharting `segment` | Console URL |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        promo_url = esc_cell(pc_console_url(PROMO_SEGMENT))
        for x in promo_verified:
            sc = esc_cell(str(x.get("set_code") or ""))
            sn = esc_cell(str(x.get("set_name") or ""))
            grp = "promo_checklist_full_html"
            lines.append(f"| `{sc}` | {sn} | {grp} | `{PROMO_SEGMENT}` | {promo_url} |")

    out = args.output.resolve()
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("Wrote", out)
    if promo_html_path:
        print(f"Promo slugs from file: {len(promo_slugs)}; still missing: {n_miss}; promo-only matches: {len(promo_verified)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
