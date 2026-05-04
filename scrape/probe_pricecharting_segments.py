#!/usr/bin/env python3
"""
Probe PriceCharting /console/{segment} URLs for every Explorer set in pokemon_sets_data.json.

Builds candidate segment slugs from set_name (with grouping rules similar to Crown Zenith +
Galarian Gallery: satellite sets map to the parent expansion's console segment).

Does not modify pokemon_sets_data.json. Writes a JSON report (default: pricecharting_segment_probe_report.json).

Examples (PriceCharting naming):
  Base -> pokemon-base-set
  Skyridge -> pokemon-skyridge
  EX Ruby & Sapphire -> pokemon-ruby-&-sapphire (ampersand in path)
  EX Team Magma vs Team Aqua -> pokemon-team-magma-&-team-aqua
  Crown Zenith Galarian Gallery -> same console as Crown Zenith (pokemon-crown-zenith)
  Brilliant Stars Trainer Gallery -> pokemon-brilliant-stars
  Hidden Fates Shiny Vault -> pokemon-hidden-fates (same console as Hidden Fates)
  Shining Fates Shiny Vault -> pokemon-shining-fates

  python scrape/probe_pricecharting_segments.py --sleep 0.25
  python scrape/probe_pricecharting_segments.py --only-set-codes base1,ecard3,ex1,ex5,swsh12pt5gg --no-card-score
  python scrape/probe_pricecharting_segments.py --dry-run-candidates  # print candidates only, no HTTP
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
SCR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCR))

from sync_pricecharting import (  # noqa: E402
    build_slug_index,
    http_get,
    load_set_paths,
    parse_console_slugs,
    pc_console_url,
    resolve_slug,
)

# Explorer set_name (exact case in JSON) -> primary PC segment (verified or hand-checked).
EXACT_SET_NAME_SEGMENT: Dict[str, str] = {
    "Base": "pokemon-base-set",
}


def effective_label_and_grouping(set_name: str, set_code: str) -> Tuple[str, Optional[str]]:
    """
    Return (label used to derive slug candidates, grouping note).
    """
    raw = (set_name or "").strip()
    if not raw:
        return raw, None

    m = re.match(r"^(.+?)\s+Trainer Gallery$", raw, flags=re.I)
    if m:
        return m.group(1).strip(), "trainer_gallery_parent"

    if re.search(r"Crown Zenith", raw, re.I) and re.search(r"Galarian Gallery", raw, re.I):
        return "Crown Zenith", "crown_zenith_galarian_gallery_parent"

    # Shiny Vault (subset) cards live on the parent expansion console (e.g. Hidden Fates Shiny Vault -> pokemon-hidden-fates).
    m = re.match(r"^(.+?)\s+Shiny Vault$", raw, flags=re.I)
    if m:
        return m.group(1).strip(), "shiny_vault_parent"

    return raw, None


def strip_ex_prefix(label: str) -> str:
    return re.sub(r"(?i)^ex\s+", "", label.strip())


def slugify_segment_core(s: str) -> str:
    """Lowercase path core: alnum and & only, hyphen-separated."""
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9&]+", "-", s)
    return re.sub(r"-+", "-", s).strip("-")


def segment_core_variants(label: str) -> List[str]:
    """
    Return unique slug bodies (without pokemon- prefix) to try for this label.
    """
    base = strip_ex_prefix(label)
    bodies: List[str] = []

    def add_body(b: str) -> None:
        b = slugify_segment_core(b)
        if b and b not in bodies:
            bodies.append(b)

    add_body(base)
    if re.search(r"\bvs\b", base, flags=re.I):
        swapped = re.sub(r"\bvs\b", "&", base, flags=re.I)
        add_body(swapped)

    return bodies


def pokemon_segments_for_label(label: str) -> List[str]:
    cores = segment_core_variants(label)
    return ["pokemon-" + c for c in cores if c]


def build_candidate_segments(set_name: str, set_code: str) -> Tuple[List[str], Optional[str], str]:
    """
    Returns (ordered unique segment list, grouping tag, effective label string).
    """
    label, group = effective_label_and_grouping(set_name, set_code)
    out: List[str] = []

    exact = EXACT_SET_NAME_SEGMENT.get(set_name.strip())
    if exact and exact not in out:
        out.append(exact)

    for seg in pokemon_segments_for_label(label):
        if seg not in out:
            out.append(seg)

    # Rare fallback: hyphenated set_code (almost never right for PC, but cheap in dry-run)
    code_slug = slugify_segment_core(set_code or "")
    if code_slug:
        guess = "pokemon-" + code_slug
        if guess not in out:
            out.append(guess)

    return out, group, label


def pop_set_url(segment: str) -> str:
    return "https://www.pricecharting.com/pop/set/" + quote(segment, safe="")


def score_console_against_cards(segment: str, html: str, top_cards: List[Dict[str, Any]]) -> Tuple[int, int]:
    slugs = parse_console_slugs(html, segment)
    if not slugs:
        return 0, 0
    by_num = build_slug_index(slugs)
    hit = 0
    n = 0
    for c in top_cards[:25]:
        if not isinstance(c, dict):
            continue
        n += 1
        if resolve_slug(slugs, by_num, str(c.get("name") or ""), c.get("number")):
            hit += 1
    return hit, n


def probe_one_set(
    row: Dict[str, Any],
    *,
    sleep_s: float,
    card_score: bool,
) -> Dict[str, Any]:
    set_code = str(row.get("set_code") or "").strip().lower()
    set_name = str(row.get("set_name") or "").strip()
    candidates, grouping, eff_label = build_candidate_segments(set_name, set_code)
    top = row.get("top_25_cards")
    top_cards = [c for c in top if isinstance(c, dict)] if isinstance(top, list) else []

    tried: List[Dict[str, Any]] = []
    best: Optional[Dict[str, Any]] = None

    for seg in candidates:
        curl = pc_console_url(seg)
        rec: Dict[str, Any] = {
            "segment": seg,
            "console_url": curl,
            "pop_set_url": pop_set_url(seg),
        }
        try:
            html = http_get(curl, sleep_s=sleep_s)
        except HTTPError as e:
            rec["http_status"] = e.code
            rec["slug_count"] = 0
            rec["error"] = repr(e)
            tried.append(rec)
            time.sleep(sleep_s)
            continue
        except BaseException as e:
            rec["http_status"] = None
            rec["slug_count"] = 0
            rec["error"] = repr(e)
            tried.append(rec)
            time.sleep(sleep_s)
            continue

        rec["http_status"] = 200
        slugs = parse_console_slugs(html, seg)
        rec["slug_count"] = len(slugs)
        if card_score and top_cards:
            hits, n = score_console_against_cards(seg, html, top_cards)
            rec["card_slug_hits"] = hits
            rec["card_slug_sampled"] = n
        else:
            rec["card_slug_hits"] = None
            rec["card_slug_sampled"] = None

        tried.append(rec)
        if rec["slug_count"] > 0:
            if best is None:
                best = rec
            elif card_score and top_cards:
                bh = int(best.get("card_slug_hits") or 0)
                ch = int(rec.get("card_slug_hits") or 0)
                if ch > bh:
                    best = rec

        time.sleep(sleep_s)

    return {
        "set_code": set_code,
        "set_name": set_name,
        "effective_label": eff_label,
        "grouping": grouping,
        "best_segment": best["segment"] if best else None,
        "best_console_url": best["console_url"] if best else None,
        "best_slug_count": best["slug_count"] if best else 0,
        "best_card_hits": best.get("card_slug_hits") if best else None,
        "candidates": tried,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Probe PriceCharting console segments for Explorer sets")
    ap.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--output", type=Path, default=ROOT / "pricecharting_segment_probe_report.json")
    ap.add_argument("--sleep", type=float, default=0.22)
    ap.add_argument("--only-set-codes", default="", help="Comma-separated set_code filter")
    ap.add_argument("--max-sets", type=int, default=0, help="0 = no cap (all matching filter)")
    ap.add_argument("--no-card-score", action="store_true", help="Only check console HTML slug counts")
    ap.add_argument("--dry-run-candidates", action="store_true", help="Print candidate segments; no network")
    args = ap.parse_args()

    data = json.loads(args.input.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        print("Expected JSON array of sets", file=sys.stderr)
        return 2

    filt = None
    if args.only_set_codes.strip():
        filt = {x.strip().lower() for x in args.only_set_codes.split(",") if x.strip()}

    existing = load_set_paths()

    rows = []
    for s in data:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        if not sc or (filt is not None and sc not in filt):
            continue
        rows.append(s)

    if args.max_sets and args.max_sets > 0:
        rows = rows[: args.max_sets]

    if args.dry_run_candidates:
        for s in rows:
            sn = str(s.get("set_name") or "")
            sc = str(s.get("set_code") or "")
            cands, grp, lab = build_candidate_segments(sn, sc)
            cur = existing.get(sc.lower())
            print(f"{sc}\t{sn!r}\tgroup={grp}\tlabel={lab!r}\tcurrent_map={cur!r}")
            for c in cands:
                print(f"    -> {c}")
        return 0

    report: Dict[str, Any] = {
        "sets": [],
        "mapped_in_pricecharting_set_paths": existing,
    }
    sleep_s = max(0.0, args.sleep)
    card_score = not args.no_card_score

    for i, s in enumerate(rows):
        sc = str(s.get("set_code") or "")
        print(f"[{i + 1}/{len(rows)}] probing {sc} …", flush=True)
        one = probe_one_set(s, sleep_s=sleep_s, card_score=card_score)
        report["sets"].append(one)
        bs = one.get("best_segment")
        print(
            f"    best={bs!r} slugs={one.get('best_slug_count')} card_hits={one.get('best_card_hits')}",
            flush=True,
        )

    not_found = [x for x in report["sets"] if not x.get("best_segment")]
    found = [x for x in report["sets"] if x.get("best_segment")]
    report["summary"] = {
        "console_base": "https://www.pricecharting.com/console/",
        "total_sets_probed": len(report["sets"]),
        "found_count": len(found),
        "not_found_count": len(not_found),
        "not_found": [
            {
                "set_code": x["set_code"],
                "set_name": x["set_name"],
                "effective_label": x.get("effective_label"),
                "grouping": x.get("grouping"),
                "candidates_tried": [
                    {
                        "segment": c.get("segment"),
                        "http_status": c.get("http_status"),
                        "slug_count": c.get("slug_count"),
                        "error": c.get("error"),
                    }
                    for c in (x.get("candidates") or [])
                ],
            }
            for x in not_found
        ],
        "grouping_applied": sorted({x["grouping"] for x in report["sets"] if x.get("grouping")}, key=str),
    }

    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print("Wrote", args.output, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
