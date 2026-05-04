#!/usr/bin/env python3
"""
Merge PriceCharting.com loose / graded (etc.) prices into top_25_cards in pokemon_sets_data.json.
Also parses the “Full Price Guide” grade table into pricecharting_grade_prices (label → USD) when present.

Hobbyist use: fetches public HTML only (no TCGPlayer storefront). For each mapped set, downloads the
set console page once to discover /game/{segment}/{card-slug} links, matches cards by collector number
(and name when needed), then fetches each card page.

Mapping file: pricecharting_set_paths.json  (set_code lowercase -> segment, e.g. me3 -> pokemon-perfect-order).
Populate it from a full probe JSON plus manual overrides:

  python scrape/probe_pricecharting_segments.py --no-card-score --output tmp/pc_console_probe_full.json
  python scrape/apply_pricecharting_probe_paths.py

Segments may include a literal ampersand (URL-encoded on fetch), e.g. pokemon-ruby-&-sapphire.

Full console slug list: the live /console/ page often only includes the first chunk of links until scrolled.
To use a full saved page (Save As in the browser after scrolling), write HTML to:
  tmp/pricecharting_console_html/{urllib.parse.quote(segment, safe='')}.html
Example: tmp/pricecharting_console_html/pokemon-hidden-fates.html for segment pokemon-hidden-fates.
When that file exists, sync loads slugs from disk instead of HTTP for that segment.

Run:
  python scrape/sync_pricecharting.py --backup --sleep 0.2 --only-set-codes me3
  python scrape/sync_pricecharting.py --sleep 0.2 --only-set-codes wb1

Resolve PriceCharting `/game/{segment}/{slug}` URLs from each set’s console page only (no per-card HTTP;
keeps existing `pricecharting_*` price fields). Then run a normal sync for subsets that need grades:

  python scrape/sync_pricecharting.py --link-map-only --sleep 0.2
  python scrape/sync_pricecharting.py --backup --sleep 0.2 --only-set-codes me3,me2pt5,me2,me1
  python scrape/sync_pricecharting.py --backup --sleep 0.25 --exclude-set-codes me1,me2,me2pt5,me3 --quiet-cards
  # wb1 -> pokemon-2004-poke-card-creator (e.g. …/game/pokemon-2004-poke-card-creator/treecko-1)

By default the output JSON is atomically rewritten **after each mapped set** so stopping the process
(Ctrl+C / crash) does not lose completed sets. Use --no-checkpoint-every-set to write only at the end.

Timestamped copy of the live file (before a long run):
  python scripts/backup_pokemon_sets_data.py
"""

from __future__ import annotations

import argparse
import difflib
import html
import json
import re
import shutil
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from urllib.parse import quote, unquote
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPT_DIR))
from json_atomic_util import write_json_atomic  # noqa: E402
from tcgtracking_merge import norm_card_name  # noqa: E402

PC = "pricecharting_"
HEADERS = {"User-Agent": "Mozilla/5.0 PokemonTCG-Explorer/sync_pricecharting (hobbyist)"}
SET_PATHS_FILE = ROOT / "pricecharting_set_paths.json"
SKIPS_LOG = ROOT / "pricecharting_sync_skips.json"
# Optional: full saved console HTML per segment (Save As after scrolling). Filenames: urllib.parse.quote(segment, safe='') + ".html"
CONSOLE_HTML_DIR = ROOT / "tmp" / "pricecharting_console_html"


def norm_collector_key(s: str) -> str:
    t = str(s or "").strip().lower()
    if not t:
        return ""
    if t.isdigit():
        return str(int(t))
    return t


def load_set_paths() -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not SET_PATHS_FILE.is_file():
        return out
    raw = json.loads(SET_PATHS_FILE.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        sk = str(k).strip()
        if sk.startswith("_") or not isinstance(v, str):
            continue
        seg = v.strip().strip("/")
        if sk.lower() and seg:
            out[sk.lower()] = seg
    return out


def clear_pc_fields(card: Dict[str, Any]) -> None:
    for k in list(card.keys()):
        if k.startswith(PC):
            del card[k]


def _http_err_kind(exc: BaseException) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        return f"HTTP {exc.code}"
    return type(exc).__name__


def http_get(url: str, *, timeout: int = 50, attempts: int = 4, sleep_s: float = 0.0) -> str:
    last: Optional[BaseException] = None
    for i in range(attempts):
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", "replace")
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as e:
            last = e
            if isinstance(e, urllib.error.HTTPError) and e.code in (404, 410):
                raise
            if i + 1 < attempts:
                time.sleep(0.6 * (i + 1) + sleep_s)
    assert last is not None
    raise last


def pc_console_url(segment: str) -> str:
    """Console index URL; path-encodes segment (e.g. pokemon-ruby-&-sapphire)."""
    return "https://www.pricecharting.com/console/" + quote(segment, safe="")


def pc_card_url(segment: str, slug: str) -> str:
    seg = quote(segment, safe="")
    sl = quote(unquote(slug.strip()), safe="")
    return f"https://www.pricecharting.com/game/{seg}/{sl}"


def parse_console_slugs(html: str, segment: str) -> List[str]:
    # Links in HTML use &amp; while segment keys use raw & (e.g. pokemon-ruby-&-sapphire).
    html = html.replace("&amp;", "&")
    esc = re.escape(segment)
    found = re.findall(rf"/game/{esc}/([^\"'>\s]+)", html, flags=re.I)
    out: List[str] = []
    seen: set[str] = set()
    for s in found:
        sl = s.strip()
        if sl and sl not in seen:
            seen.add(sl)
            out.append(sl)
    return out


def load_console_slugs_from_saved_html(path: Path, segment: str) -> List[str]:
    """
    Parse /game/{segment}/… slugs from a full saved console or checklist HTML file.

    The live `/console/...` response often includes only the first screen of links; after scrolling,
    “Save As” in the browser captures the full DOM so promo (2000+ cards) can be matched locally.
    """
    html = path.read_text(encoding="utf-8", errors="replace")
    return parse_console_slugs(html, segment)


def slug_trailing_number(slug: str) -> Optional[str]:
    m = re.search(r"-(\d+)$", slug)
    if not m:
        return None
    return norm_collector_key(m.group(1))


def slug_match_base(slug: str) -> str:
    return re.sub(r"-\d+$", "", slug).replace("-", " ").lower()


def slugify_name_for_pc_url(name: str) -> str:
    s = norm_card_name(name or "").lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return re.sub(r"-+", "-", s)


def pick_best_slug(candidates: List[str], card_name: str) -> str:
    if len(candidates) == 1:
        return candidates[0]
    cn = norm_card_name(card_name or "").lower()
    cn_compact = re.sub(r"[^a-z0-9]+", " ", cn).strip()
    best = candidates[0]
    best_r = -1.0
    for s in candidates:
        base = slug_match_base(s)
        r = difflib.SequenceMatcher(None, cn_compact, base).ratio()
        if r > best_r:
            best_r = r
            best = s
    return best


def build_slug_index(slugs: List[str]) -> Dict[str, List[str]]:
    """
    Map collector-style keys -> slug(s). Keys include trailing digits (e.g. ...-9 -> \"9\")
    and full SV-style suffixes (e.g. ...-sv82 -> \"sv82\") so Explorer numbers like SV82 / SV076 match PC.
    """
    by_num: Dict[str, List[str]] = {}
    for s in slugs:
        n = slug_trailing_number(s)
        if n is not None:
            by_num.setdefault(n, []).append(s)
        m = re.search(r"-(sv\d+)$", s, flags=re.I)
        if m:
            key = norm_collector_key(m.group(1))
            if key:
                by_num.setdefault(key, []).append(s)
    return by_num


def console_saved_html_path(segment: str) -> Path:
    """Where to place a full-scroll saved console HTML file for this segment (path-encoded filename)."""
    return CONSOLE_HTML_DIR / (quote(segment, safe="") + ".html")


def resolve_slug(
    all_slugs: List[str],
    by_num: Dict[str, List[str]],
    card_name: str,
    card_number: Any,
) -> Optional[str]:
    nk = norm_collector_key(str(card_number or ""))
    if not nk:
        return None
    cands = by_num.get(nk)
    if cands:
        return pick_best_slug(cands, card_name)
    cands2 = [s for s in all_slugs if slug_trailing_number(s) == nk]
    if cands2:
        return pick_best_slug(cands2, card_name)
    hint = f"{slugify_name_for_pc_url(str(card_name))}-{nk}".strip("-")
    if hint:
        close = difflib.get_close_matches(hint, all_slugs, n=1, cutoff=0.72)
        if close:
            return close[0]
    best: Optional[str] = None
    best_r = 0.0
    for s in all_slugs:
        r = difflib.SequenceMatcher(None, hint, s).ratio()
        if r > best_r:
            best_r = r
            best = s
    if best is not None and best_r >= 0.55:
        return best
    return None


def parse_pricecharting_full_price_guide(page_html: str) -> Optional[Dict[str, float]]:
    """
    Parse the 'Full Price Guide' grade table (Ungraded, Grade 9.5, PSA 10, BGS 10 Black, …) into
    a flat dict label -> USD. HTML varies; we scan a window after the heading and match table rows.
    """
    low = page_html.lower()
    i = low.find("full price guide")
    if i < 0:
        return None
    chunk = page_html[i : i + 180000]
    prices: Dict[str, float] = {}
    # Typical row: <tr>...<td>PSA 10</td>...<td>...$4,102.08...
    row_pat = re.compile(
        r"<tr[^>]*>\s*<td[^>]*>\s*([^<]+?)\s*</td>\s*<td[^>]*>([\s\S]*?)</td>\s*</tr>",
        re.I,
    )
    price_pat = re.compile(r"\$\s*([\d,]+\.?\d*)")
    for m in row_pat.finditer(chunk):
        label = html.unescape(m.group(1)).strip()
        label = re.sub(r"\s+", " ", label).replace("\xa0", " ")
        if not label or len(label) > 96:
            continue
        inner = m.group(2)
        pm = price_pat.search(inner)
        if not pm:
            continue
        try:
            val = float(pm.group(1).replace(",", ""))
        except ValueError:
            continue
        if val <= 0:
            continue
        prices[label] = val
    if len(prices) >= 2:
        return prices
    return None


def parse_pricecharting_card_page(page_html: str, url: str) -> Optional[Dict[str, Any]]:
    can = re.search(r'rel="canonical" href="([^"]+)"', page_html)
    canonical = html.unescape(can.group(1)) if can else ""
    if "search-products" in canonical:
        return None

    def price_from_div(div_id: str) -> Optional[float]:
        # First <span class="price js-price"> in this cell only (avoid "change" $ deltas / next <td>).
        m = re.search(
            rf'id="{re.escape(div_id)}"[^>]*>\s*<span class="price js-price">\s*(.*?)\s*</span>',
            page_html,
            flags=re.I | re.S,
        )
        if not m:
            return None
        inner = re.sub(r"\s+", " ", m.group(1)).strip()
        if inner in ("-", "—", ""):
            return None
        pm = re.search(r"\$\s*([\d,]+\.?\d*)", inner)
        if not pm:
            return None
        try:
            return float(pm.group(1).replace(",", ""))
        except ValueError:
            return None

    out: Dict[str, Any] = {"url": url}
    for key, div in (
        ("used_price_usd", "used_price"),
        ("complete_price_usd", "complete_price"),
        ("graded_price_usd", "graded_price"),
        ("new_price_usd", "new_price"),
        ("box_only_price_usd", "box_only_price"),
    ):
        v = price_from_div(div)
        if v is not None:
            out[key] = v

    mpid = re.search(r'data-product-id="(\d+)"', page_html)
    if mpid:
        out["product_id"] = int(mpid.group(1))

    vg = re.search(r"VGPC\.chart_data\s*=\s*(\{[^;]+\});", page_html, re.S)
    if vg:
        try:
            out["chart_data"] = json.loads(vg.group(1))
        except json.JSONDecodeError:
            pass

    gp = parse_pricecharting_full_price_guide(page_html)
    if gp:
        out["grade_prices"] = gp
    vgp = re.search(r"VGPC\.product\s*=\s*\{([^}]+)\}", page_html, re.S)
    if vgp and "product_id" not in out:
        mm = re.search(r"\bid\s*:\s*(\d+)", vgp.group(1))
        if mm:
            out["product_id"] = int(mm.group(1))

    if len(out) <= 1 and not out.get("product_id"):
        return None
    return out


def record_skip(
    rep: Dict[str, Any],
    *,
    set_code: str,
    set_name: str,
    card_number: Any,
    card_name: Any,
    reason_code: str,
    detail: str = "",
) -> None:
    rep.setdefault("skips", []).append(
        {
            "set_code": str(set_code or "").strip(),
            "set_name": str(set_name or "").strip().replace("\t", " "),
            "card_number": str(card_number) if card_number is not None else "",
            "card_name": str(card_name or "").strip().replace("\t", " "),
            "reason_code": reason_code,
            "detail": str(detail or "").replace("\t", " ").replace("\n", " ")[:500],
        }
    )


def append_skips_log(*, input_path: Path, output_path: Path, rep: Dict[str, Any], max_runs: int = 80) -> None:
    run_entry = {
        "sync_iso": datetime.now(timezone.utc).isoformat(),
        "input": str(input_path),
        "output": str(output_path),
        "stats": {
            k: rep.get(k)
            for k in (
                "cards_considered",
                "cards_merged",
                "cards_skipped_no_slug",
                "cards_skipped_fetch",
                "cards_skipped_unparsed",
                "console_fetches",
                "card_fetches",
            )
        },
        "skips": rep.get("skips") or [],
    }
    prev: Dict[str, Any] = {"runs": []}
    if SKIPS_LOG.is_file():
        try:
            raw = json.loads(SKIPS_LOG.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and isinstance(raw.get("runs"), list):
                prev = raw
        except (json.JSONDecodeError, OSError):
            prev = {"runs": []}
    runs = [x for x in prev.get("runs") or [] if isinstance(x, dict)]
    runs.append(run_entry)
    if len(runs) > max_runs:
        runs = runs[-max_runs:]
    SKIPS_LOG.write_text(json.dumps({"runs": runs}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def run(
    input_path: Path,
    output_path: Path,
    *,
    sleep_s: float,
    only_set_codes: Optional[str],
    exclude_set_codes: Optional[str],
    max_sets: int,
    max_cards: int,
    checkpoint_every_set: bool = True,
    link_map_only: bool = False,
    quiet_cards: bool = False,
) -> Dict[str, Any]:
    data = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("Expected pokemon_sets_data.json as a JSON array")

    set_paths = load_set_paths()
    filter_codes = None
    if only_set_codes:
        filter_codes = {x.strip().lower() for x in only_set_codes.split(",") if x.strip()}
    exclude_codes = None
    if exclude_set_codes:
        exclude_codes = {x.strip().lower() for x in exclude_set_codes.split(",") if x.strip()}

    rep: Dict[str, Any] = {
        "sync_iso": datetime.now(timezone.utc).isoformat(),
        "cards_considered": 0,
        "cards_merged": 0,
        "cards_skipped_no_slug": 0,
        "cards_skipped_fetch": 0,
        "cards_skipped_unparsed": 0,
        "sets_skipped_unmapped": 0,
        "console_fetches": 0,
        "card_fetches": 0,
        "skips": [],
        "max_sets": max_sets,
        "max_cards": max_cards,
        "only_set_codes": only_set_codes or "",
        "exclude_set_codes": exclude_set_codes or "",
        "link_map_only": bool(link_map_only),
        "quiet_cards": bool(quiet_cards),
    }

    mapped_codes = sorted(set_paths.keys())
    jobs_total = 0
    for s in data:
        if not isinstance(s, dict):
            continue
        jsc = str(s.get("set_code") or "").strip().lower()
        if not jsc:
            continue
        if filter_codes is not None and jsc not in filter_codes:
            continue
        if exclude_codes is not None and jsc in exclude_codes:
            continue
        if not set_paths.get(jsc):
            continue
        if not isinstance(s.get("top_25_cards"), list):
            continue
        jobs_total += 1

    print(
        f"PriceCharting: {len(mapped_codes)} set(s) in {SET_PATHS_FILE.name}; "
        f"jobs={jobs_total} link_map_only={link_map_only} quiet_cards={quiet_cards} "
        f"checkpoint_every_set={checkpoint_every_set}; output={output_path}",
        flush=True,
    )

    sets_done = 0
    set_job_idx = 0
    cards_merged_cap = 0
    console_cache: Dict[str, Tuple[List[str], Dict[str, List[str]]]] = {}

    for s in data:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        if not sc:
            continue
        if filter_codes is not None and sc not in filter_codes:
            continue
        if exclude_codes is not None and sc in exclude_codes:
            continue
        segment = set_paths.get(sc)
        if not segment:
            rep["sets_skipped_unmapped"] += 1
            continue
        if max_sets and sets_done >= max_sets:
            break

        set_name = str(s.get("set_name") or "")
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            sets_done += 1
            continue

        set_job_idx += 1
        print(
            f"[PC PROGRESS] set {set_job_idx}/{jobs_total} code={sc!r} name={set_name[:56]!r}",
            flush=True,
        )

        if segment not in console_cache:
            saved = console_saved_html_path(segment)
            curl = pc_console_url(segment)
            used_saved_html = False
            try:
                if saved.is_file():
                    html = saved.read_text(encoding="utf-8", errors="replace")
                    used_saved_html = True
                    print(
                        f"[{sc}] console: loading slugs from saved HTML `{saved.relative_to(ROOT)}` "
                        f"(full scroll capture; avoids truncated live console)",
                        flush=True,
                    )
                else:
                    html = http_get(curl, sleep_s=sleep_s)
            except BaseException as e:
                rep["sets_skipped_unmapped"] += 1
                record_skip(
                    rep,
                    set_code=sc,
                    set_name=set_name,
                    card_number="",
                    card_name="",
                    reason_code="CONSOLE_FETCH_FAILED",
                    detail=f"{_http_err_kind(e)} url={curl} err={e!r}",
                )
                print(f"[{sc}] console fetch failed: {e!r}", flush=True)
                sets_done += 1
                continue
            slugs = parse_console_slugs(html, segment)
            console_cache[segment] = (slugs, build_slug_index(slugs))
            if not used_saved_html:
                rep["console_fetches"] += 1
            print(f"[{sc}] console: {len(slugs)} game slugs", flush=True)
            time.sleep(sleep_s)

        _slugs, by_num = console_cache[segment]

        merged_run_start = rep["cards_merged"]
        considered_run_start = rep["cards_considered"]

        for c in top:
            if not isinstance(c, dict):
                continue
            if max_cards and cards_merged_cap >= max_cards:
                break
            rep["cards_considered"] += 1
            nm = c.get("name")
            num = c.get("number")
            slug = resolve_slug(_slugs, by_num, str(nm or ""), num)
            if not slug:
                rep["cards_skipped_no_slug"] += 1
                if link_map_only:
                    c.pop(f"{PC}url", None)
                if not link_map_only:
                    record_skip(
                        rep,
                        set_code=sc,
                        set_name=set_name,
                        card_number=num,
                        card_name=nm,
                        reason_code="NO_SLUG_FOR_NUMBER",
                        detail=f"segment={segment}",
                    )
                print(
                    f"  CARD DONE pricecharting set={sc} name={nm!r} #{num} status=SKIP_NO_SLUG",
                    flush=True,
                )
                continue

            card_url = pc_card_url(segment, slug)
            if link_map_only:
                c[f"{PC}url"] = card_url
                rep["cards_merged"] += 1
                cards_merged_cap += 1
                if not quiet_cards:
                    print(
                        f"  CARD LINK-MAP set={sc} name={nm!r} #{num} slug={slug!r} url={card_url}",
                        flush=True,
                    )
                continue

            try:
                page = http_get(card_url, sleep_s=sleep_s)
                rep["card_fetches"] += 1
            except BaseException as e:
                rep["cards_skipped_fetch"] += 1
                record_skip(
                    rep,
                    set_code=sc,
                    set_name=set_name,
                    card_number=num,
                    card_name=nm,
                    reason_code="CARD_FETCH_FAILED",
                    detail=f"{_http_err_kind(e)} url={card_url}",
                )
                print(
                    f"  CARD DONE pricecharting set={sc} name={nm!r} #{num} status=SKIP_FETCH err={e!r}",
                    flush=True,
                )
                time.sleep(sleep_s)
                continue

            parsed = parse_pricecharting_card_page(page, card_url)
            if not parsed:
                rep["cards_skipped_unparsed"] += 1
                record_skip(
                    rep,
                    set_code=sc,
                    set_name=set_name,
                    card_number=num,
                    card_name=nm,
                    reason_code="PAGE_UNPARSED_OR_SEARCH",
                    detail=card_url,
                )
                print(
                    f"  CARD DONE pricecharting set={sc} name={nm!r} #{num} status=SKIP_UNPARSED url={card_url}",
                    flush=True,
                )
                time.sleep(sleep_s)
                continue

            clear_pc_fields(c)
            for k, v in parsed.items():
                if v is not None:
                    c[f"{PC}{k}"] = v
            rep["cards_merged"] += 1
            cards_merged_cap += 1
            if not quiet_cards:
                print(
                    f"  CARD DONE pricecharting set={sc} name={nm!r} #{num} status=MERGED slug={slug!r} "
                    f"(cumulative merged={rep['cards_merged']})",
                    flush=True,
                )
            time.sleep(sleep_s)

        if checkpoint_every_set:
            try:
                write_json_atomic(output_path, data)
            except OSError as e:
                print(f"[PC CHECKPOINT ERROR] {e!r}", flush=True)
                raise
            n_top = sum(1 for x in top if isinstance(x, dict))
            merged_this = rep["cards_merged"] - merged_run_start
            considered_this = rep["cards_considered"] - considered_run_start
            print(
                f"[PC SET CHECKPOINT] set={set_job_idx}/{jobs_total} code={sc!r} name={set_name[:56]!r} | "
                f"this_set top_rows={n_top} merged+={merged_this} considered+={considered_this} | "
                f"run totals merged={rep['cards_merged']} considered={rep['cards_considered']} "
                f"no_slug={rep['cards_skipped_no_slug']} fetch_fail={rep['cards_skipped_fetch']} "
                f"unparsed={rep['cards_skipped_unparsed']} | saved -> {output_path}",
                flush=True,
            )
        sets_done += 1

    write_json_atomic(output_path, data)
    try:
        append_skips_log(input_path=input_path, output_path=output_path, rep=rep)
        print(f"Wrote skip log: {SKIPS_LOG}", flush=True)
    except OSError as e:
        print(f"Warning: could not write {SKIPS_LOG}: {e}", flush=True)
    suffix = ".pricecharting_link_map_report.json" if link_map_only else ".pricecharting_sync_report.json"
    rep_path = output_path.parent / (output_path.name + suffix)
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(json.dumps(rep, indent=2))
    return rep


def main() -> int:
    ap = argparse.ArgumentParser(description="Merge PriceCharting prices into pokemon_sets_data.json")
    ap.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--output", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--sleep", type=float, default=0.2)
    ap.add_argument("--max-sets", type=int, default=0, help="0 = no cap")
    ap.add_argument("--max-cards", type=int, default=0, help="0 = no cap (per entire run)")
    ap.add_argument("--only-set-codes", default="", help="Comma-separated set_code values")
    ap.add_argument("--backup", action="store_true")
    ap.add_argument(
        "--no-checkpoint-every-set",
        action="store_true",
        help="Disable per-set atomic writes (default: rewrite JSON after each mapped set so Ctrl+C keeps progress).",
    )
    ap.add_argument(
        "--link-map-only",
        action="store_true",
        help="Only resolve /game/{segment}/{slug} URLs from console HTML (saved or fetch); do not fetch card pages.",
    )
    ap.add_argument(
        "--exclude-set-codes",
        default="",
        help="Comma-separated set_code values to skip (applied with --only-set-codes if both set).",
    )
    ap.add_argument(
        "--quiet-cards",
        action="store_true",
        help="Do not print per-card MERGED / LINK-MAP lines (still prints [PC PROGRESS], checkpoints, errors, SKIP_NO_SLUG).",
    )
    args = ap.parse_args()

    inp = args.input.resolve()
    out = args.output.resolve()
    if args.backup and inp == out and inp.is_file():
        bak = inp.with_suffix(inp.suffix + ".bak")
        shutil.copy2(inp, bak)
        print("Wrote backup", bak, flush=True)

    max_sets = args.max_sets if args.max_sets and args.max_sets > 0 else 0
    max_cards = args.max_cards if args.max_cards and args.max_cards > 0 else 0
    only = args.only_set_codes.strip() or None
    excl = args.exclude_set_codes.strip() or None

    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
        except Exception:
            try:
                sys.stdout.reconfigure(line_buffering=True)
            except Exception:
                pass

    run(
        inp,
        out,
        sleep_s=max(0.0, args.sleep),
        only_set_codes=only,
        exclude_set_codes=excl,
        max_sets=max_sets,
        max_cards=max_cards,
        checkpoint_every_set=not bool(args.no_checkpoint_every_set),
        link_map_only=bool(args.link_map_only),
        quiet_cards=bool(args.quiet_cards),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
