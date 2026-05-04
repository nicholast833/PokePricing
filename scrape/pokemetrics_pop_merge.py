"""
Attach PokéMetrics PSA-style population fields to pokemon_sets_data.json cards.

Data source: https://api.pokemetrics.org/ (static JSON; robots.txt on pokemetrics.org is open).
The SPA fetches `version.json`, `sets.json`, and per-set `sets/{slug}.json` — we mirror that.

Per card (matched by set + number + name + supertype when needed), we store:
  pokemetrics_psa_total_pop   — total graded count (`t` in API)
  pokemetrics_psa_gem10      — PSA 10 slab count (`ps` in API)
  pokemetrics_grade_breakdown — raw grade histogram (`f` in API)
  pokemetrics_set_slug        — which PokéMetrics set JSON was used
  pokemetrics_pm_index_set_name — PM index title for that slug (for tooltips)

On the **set** row (not duplicated per card): `pokemetrics_pop_report_version` = API `v`, `pokemetrics_pop_source`.
No snapshot ISO is written onto cards (avoids attaching a calendar “year” to each listing).

Set resolution (in order):
  1) pokemetrics_set_overrides.json entry keyed by your `set_code` → `{ "slug": "18ll" }`
  2) Auto: group PokéMetrics **English** product rows (excludes Thai / JP slugs / other-locale titles, etc.),
     using normalized titles with `XY EN-…` / `Svi EN-…` locale prefixes stripped for matching.
     When several slugs share a norm key, pick by **title similarity** to `set_name`, then PM card count `c`.
     Slug resolution does **not** use your set’s release year (population stays independent of card-era UX).
  Alternate population sources (no merge here yet): **GemRate** (https://gemrate.com/) aggregates PSA-style
  pops; contact them for data access if you outgrow PokéMetrics.

Override file example:
  { "base1": { "slug": "18ll" }, "sv1": { "slug": "2abc" } }

Run:
  python scrape/pokemetrics_pop_merge.py
  python scrape/pokemetrics_pop_merge.py --max-sets 5   # smoke test (first N sets in JSON order = oldest)
  python scrape/pokemetrics_pop_merge.py --trial-recent-sets 4 --no-backup   # ~4 newest sets with PM card hits (see --trial-tail-scan)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from tcgtracking_merge import norm_card_name, norm_card_number, norm_set_key

API_BASE = "https://api.pokemetrics.org"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; PokemonTCG-Explorer/1.0; +local merge)"}
CACHE_DIR = "pokemetrics_cache"
OVERRIDES_PATH = "pokemetrics_set_overrides.json"

PM_PREFIX = "pokemetrics_"

# PokéMetrics mixes EN product as "Pal EN-Paldea Evolved"; exclude other locales and obvious non-English product lines.
_PM_NON_EN_TITLE = re.compile(
    r"\s(?:FR|DE|IT|PT|ES)-|\sde-|\sfr-|\sit-|\spt-|\ses-|"
    r"Simplified|Traditional\s+Chinese|Traditional\s+Sv|Thai\s|Thai[-\s]|^Thai\s|"
    r"Japanese|Korean|Indonesian|Go\s+Chinese|Go\s+Korean|Go\s+Thai|Go\s+Indonesian",
    re.I,
)


def _http_get_json(url: str, timeout: int = 120) -> Any:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _cache_path(*parts: str) -> str:
    return os.path.join(CACHE_DIR, *parts)


def _ensure_dir(p: str) -> None:
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)


def fetch_version() -> Tuple[int, int]:
    data = _http_get_json(f"{API_BASE}/version.json", timeout=30)
    v = int(data.get("v", 0))
    d = int(data.get("d", 0))
    return v, d


def load_or_fetch_sets_index(version: int) -> Dict[str, Any]:
    os.makedirs(CACHE_DIR, exist_ok=True)
    vpath = _cache_path(f"v{version}", "sets.json")
    if os.path.isfile(vpath):
        with open(vpath, "r", encoding="utf-8") as f:
            return json.load(f)
    data = _http_get_json(f"{API_BASE}/sets.json", timeout=180)
    _ensure_dir(vpath)
    with open(vpath, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return data


def load_or_fetch_set_cards(version: int, slug: str) -> List[Dict[str, Any]]:
    vpath = _cache_path(f"v{version}", "sets", f"{slug}.json")
    if os.path.isfile(vpath):
        with open(vpath, "r", encoding="utf-8") as f:
            blob = json.load(f)
    else:
        time.sleep(0.15)
        blob = _http_get_json(f"{API_BASE}/sets/{slug}.json", timeout=90)
        _ensure_dir(vpath)
        with open(vpath, "w", encoding="utf-8") as f:
            json.dump(blob, f)
    cards = blob.get("c")
    if not isinstance(cards, list):
        return []
    return cards


def is_pm_english_row(slug: str, row: Dict[str, Any]) -> bool:
    s = str(slug or "").lower()
    if s.startswith("3ajp") or s.startswith("2pjp"):
        return False
    if re.fullmatch(r"[a-z0-9]{2,8}jp", s):
        return False
    if s.endswith("jp") and len(s) <= 10:
        return False
    n = row.get("n")
    if not isinstance(n, str):
        n = str(n or "")
    if _PM_NON_EN_TITLE.search(n):
        return False
    if re.search(r"(?i)\bchinese\b", n) and not re.search(r"(?i)\ben-\b", n):
        return False
    return True


def strip_pm_en_locale_prefix(title: str) -> str:
    """Turn 'Svi EN-Scarlet & Violet' / 'Mew EN-151' into the English product name for matching."""
    t = (title or "").strip()
    if not t:
        return ""
    if re.match(r"(?i)^mew\s+en-\s*", t):
        return re.sub(r"(?i)^mew\s+en-\s*", "", t).strip()
    m = re.match(r"(?i)^[a-z0-9]{2,6}\s+en-\s*(.+)$", t)
    if m:
        return m.group(1).strip()
    return t


def _user_set_title_variants(set_name: str) -> List[str]:
    raw = (set_name or "").strip()
    if not raw:
        return []
    out: List[str] = [raw]
    fixed = re.sub(r"(?i)firered", "Fire Red", raw)
    fixed = re.sub(r"(?i)leafgreen", "Leaf Green", fixed)
    if fixed != raw:
        out.append(fixed)
    if re.search(r"(?i)promos\b", raw):
        out.append(re.sub(r"(?i)promos\b", "promo", raw))
    return out


def _norm_keys_for_texts(texts: List[str]) -> List[str]:
    keys: List[str] = []
    for raw in texts:
        raw = (raw or "").strip()
        if not raw:
            continue
        nk = norm_set_key(raw)
        if nk:
            keys.append(nk)
        nk2 = norm_set_key(raw + " Set")
        if nk2 and nk2 != nk:
            keys.append(nk2)
    out: List[str] = []
    seen = set()
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _candidate_norm_keys(set_name: str) -> List[str]:
    texts: List[str] = []
    for v in _user_set_title_variants(set_name):
        texts.append(v)
    return _norm_keys_for_texts(texts)


def _pm_title_match_texts(pm_title: str) -> List[str]:
    t = (pm_title or "").strip()
    if not t:
        return []
    stripped = strip_pm_en_locale_prefix(t)
    if stripped == t:
        return [t]
    return [stripped, t]


def build_pm_slug_groups(sets_blob: Dict[str, Any]) -> Dict[str, List[Tuple[str, Dict[str, Any]]]]:
    groups: Dict[str, List[Tuple[str, Dict[str, Any]]]] = defaultdict(list)
    for slug, row in sets_blob.items():
        if not isinstance(row, dict):
            continue
        title = row.get("n")
        if not title:
            continue
        if not is_pm_english_row(str(slug), row):
            continue
        for text in _pm_title_match_texts(str(title)):
            for ck in _norm_keys_for_texts([text]):
                groups[ck].append((str(slug), row))
    return groups


def _title_similarity(set_name: str, pm_title: str) -> float:
    a = norm_set_key(set_name)
    st = strip_pm_en_locale_prefix(pm_title or "")
    b = norm_set_key(st)
    if not a or not b:
        return 0.0
    return float(SequenceMatcher(None, a, b).ratio())


def pick_slug_for_set(
    set_name: str,
    set_code: str,
    groups: Dict[str, List[Tuple[str, Dict[str, Any]]]],
    overrides: Dict[str, Any],
) -> Optional[str]:
    sc = str(set_code or "").strip()
    if sc and isinstance(overrides, dict) and sc in overrides:
        ent = overrides[sc]
        if isinstance(ent, dict) and ent.get("slug"):
            return str(ent["slug"]).strip()
        if isinstance(ent, str) and ent.strip():
            return ent.strip()
    candidates: Dict[str, Dict[str, Any]] = {}
    for ck in _candidate_norm_keys(set_name):
        for slug, row in groups.get(ck) or []:
            candidates[str(slug)] = row
    if not candidates:
        return None
    best_slug: Optional[str] = None
    best_key: Optional[Tuple[float, int]] = None
    for slug, row in candidates.items():
        sim = _title_similarity(set_name, str(row.get("n") or ""))
        try:
            ci = int(row.get("c") or 0)
        except (TypeError, ValueError):
            ci = 0
        key = (-sim, -ci)
        if best_key is None or key < best_key:
            best_key = key
            best_slug = slug
    return best_slug


def clear_pokemetrics_fields(card: Dict[str, Any]) -> None:
    for k in list(card.keys()):
        if k.startswith(PM_PREFIX):
            del card[k]


POP_META_KEYS = ("pokemetrics_pop_report_version", "pokemetrics_pop_source")


def clear_pokemetrics_set_meta(set_row: Dict[str, Any]) -> None:
    for k in POP_META_KEYS:
        set_row.pop(k, None)


def apply_pm_card(
    card: Dict[str, Any],
    pm: Dict[str, Any],
    slug: str,
    pm_index_title: str = "",
) -> bool:
    """Return True if merged."""
    tot = pm.get("t")
    g10 = pm.get("ps")
    grades = pm.get("f")
    try:
        ti = int(tot) if tot is not None else None
    except (TypeError, ValueError):
        ti = None
    try:
        gi = int(g10) if g10 is not None else None
    except (TypeError, ValueError):
        gi = None
    if ti is None and gi is None and not grades:
        return False
    if ti is not None:
        card["pokemetrics_psa_total_pop"] = ti
    if gi is not None:
        card["pokemetrics_psa_gem10"] = gi
    if isinstance(grades, dict):
        card["pokemetrics_grade_breakdown"] = grades
    card["pokemetrics_set_slug"] = slug
    if pm_index_title and str(pm_index_title).strip():
        card["pokemetrics_pm_index_set_name"] = str(pm_index_title).strip()
    return True


_PM_TRAINERISH_X = re.compile(r"\b(supporter|item|stadium|tool)\b", re.I)


def _norm_super(st: Any) -> str:
    if st is None:
        return ""
    return re.sub(r"\s+", " ", str(st).strip().lower())


def pm_row_looks_trainerish(pm: Dict[str, Any]) -> bool:
    xs = pm.get("x")
    if not isinstance(xs, list):
        return False
    blob = " ".join(str(x) for x in xs).lower()
    return bool(_PM_TRAINERISH_X.search(blob + " "))


def count_pm_matches_for_set_top(
    s: Dict[str, Any],
    pm_cards: List[Dict[str, Any]],
) -> int:
    n = 0
    for card in s.get("top_25_cards") or []:
        if not isinstance(card, dict):
            continue
        if find_pm_card_row(pm_cards, card.get("number"), card.get("name"), card.get("supertype")):
            n += 1
    return n


def select_trial_recent_sets_with_pm(
    sets_data: List[Dict[str, Any]],
    *,
    k: int,
    tail_scan: int,
    min_card_matches: int,
    skip_set_codes_lower: set,
    groups: Dict[str, List[Tuple[str, Dict[str, Any]]]],
    overrides: Dict[str, Any],
    v: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Walk newest-first through the tail of `sets_data` (array is oldest-first in this repo).
    Keep sets that resolve a PM slug, load set JSON, and have >= min_card_matches in top_25_cards.
    """
    meta: Dict[str, Any] = {"candidates_scanned": 0, "skipped": []}
    tail = [x for x in sets_data[-max(1, tail_scan) :] if isinstance(x, dict)]
    chosen: List[Dict[str, Any]] = []
    for s in reversed(tail):
        if len(chosen) >= k:
            break
        meta["candidates_scanned"] += 1
        set_code = str(s.get("set_code") or "").strip()
        scl = set_code.lower()
        set_name = str(s.get("set_name") or "")
        if scl in skip_set_codes_lower or "perfect order" in set_name.lower():
            meta["skipped"].append({"reason": "skip_list", "set_code": set_code})
            continue
        slug = pick_slug_for_set(set_name, set_code, groups, overrides)
        if not slug:
            meta["skipped"].append({"reason": "no_slug", "set_code": set_code, "set_name": set_name[:72]})
            continue
        try:
            pm_cards = load_or_fetch_set_cards(v, slug)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            meta["skipped"].append({"reason": "fetch_fail", "set_code": set_code, "slug": slug, "error": str(e)[:160]})
            continue
        mc = count_pm_matches_for_set_top(s, pm_cards)
        if mc < min_card_matches:
            meta["skipped"].append({"reason": "low_matches", "set_code": set_code, "slug": slug, "matches": mc})
            continue
        chosen.append(s)
        print(
            f"[trial] pick {len(chosen)}/{k}  {set_code!r}  {set_name[:56]!r}  slug={slug}  top_25_matches={mc}",
            flush=True,
        )
    return chosen, meta


def find_pm_card_row(
    cards: List[Dict[str, Any]],
    number: Any,
    name: str,
    supertype: Any = None,
) -> Optional[Dict[str, Any]]:
    want_n = norm_card_name(name)
    want_e = norm_card_number(number)
    hits: List[Dict[str, Any]] = []
    for pm in cards:
        if not isinstance(pm, dict):
            continue
        if norm_card_name(pm.get("n")) != want_n:
            continue
        if norm_card_number(pm.get("e")) != want_e:
            continue
        hits.append(pm)
    if not hits:
        return None
    if len(hits) == 1:
        return hits[0]
    st = _norm_super(supertype)
    if st == "trainer":
        for m in hits:
            if pm_row_looks_trainerish(m):
                return m
    if st in ("pokémon", "pokemon"):
        for m in hits:
            if not pm_row_looks_trainerish(m):
                return m
    return hits[0]


def run_merge(
    input_path: str,
    output_path: str,
    backup: bool,
    max_sets: int,
    *,
    trial_recent_sets: int = 0,
    trial_tail_scan: int = 80,
    trial_min_card_matches: int = 1,
    trial_skip_set_codes: str = "me3",
    verbose_cards: bool = False,
    log_first_cards: int = 0,
    log_every: int = 2500,
) -> Dict[str, Any]:
    if not os.path.isfile(input_path):
        raise FileNotFoundError(input_path)
    with open(input_path, "r", encoding="utf-8") as f:
        sets_data = json.load(f)
    if not isinstance(sets_data, list):
        raise ValueError("Expected JSON array of sets")

    overrides: Dict[str, Any] = {}
    if os.path.isfile(OVERRIDES_PATH):
        with open(OVERRIDES_PATH, "r", encoding="utf-8") as f:
            overrides = json.load(f)
        if not isinstance(overrides, dict):
            overrides = {}

    v, d_ms = fetch_version()
    as_of = (
        datetime.fromtimestamp(d_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if d_ms
        else ""
    )

    sets_blob = load_or_fetch_sets_index(v)
    if not isinstance(sets_blob, dict):
        raise ValueError("sets.json must be an object")

    groups = build_pm_slug_groups(sets_blob)

    if backup and os.path.isfile(output_path):
        shutil.copy2(output_path, output_path + ".bak")

    sets_list: List[Dict[str, Any]] = sets_data
    trial_meta: Optional[Dict[str, Any]] = None
    if trial_recent_sets > 0:
        skip_set = {x.strip().lower() for x in (trial_skip_set_codes or "").split(",") if x.strip()}
        sets_list, trial_meta = select_trial_recent_sets_with_pm(
            sets_data,
            k=trial_recent_sets,
            tail_scan=trial_tail_scan,
            min_card_matches=trial_min_card_matches,
            skip_set_codes_lower=skip_set,
            groups=groups,
            overrides=overrides,
            v=v,
        )
        n_top = sum(len(s.get("top_25_cards") or []) for s in sets_list if isinstance(s, dict))
        print(
            f"[trial] mode: {len(sets_list)} set(s), ~{n_top} top-list card rows to merge (target ~{trial_recent_sets * 25})",
            flush=True,
        )
    elif max_sets > 0:
        sets_list = sets_list[:max_sets]

    stats = {
        "api_version": v,
        "as_of": as_of,
        "sets_attempted": 0,
        "sets_slug_resolved": 0,
        "sets_slug_missing": 0,
        "sets_fetch_failed": 0,
        "cards_matched": 0,
        "trial_recent": bool(trial_recent_sets),
        "trial_meta": trial_meta,
    }

    n_sets = len([x for x in sets_list if isinstance(x, dict)])
    total_top = sum(
        len(s.get("top_25_cards") or []) for s in sets_list if isinstance(s, dict)
    )
    print(f"[Pokemetrics pop] merging top-list cards across {n_sets} set(s), {total_top} card row(s) ...", flush=True)

    si = 0
    global_card_n = 0
    for s in sets_list:
        if not isinstance(s, dict):
            continue
        stats["sets_attempted"] += 1
        si += 1
        set_name = s.get("set_name") or ""
        set_code = str(s.get("set_code") or "")
        print(f"[Pokemetrics pop {si}/{n_sets}] set_code={set_code!r} ...", flush=True)
        slug = pick_slug_for_set(set_name, set_code, groups, overrides)
        if not slug:
            stats["sets_slug_missing"] += 1
            clear_pokemetrics_set_meta(s)
            for card in s.get("top_25_cards") or []:
                if isinstance(card, dict):
                    clear_pokemetrics_fields(card)
            continue
        stats["sets_slug_resolved"] += 1
        idx_entry = sets_blob.get(slug) if isinstance(sets_blob, dict) else None
        pm_index_title = ""
        if isinstance(idx_entry, dict):
            pm_index_title = str(idx_entry.get("n") or "").strip()
        try:
            pm_cards = load_or_fetch_set_cards(v, slug)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            stats["sets_fetch_failed"] += 1
            clear_pokemetrics_set_meta(s)
            for card in s.get("top_25_cards") or []:
                if isinstance(card, dict):
                    clear_pokemetrics_fields(card)
            continue
        s["pokemetrics_pop_report_version"] = v
        s["pokemetrics_pop_source"] = "api.pokemetrics.org"
        ci = 0
        for card in s.get("top_25_cards") or []:
            if not isinstance(card, dict):
                continue
            ci += 1
            global_card_n += 1
            clear_pokemetrics_fields(card)
            pm_row = find_pm_card_row(pm_cards, card.get("number"), card.get("name"), card.get("supertype"))
            matched = bool(pm_row) and apply_pm_card(card, pm_row, slug, pm_index_title)
            if matched:
                stats["cards_matched"] += 1

            def _card_log_line() -> str:
                nm = str(card.get("name") or "")[:44]
                num = str(card.get("number") or "")[:14]
                st = "MATCH" if matched else "no row"
                if not matched:
                    tot_s = "-"
                else:
                    tot = card.get("pokemetrics_psa_total_pop")
                    try:
                        tot_s = str(int(tot)) if tot is not None else "-"
                    except (TypeError, ValueError):
                        tot_s = "-"
                return f"  [{global_card_n}/{total_top}] {st}  {set_code!r}  #{num}  {nm}  pop={tot_s}"

            if verbose_cards:
                print(_card_log_line(), flush=True)
            elif log_first_cards and global_card_n <= log_first_cards:
                print(_card_log_line(), flush=True)
            elif log_every > 0 and global_card_n > log_first_cards and global_card_n % log_every == 0:
                print(
                    f"  ... progress {global_card_n}/{total_top} cards, {stats['cards_matched']} matches so far",
                    flush=True,
                )

    tmp_path = f"{output_path}.tmp.{os.getpid()}"
    print(f"[Pokemetrics pop] writing {output_path!r} (via temp) ...", flush=True)
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(sets_data, f, indent=4)
        os.replace(tmp_path, output_path)
    finally:
        if os.path.isfile(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    print("[Pokemetrics pop] write complete.", flush=True)

    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge PokéMetrics PSA population into pokemon_sets_data.json")
    ap.add_argument("--input", default="pokemon_sets_data.json")
    ap.add_argument("--output", default="pokemon_sets_data.json")
    ap.add_argument("--no-backup", action="store_true")
    ap.add_argument("--max-sets", type=int, default=0, help="0 = all sets. If >0: first N sets in file (oldest) — use --trial-recent-sets for newest.")
    ap.add_argument(
        "--trial-recent-sets",
        type=int,
        default=0,
        metavar="K",
        help="Trial: merge only up to K newest sets (from end of JSON) that have PM slug + >= min matches in top_25.",
    )
    ap.add_argument(
        "--trial-tail-scan",
        type=int,
        default=80,
        metavar="N",
        help="With --trial-recent-sets: consider only the last N sets in the file when scanning newest-first.",
    )
    ap.add_argument(
        "--trial-min-matches",
        type=int,
        default=1,
        metavar="M",
        help="With --trial-recent-sets: require at least M top_25 cards to match PM rows before keeping a set.",
    )
    ap.add_argument(
        "--trial-skip-set-codes",
        default="me3",
        metavar="CSV",
        help="Comma-separated set_code values to skip (default me3 = Perfect Order).",
    )
    ap.add_argument(
        "--verbose-cards",
        action="store_true",
        help="Print one console line per top-list card (name, number, MATCH/no row, pop when matched).",
    )
    ap.add_argument(
        "--log-first-cards",
        type=int,
        default=0,
        metavar="N",
        help="Print per-card lines for the first N cards only (0 = off unless --verbose-cards).",
    )
    ap.add_argument(
        "--log-every",
        type=int,
        default=2500,
        metavar="M",
        help="After --log-first-cards, print a progress summary every M cards (0 = off).",
    )
    args = ap.parse_args()
    if args.trial_recent_sets > 0 and args.max_sets > 0:
        raise SystemExit("Use either --trial-recent-sets or --max-sets, not both.")
    if args.verbose_cards and args.log_first_cards > 0:
        raise SystemExit("Use either --verbose-cards or --log-first-cards, not both.")
    info = run_merge(
        args.input,
        args.output,
        backup=not args.no_backup,
        max_sets=args.max_sets,
        trial_recent_sets=max(0, args.trial_recent_sets),
        trial_tail_scan=max(1, args.trial_tail_scan),
        trial_min_card_matches=max(0, args.trial_min_matches),
        trial_skip_set_codes=str(args.trial_skip_set_codes or ""),
        verbose_cards=bool(args.verbose_cards),
        log_first_cards=max(0, args.log_first_cards),
        log_every=max(0, args.log_every),
    )
    print(json.dumps(info, indent=2), flush=True)


if __name__ == "__main__":
    main()
