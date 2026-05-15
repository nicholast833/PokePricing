#!/usr/bin/env python3
"""
Merge Pokemon Wizard (pokemonwizard.com) card price summary + table history onto
top_25_cards rows in pokemon_sets_data.json.

Requires a TCGPlayer numeric product id on each card, from Collectrics sync:
  collectrics_tcg_player_id   (preferred)
or TCGTracking merge:
  tcgtracking_product_id

If both are missing, the sync can resolve ids from a Pokémon Wizard **set listing** when
`pokemon_wizard_set_paths.json` maps our `set_code` (lowercase) to `{setId}/{slug}` as on
`https://www.pokemonwizard.com/sets/{setId}/{slug}` (table rows link to `/cards/{productId}/...`).

Regenerate paths from the official sets listing:
  python scrape/extract_pokemon_wizard_set_paths.py path/to/view-source_sets.html
  python scrape/merge_pokemon_wizard_set_paths.py
  # or: python scrape/extract_pokemon_wizard_set_paths.py --fetch

Slug for Wizard URLs is derived from TCGPlayer mp-search `productUrlName` when that API succeeds:
  "Klinklang 141 086" -> klinklang-141-086 -> klinklang-141086 (merge trailing number pair)

Data sources:
  GET https://mp-search-api.tcgplayer.com/v1/product/{id}/details
  GET https://www.pokemonwizard.com/cards/{id}/{slug}

Price history: parses the #pricehistory <tbody> or chart series; each successful run **merges**
new rows onto any existing ``pokemon_wizard_price_history`` (by date key) so long-running jobs
accumulate coverage beyond a single scrape window where rows overlap.

Each run appends a record to reports/pokemon_wizard_sync_skips.json (skipped cards with reason codes).
Regenerate reports/gaps/sets_missing_wizard_price_history.txt to include those rows in the skip subsection.

By default the output JSON is atomically rewritten **after each set** that passes the filter, so an
interrupted run keeps all fully completed sets. Use --no-checkpoint-every-set for a single write at
the end (less disk I/O, riskier). Timestamped backup before long runs:
  python scripts/backup_pokemon_sets_data.py

Continue a bulk run without re-hitting Wizard for cards that already have pokemon_wizard_url:
  python scrape/sync_pokemon_wizard.py --sleep 0.12 --resume-skip-has-url

Continue after a hang mid-run (same card order as a full sync; skips network for the first N rows):
  python scrape/sync_pokemon_wizard.py --sleep 0.12 --skip-first-cards 3800

Run after Collectrics (or ensure tcg ids exist):
  python scrape/sync_pokemon_wizard.py --backup --sleep 0.15
  python scrape/sync_pokemon_wizard.py --only-set-codes zsv10pt5 --sleep 0.15

Gold / White Star (Unicode ★☆ vs Wizard \"*\"): only rows that still need Wizard data:
  python scrape/sync_pokemon_wizard.py --only-star-listing-fix --sleep 0.15

Top-list cards still missing usable Wizard price history (one set page + card pages only):
  python scrape/sync_pokemon_wizard.py --only-missing-price-history --sleep 0.15
  python scrape/sync_pokemon_wizard.py --only-set-codes ex13 --only-missing-price-history --sleep 0.15

Optional CI guard (exits 2 if any top-list card in a Wizard-mapped set still lacks pokemon_wizard_url):
  python scrape/sync_pokemon_wizard.py --backup --strict --sleep 0.15
"""

from __future__ import annotations

import argparse
import difflib
import html as html_module
import json
import re
import shutil
from datetime import datetime, timedelta, timezone
import socket
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

_T = TypeVar("_T")

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "github_actions"))
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPT_DIR))
from dataset_report_paths import WIZARD_SYNC_SKIPS_LOG, dataset_sidecar_report_path  # noqa: E402
from json_atomic_util import write_json_atomic  # noqa: E402
from tcgtracking_merge import norm_card_name  # noqa: E402
from price_history_merge import merge_wizard_price_history_rows  # noqa: E402

WIZ = "pokemon_wizard_"
HEAD = {"User-Agent": "Mozilla/5.0 PokemonTCG-Explorer/sync_pokemon_wizard"}
SET_PATHS_FILE = ROOT / "pokemon_wizard_set_paths.json"
_WIZARD_SET_PAGE_HTML: Dict[str, str] = {}
SLOW_FETCH_SEC = 12.0
# When strict (collector,name) and single-name lookup fail, pick best listing row by string similarity.
WIZARD_LISTING_FUZZY_MIN_RATIO = 0.86
WIZARD_LISTING_FUZZY_MIN_MARGIN = 0.04

# Explorer set_code -> extra Wizard /sets/{id}/{slug} paths to merge into one browse index
# (Gym Heroes / Gym Challenge split some "Rocket's" cards across the two Wizard set pages).
# Subset / gallery pages (Classic Collection, Trainer Gallery, Shiny Vault, Galarian Gallery) use
# different paths than the main set listing on pokemonwizard.com — merge them for listing lookup.
BUDDY_WIZARD_SET_PATHS: Dict[str, Tuple[str, ...]] = {
    "gym1": ("1440/gym-challenge",),
    "gym2": ("1441/gym-heroes",),
    "cel25": ("2931/celebrations-classic-collection",),
    "swsh12pt5": ("17689/crown-zenith-galarian-gallery",),
    "swsh45": ("2781/shining-fates-shiny-vault",),
    "swsh9": ("3020/brilliant-stars-trainer-gallery",),
    "swsh10": ("3068/astral-radiance-trainer-gallery",),
}


def norm_collector_key(s: str) -> str:
    """Match '04' on cards to '4' in listing cells when purely numeric."""
    t = str(s or "").strip().lower()
    if not t:
        return ""
    if t.isdigit():
        return str(int(t))
    return t


def record_wizard_skip(
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


def append_wizard_skip_run_log(
    *,
    input_path: Path,
    output_path: Path,
    rep: Dict[str, Any],
    max_runs: int = 100,
) -> None:
    """Append this sync run to pokemon_wizard_sync_skips.json (capped). Used by gap summary."""
    run_entry = {
        "sync_iso": datetime.now(timezone.utc).isoformat(),
        "input": str(input_path),
        "output": str(output_path),
        "stats": {
            "cards_considered": rep.get("cards_considered"),
            "cards_merged": rep.get("cards_merged"),
            "cards_skipped_no_tcg_id": rep.get("cards_skipped_no_tcg_id"),
            "cards_skipped_tcg_api": rep.get("cards_skipped_tcg_api"),
            "cards_skipped_wizard_fetch": rep.get("cards_skipped_wizard_fetch"),
            "wizard_set_index_fetches": rep.get("wizard_set_index_fetches"),
            "max_cards": rep.get("max_cards"),
            "only_missing_price_history": rep.get("only_missing_price_history"),
        },
        "skips": rep.get("skips") or [],
    }
    prev: Dict[str, Any] = {"runs": []}
    if WIZARD_SYNC_SKIPS_LOG.is_file():
        try:
            raw = json.loads(WIZARD_SYNC_SKIPS_LOG.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and isinstance(raw.get("runs"), list):
                prev = raw
        except (json.JSONDecodeError, OSError):
            prev = {"runs": []}
    runs = [x for x in prev.get("runs") or [] if isinstance(x, dict)]
    runs.append(run_entry)
    if len(runs) > max_runs:
        runs = runs[-max_runs:]
    WIZARD_SYNC_SKIPS_LOG.parent.mkdir(parents=True, exist_ok=True)
    WIZARD_SYNC_SKIPS_LOG.write_text(
        json.dumps({"runs": runs}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def clear_wizard_fields(card: Dict[str, Any]) -> None:
    for k in list(card.keys()):
        if k.startswith(WIZ):
            del card[k]


def product_url_name_to_wizard_slug(product_url_name: str) -> str:
    s = product_url_name.strip().lower().replace(" ", "-")
    s = re.sub(r"-+", "-", s)
    return re.sub(r"-(\d+)-(\d+)$", r"-\1\2", s)


def _http_err_kind(exc: BaseException) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        return f"HTTP {exc.code}"
    return type(exc).__name__


def http_json(
    url: str,
    timeout: int = 45,
    *,
    attempts: int = 4,
    log_label: str = "",
) -> Any:
    last: Optional[BaseException] = None
    for i in range(max(1, attempts)):
        try:
            req = urllib.request.Request(url, headers={**HEAD, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429:
                print(f"  [RATE LIMITED 429] {log_label or url[:90]}", flush=True)
            if i + 1 >= attempts:
                raise
            print(
                f"  [retry {i + 1}/{attempts}] json {_http_err_kind(e)}: {log_label or url[:90]} - sleeping...",
                flush=True,
            )
            time.sleep(1.5 * (2**i))
        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as e:
            last = e
            if i + 1 >= attempts:
                raise
            print(
                f"  [retry {i + 1}/{attempts}] json {_http_err_kind(e)}: {log_label or url[:90]} - sleeping...",
                flush=True,
            )
            time.sleep(1.5 * (2**i))
    raise last  # pragma: no cover


def http_text(
    url: str,
    timeout: int = 60,
    *,
    attempts: int = 4,
    log_label: str = "",
) -> str:
    last: Optional[BaseException] = None
    for i in range(max(1, attempts)):
        try:
            req = urllib.request.Request(url, headers=HEAD)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429:
                print(f"  [RATE LIMITED 429] {log_label or url[:90]}", flush=True)
            if i + 1 >= attempts:
                raise
            print(
                f"  [retry {i + 1}/{attempts}] html {_http_err_kind(e)}: {log_label or url[:90]} - sleeping...",
                flush=True,
            )
            time.sleep(1.5 * (2**i))
        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as e:
            last = e
            if i + 1 >= attempts:
                raise
            print(
                f"  [retry {i + 1}/{attempts}] html {_http_err_kind(e)}: {log_label or url[:90]} - sleeping...",
                flush=True,
            )
            time.sleep(1.5 * (2**i))
    raise last  # pragma: no cover


def _timed(label: str, fn: Callable[[], _T]) -> _T:
    t0 = time.monotonic()
    try:
        return fn()
    finally:
        dt = time.monotonic() - t0
        if dt >= SLOW_FETCH_SEC:
            print(f"  [slow {dt:.1f}s] {label}", flush=True)


def fetch_tcgplayer_product_name(product_id: str, *, log_label: str = "") -> Optional[str]:
    url = f"https://mp-search-api.tcgplayer.com/v1/product/{product_id}/details"
    try:
        d = http_json(url, log_label=log_label or f"TCGPlayer product/{product_id}")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError):
        return None
    if not isinstance(d, dict):
        return None
    n = d.get("productUrlName")
    return str(n).strip() if n else None


def _wizard_parse_usd_fragment(txt: str) -> Optional[float]:
    """Parse first $… amount in text; allows thousands separators (e.g. $1,995.52)."""
    pm = re.search(r"\$\s*([\d,]+(?:\.\d{1,6})?)", txt)
    if not pm:
        return None
    try:
        return float(pm.group(1).replace(",", ""))
    except ValueError:
        return None


def _wizard_parse_numeric(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _extract_json_array_after_token(raw: str, token: str) -> Optional[str]:
    i = raw.find(token)
    if i < 0:
        return None
    j = raw.find("[", i + len(token))
    if j < 0:
        return None
    depth = 0
    in_str = False
    esc = False
    for k in range(j, len(raw)):
        ch = raw[k]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return raw[j : k + 1]
    return None


def _parse_chart_history_rows_from_html(html: str) -> List[Dict[str, Any]]:
    """
    Parse Next.js chart payload rows under Price History.
    Expected row keys include: date, market_price, low_price, mid_price, high_price, trend_alltime.
    """
    candidates: List[str] = []
    arr_plain = _extract_json_array_after_token(html, '"history":')
    if arr_plain:
        candidates.append(arr_plain)
    # Next.js flight chunks usually embed escaped JSON strings.
    for m in re.finditer(r'\\"history\\":\[(.*?)\](?:[,}])', html, re.S):
        arr_esc = "[" + m.group(1) + "]"
        candidates.append(arr_esc)
    if not candidates:
        return []

    best_rows: List[Dict[str, Any]] = []
    for arr_txt in candidates:
        parsed: Any = None
        try:
            parsed = json.loads(arr_txt)
        except json.JSONDecodeError:
            pass
        if parsed is None:
            try:
                unesc = arr_txt.replace('\\"', '"').replace("\\/", "/")
                parsed = json.loads(unesc)
            except json.JSONDecodeError:
                continue
        if not isinstance(parsed, list):
            continue
        rows: List[Dict[str, Any]] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            dt = str(item.get("date") or "").strip()
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", dt):
                continue
            market = _wizard_parse_numeric(item.get("market_price"))
            low = _wizard_parse_numeric(item.get("low_price"))
            mid = _wizard_parse_numeric(item.get("mid_price"))
            high = _wizard_parse_numeric(item.get("high_price"))
            trend_pct = _wizard_parse_numeric(item.get("trend_alltime"))
            rows.append(
                {
                    "sort_key": dt,
                    "label": dt,
                    "date": dt,
                    # Backward compatibility with existing consumers.
                    "price_usd": market,
                    "trend": f"{trend_pct:.2f}%" if trend_pct is not None else None,
                    # New chart variables.
                    "market_price_usd": market,
                    "low_price_usd": low,
                    "mid_price_usd": mid,
                    "high_price_usd": high,
                    "trend_alltime_pct": trend_pct,
                    "variant": str(item.get("variant") or "").strip() or None,
                }
            )
        if len(rows) > len(best_rows):
            best_rows = rows
    best_rows.sort(key=lambda r: str(r.get("sort_key") or ""))
    # Keep 1-year window ending at the latest chart date.
    if best_rows:
        try:
            end_dt = datetime.strptime(str(best_rows[-1].get("date") or ""), "%Y-%m-%d")
            start_dt = end_dt - timedelta(days=365)
            trimmed = []
            for r in best_rows:
                d = str(r.get("date") or "")
                try:
                    dt = datetime.strptime(d, "%Y-%m-%d")
                except ValueError:
                    continue
                if dt >= start_dt:
                    trimmed.append(r)
            if trimmed:
                best_rows = trimmed
        except ValueError:
            pass
    return best_rows


def parse_wizard_card_page(html: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    m = re.search(
        r"Current Price</strong></td>\s*<td><h4>.*?\$\s*([\d,]+(?:\.\d+)?).*?(\d+\.\d+)\s*%",
        html,
        re.S | re.I,
    )
    if m:
        try:
            out["current_price_usd"] = float(m.group(1).replace(",", ""))
        except ValueError:
            pass
        try:
            out["current_trend_pct"] = float(m.group(2))
        except ValueError:
            pass

    def pct_after(label: str) -> Optional[float]:
        pat = rf"{re.escape(label)}</strong>\s*</td>\s*<td>.*?(\d+\.\d+)\s*%"
        mx = re.search(pat, html, re.S | re.I)
        if mx:
            try:
                return float(mx.group(1))
            except ValueError:
                return None
        return None

    v7 = pct_after("Last 7 Days")
    if v7 is not None:
        out["last_7d_pct"] = v7
    v30 = pct_after("Last 30 Days")
    if v30 is not None:
        out["last_30d_pct"] = v30
    vy = pct_after("YTD")
    if vy is not None:
        out["ytd_pct"] = vy

    chart_rows = _parse_chart_history_rows_from_html(html)
    if chart_rows:
        out["price_history"] = chart_rows
        out["price_history_source"] = "chart_history"
        return out

    rows: List[Dict[str, Any]] = []
    mtab = re.search(r'id=["\']pricehistory["\'][^>]*>(.*?)</table>', html, re.S | re.I)
    if mtab:
        frag = mtab.group(1)
        tb = re.search(r"<tbody[^>]*>(.*?)</tbody>", frag, re.S | re.I)
        body = tb.group(1) if tb else frag
        for tr in re.finditer(r"<tr[^>]*>(.*?)</tr>", body, re.S | re.I):
            chunk = tr.group(1)
            tds = re.findall(r"<td[^>]*>(.*?)</td>", chunk, re.S | re.I)
            if len(tds) < 4:
                continue
            hidden = re.sub(r"<[^>]+>", "", tds[0])
            hidden = re.sub(r"<\.?span>", "", hidden, flags=re.I).strip()
            label = re.sub(r"<[^>]+>", "", tds[1]).strip()
            price_txt = re.sub(r"<[^>]+>", "", tds[2])
            trend_txt = re.sub(r"<[^>]+>", "", tds[3]).strip()
            if label.lower() in ("date", "price", "trend"):
                continue
            price = _wizard_parse_usd_fragment(price_txt)
            rows.append(
                {
                    "sort_key": hidden,
                    "label": label,
                    "price_usd": price,
                    "trend": trend_txt or None,
                }
            )
    if rows:
        out["price_history"] = rows
        out["price_history_source"] = "table_history"
    return out


def card_tcg_product_id(card: Dict[str, Any]) -> Optional[str]:
    for k in ("collectrics_tcg_player_id", "tcgtracking_product_id"):
        v = card.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s.isdigit():
            return s
    return None


def load_wizard_set_paths() -> Dict[str, str]:
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
        path = v.strip().strip("/")
        if sk.lower() and path:
            out[sk.lower()] = path
    return out


_STAR_UNICODE_NAME_RE = re.compile(r"[\u2605\u2606\u272f\u273f\u2b50\u272e]")


def norm_wizard_match_key(name: Optional[str]) -> str:
    """Normalize API card names for Wizard listing rows (hyphenated EX/GX vs spaced titles)."""
    s = norm_card_name(name)
    if not s:
        return ""
    # Gold / White Star: DBs use Unicode (U+2606 etc.); Wizard titles often use ASCII *.
    s = re.sub(r"[\u2605\u2606\u272f\u273f\u2b50\u272e]", "*", s)
    s = re.sub(r"\s+star\s*$", " *", s)
    # Delta Species / δ (U+03B4) vs Wizard "Delta" or "D" in slugs
    s = re.sub(r"[\u03b4\u0394]", " delta ", s)
    # Wizard titles often spell out "Delta Species"; Explorer uses δ on the name line.
    s = re.sub(r"\bdelta\s+species\b", "delta", s)
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _filtered_wizard_history_row_count(ph: Any) -> int:
    """Same header/sentinel filtering as report_wizard_price_history_gaps / app.js."""
    if not isinstance(ph, list):
        return 0
    n = 0
    for row in ph:
        if not isinstance(row, dict):
            continue
        l = str(row.get("label") or "").strip().lower()
        if l in ("date", "price", "trend", "when", "label", "sort_key"):
            continue
        sk = str(row.get("sort_key") or "").strip().lower()
        if sk in ("date", "price", "trend"):
            continue
        n += 1
    return n


def card_needs_star_listing_wizard_resync(card: Dict[str, Any]) -> bool:
    """
    Top-list cards whose names use Unicode star glyphs that failed Wizard set-list matching
    before norm_wizard_match_key mapped them to Wizard's ASCII * titles — only if merge is
    still missing or has no usable history rows.
    """
    name = str(card.get("name") or "")
    if not _STAR_UNICODE_NAME_RE.search(name):
        return False
    u = card.get("pokemon_wizard_url")
    if u is None or (isinstance(u, str) and not u.strip()):
        return True
    if _filtered_wizard_history_row_count(card.get("pokemon_wizard_price_history")) < 1:
        return True
    return False


def explorer_name_match_keys_for_card(card_name: Optional[str]) -> List[str]:
    """
    Normalized keys derived from our card title for Wizard listing lookup.
    Handles Unicode ★/☆ vs Wizard spelling \"Star\", and apostrophes vs Wizard ASCII (e.g. Rocket's).
    """
    out: List[str] = []
    cn = (card_name or "").strip()
    if not cn:
        return out
    raw_variants = [
        cn,
        cn.replace("\u2019", "").replace("\u2018", "").replace("'", "").replace("`", ""),
    ]
    seen: set = set()
    for raw in raw_variants:
        nk = norm_wizard_match_key(raw)
        if nk and nk not in seen:
            seen.add(nk)
            out.append(nk)
        if nk and "*" in nk:
            alt = re.sub(r"\*", " star ", nk)
            alt = re.sub(r"\s+", " ", alt).strip()
            if alt and alt not in seen:
                seen.add(alt)
                out.append(alt)
    return out


def wizard_listing_name_match_keys(card_name: str, collector: str) -> List[str]:
    """
    Build lookup keys for (collector, key) matching.
    Wizard chase titles often append the collector number and 'Full Art'
    (e.g. 'Blastoise EX 142 Full Art') while the API uses 'Blastoise-EX' + number '142/146'.
    """
    out: List[str] = []
    cn = (card_name or "").strip()
    if not cn:
        return out
    col = (collector or "").strip().lower()
    nk_full = norm_wizard_match_key(cn)
    if nk_full:
        out.append(nk_full)
    if col:
        tail = re.compile(rf"\s+{re.escape(col)}\s+full\s+art\s*$", re.I)
        stripped = tail.sub("", cn).strip()
        if stripped and stripped.lower() != cn.lower():
            nk = norm_wizard_match_key(stripped)
            if nk and nk not in out:
                out.append(nk)
    return out


def merge_wizard_listing_indexes(*parts: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple parse_wizard_set_listing_index() dicts; later parts override by_pair/by_name/by_pid on conflicts."""
    by_pair: Dict[Tuple[str, str], Tuple[str, str]] = {}
    by_name: Dict[str, Dict[str, str]] = {}
    by_pid: Dict[str, str] = {}
    rows: List[Dict[str, Any]] = []
    for p in parts:
        if not isinstance(p, dict):
            continue
        rows.extend(p.get("rows") or [])
    for p in reversed([x for x in parts if isinstance(x, dict)]):
        by_pair.update(p.get("by_pair") or {})
        for nk, pmap in (p.get("by_name") or {}).items():
            if isinstance(pmap, dict):
                by_name.setdefault(nk, {}).update(pmap)
        by_pid.update(p.get("by_pid") or {})
    return {"by_pair": by_pair, "by_name": by_name, "by_pid": by_pid, "rows": rows}


def _prefer_non_prerelease_listing_rows(
    card_name: Optional[str], rows_in: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """When the API title omits 'Prerelease', drop listing rows for Prerelease promos (same collector #)."""
    cn = str(card_name or "").strip().lower()
    if "prerelease" in cn or not rows_in:
        return rows_in

    def is_prerelease_row(r: Dict[str, Any]) -> bool:
        blob = f"{r.get('slug') or ''} {r.get('name_raw') or ''}".lower()
        return "prerelease" in blob

    plain = [r for r in rows_in if not is_prerelease_row(r)]
    return plain if plain else rows_in


def _prefer_non_master_ball_pattern_rows(
    card_name: Optional[str], rows_in: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """When the API title omits 'Master Ball Pattern', avoid listing rows for that parallel."""
    cn = str(card_name or "").strip().lower()
    if "master ball" in cn or not rows_in:
        return rows_in

    def is_master_ball_row(r: Dict[str, Any]) -> bool:
        return "master-ball" in str(r.get("slug") or "").lower()

    plain = [r for r in rows_in if not is_master_ball_row(r)]
    return plain if plain else rows_in


def _prefer_non_ball_pattern_rows(
    card_name: Optional[str], rows_in: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """When the API title omits a '* Ball Pattern' parallel, drop those listing rows (same collector #)."""
    cn = str(card_name or "").strip().lower()
    if "pattern" in cn or not rows_in:
        return rows_in

    def is_ball_pattern_row(r: Dict[str, Any]) -> bool:
        s = str(r.get("slug") or "").lower()
        return "ball-pattern" in s

    plain = [r for r in rows_in if not is_ball_pattern_row(r)]
    return plain if plain else rows_in


def resolve_wizard_card_from_set_index(idx: Dict[str, Any], card: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """Return (product_id, wizard_slug) or None."""
    num_raw = str(card.get("number") or "").strip()
    collector_raw = num_raw.split("/")[0].strip().lower() if num_raw else ""
    collector_key = norm_collector_key(collector_raw)
    ex_keys = explorer_name_match_keys_for_card(card.get("name"))

    by_pair: Dict[Tuple[str, str], Tuple[str, str]] = idx.get("by_pair") or {}
    if collector_raw:
        for nk in ex_keys:
            hit = by_pair.get((collector_raw, nk)) or by_pair.get((collector_key, nk))
            if hit:
                return hit

    by_name: Dict[str, Dict[str, str]] = idx.get("by_name") or {}
    for nk in ex_keys:
        pmap = by_name.get(nk) or {}
        if len(pmap) == 1:
            pid, slug = next(iter(pmap.items()))
            return (pid, slug)

    rows: List[Dict[str, Any]] = idx.get("rows") or []
    if not rows:
        return None

    def row_collector_key(r: Dict[str, Any]) -> str:
        return norm_collector_key(str(r.get("collector") or ""))

    if collector_key:
        same_col = [r for r in rows if row_collector_key(r) == collector_key]
        same_col = _prefer_non_prerelease_listing_rows(card.get("name"), same_col)
        same_col = _prefer_non_master_ball_pattern_rows(card.get("name"), same_col)
        same_col = _prefer_non_ball_pattern_rows(card.get("name"), same_col)
        if len(same_col) == 1:
            return (same_col[0]["pid"], same_col[0]["slug"])
        if len(same_col) > 1:
            pids = {str(r.get("pid")) for r in same_col}
            if len(pids) == 1:
                return (same_col[0]["pid"], same_col[0]["slug"])
        if len(same_col) > 1 and ex_keys:

            def best_key_vs_row(nk_row: str) -> float:
                return max((difflib.SequenceMatcher(None, ek, nk_row).ratio() for ek in ex_keys), default=0.0)

            scored = [(best_key_vs_row(r["nk"]), r["pid"], r["slug"]) for r in same_col]
            scored.sort(key=lambda x: -x[0])
            top = scored[0][0]
            second = scored[1][0] if len(scored) > 1 else 0.0
            if top >= WIZARD_LISTING_FUZZY_MIN_RATIO and (
                len(scored) == 1 or (top - second) >= WIZARD_LISTING_FUZZY_MIN_MARGIN
            ):
                return (scored[0][1], scored[0][2])

    if not ex_keys:
        return None

    def best_key_vs_row(nk_row: str) -> float:
        return max((difflib.SequenceMatcher(None, ek, nk_row).ratio() for ek in ex_keys), default=0.0)

    # API "number" can differ from Wizard's sheet index for high-rarity prints (e.g. SV10 lists 241 vs 232/256).
    # Use a looser name threshold than strict collector match (listing titles vary on punctuation/spacing).
    _alt_name_min = 0.78
    if collector_key and collector_key.isdigit() and int(collector_key) >= 150:
        pool = _prefer_non_prerelease_listing_rows(card.get("name"), rows)
        alt: List[Tuple[int, float, str, str]] = []
        for r in pool:
            rc = row_collector_key(r)
            if not rc.isdigit():
                continue
            sheet = int(rc)
            br = best_key_vs_row(r["nk"])
            if br < _alt_name_min:
                continue
            alt.append((sheet, br, str(r["pid"]), str(r["slug"])))
        if alt:
            alt.sort(key=lambda x: (-x[0], -x[1]))
            best_sheet = alt[0][0]
            tier = [x for x in alt if x[0] == best_sheet]
            tier.sort(key=lambda x: -x[1])
            top_r = tier[0][1]
            second_r = tier[1][1] if len(tier) > 1 else 0.0
            if top_r >= _alt_name_min and (
                len(tier) == 1
                or (top_r - second_r) >= WIZARD_LISTING_FUZZY_MIN_MARGIN
                or (len(tier) > 1 and tier[0][2] == tier[1][2])
            ):
                return (tier[0][2], tier[0][3])

    scoped = [r for r in rows if row_collector_key(r) == collector_key] if collector_key else rows
    if not scoped:
        scoped = rows

    scored2: List[Tuple[float, str, str]] = [(best_key_vs_row(r["nk"]), r["pid"], r["slug"]) for r in scoped]
    scored2.sort(key=lambda x: -x[0])
    top = scored2[0][0]
    second = scored2[1][0] if len(scored2) > 1 else 0.0
    if top < WIZARD_LISTING_FUZZY_MIN_RATIO:
        return None
    if len(scored2) > 1 and (top - second) < WIZARD_LISTING_FUZZY_MIN_MARGIN:
        return None
    return (scored2[0][1], scored2[0][2])


def fetch_wizard_set_page_html(set_path: str, *, log_label: str = "") -> str:
    set_path = set_path.strip().strip("/")
    if set_path in _WIZARD_SET_PAGE_HTML:
        return _WIZARD_SET_PAGE_HTML[set_path]
    url = f"https://www.pokemonwizard.com/sets/{set_path}"
    label = log_label or f"Wizard set page {set_path}"
    html = _timed(label, lambda: http_text(url, log_label=label))
    _WIZARD_SET_PAGE_HTML[set_path] = html
    return html


def parse_wizard_set_listing_index(html: str) -> Dict[str, Any]:
    """Build indexes from a /sets/{id}/{slug} HTML listing (including Browse Cards table rows)."""
    by_pair: Dict[Tuple[str, str], Tuple[str, str]] = {}
    by_name: Dict[str, Dict[str, str]] = {}
    by_pid: Dict[str, str] = {}
    rows: List[Dict[str, Any]] = []
    seen_row: set = set()
    for tr in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.I | re.S):
        chunk = tr.group(1)
        if "/cards/" not in chunk:
            continue
        m = re.search(r'href="/cards/(\d+)/([^"]+)"', chunk)
        if not m:
            continue
        pid, slug = m.group(1), m.group(2)
        if ".jpg" in slug.lower() or ".jpeg" in slug.lower() or ".png" in slug.lower():
            continue
        h3_m = re.search(
            r'<h3 class="h3-link"[^>]*>.*?<a href="/cards/\d+/[^"]+">([^<]+)</a>',
            chunk,
            re.S | re.I,
        )
        strong_m = re.search(
            r'href="/cards/\d+/[^"]+"[^>]*>\s*<strong>([^<]+)</strong>',
            chunk,
            re.I | re.S,
        )
        alt_m = re.search(r'alt="([^"]+)"', chunk)
        if h3_m:
            card_name = h3_m.group(1).strip()
        elif strong_m:
            card_name = strong_m.group(1).strip()
        elif alt_m:
            card_name = re.sub(r"\s*\([^)]*\)\s*$", "", alt_m.group(1).strip()).strip()
        else:
            continue
        card_name = html_module.unescape(card_name)
        if not card_name:
            continue
        nk = norm_wizard_match_key(card_name)
        collector: Optional[str] = None
        for raw in re.findall(r'<td[^>]*>\s*([^<]+?)\s*</td>', chunk):
            s = raw.strip()
            mnum = re.match(r"^([A-Za-z0-9]+)\s*/\s*\d+$", s)
            if mnum:
                collector = mnum.group(1).strip().lower()
                break
        by_pid[pid] = slug
        row_key = (pid, slug)
        if row_key in seen_row:
            continue
        seen_row.add(row_key)
        rows.append(
            {
                "collector": collector or "",
                "nk": nk,
                "pid": pid,
                "slug": slug,
                "name_raw": card_name,
            }
        )
        if collector:
            for nk_key in wizard_listing_name_match_keys(card_name, collector):
                by_pair[(collector, nk_key)] = (pid, slug)
        by_name.setdefault(nk, {})[pid] = slug
        for nk_key in wizard_listing_name_match_keys(card_name, collector or ""):
            if nk_key != nk:
                by_name.setdefault(nk_key, {})[pid] = slug
    # Next.js fallback: set pages may no longer render card rows as <tr> in HTML.
    if not rows:
        for m in re.finditer(
            r'\\"tcgplayer_product_id\\":(\d+),\\"name\\":\\"([^\\"]+)\\",\\"slug\\":\\"([^\\"]+)\\",\\"card_number\\":\\"([^\\"]+)\\"',
            html,
            re.I,
        ):
            pid = str(m.group(1)).strip()
            card_name = html_module.unescape(str(m.group(2)).strip())
            slug = str(m.group(3)).strip()
            collector = str(m.group(4)).strip().lower()
            if not (pid and slug and card_name):
                continue
            nk = norm_wizard_match_key(card_name)
            row_key = (pid, slug)
            if row_key in seen_row:
                continue
            seen_row.add(row_key)
            by_pid[pid] = slug
            rows.append(
                {
                    "collector": collector or "",
                    "nk": nk,
                    "pid": pid,
                    "slug": slug,
                    "name_raw": card_name,
                }
            )
            if collector:
                for nk_key in wizard_listing_name_match_keys(card_name, collector):
                    by_pair[(collector, nk_key)] = (pid, slug)
            by_name.setdefault(nk, {})[pid] = slug
            for nk_key in wizard_listing_name_match_keys(card_name, collector or ""):
                if nk_key != nk:
                    by_name.setdefault(nk_key, {})[pid] = slug
    return {"by_pair": by_pair, "by_name": by_name, "by_pid": by_pid, "rows": rows}


def _card_has_wizard_url(card: Dict[str, Any]) -> bool:
    u = card.get("pokemon_wizard_url")
    return bool(u and str(u).strip())


def count_wizard_work_units(
    data: List[Any],
    filter_codes: Optional[set],
    max_sets: int,
    *,
    only_star_listing_fix: bool,
    only_missing_price_history: bool,
    set_paths: Dict[str, str],
    resume_skip_has_url: bool,
) -> int:
    """How many top_25 card rows we will visit (same filters as run)."""
    processed = 0
    n = 0
    for s in data:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        if filter_codes is not None and sc not in filter_codes:
            continue
        if max_sets > 0 and processed >= max_sets:
            break
        processed += 1
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue
        for c in top:
            if not isinstance(c, dict):
                continue
            if only_missing_price_history:
                if not set_paths.get(sc):
                    continue
                if _filtered_wizard_history_row_count(c.get("pokemon_wizard_price_history")) >= 1:
                    continue
            if only_star_listing_fix:
                if not set_paths.get(sc):
                    continue
                if not card_needs_star_listing_wizard_resync(c):
                    continue
            if resume_skip_has_url and _card_has_wizard_url(c):
                continue
            n += 1
    return n


def assert_wizard_top25_coverage_strict(
    data: List[Any],
    set_paths: Dict[str, str],
    only_codes: Optional[set],
) -> None:
    """
    Exit non-zero if any top_25_cards row in a set that has a Wizard set path still lacks
    pokemon_wizard_url after sync (CI / regression guard).
    When only_codes is set (same filter as --only-set-codes), only those set_codes are checked;
    otherwise every set_code present in set_paths is checked.
    """
    if only_codes is not None:
        codes_to_check = {c.strip().lower() for c in only_codes if c and str(c).strip().lower() in set_paths}
    else:
        codes_to_check = set(set_paths.keys())
    missing: List[Tuple[str, Any, Any]] = []
    for s in data:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        if sc not in codes_to_check:
            continue
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue
        for c in top:
            if not isinstance(c, dict):
                continue
            u = c.get("pokemon_wizard_url")
            if u is None or (isinstance(u, str) and not u.strip()):
                missing.append((sc, c.get("number"), c.get("name")))
    if not missing:
        return
    print("STRICT: pokemon_wizard_url missing for top-list cards in mapped sets:", flush=True)
    for sc, num, nm in missing[:100]:
        print(f"  set={sc!r} num={num!r} name={nm!r}", flush=True)
    if len(missing) > 100:
        print(f"  ... and {len(missing) - 100} more", flush=True)
    raise SystemExit(2)


def run(
    input_path: Path,
    output_path: Path,
    sleep_s: float,
    max_sets: int,
    only_set_codes: Optional[str],
    *,
    strict_coverage: bool,
    only_star_listing_fix: bool,
    only_missing_price_history: bool,
    max_cards: int,
    checkpoint_every_set: bool = True,
    resume_skip_has_url: bool = False,
    skip_first_cards: int = 0,
) -> Dict[str, Any]:
    data = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("pokemon_sets_data.json must be a list")

    filter_codes: Optional[set] = None
    if only_set_codes:
        filter_codes = {x.strip().lower() for x in only_set_codes.split(",") if x.strip()}

    rep: Dict[str, Any] = {
        "cards_considered": 0,
        "cards_merged": 0,
        "cards_skipped_no_tcg_id": 0,
        "cards_skipped_tcg_api": 0,
        "cards_skipped_wizard_fetch": 0,
        "cards_resolved_set_page": 0,
        "wizard_set_index_fetches": 0,
        "only_star_listing_fix": bool(only_star_listing_fix),
        "only_missing_price_history": bool(only_missing_price_history),
        "max_cards": int(max_cards) if max_cards else 0,
        "resume_skip_has_url": bool(resume_skip_has_url),
        "cards_skipped_resume_has_url": 0,
        "skip_first_cards": int(skip_first_cards) if skip_first_cards > 0 else 0,
        "cards_skipped_first_n_offset": 0,
        "skips": [],
    }

    set_paths = load_wizard_set_paths()
    set_index_cache: Dict[str, Dict[str, Any]] = {}

    total_cards = count_wizard_work_units(
        data,
        filter_codes,
        max_sets,
        only_star_listing_fix=only_star_listing_fix,
        only_missing_price_history=only_missing_price_history,
        set_paths=set_paths,
        resume_skip_has_url=resume_skip_has_url,
    )
    scope_note = ""
    if only_star_listing_fix:
        scope_note = " (Unicode ★/☆ rows missing Wizard merge only)"
    elif only_missing_price_history:
        scope_note = " (top-list cards missing usable Wizard price history only)"
    cap_total = min(total_cards, max_cards) if max_cards > 0 else total_cards
    if max_cards > 0:
        scope_note += f" [cap {max_cards} cards]"
    if resume_skip_has_url:
        scope_note += " [resume: skip rows that already have pokemon_wizard_url]"
    if skip_first_cards > 0:
        scope_note += f" [skip first {skip_first_cards} card row(s) in stream; no HTTP for those]"
    print(
        f"Pokemon Wizard sync: {total_cards} top-25 card rows to consider "
        f"(working on {cap_total} this run; filter="
        f"{'all' if filter_codes is None else len(filter_codes)} set_codes){scope_note}.",
        flush=True,
    )

    processed = 0
    stream_i = 0
    stop_all = False
    for s in data:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        if filter_codes is not None and sc not in filter_codes:
            continue
        if max_sets > 0 and processed >= max_sets:
            break
        processed += 1

        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue

        set_name = str(s.get("set_name") or "")
        set_path = set_paths.get(sc)
        for c in top:
            if not isinstance(c, dict):
                continue
            if only_missing_price_history:
                if not set_path:
                    continue
                if _filtered_wizard_history_row_count(c.get("pokemon_wizard_price_history")) >= 1:
                    continue
            if only_star_listing_fix:
                if not set_path:
                    continue
                if not card_needs_star_listing_wizard_resync(c):
                    continue
            if resume_skip_has_url and _card_has_wizard_url(c):
                rep["cards_skipped_resume_has_url"] += 1
                continue
            stream_i += 1
            if skip_first_cards > 0 and stream_i <= skip_first_cards:
                rep["cards_skipped_first_n_offset"] += 1
                continue
            if max_cards > 0 and rep["cards_considered"] >= max_cards:
                stop_all = True
                break
            left = total_cards - stream_i
            nm = str(c.get("name") or "?")
            num = str(c.get("number") or "?")
            rep["cards_considered"] += 1
            print(
                f"[{stream_i}/{total_cards}] left={left} set={sc} ({set_name[:50]}) "
                f"card={nm!r} #{num}",
                flush=True,
            )
            pid = card_tcg_product_id(c)
            slug_from_set: Optional[str] = None
            need_listing = bool(
                set_path
                and (
                    not pid
                    or _filtered_wizard_history_row_count(c.get("pokemon_wizard_price_history")) < 1
                )
            )
            if need_listing and set_path and set_path not in set_index_cache:
                print(f"  -> fetch Wizard set index ... {set_path}", flush=True)
                time.sleep(sleep_s)
                set_html = fetch_wizard_set_page_html(
                    set_path,
                    log_label=f"{sc} set-index {set_path}",
                )
                set_index_cache[set_path] = parse_wizard_set_listing_index(set_html)
                rep["wizard_set_index_fetches"] += 1

            buddy_paths = [p for p in BUDDY_WIZARD_SET_PATHS.get(sc, ()) if p and p != set_path]
            for bp in buddy_paths:
                if need_listing and bp not in set_index_cache:
                    print(f"  -> fetch buddy Wizard set index ... {bp}", flush=True)
                    time.sleep(sleep_s)
                    set_index_cache[bp] = parse_wizard_set_listing_index(
                        fetch_wizard_set_page_html(bp, log_label=f"{sc} buddy-index {bp}")
                    )
                    rep["wizard_set_index_fetches"] += 1

            if need_listing and set_path and set_path in set_index_cache:
                idx_parts: List[Dict[str, Any]] = [set_index_cache[set_path]]
                for bp in buddy_paths:
                    if bp in set_index_cache:
                        idx_parts.append(set_index_cache[bp])
                work_idx = merge_wizard_listing_indexes(*idx_parts) if len(idx_parts) > 1 else idx_parts[0]
                if pid:
                    slug_from_pid = (work_idx.get("by_pid") or {}).get(pid)
                    if slug_from_pid:
                        slug_from_set = slug_from_pid
                if not pid:
                    resolved = resolve_wizard_card_from_set_index(work_idx, c)
                    if resolved:
                        pid, slug_from_set = resolved
                        rep["cards_resolved_set_page"] += 1
                        print(f"  -> resolved product_id={pid} from set listing", flush=True)
            if not pid:
                rep["cards_skipped_no_tcg_id"] += 1
                record_wizard_skip(
                    rep,
                    set_code=sc,
                    set_name=set_name,
                    card_number=num,
                    card_name=nm,
                    reason_code="NO_TCG_ID_NO_LISTING",
                    detail="No collectrics_tcg_player_id/tcgtracking_product_id and set listing did not resolve a product id",
                )
                print(
                    "  -> skip: no TCG product id and no set-listing match "
                    "(add scrape/pricecharting_game_slugs.json + run scrape/sync_pricecharting.py for PC comps)",
                    flush=True,
                )
                continue

            slug: Optional[str] = slug_from_set
            time.sleep(sleep_s)
            tcg_lbl = f"TCGPlayer {sc} #{num} pid={pid}"
            pname: Optional[str] = None
            if not slug:
                print("  -> fetch TCGPlayer product name ...", flush=True)
                pname = _timed(
                    tcg_lbl,
                    lambda: fetch_tcgplayer_product_name(pid, log_label=tcg_lbl),
                )
                if not pname:
                    rep["cards_skipped_tcg_api"] += 1
                    record_wizard_skip(
                        rep,
                        set_code=sc,
                        set_name=set_name,
                        card_number=num,
                        card_name=nm,
                        reason_code="TCGPLAYER_PRODUCT_EMPTY",
                        detail=f"pid={pid}",
                    )
                    print("  -> skip: TCGPlayer product details empty", flush=True)
                    continue
                slug = product_url_name_to_wizard_slug(pname)
            else:
                if only_missing_price_history:
                    pname = None
                else:
                    pname = _timed(
                        tcg_lbl,
                        lambda: fetch_tcgplayer_product_name(pid, log_label=tcg_lbl),
                    )
            if not slug:
                rep["cards_skipped_tcg_api"] += 1
                record_wizard_skip(
                    rep,
                    set_code=sc,
                    set_name=set_name,
                    card_number=num,
                    card_name=nm,
                    reason_code="NO_WIZARD_SLUG",
                    detail=f"pid={pid} productUrlName={pname!r}",
                )
                print("  -> skip: could not derive Wizard slug", flush=True)
                continue

            wurl = f"https://www.pokemonwizard.com/cards/{pid}/{slug}"

            try:
                time.sleep(sleep_s)
                wiz_lbl = f"Wizard card {sc} #{num} pid={pid}"
                print(f"  -> fetch Wizard card page ... {slug[:48]}", flush=True)
                html = _timed(
                    wiz_lbl,
                    lambda u=wurl, lbl=wiz_lbl: http_text(u, log_label=lbl),
                )
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as e:
                rep["cards_skipped_wizard_fetch"] += 1
                record_wizard_skip(
                    rep,
                    set_code=sc,
                    set_name=set_name,
                    card_number=num,
                    card_name=nm,
                    reason_code="WIZARD_FETCH_FAILED",
                    detail=f"{_http_err_kind(e)} url={wurl} err={e!r}",
                )
                print(f"  -> skip: Wizard card fetch failed ({_http_err_kind(e)}): {e!r}", flush=True)
                continue

            parsed = parse_wizard_card_page(html)
            if not parsed:
                rep["cards_skipped_wizard_fetch"] += 1
                record_wizard_skip(
                    rep,
                    set_code=sc,
                    set_name=set_name,
                    card_number=num,
                    card_name=nm,
                    reason_code="WIZARD_PAGE_UNPARSED",
                    detail=f"url={wurl}",
                )
                print("  -> skip: Wizard page did not parse (empty or layout change)", flush=True)
                continue

            old_wizard_hist = c.get("pokemon_wizard_price_history") if isinstance(c.get("pokemon_wizard_price_history"), list) else []
            clear_wizard_fields(c)
            c[f"{WIZ}url"] = wurl
            if pname:
                c[f"{WIZ}tcgplayer_product_url_name"] = pname
            for k, v in parsed.items():
                if v is None:
                    continue
                if k == "price_history" and isinstance(v, list):
                    c[f"{WIZ}{k}"] = merge_wizard_price_history_rows(old_wizard_hist, v)
                else:
                    c[f"{WIZ}{k}"] = v
            rep["cards_merged"] += 1
            print(
                f"  CARD DONE wizard set={sc} name={nm!r} #{num} status=MERGED "
                f"(cumulative merged={rep['cards_merged']})",
                flush=True,
            )

        if checkpoint_every_set:
            # With --skip-first-cards, avoid rewriting the whole JSON on every set while still in the skip zone.
            if skip_first_cards > 0 and stream_i <= skip_first_cards:
                pass
            else:
                try:
                    write_json_atomic(output_path, data)
                except OSError as e:
                    print(f"[wizard CHECKPOINT ERROR] {e!r}", flush=True)
                    raise
                print(
                    f"[wizard SET CHECKPOINT] set={sc!r} set_name={set_name[:56]!r} | "
                    f"totals: merged={rep['cards_merged']} considered={rep['cards_considered']} "
                    f"resume_skip_url={rep.get('cards_skipped_resume_has_url', 0)} "
                    f"skip_no_tcg={rep['cards_skipped_no_tcg_id']} skip_tcg_api={rep['cards_skipped_tcg_api']} "
                    f"skip_fetch={rep['cards_skipped_wizard_fetch']} | "
                    f"saved -> {output_path}",
                    flush=True,
                )

        if stop_all:
            break

    write_json_atomic(output_path, data)
    try:
        append_wizard_skip_run_log(input_path=input_path, output_path=output_path, rep=rep)
        print(f"Wrote skip log: {WIZARD_SYNC_SKIPS_LOG}", flush=True)
    except OSError as e:
        print(f"Warning: could not write {WIZARD_SYNC_SKIPS_LOG}: {e}", flush=True)
    rep_path = dataset_sidecar_report_path(output_path, ".pokemon_wizard_sync_report.json")
    rep_path.parent.mkdir(parents=True, exist_ok=True)
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(json.dumps(rep, indent=2))
    if strict_coverage and not only_star_listing_fix:
        assert_wizard_top25_coverage_strict(data, set_paths, filter_codes)
    return rep


def main() -> int:
    ap = argparse.ArgumentParser(description="Merge Pokemon Wizard price history into pokemon_sets_data.json")
    ap.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--output", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--sleep", type=float, default=0.15)
    ap.add_argument("--max-sets", type=int, default=0, help="0 = all sets (filter still applies)")
    ap.add_argument(
        "--max-cards",
        type=int,
        default=0,
        help="Stop after this many matching top-list cards (0 = no cap). Useful with --only-missing-price-history.",
    )
    ap.add_argument("--only-set-codes", default="", help="Comma-separated set_code values")
    ap.add_argument("--backup", action="store_true")
    ap.add_argument(
        "--no-checkpoint-every-set",
        action="store_true",
        help="By default the JSON file is atomically re-written after each set finishes so an interrupted run "
        "keeps all completed sets. Pass this to only write once at the end (faster disk, riskier).",
    )
    ap.add_argument(
        "--strict",
        action="store_true",
        help="After sync, exit 2 if any top_25_cards row in a set listed in pokemon_wizard_set_paths.json "
        "still lacks pokemon_wizard_url (use in CI after fixing matchers).",
    )
    ap.add_argument(
        "--only-star-listing-fix",
        action="store_true",
        help="Only process top_25_cards rows whose name contains Unicode ★/☆-class stars (Wizard listing "
        "used ASCII * / 'Star') and that still lack pokemon_wizard_url or usable price history. "
        "Skips all other cards to limit network use.",
    )
    ap.add_argument(
        "--only-missing-price-history",
        action="store_true",
        help="Only process top_25_cards in Wizard-mapped sets that still have zero usable "
        "pokemon_wizard_price_history rows after header filtering. Fetches each set listing once per set "
        "for slug/name matching, then only those cards' Wizard pages (skips TCGPlayer when listing provides slug).",
    )
    ap.add_argument(
        "--resume-skip-has-url",
        action="store_true",
        help="Skip cards that already have pokemon_wizard_url (no HTTP). Use after a stopped bulk run so "
        "only unfinished rows are scraped.",
    )
    ap.add_argument(
        "--skip-first-cards",
        type=int,
        default=0,
        help="Skip the first N top-list card rows in the same order as a full sync (no HTTP). Use after a hang "
        "to continue near card N+1 (e.g. --skip-first-cards 3800). Do not combine with --resume-skip-has-url unless "
        "you understand the stream order.",
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
    skip_first = max(0, int(args.skip_first_cards or 0))
    if skip_first and bool(args.resume_skip_has_url):
        print(
            "Warning: --skip-first-cards with --resume-skip-has-url changes stream order vs a plain bulk run; "
            "counts may not match a prior [n/total] log.",
            flush=True,
        )

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
        max_sets=max_sets,
        only_set_codes=only,
        strict_coverage=bool(args.strict),
        only_star_listing_fix=bool(args.only_star_listing_fix),
        only_missing_price_history=bool(args.only_missing_price_history),
        max_cards=max_cards,
        checkpoint_every_set=not bool(args.no_checkpoint_every_set),
        resume_skip_has_url=bool(args.resume_skip_has_url),
        skip_first_cards=skip_first,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
