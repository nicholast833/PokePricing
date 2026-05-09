#!/usr/bin/env python3
"""
Merge active-listing discovery from eBay Buy Browse API into pokemon_sets_data.json card rows.

Uses application-only OAuth (client_credentials): EBAY_APP_ID + EBAY_CERT_ID as client_id /
client_secret. No user sign-in. Does not scrape HTML.

Respectful defaults:
  - Official REST endpoints only (Identity + Browse).
  - Throttled requests (--sleep / EBAY_BROWSE_SLEEP_SECONDS, default ~0.55s between searches).
  - Small page size (--limit, default 10) to minimize payload.
  - Refreshes access token once on 401.

Stored fields are limited to listing URLs/titles/prices, hit counts, and search URL — no seller identities, feedback, buying options, or item IDs.

Example (Perfect Order only):
  python scrape/sync_ebay_browse_listings.py --only-set-codes me3 --backup

Env file: scrape/ebay_listing_checker.env (see ebay_listing_checker.env.example)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shutil
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from curl_cffi import requests as cffi_requests
    _CFFI_SESSION = cffi_requests.Session(impersonate="chrome110")
except ImportError:
    raise SystemExit("curl_cffi is required: pip install curl_cffi")

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(ROOT / "scrape"))

from dataset_report_paths import dataset_sidecar_report_path  # noqa: E402
from json_atomic_util import write_json_atomic  # noqa: E402

DEFAULT_ENV = SCRIPT_DIR / "ebay_listing_checker.env"
EBAY_FIELD_PREFIX = "ebay_browse_"


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            continue
        k, _, v = s.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and (k not in os.environ or os.environ[k] == ""):
            os.environ[k] = v


def load_ebay_env(cli_env: Path) -> List[Path]:
    """Load env files in order (later files only fill missing keys). Returns paths that existed."""
    tried: List[Path] = []
    seen: set[str] = set()
    primary = cli_env.resolve()
    candidates = [primary]
    if primary == DEFAULT_ENV.resolve():
        root_env = (ROOT / "ebay_listing_checker.env").resolve()
        if root_env != primary:
            candidates.append(root_env)
    for p in candidates:
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        tried.append(p)
        load_env_file(p)
    return [p for p in tried if p.is_file()]


def _env(name: str, default: Optional[str] = None) -> str:
    return (os.environ.get(name, default) or "").strip()


def _api_base() -> str:
    if _env("EBAY_USE_SANDBOX", "false").lower() in ("1", "true", "yes"):
        return "https://api.sandbox.ebay.com"
    return "https://api.ebay.com"


def clear_ebay_browse_fields(card: Dict[str, Any]) -> None:
    for k in list(card.keys()):
        if isinstance(k, str) and k.startswith(EBAY_FIELD_PREFIX):
            del card[k]


def fetch_application_token() -> Tuple[str, int]:
    """Returns (access_token, expires_in_seconds)."""
    cid = _env("EBAY_APP_ID")
    csec = _env("EBAY_CERT_ID")
    if not cid or not csec:
        raise SystemExit("EBAY_APP_ID and EBAY_CERT_ID are required for Browse API (OAuth client credentials).")

    scope = _env("EBAY_OAUTH_SCOPE", "https://api.ebay.com/oauth/api_scope")
    url = f"{_api_base()}/identity/v1/oauth2/token"
    basic = base64.b64encode(f"{cid}:{csec}".encode("utf-8")).decode("ascii")
    try:
        resp = _CFFI_SESSION.post(
            url,
            data={"grant_type": "client_credentials", "scope": scope},
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {basic}",
                "User-Agent": "PokemonTCG-Explorer/sync_ebay_browse_listings (hobbyist; OAuth)",
            },
            timeout=60,
        )
    except Exception as exc:
        raise SystemExit(f"OAuth token request failed: {exc}") from exc

    if resp.status_code != 200:
        raise SystemExit(f"OAuth token request failed HTTP {resp.status_code}: {resp.text[:800]}")

    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise SystemExit(f"OAuth response missing access_token: {data!r}")
    exp = int(data.get("expires_in", 7200) or 7200)
    return str(token), exp


def browse_search(
    token: str,
    *,
    q: str,
    limit: int,
    marketplace_id: str,
) -> Tuple[int, Dict[str, Any]]:
    """Returns (http_status, parsed_json)."""
    params = urllib.parse.urlencode({"q": q, "limit": str(max(1, min(limit, 50)))})
    url = f"{_api_base()}/buy/browse/v1/item_summary/search?{params}"
    try:
        resp = _CFFI_SESSION.get(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "X-EBAY-C-MARKETPLACE-ID": marketplace_id,
                "Content-Type": "application/json",
                "User-Agent": "PokemonTCG-Explorer/sync_ebay_browse_listings (hobbyist; Buy Browse)",
            },
            timeout=60,
        )
    except Exception as exc:
        return 0, {"error": str(exc)}

    try:
        payload = resp.json()
    except Exception:
        payload = {"raw": resp.text[:1200]}
    return resp.status_code, payload


def build_search_query(set_name: str, card_name: str, card_number: Any) -> str:
    parts = ["Pokemon TCG", set_name.strip(), str(card_name or "").strip(), f"#{str(card_number or '').strip()}"]
    q = " ".join(p for p in parts if p and p != "#")
    q = re.sub(r"\s+", " ", q).strip()
    return q[:350]


def ebay_sch_i_html_url(q: str) -> str:
    """Same-keyword search on eBay web (all matching listings), not a single item."""
    return "https://www.ebay.com/sch/i.html?" + urllib.parse.urlencode({"_nkw": q})


def _snippet_from_hit(hit: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Compact listing row for UI (title, link, price)."""
    if not isinstance(hit, dict):
        return None
    out: Dict[str, Any] = {}
    title = hit.get("title")
    if title:
        out["title"] = str(title)[:240]
    url = hit.get("itemWebUrl")
    if url:
        out["url"] = str(url)
    price = hit.get("price") if isinstance(hit.get("price"), dict) else {}
    val = price.get("value")
    if val is not None:
        try:
            out["price_value"] = float(val)
        except (TypeError, ValueError):
            out["price_value"] = val
    cur = price.get("currency")
    if cur:
        out["price_currency"] = str(cur)
    img = hit.get("image")
    if isinstance(img, dict):
        iu = img.get("imageUrl") or img.get("image_url")
        if iu:
            out["image_url"] = str(iu)[:800]
    thumbs = hit.get("thumbnailImages")
    if not out.get("image_url") and isinstance(thumbs, list) and thumbs:
        t0 = thumbs[0] if isinstance(thumbs[0], dict) else None
        if isinstance(t0, dict):
            iu = t0.get("imageUrl") or t0.get("image_url")
            if iu:
                out["image_url"] = str(iu)[:800]
    if not out.get("title") and not out.get("url"):
        return None
    return out


def merge_first_hit(card: Dict[str, Any], data: Dict[str, Any], q: str) -> None:
    clear_ebay_browse_fields(card)
    now = datetime.now(timezone.utc).isoformat()
    card[f"{EBAY_FIELD_PREFIX}sync_iso"] = now
    card[f"{EBAY_FIELD_PREFIX}query"] = q
    card[f"{EBAY_FIELD_PREFIX}search_url"] = ebay_sch_i_html_url(q)
    total = data.get("total")
    if total is not None:
        try:
            card[f"{EBAY_FIELD_PREFIX}result_total"] = int(total)
        except (TypeError, ValueError):
            card[f"{EBAY_FIELD_PREFIX}result_total"] = total

    summaries = data.get("itemSummaries") if isinstance(data.get("itemSummaries"), list) else []
    snippets: List[Dict[str, Any]] = []
    for raw in summaries:
        if not isinstance(raw, dict):
            continue
        sn = _snippet_from_hit(raw)
        if sn:
            snippets.append(sn)
    if snippets:
        card[f"{EBAY_FIELD_PREFIX}item_summaries"] = snippets

    if not summaries:
        card[f"{EBAY_FIELD_PREFIX}first_item_url"] = None
        return

    hit = summaries[0] if isinstance(summaries[0], dict) else {}
    url = hit.get("itemWebUrl")
    if url:
        card[f"{EBAY_FIELD_PREFIX}first_item_url"] = url
    title = hit.get("title")
    if title:
        card[f"{EBAY_FIELD_PREFIX}first_item_title"] = title
    price = hit.get("price") if isinstance(hit.get("price"), dict) else {}
    val = price.get("value")
    cur = price.get("currency")
    if val is not None:
        try:
            card[f"{EBAY_FIELD_PREFIX}first_item_price_value"] = float(val)
        except (TypeError, ValueError):
            card[f"{EBAY_FIELD_PREFIX}first_item_price_value"] = val
    if cur:
        card[f"{EBAY_FIELD_PREFIX}first_item_price_currency"] = str(cur)


def run(
    *,
    input_path: Path,
    output_path: Path,
    only_set_codes: Optional[set[str]],
    limit: int,
    sleep_s: float,
    marketplace_id: str,
    initial_token: str,
) -> Dict[str, Any]:
    data = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("Expected pokemon_sets_data.json as a JSON array")

    token = initial_token
    rep: Dict[str, Any] = {
        "sync_iso": datetime.now(timezone.utc).isoformat(),
        "sets_processed": 0,
        "cards_considered": 0,
        "cards_merged": 0,
        "http_errors": 0,
        "token_refreshed_mid_run": False,
        "ebay_cards_with_positive_total": 0,
        "ebay_cards_zero_total": 0,
    }

    for s in data:
        if not isinstance(s, dict):
            continue
        sc = str(s.get("set_code") or "").strip().lower()
        if only_set_codes is not None and sc not in only_set_codes:
            continue
        top = s.get("top_25_cards")
        if not isinstance(top, list):
            continue
        set_name = str(s.get("set_name") or sc)
        rep["sets_processed"] += 1
        rows = [x for x in top if isinstance(x, dict)]
        nrows = len(rows)
        print(f"[eBay Browse] set={sc!r} name={set_name[:48]!r} cards={nrows}", flush=True)

        for idx, c in enumerate(rows, start=1):
            rep["cards_considered"] += 1
            nm = str(c.get("name") or "")
            num = c.get("number")
            q = build_search_query(set_name, nm, num)
            print(f"  [eBay PROGRESS] card {idx}/{nrows} #{num} {nm[:40]!r} …", flush=True)
            status, payload = browse_search(token, q=q, limit=limit, marketplace_id=marketplace_id)
            if status == 401:
                token, _ = fetch_application_token()
                rep["token_refreshed_mid_run"] = True
                print("  [eBay PROGRESS] refreshed OAuth token (401)", flush=True)
                status, payload = browse_search(token, q=q, limit=limit, marketplace_id=marketplace_id)
            if status != 200:
                rep["http_errors"] += 1
                print(f"  [eBay PROGRESS] card {idx}/{nrows} HTTP {status} q={q[:72]!r}", flush=True)
                print(f"  WARN body={str(payload)[:220]!r}", flush=True)
                clear_ebay_browse_fields(c)
                c[f"{EBAY_FIELD_PREFIX}sync_iso"] = datetime.now(timezone.utc).isoformat()
                c[f"{EBAY_FIELD_PREFIX}query"] = q
                c[f"{EBAY_FIELD_PREFIX}search_url"] = ebay_sch_i_html_url(q)
                c[f"{EBAY_FIELD_PREFIX}http_error"] = status
            else:
                merge_first_hit(c, payload, q)
                rep["cards_merged"] += 1
                tot = c.get(f"{EBAY_FIELD_PREFIX}result_total")
                try:
                    ti = int(tot) if tot is not None else 0
                except (TypeError, ValueError):
                    ti = 0
                if ti > 0:
                    rep["ebay_cards_with_positive_total"] += 1
                else:
                    rep["ebay_cards_zero_total"] += 1
                url = c.get(f"{EBAY_FIELD_PREFIX}first_item_url")
                price = c.get(f"{EBAY_FIELD_PREFIX}first_item_price_value")
                print(
                    f"  [eBay PROGRESS] card {idx}/{nrows} OK total={tot!r} "
                    f"price={price!r} url={'yes' if url else 'no'}",
                    flush=True,
                )
            time.sleep(max(0.0, sleep_s))

        write_json_atomic(output_path, data)
        print(f"  checkpoint saved -> {output_path}", flush=True)

    return rep


def main() -> int:
    ap = argparse.ArgumentParser(description="eBay Buy Browse API listing discovery (application OAuth)")
    ap.add_argument("--input", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--output", type=Path, default=ROOT / "pokemon_sets_data.json")
    ap.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    ap.add_argument("--only-set-codes", default="", help="Comma-separated set_code values (e.g. me3)")
    ap.add_argument("--limit", type=int, default=10, help="Max item summaries per search (1-50)")
    ap.add_argument("--sleep", type=float, default=-1.0, help="Seconds between searches; default from env or 0.55")
    ap.add_argument("--marketplace", default="", help="Override X-EBAY-C-MARKETPLACE-ID (default EBAY_US or EBAY_MARKETPLACE_ID)")
    ap.add_argument("--backup", action="store_true")
    ap.add_argument(
        "--all-sets",
        action="store_true",
        help="Allow syncing every set's top_25_cards (many eBay calls). Without this, you must pass --only-set-codes.",
    )
    args = ap.parse_args()

    cli_e = args.env_file.resolve()
    loaded_from = load_ebay_env(cli_e)
    if not _env("EBAY_APP_ID") or not _env("EBAY_CERT_ID"):
        candidates = [cli_e]
        if cli_e == DEFAULT_ENV.resolve():
            root_e = (ROOT / "ebay_listing_checker.env").resolve()
            if root_e != cli_e:
                candidates.append(root_e)
        tried = ", ".join(str(p) for p in candidates)
        found = ", ".join(str(p) for p in loaded_from) if loaded_from else "(none exist on disk)"
        raise SystemExit(
            "EBAY_APP_ID and EBAY_CERT_ID are required for Browse API (OAuth client credentials).\n"
            f"Looked for: {tried}\n"
            f"Found/read: {found}\n"
            f"Create {DEFAULT_ENV} (copy from ebay_listing_checker.env.example) or export EBAY_APP_ID / EBAY_CERT_ID."
        )

    only = None
    if args.only_set_codes.strip():
        only = {x.strip().lower() for x in args.only_set_codes.split(",") if x.strip()}
    if not only and not args.all_sets:
        raise SystemExit(
            "Refusing a full-catalog run: pass --only-set-codes me3 (or comma list), or --all-sets to sync every set."
        )

    sleep_s = args.sleep
    if sleep_s < 0:
        try:
            sleep_s = float(_env("EBAY_BROWSE_SLEEP_SECONDS", "0.55") or "0.55")
        except ValueError:
            sleep_s = 0.55

    mkt = (args.marketplace.strip() or _env("EBAY_MARKETPLACE_ID", "EBAY_US") or "EBAY_US").strip()

    inp = args.input.resolve()
    out = args.output.resolve()

    print("OAuth: fetching application access token…", flush=True)
    initial_token, _exp = fetch_application_token()
    print("OAuth: OK (token acquired)", flush=True)

    if args.backup and inp == out and inp.is_file():
        bak = inp.with_suffix(inp.suffix + ".bak")
        shutil.copy2(inp, bak)
        print("Wrote backup", bak, flush=True)

    rep = run(
        input_path=inp,
        output_path=out,
        only_set_codes=only,
        limit=max(1, min(int(args.limit), 50)),
        sleep_s=max(0.0, sleep_s),
        marketplace_id=mkt,
        initial_token=initial_token,
    )
    rep["browse_api_note"] = (
        "ebay_browse_result_total is the Browse search hit count (active listings for the query). "
        "ebay_browse_search_url is the eBay web search for the same keywords (all matching listings). "
        "ebay_browse_item_summaries is a compact copy of returned item summaries (active listings, not sold history). "
        "Sold time series: use collectrics_history_ebay / PriceCharting, not Browse item_summary/search."
    )
    rep_path = dataset_sidecar_report_path(out, ".ebay_browse_sync_report.json")
    rep_path.parent.mkdir(parents=True, exist_ok=True)
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(json.dumps(rep, indent=2), flush=True)
    print("Wrote", rep_path, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
