#!/usr/bin/env python3
"""
Download all Pokémon TCG episodes from RapidAPI (TCGGO) with pagination.

GET https://pokemon-tcg-api.p.rapidapi.com/episodes?page=N

Reads ``RAPIDAPI_KEY_TCGGO`` or ``RAPIDAPI_KEY`` from ``ebay_listing_checker.env``.
Writes ``tcggo_episodes_all.json`` in this folder by default: ``{ \"data\": [...] }`` with only
``game.slug == \"pokemon\"`` rows (deduped by episode ``id``).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from fetch_tcggo_top_card_price_history import rapidapi_tcggo_key, tcggo_gateway_headers_query

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
DEFAULT_ENV = SCRIPT_DIR / "ebay_listing_checker.env"
DEFAULT_OUT = SCRIPT_DIR / "tcggo_episodes_all.json"

HOST = "pokemon-tcg-api.p.rapidapi.com"
PATH = "/episodes"


def load_env(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, _, v = s.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def fetch_page(key: str, page: int, timeout: float) -> dict[str, Any]:
    hdrs, params = tcggo_gateway_headers_query(key, HOST, {"page": str(page)})
    q = urllib.parse.urlencode(params)
    url = f"https://{HOST}{PATH}?{q}"
    req = urllib.request.Request(url, headers=hdrs, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output JSON path")
    ap.add_argument("--sleep", type=float, default=0.05, help="Seconds between page requests")
    ap.add_argument("--max-pages", type=int, default=50, help="Safety cap on pages (default 50)")
    args = ap.parse_args()

    load_env(args.env_file)
    key = rapidapi_tcggo_key()
    if not key:
        print("Set RAPIDAPI_KEY or RAPIDAPI_KEY_TCGGO in", args.env_file, file=sys.stderr)
        return 1

    all_rows: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    paging_meta: dict[str, Any] = {}
    page = 1
    total_pages_hint: int | None = None

    while page <= args.max_pages:
        try:
            payload = fetch_page(key, page, timeout=60.0)
        except urllib.error.HTTPError as e:
            print(f"HTTP {e.code} on page {page}:", e.read()[:500].decode(errors="replace"), file=sys.stderr)
            return 1

        rows = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise SystemExit(f"Unexpected response (no list data) on page {page}")

        pg = payload.get("paging") if isinstance(payload, dict) else None
        if isinstance(pg, dict) and page == 1:
            paging_meta = dict(pg)
            total_pages_hint = int(pg["total"]) if pg.get("total") is not None else None

        added = 0
        for r in rows:
            if not isinstance(r, dict):
                continue
            if (r.get("game") or {}).get("slug") != "pokemon":
                continue
            eid = r.get("id")
            try:
                iid = int(eid)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                continue
            if iid in seen_ids:
                continue
            seen_ids.add(iid)
            all_rows.append(r)
            added += 1

        print(f"page {page}: {len(rows)} raw rows, +{added} new pokemon episodes (total {len(all_rows)})")

        if len(rows) == 0:
            break
        if total_pages_hint is not None and page >= total_pages_hint:
            break
        if len(rows) < (paging_meta.get("per_page") or 20):
            break

        page += 1
        if args.sleep > 0:
            time.sleep(args.sleep)

    out_obj = {
        "data": all_rows,
        "paging": {**paging_meta, "pages_fetched": page, "pokemon_episodes": len(all_rows)},
        "results": len(all_rows),
    }
    args.out.write_text(json.dumps(out_obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {args.out} ({len(all_rows)} pokemon episodes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
