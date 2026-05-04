#!/usr/bin/env python3
"""
Fetch TCGGO ``/episodes/{id}/cards`` (price-sorted) and merge a slim snapshot onto existing
``top_25_cards`` rows (matched by ``name`` + ``number``).

Uses ``tcggo.episode_id`` on each set. Reads ``RAPIDAPI_KEY_TCGGO`` or ``RAPIDAPI_KEY`` from ``ebay_listing_checker.env``.

Default: 15 newest sets (by ``release_date``) that have ``tcggo.episode_id``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fetch_tcggo_top_card_price_history import rapidapi_tcggo_key, tcggo_gateway_headers_query

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
DEFAULT_ENV = SCRIPT_DIR / "ebay_listing_checker.env"
DEFAULT_DATA = ROOT / "pokemon_sets_data.json"

HOST = "pokemon-tcg-api.p.rapidapi.com"


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


def norm_name(name: str) -> str:
    s = unicodedata.normalize("NFC", (name or "").strip()).casefold()
    return " ".join(s.split())


def norm_number(num: Any) -> str:
    return str(num if num is not None else "").strip()


def card_key(name: str, number: Any) -> tuple[str, str]:
    return (norm_name(name), norm_number(number))


def slim_api_card(row: dict[str, Any]) -> dict[str, Any]:
    prices = row.get("prices") if isinstance(row.get("prices"), dict) else {}
    tcg = prices.get("tcg_player") if isinstance(prices.get("tcg_player"), dict) else {}
    cm = prices.get("cardmarket") if isinstance(prices.get("cardmarket"), dict) else {}
    def pick(d: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
        return {k: d[k] for k in keys if k in d and d[k] is not None}

    out: dict[str, Any] = {
        "id": row.get("id"),
        "slug": row.get("slug"),
        "tcgid": row.get("tcgid"),
        "tcgplayer_id": row.get("tcgplayer_id"),
        "cardmarket_id": row.get("cardmarket_id"),
        "name_numbered": row.get("name_numbered"),
        "card_code_number": row.get("card_code_number"),
        "image": row.get("image"),
        "tcggo_url": row.get("tcggo_url"),
        "links": row.get("links") if isinstance(row.get("links"), dict) else None,
        "prices": {
            "tcg_player": pick(tcg, ("currency", "market_price", "mid_price")),
            "cardmarket": pick(cm, ("currency", "lowest_near_mint", "30d_average", "7d_average")),
        },
    }
    if out["links"] is None:
        del out["links"]
    return out


def fetch_cards(key: str, episode_id: int, *, per_page: int, sort: str, timeout: float) -> dict[str, Any]:
    hdrs, params = tcggo_gateway_headers_query(key, HOST, {"sort": sort, "per_page": str(per_page)})
    q = urllib.parse.urlencode(params)
    url = f"https://{HOST}/episodes/{int(episode_id)}/cards?{q}"
    req = urllib.request.Request(url, headers=hdrs, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def parse_release(s: str) -> datetime:
    s = (s or "").strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return datetime.min.replace(tzinfo=None)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    ap.add_argument("--data", type=Path, default=DEFAULT_DATA)
    ap.add_argument("--limit", type=int, default=15, help="Number of newest sets to fetch")
    ap.add_argument("--per-page", type=int, default=40)
    ap.add_argument("--sort", default="price_highest")
    ap.add_argument("--sleep", type=float, default=0.06, help="Seconds between API calls")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    load_env(args.env_file)
    key = rapidapi_tcggo_key()
    if not key:
        print("Set RAPIDAPI_KEY or RAPIDAPI_KEY_TCGGO in", args.env_file, file=sys.stderr)
        return 1

    data = json.loads(args.data.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("pokemon_sets_data.json must be a JSON array")

    candidates: list[tuple[datetime, dict[str, Any]]] = []
    for s in data:
        if not isinstance(s, dict):
            continue
        tg = s.get("tcggo")
        if not isinstance(tg, dict):
            continue
        eid = tg.get("episode_id")
        if eid is None:
            continue
        try:
            int(eid)
        except (TypeError, ValueError):
            continue
        dt = parse_release(str(s.get("release_date") or ""))
        candidates.append((dt, s))

    candidates.sort(key=lambda x: x[0], reverse=True)
    picked = [s for _, s in candidates[: max(0, args.limit)]]

    now = datetime.now(timezone.utc).isoformat()
    report: dict[str, Any] = {
        "sync_iso": now,
        "limit": args.limit,
        "per_page": args.per_page,
        "sort": args.sort,
        "sets": [],
    }

    for s in picked:
        tg = s.get("tcggo") or {}
        eid = int(tg["episode_id"])
        set_name = str(s.get("set_name") or "")
        set_code = s.get("set_code")

        try:
            payload = fetch_cards(key, eid, per_page=args.per_page, sort=args.sort, timeout=75.0)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")[:2000]
            print(f"HTTP {e.code} episode {eid} ({set_name}): {body}", file=sys.stderr)
            return 1

        rows = payload.get("data")
        if not isinstance(rows, list):
            print(f"No data[] for episode {eid} ({set_name})", file=sys.stderr)
            return 1

        by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for r in rows:
            if not isinstance(r, dict):
                continue
            k = card_key(str(r.get("name") or ""), r.get("card_number"))
            by_key[k] = r

        top = s.get("top_25_cards")
        if not isinstance(top, list):
            top = []
        matched = 0
        missing: list[dict[str, str]] = []
        for card in top:
            if not isinstance(card, dict):
                continue
            ck = card_key(str(card.get("name") or ""), card.get("number"))
            api_row = by_key.get(ck)
            if api_row:
                card["tcggo"] = slim_api_card(api_row)
                matched += 1
            else:
                missing.append({"name": str(card.get("name") or ""), "number": norm_number(card.get("number"))})

        s["tcggo_cards_sync"] = {
            "iso": now,
            "episode_id": eid,
            "per_page": args.per_page,
            "sort": args.sort,
            "api_results": payload.get("results"),
            "paging": payload.get("paging"),
            "matched_top25": matched,
            "unmatched_top25": missing,
        }

        report["sets"].append(
            {
                "set_code": set_code,
                "set_name": set_name,
                "episode_id": eid,
                "api_results": payload.get("results"),
                "matched_top25": matched,
                "unmatched_top25_count": len(missing),
            }
        )

        if args.sleep > 0:
            time.sleep(args.sleep)

    rep_path = args.data.with_suffix(".json.tcggo_cards_sync_report.json")
    rep_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    if args.dry_run:
        print(json.dumps(report, indent=2))
        return 0

    args.data.write_text(json.dumps(data, indent=4, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Updated {len(picked)} sets; report {rep_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
