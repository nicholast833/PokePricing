#!/usr/bin/env python3
"""
Upsert predictor/analytics sidecar JSON into Supabase ``predictor_analytics_assets``.

Reads each file from the **first path that exists** (in order):

  - ``$PREDICTOR_ANALYTICS_JSON_DIR/<name>.json`` if the env var is set (CI: point at a checkout or artifact dir)
  - ``<repo>/data/assets/<name>.json`` (track these via ``.gitignore`` exceptions under ``data/assets/``)
  - ``<repo>/scrape/output/<name>.json``
  - ``<repo>/<name>.json`` (repo root)

The repo ``.gitignore`` used to ignore all ``*.json`` and ``data/``; whitelisted paths under ``data/assets/`` exist so
Actions checkouts can include the four sidecar files once you add them.

Env (same as supabase_wizard_dataset_bridge): SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY.

Usage:
  python github_actions/sync_predictor_analytics_assets.py
  python github_actions/sync_predictor_analytics_assets.py --dry-run
  python github_actions/sync_predictor_analytics_assets.py --strict   # exit 1 if any file missing
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "github_actions"))

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


ASSETS: List[Tuple[str, str]] = [
    ("character_premium_scores", "character_premium_scores.json"),
    ("google_trends_momentum", "google_trends_momentum.json"),
    ("artist_scores", "artist_scores.json"),
    ("tcg_macro_interest_by_year", "tcg_macro_interest_by_year.json"),
]


def _load_env() -> None:
    if not load_dotenv:
        return
    load_dotenv(ROOT / ".env")
    load_dotenv(ROOT / "scrape" / "ebay_listing_checker.env")


def _supabase():
    _load_env()
    from supabase import create_client

    url = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
    key = (
        (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
        or (os.environ.get("SUPABASE_KEY") or "").strip()
    )
    if not url or not key:
        print("Missing SUPABASE_URL or SUPABASE_KEY / SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
        raise SystemExit(1)
    return create_client(url, key)


def _candidate_paths(name: str) -> List[Path]:
    extra = (os.environ.get("PREDICTOR_ANALYTICS_JSON_DIR") or "").strip()
    out: List[Path] = []
    if extra:
        out.append(Path(extra).expanduser().resolve() / name)
    out.extend(
        [
            ROOT / "data" / "assets" / name,
            ROOT / "scrape" / "output" / name,
            ROOT / name,
        ]
    )
    return out


def _read_json_file(name: str) -> Optional[Any]:
    for p in _candidate_paths(name):
        if p.is_file():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as e:
                print(f"WARN skip {name} at {p}: {e!r}", flush=True)
                return None
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Sync predictor analytics JSON sidecars to Supabase")
    ap.add_argument("--dry-run", action="store_true", help="Print payloads sizes only; no DB writes")
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 1 unless all four JSON files are found (default: exit 0 when nothing to upload so CI stays green)",
    )
    args = ap.parse_args()

    rows: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()
    for key, fname in ASSETS:
        data = _read_json_file(fname)
        if data is None:
            print(f"  skip {key}: file not found ({fname})", flush=True)
            continue
        rows.append({"asset_key": key, "payload": data, "updated_at": now})
        n = len(json.dumps(data)) if data is not None else 0
        print(f"  ok {key}: json chars ~{n}", flush=True)

    if not rows:
        print(
            "Nothing to upload (no JSON files found in data/assets/, scrape/output/, PREDICTOR_ANALYTICS_JSON_DIR, or repo root).",
            flush=True,
        )
        print(
            "  Add the four files under data/assets/ (see .gitignore whitelists) or set PREDICTOR_ANALYTICS_JSON_DIR.",
            flush=True,
        )
        return 1 if args.strict else 0

    if args.dry_run:
        print(f"Dry-run: would upsert {len(rows)} row(s).", flush=True)
        return 0

    client = _supabase()
    try:
        client.table("predictor_analytics_assets").upsert(rows, on_conflict="asset_key").execute()
    except Exception as e:
        print(f"Upsert failed: {e!r}", file=sys.stderr)
        print("Create table via supabase/migrations/20260209130000_predictor_analytics_assets.sql", file=sys.stderr)
        raise SystemExit(1)

    print(f"Upserted {len(rows)} asset(s) into predictor_analytics_assets.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
