"""
PSA Card Public API — **certificate lookup only** (official scope as of 2026).

Docs: https://www.psacard.com/publicapi/documentation
Swagger: https://api.psacard.com/publicapi/swagger

There is **no** documented bulk endpoint for population reports on this API.
A bearer token is used like:

  GET https://api.psacard.com/publicapi/cert/GetByCertNumber/{certNo}
  Header: Authorization: Bearer <token>

Free tier is **100 calls/day**; paid tiers exist. Use for slab verification,
not for filling `graded_population.json` at scale.

Get a token from the PSA developer portal after signing in:
  https://www.psacard.com/publicapi

Environment:
  PSA_BEARER_TOKEN   required for CLI (do not commit real tokens).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

BASE = "https://api.psacard.com/publicapi"
HEADERS_JSON = {"User-Agent": "Mozilla/5.0 PokemonTCG-Explorer/1.0 (psa_publicapi)"}


def cert_get_by_number(cert_no: str, bearer_token: str, timeout: int = 45) -> Dict[str, Any]:
    """Return parsed JSON from PSA cert lookup. Raises on HTTP errors."""
    digits = re.sub(r"\D", "", str(cert_no or ""))
    if not digits:
        raise ValueError("cert_no must contain digits")
    url = f"{BASE}/cert/GetByCertNumber/{digits}"
    req = urllib.request.Request(
        url,
        headers={
            **HEADERS_JSON,
            # PSA samples use "bearer"; scheme is case-insensitive per RFC 6757.
            "Authorization": f"bearer {bearer_token.strip()}",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    ap = argparse.ArgumentParser(description="PSA Public API: single cert lookup (demo)")
    ap.add_argument("cert", nargs="?", help="PSA cert number (digits)")
    ap.add_argument(
        "--token",
        default=os.environ.get("PSA_BEARER_TOKEN", ""),
        help="Bearer token or set PSA_BEARER_TOKEN",
    )
    args = ap.parse_args()
    if not args.cert:
        print(__doc__, file=sys.stderr)
        ap.error("cert number required")
    if not args.token:
        ap.error("Missing token: pass --token or set PSA_BEARER_TOKEN")
    try:
        data = cert_get_by_number(args.cert, args.token)
    except urllib.error.HTTPError as e:
        print("HTTP", e.code, e.read().decode("utf-8", errors="replace")[:500], file=sys.stderr)
        sys.exit(1)
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
