#!/usr/bin/env python3
"""
Probe ScrapeChain eBay completed (sold) listings API for a specific SIR card.

Docs: https://github.com/colindaniels/eBay-sold-items-documentation
Endpoint: POST https://ebay-api.scrapechain.com/findCompletedItems

Prints aggregate stats (results count = products returned, total_results) and a short
sample of products so you can confirm sales volume / fields for Meowth ex #121 (ME3).
"""
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

URL = "https://ebay-api.scrapechain.com/findCompletedItems"

# Primary: strict category + long tail (may return 0 if eBay has no sold rows in that slice).
PAYLOAD_STRICT = {
    "keywords": "Pokemon Meowth ex 121 ME3 SIR special illustration",
    "excluded_keywords": "lot bulk proxy custom",
    "max_search_results": 120,
    "remove_outliers": True,
    "site_id": "0",
    "category_id": "183454",
}

# Fallback: broader keywords, no category (API warns; often more sold hits — good for volume check).
PAYLOAD_BROAD = {
    "keywords": "Meowth ex 121 Pokemon Perfect Order",
    "excluded_keywords": "lot bulk proxy",
    "max_search_results": 120,
    "remove_outliers": False,
    "site_id": "0",
}


def fetch(payload: dict) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def main() -> int:
    try:
        data = fetch(PAYLOAD_STRICT)
    except urllib.error.HTTPError as e:
        print("HTTP error:", e.code, e.reason, file=sys.stderr)
        print(e.read().decode("utf-8", errors="replace")[:2000], file=sys.stderr)
        return 1
    except urllib.error.URLError as e:
        print("URL error:", e, file=sys.stderr)
        return 1
    except json.JSONDecodeError as e:
        print("Invalid JSON:", e, file=sys.stderr)
        return 1

    used = PAYLOAD_STRICT
    if not data.get("results") and data.get("success"):
        print("(strict query returned 0 products; retrying broader payload)\n")
        data = fetch(PAYLOAD_BROAD)
        used = PAYLOAD_BROAD

    print("=== ScrapeChain findCompletedItems ===")
    print("payload:", json.dumps(used, indent=2))
    if data.get("warning"):
        print("warning:", data.get("warning"))
    print("success:", data.get("success"))
    print("results (products in this response):", data.get("results"))
    print("total_results (eBay sold pool size):", data.get("total_results"))
    print("average_price:", data.get("average_price"), "| median_price:", data.get("median_price"))
    print("min_price:", data.get("min_price"), "| max_price:", data.get("max_price"))
    print("response_url:", data.get("response_url"))
    products = data.get("products") or []
    print("\n--- first 5 products ---")
    for i, p in enumerate(products[:5]):
        print(i + 1, "|", p.get("date_sold"), "|", p.get("sale_price"), p.get("currency"), "|", (p.get("title") or "")[:90])
    return 0 if data.get("success") else 2


if __name__ == "__main__":
    raise SystemExit(main())
