import re
import json
import urllib.request
import urllib.parse
import urllib.error
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple

FINDING_BASE = "https://svcs.ebay.com/services/search/FindingService/v1"
GRADED_KEYWORDS = frozenset(["psa", "cgc", "bgs", "beckett", "ace", "pca", "graded", "gem mint"])

def _is_graded(title: str) -> bool:
    lower = title.lower()
    return any(kw in lower for kw in GRADED_KEYWORDS)

def _unwrap(v: Any) -> Any:
    while isinstance(v, list) and len(v) == 1:
        v = v[0]
    return v

def _as_str(v: Any) -> str:
    v = _unwrap(v)
    if v is None:
        return ""
    if isinstance(v, dict):
        if "__value__" in v:
            return str(v["__value__"])
        return str(v)
    return str(v)

def _parse_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    title = _as_str(item.get("title")).strip()
    if not title or "shop on ebay" in title.lower():
        return None

    selling = item.get("sellingStatus")
    selling = _unwrap(selling)
    if not isinstance(selling, dict):
        return None
    price_raw = _unwrap(selling.get("currentPrice"))
    if not isinstance(price_raw, dict):
        return None
    val = price_raw.get("__value__")
    try:
        price = float(val)
    except (TypeError, ValueError):
        return None

    listing_info = item.get("listingInfo")
    listing_info = _unwrap(listing_info)
    if not isinstance(listing_info, dict):
        return None
    end_s = _as_str(listing_info.get("endTime")).strip()
    if not end_s:
        return None
    
    try:
        if end_s.endswith("Z"):
            dt = datetime.fromisoformat(end_s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(end_s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        date_only = dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
    except ValueError:
        return None

    return {"date": date_only, "price": price, "title": title}

def finding_find_completed_page(
    keywords: str,
    app_id: str,
    end_from_utc: datetime,
    end_to_utc: datetime,
    page: int = 1,
    per_page: int = 100,
    global_id: str = "EBAY-US",
    category_id: str = "183454"
) -> Tuple[str, List[Dict[str, Any]], int]:
    
    def _iso_z(dt: datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    q = [
        ("OPERATION-NAME", "findCompletedItems"),
        ("SERVICE-VERSION", "1.13.0"),
        ("SECURITY-APPNAME", app_id),
        ("GLOBAL-ID", global_id),
        ("RESPONSE-DATA-FORMAT", "JSON"),
        ("REST-PAYLOAD", ""),
        ("keywords", keywords[:350]),
        ("paginationInput.entriesPerPage", str(max(1, min(per_page, 100)))),
        ("paginationInput.pageNumber", str(max(1, page))),
        ("sortOrder", "EndTimeNewest"),
        ("itemFilter(0).name", "SoldItemsOnly"),
        ("itemFilter(0).value", "true"),
        ("itemFilter(1).name", "EndTimeFrom"),
        ("itemFilter(1).value", _iso_z(end_from_utc)),
        ("itemFilter(2).name", "EndTimeTo"),
        ("itemFilter(2).value", _iso_z(end_to_utc)),
    ]
    if category_id:
        q.append(("categoryId", category_id))
        
    url = FINDING_BASE + "?" + urllib.parse.urlencode(q)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PokemonTCG-Explorer (Finding API)"}, method="GET")
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"eBay HTTP Error: {e}")
        return "Failure", [], 0

    try:
        root = json.loads(raw)
    except json.JSONDecodeError:
        return "Failure", [], 0

    resp_block = _unwrap(root.get("findCompletedItemsResponse"))
    if not isinstance(resp_block, dict):
        return "Failure", [], 0

    ack = _as_str(resp_block.get("ack")).lower()
    ack_norm = "Success" if "success" in ack else "Warning" if "warning" in ack else "Failure"

    if ack_norm == "Failure":
        return ack_norm, [], 0

    sr = _unwrap(resp_block.get("searchResult"))
    if not isinstance(sr, dict):
        return ack_norm, [], 0

    total = 0
    try:
        tc = sr.get("@count")
        if tc is not None:
            total = int(_unwrap(tc))
    except Exception:
        pass

    raw_items = sr.get("item")
    if not raw_items:
        return ack_norm, [], total
    if isinstance(raw_items, dict):
        raw_items = [raw_items]
        
    out = []
    for it in raw_items:
        parsed = _parse_item(it) if isinstance(it, dict) else None
        if parsed:
            out.append(parsed)
            
    return ack_norm, out, total

def fetch_ebay_sold_listings(keywords: str, app_id: str, days: int = 30) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetches the last N days of eBay sold listings for the given keywords.
    Returns separated graded and ungraded lists.
    """
    now = datetime.now(timezone.utc)
    end_from = now - timedelta(days=max(1, days))
    seen = set()
    merged = []
    
    # Try with and without category limit to ensure we hit results
    for cat in ["183454", ""]:
        ack, items, total = finding_find_completed_page(
            keywords=keywords,
            app_id=app_id,
            end_from_utc=end_from,
            end_to_utc=now,
            page=1,
            per_page=100,
            category_id=cat
        )
        if ack != "Failure":
            for it in items:
                key = (it["date"], round(float(it["price"]), 2), it["title"][:96])
                if key not in seen:
                    seen.add(key)
                    merged.append(it)
            if items:
                break
                
    cutoff = end_from.date().isoformat()
    merged = [x for x in merged if x.get("date") and x["date"] >= cutoff]
    merged.sort(key=lambda x: x["date"])
    
    return {
        "graded": [x for x in merged if _is_graded(x["title"])],
        "ungraded": [x for x in merged if not _is_graded(x["title"])]
    }
