"""eBay helpers for GitHub Actions.

**Finding API** ``findCompletedItems``: legacy sold/completed search; often returns **zero**
results for new keys, GitHub Actions IPs, or policy limits. **Buy Browse** cannot return sold
items — only **active** listings (OAuth: ``EBAY_APP_ID`` + ``EBAY_CERT_ID``).

``run_daily_api_queue`` uses **Buy Browse** via ``fetch_ebay_active_listing_snapshot`` (hit
``total`` plus a few **price-only** listing rows for the UI; item ids/titles/URLs are not stored).

``anonymous_cohort`` holds SHA-256(itemId + salt) plus rounded USD for day-over-day “listing
ended” heuristics without persisting eBay listing identifiers.
"""

import base64
import hashlib
import json
import math
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

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


def _ebay_api_base() -> str:
    if (os.environ.get("EBAY_USE_SANDBOX") or "").strip().lower() in ("1", "true", "yes"):
        return "https://api.sandbox.ebay.com"
    return "https://api.ebay.com"


def fetch_ebay_application_token(app_id: str, cert_id: str) -> Tuple[str, int]:
    """
    Client-credentials token for Buy Browse (same as scrape/sync_ebay_browse_listings).
    Requires EBAY_APP_ID (OAuth client_id) and EBAY_CERT_ID (client_secret).
    Returns ``(access_token, expires_in_seconds)``.
    """
    scope = (os.environ.get("EBAY_OAUTH_SCOPE") or "https://api.ebay.com/oauth/api_scope").strip()
    url = f"{_ebay_api_base()}/identity/v1/oauth2/token"
    basic = base64.b64encode(f"{app_id}:{cert_id}".encode("utf-8")).decode("ascii")
    resp = requests.post(
        url,
        data={"grant_type": "client_credentials", "scope": scope},
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic}",
            "User-Agent": "PokemonTCG-Explorer/github_actions (Buy Browse)",
        },
        timeout=45,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"eBay OAuth HTTP {resp.status_code}: {resp.text[:500]}")
    data = resp.json()
    tok = data.get("access_token")
    if not tok:
        raise RuntimeError(f"eBay OAuth missing access_token: {data!r}")
    exp = int(data.get("expires_in", 7200) or 7200)
    return str(tok), exp


_OAUTH_CACHE: Dict[str, Any] = {"app_id": "", "cert_id": "", "token": "", "exp_mono": 0.0}


def _get_browse_oauth_token(app_id: str, cert_id: str) -> str:
    """Reuse one application token across many Browse searches (daily queue)."""
    now = time.monotonic()
    if (
        _OAUTH_CACHE["token"]
        and _OAUTH_CACHE["app_id"] == app_id
        and _OAUTH_CACHE["cert_id"] == cert_id
        and now < _OAUTH_CACHE["exp_mono"]
    ):
        return str(_OAUTH_CACHE["token"])
    tok, exp = fetch_ebay_application_token(app_id, cert_id)
    _OAUTH_CACHE.update(
        {
            "app_id": app_id,
            "cert_id": cert_id,
            "token": tok,
            "exp_mono": now + max(120.0, float(exp) - 120.0),
        }
    )
    return tok


def invalidate_browse_oauth_cache() -> None:
    _OAUTH_CACHE["token"] = ""
    _OAUTH_CACHE["exp_mono"] = 0.0


def ebay_listing_hash_salt(app_id: str, cert_id: str) -> str:
    custom = (os.environ.get("EBAY_LISTING_HASH_SALT") or "").strip()
    if custom:
        return custom
    return hashlib.sha256(f"{app_id}|{cert_id}|ebay_anon_v1".encode("utf-8")).hexdigest()


def ebay_redacted_active_snapshots(item_summaries: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    """Buy Browse item summaries reduced to non-identifying price fields only."""
    out: List[Dict[str, Any]] = []
    lim = max(1, min(int(limit), 50))
    for raw in item_summaries[:lim]:
        if not isinstance(raw, dict):
            continue
        price = raw.get("price") if isinstance(raw.get("price"), dict) else {}
        val = price.get("value")
        cur = price.get("currency")
        sn: Dict[str, Any] = {}
        if val is not None:
            try:
                sn["price_value"] = float(val)
            except (TypeError, ValueError):
                sn["price_value"] = val
        if cur:
            sn["price_currency"] = str(cur)
        if sn:
            out.append(sn)
    return out


def ebay_anonymous_listing_cohort(
    item_summaries: List[Dict[str, Any]],
    *,
    salt: str,
    limit: int,
) -> List[Dict[str, Any]]:
    """SHA-256(itemId + salt) per sampled listing + rounded BIN price (no titles or URLs)."""
    out: List[Dict[str, Any]] = []
    lim = max(1, min(int(limit), 50))
    for raw in item_summaries[:lim]:
        if not isinstance(raw, dict):
            continue
        iid = raw.get("itemId") or raw.get("item_id")
        if not iid:
            continue
        price = raw.get("price") if isinstance(raw.get("price"), dict) else {}
        val = price.get("value")
        try:
            pv = float(val)
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(pv) and pv > 0):
            continue
        sig = hashlib.sha256(f"{salt}|{iid}".encode("utf-8")).hexdigest()
        out.append({"sig": sig, "bin_usd": round(pv, 2)})
    return out


def fetch_ebay_active_listing_snapshot(
    keywords: str,
    *,
    app_id: str,
    cert_id: str,
    limit: int = 5,
    marketplace_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    **Buy Browse API** — active listings only (eBay does not expose completed sales here).

    Returns ``total`` from the API, **price-only** ``snapshots`` (no titles or listing URLs),
    and ``anonymous_cohort`` (hashed listing ids + rounded prices) for longitudinal storage.
    """
    mp = (marketplace_id or os.environ.get("EBAY_MARKETPLACE_ID") or "EBAY_US").strip()
    token = _get_browse_oauth_token(app_id, cert_id)
    qenc = urllib.parse.urlencode(
        {"q": keywords[:350], "limit": str(max(1, min(int(limit), 50)))}
    )
    url = f"{_ebay_api_base()}/buy/browse/v1/item_summary/search?{qenc}"
    resp = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": mp,
            "Content-Type": "application/json",
            "User-Agent": "PokemonTCG-Explorer/github_actions (Buy Browse)",
        },
        timeout=45,
    )
    if resp.status_code == 401:
        invalidate_browse_oauth_cache()
        token = _get_browse_oauth_token(app_id, cert_id)
        resp = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "X-EBAY-C-MARKETPLACE-ID": mp,
                "Content-Type": "application/json",
                "User-Agent": "PokemonTCG-Explorer/github_actions (Buy Browse)",
            },
            timeout=45,
        )
    out: Dict[str, Any] = {
        "http_status": resp.status_code,
        "search_url": "https://www.ebay.com/sch/i.html?" + urllib.parse.urlencode({"_nkw": keywords[:350]}),
        "total": None,
        "snapshots": [],
        "anonymous_cohort": [],
        "raw_error": None,
    }
    try:
        data = resp.json()
    except Exception:
        out["raw_error"] = resp.text[:800]
        return out

    if resp.status_code != 200:
        out["raw_error"] = str(data)[:800]
        return out

    tot = data.get("total")
    if tot is not None:
        try:
            out["total"] = int(tot)
        except (TypeError, ValueError):
            out["total"] = tot

    summaries = data.get("itemSummaries") if isinstance(data.get("itemSummaries"), list) else []
    lim = max(1, min(int(limit), 50))
    salt = ebay_listing_hash_salt(app_id, cert_id)
    out["snapshots"] = ebay_redacted_active_snapshots(summaries, lim)
    out["anonymous_cohort"] = ebay_anonymous_listing_cohort(summaries, salt=salt, limit=lim)
    return out


def build_ebay_active_search_query(card: Dict[str, Any]) -> str:
    """Browse search string aligned with ``sync_ebay_browse_listings.build_search_query`` when set_name is missing."""
    set_code = str(card.get("set_code") or "").strip()
    name = str(card.get("name") or "").strip()
    num = str(card.get("number") or "").strip()
    metrics = card.get("metrics") if isinstance(card.get("metrics"), dict) else {}
    set_name = str(metrics.get("set_name") or metrics.get("set_title") or "").strip()
    if not set_name:
        set_name = set_code
    parts = ["Pokemon TCG", set_name, name, f"#{num}" if num else ""]
    q = " ".join(p for p in parts if p and p != "#")
    q = re.sub(r"\s+", " ", q).strip()
    return q[:350]
