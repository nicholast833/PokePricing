import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

RAPIDAPI_HOST = "pokemon-tcg-api.p.rapidapi.com"
HISTORY_PATH = "/history-prices"
# Sealed / accessories catalog (same gateway as episodes/cards); see https://www.tcggo.com/api-docs/v1/
POKEMON_PRODUCTS_PATH = "/pokemon/products"

def tcggo_gateway_headers_query(key: str, query: Dict[str, str]) -> tuple[Dict[str, str], Dict[str, str]]:
    params = dict(query)
    headers = {"X-RapidAPI-Host": RAPIDAPI_HOST, "Accept": "application/json"}
    
    # Check if this is a direct TCGGO key or a RapidAPI key
    if key.startswith("tcggo_"):
        params["rapidapi-key"] = key
    else:
        headers["X-RapidAPI-Key"] = key
        
    return headers, params

def fetch_tcggo_price_history(tcggo_id: int, api_key: str, days: int = 730) -> Optional[Dict[str, Any]]:
    """Backward-compatible: history for a TCGGO **internal** entity id (cards or products)."""
    return fetch_tcggo_price_history_query(
        api_key,
        days=days,
        tcggo_id=int(tcggo_id),
    )


def fetch_tcggo_price_history_query(
    api_key: str,
    *,
    days: int = 730,
    tcggo_id: Optional[int] = None,
    tcgplayer_id: Optional[int] = None,
    cardmarket_id: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """
    GET /history-prices — TCGGO supports lookup by internal id, TCGPlayer product id, or Cardmarket id
    (see API docs: Historical Prices). Exactly one of tcggo_id / tcgplayer_id / cardmarket_id must be set.
    """
    n = sum(1 for x in (tcggo_id, tcgplayer_id, cardmarket_id) if x is not None)
    if n != 1:
        raise ValueError("fetch_tcggo_price_history_query: pass exactly one of tcggo_id, tcgplayer_id, cardmarket_id")

    end = date.today()
    start = end - timedelta(days=max(1, days))

    q: Dict[str, str] = {
        "date_from": start.isoformat(),
        "date_to": end.isoformat(),
        "page": "1",
        "sort": "desc",
    }
    if tcggo_id is not None:
        q["id"] = str(int(tcggo_id))
    elif tcgplayer_id is not None:
        q["tcgplayer_id"] = str(int(tcgplayer_id))
    else:
        q["cardmarket_id"] = str(int(cardmarket_id))

    headers, query_params = tcggo_gateway_headers_query(api_key, q)
    url = f"https://{RAPIDAPI_HOST}{HISTORY_PATH}?{urllib.parse.urlencode(query_params)}"

    req = urllib.request.Request(url, headers=headers, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return json.loads(body)

    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        raise Exception(f"HTTP {e.code}: {err_body[:500]}")
    except Exception as e:
        raise Exception(f"Request Error: {e}")


def _tcggo_get_json(api_key: str, path: str, query: Optional[Dict[str, str]] = None) -> Any:
    q = dict(query or {})
    headers, query_params = tcggo_gateway_headers_query(api_key, q)
    url = f"https://{RAPIDAPI_HOST}{path}?{urllib.parse.urlencode(query_params)}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def fetch_episode_products_page(
    episode_id: int,
    api_key: str,
    *,
    page: int = 1,
    per_page: int = 100,
    sort: str = "price_highest",
) -> List[Dict[str, Any]]:
    """
    GET /episodes/{episodeId}/products — sealed SKUs for that episode (TCGGO catalog).
    Response shape matches /cards: top-level ``data`` list.
    """
    q = {"per_page": str(per_page), "page": str(page), "sort": sort}
    try:
        raw = _tcggo_get_json(api_key, f"/episodes/{int(episode_id)}/products", q)
    except Exception:
        return []
    data = raw.get("data") if isinstance(raw, dict) else None
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def fetch_episode_products_all(episode_id: int, api_key: str, *, sleep_s: float = 0.35) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    page = 1
    while True:
        batch = fetch_episode_products_page(episode_id, api_key, page=page)
        if not batch:
            break
        out.extend(batch)
        if len(batch) < 100:
            break
        page += 1
        time.sleep(max(0.0, sleep_s))
    return out


def fetch_pokemon_products_page(
    api_key: str,
    *,
    page: int = 1,
    per_page: int = 100,
) -> List[Dict[str, Any]]:
    """
    GET /pokemon/products — global product catalog page (TCGGO v1 docs: List All Products).
    """
    q = {"per_page": str(per_page), "page": str(page)}
    try:
        raw = _tcggo_get_json(api_key, POKEMON_PRODUCTS_PATH, q)
    except Exception:
        return []
    data = raw.get("data") if isinstance(raw, dict) else None
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def tcggo_product_tcgplayer_id(row: Dict[str, Any]) -> Optional[int]:
    """Normalize common field names on TCGGO product payloads."""
    for k in ("tcgplayer_id", "tcgplayerId", "tcg_player_id", "tcgPlayerId"):
        v = row.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    return None


def tcggo_product_internal_id(row: Dict[str, Any]) -> Optional[int]:
    v = row.get("id")
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def find_tcggo_product_row_for_tcgplayer_id(
    products: List[Dict[str, Any]],
    tcgplayer_pid: int,
) -> Optional[Dict[str, Any]]:
    want = int(tcgplayer_pid)
    for row in products:
        tid = tcggo_product_tcgplayer_id(row)
        if tid is not None and tid == want:
            return row
    return None


def fetch_all_episodes(api_key: str, *, sleep_s: float = 0.35) -> List[Dict[str, Any]]:
    """GET /episodes (paginated) — same pattern as backfill_tcggo_ids."""
    all_rows: List[Dict[str, Any]] = []
    page = 1
    while True:
        q = {"page": str(page)}
        try:
            raw = _tcggo_get_json(api_key, "/episodes", q)
        except Exception:
            break
        chunk = raw.get("data") if isinstance(raw, dict) else None
        if not isinstance(chunk, list) or not chunk:
            break
        all_rows.extend([x for x in chunk if isinstance(x, dict)])
        page += 1
        time.sleep(max(0.0, sleep_s))
    return all_rows

def extract_latest_market_price(tcggo_response: Dict[str, Any]) -> Optional[float]:
    """
    Parses the TCGGO history response and extracts the most recent non-null market price.
    Iterates through all date entries (sorted desc) to find the latest valid price.
    """
    if not tcggo_response or "data" not in tcggo_response:
        return None
        
    data = tcggo_response["data"]
    if not data:
        return None
        
    # Sort keys descending to get most recent first
    sorted_dates = sorted(data.keys(), reverse=True)
    for date_key in sorted_dates:
        row = data[date_key]
        price = row.get("tcg_player_market")
        if price is not None:
            try:
                p = float(price)
                if p > 0:
                    return p
            except (TypeError, ValueError):
                continue
    return None

def extract_full_price_history(tcggo_response: Dict[str, Any]) -> list:
    """
    Extracts the complete price history as a list of {date, price_usd, cm_low} entries.
    """
    if not tcggo_response or "data" not in tcggo_response:
        return []
    
    data = tcggo_response["data"]
    history = []
    for date_key in sorted(data.keys()):
        row = data[date_key]
        entry = {"date": date_key}
        tcg_market = row.get("tcg_player_market")
        if tcg_market is not None:
            try:
                entry["price_usd"] = float(tcg_market)
            except (TypeError, ValueError):
                pass
        cm_low = row.get("cm_low")
        if cm_low is not None:
            try:
                entry["cm_low"] = float(cm_low)
            except (TypeError, ValueError):
                pass
        history.append(entry)
    return history

def fetch_tcggo_ebay_sold(tcggo_id: int, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Fetches graded eBay sold price data from TCGGO's /ebay-sold-prices endpoint.
    Returns aggregated median prices per grading company/grade.
    """
    q = {"id": str(tcggo_id)}
    headers, query_params = tcggo_gateway_headers_query(api_key, q)
    url = f"https://{RAPIDAPI_HOST}/ebay-sold-prices?{urllib.parse.urlencode(query_params)}"
    
    req = urllib.request.Request(url, headers=headers, method="GET")
    
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return json.loads(body)
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        raise Exception(f"HTTP {e.code}: {err_body[:500]}")
    except Exception as e:
        raise Exception(f"Request Error: {e}")
