import os
import json
import urllib.request
import urllib.error
import urllib.parse
from datetime import date, timedelta
from typing import Dict, Any, Optional

RAPIDAPI_HOST = "pokemon-tcg-api.p.rapidapi.com"
HISTORY_PATH = "/history-prices"

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
    """
    Fetches price history from the TCGGO API using the tcggo_id.
    Uses a wide date range (default 730 days) to ensure we capture data even if TCGGO
    hasn't updated the card recently.
    """
    end = date.today()
    start = end - timedelta(days=max(1, days))
    
    q = {
        "date_from": start.isoformat(),
        "date_to": end.isoformat(),
        "page": "1",
        "sort": "desc",
        "id": str(tcggo_id)
    }
    
    headers, query_params = tcggo_gateway_headers_query(api_key, q)
    url = f"https://{RAPIDAPI_HOST}{HISTORY_PATH}?{urllib.parse.urlencode(query_params)}"
    
    req = urllib.request.Request(url, headers=headers, method="GET")
    
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            data = json.loads(body)
            return data
            
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        raise Exception(f"HTTP {e.code}: {err_body[:500]}")
    except Exception as e:
        raise Exception(f"Request Error: {e}")

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
