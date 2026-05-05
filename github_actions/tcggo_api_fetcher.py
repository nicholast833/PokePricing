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

def fetch_tcggo_price_history(tcggo_id: int, api_key: str, days: int = 31) -> Optional[Dict[str, Any]]:
    """
    Fetches the last N days of price history from the TCGGO API using the tcggo_id.
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
    Parses the massive TCGGO history response and extracts just today's market price.
    """
    if not tcggo_response or "data" not in tcggo_response:
        return None
        
    data = tcggo_response["data"]
    if not data:
        return None
        
    # Get the most recent day's data (first item since it's sorted desc)
    try:
        first_key = list(data.keys())[0]
        row = data[first_key]
        return row.get("tcg_player_market")
    except Exception:
        return None
