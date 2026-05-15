import os
import datetime
import logging
from statistics import median
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables (from local file if it exists, otherwise rely on GitHub Action Secrets)
try:
    load_dotenv('scrape/ebay_listing_checker.env')
except:
    pass

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("Error: SUPABASE_URL or SUPABASE_KEY not found in environment")
    exit(1)

supabase: Client = create_client(url, key)

# --- Set up Logging ---
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
today_str = datetime.datetime.now().strftime("%Y-%m-%d")
log_filename = os.path.join(log_dir, f"sync_{today_str}.log")

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler() # Also print to console
    ]
)

# TCGGO history uses up to 3 paginated GETs per card (~90d) + 1 eBay sold + 1 Browse; cap vs RapidAPI daily budget.
# Batch doubled (was 270 / 380) — runs were using ~half the budget.
BATCH_SIZE = 540

def _env_truthy(name: str) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def run_queue():
    skip_tcggo = _env_truthy("DAILY_SYNC_SKIP_TCGGO")
    if skip_tcggo:
        logging.info("DAILY_SYNC_SKIP_TCGGO is set: skipping TCGGO price history + TCGGO eBay sold (TCGPro) for this run.")

    logging.info("==========================================")
    logging.info(f"Starting Daily Sync Queue (Batch Size: {BATCH_SIZE})")
    
    # 1. Fetch the oldest/un-synced cards
    logging.info("Querying Supabase for the most urgent cards...")
    
    # Order by last_synced_at ascending. Nulls first means cards never synced go first.
    try:
        response = supabase.table('pokemon_cards') \
            .select('unique_card_id, set_code, name, number, metrics, price_history, last_synced_at') \
            .order('last_synced_at', nullsfirst=True) \
            .limit(BATCH_SIZE) \
            .execute()
        
        cards = response.data
    except Exception as e:
        logging.error(f"Failed to query Supabase queue: {e}")
        return

    if not cards:
        logging.info("No cards found in database. Exiting.")
        return
        
    logging.info(f"Successfully retrieved {len(cards)} cards from the queue.")
    
    # Track statistics
    success_count = 0
    error_count = 0
    
    # 2. Process each card
    from tcggo_api_fetcher import fetch_tcggo_price_history, extract_latest_market_price, extract_full_price_history, fetch_tcggo_ebay_sold
    from ebay_api_fetcher import build_ebay_active_search_query, fetch_ebay_active_listing_snapshot
    from price_history_merge import append_ebay_anonymous_cohort_daily, merge_tcggo_market_history_by_date

    # Check multiple possible environment variable names for the API key
    tcgpro_key = os.environ.get("TCGPRO_API_KEY") or os.environ.get("RAPIDAPI_KEY_TCGGO") or os.environ.get("RAPIDAPI_KEY")
    ebay_app_id = (os.environ.get("EBAY_APP_ID") or "").strip()
    ebay_cert_id = (os.environ.get("EBAY_CERT_ID") or "").strip()
    
    if not tcgpro_key:
        logging.warning("WARNING: No TCGPRO_API_KEY found in environment variables! TCGGO data will be skipped.")
    
    for index, card in enumerate(cards):
        card_id = card['unique_card_id']
        name = card['name']
        set_code = card['set_code']
        metrics = card.get('metrics') or {}
        ph_src = card.get('price_history')
        price_history = dict(ph_src) if isinstance(ph_src, dict) else {}
        last_sync = card['last_synced_at'] or "NEVER"
        
        logging.info(f"[{index+1}/{len(cards)}] Processing: {name} ({set_code}) (Last sync: {last_sync})")
        
        try:
            updates_made = False
            today_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
            
            # --- 1. TCGGO Price History API ---
            tcggo_id = metrics.get('tcggo_id')
            if tcggo_id and tcgpro_key and not skip_tcggo:
                logging.info(f"  -> Fetching TCGGO history for tcggo_id={tcggo_id}")
                try:
                    hist_days = max(7, int(os.environ.get("TCGGO_HISTORY_DAYS", "90")))
                    tcg_data = fetch_tcggo_price_history(tcggo_id, tcgpro_key, days=hist_days)
                    
                    # Save full history
                    full_history = extract_full_price_history(tcg_data)
                    if full_history:
                        merged_hist = merge_tcggo_market_history_by_date(
                            price_history.get("tcggo_market_history"),
                            full_history,
                        )
                        price_history["tcggo_market_history"] = merged_hist
                        updates_made = True
                        pages_meta = (tcg_data or {}).get("_tcggo_pages_fetched")
                        merged_n = (tcg_data or {}).get("_tcggo_points_merged")
                        extra = ""
                        if pages_meta is not None:
                            extra = f" (pages={pages_meta}"
                            if merged_n is not None:
                                extra += f", merged_keys={merged_n}"
                            extra += ")"
                        logging.info(f"  -> TCGGO: merged to {len(merged_hist)} price points (daily append){extra}")
                    
                    # Extract latest market price for quick reference
                    latest_market = extract_latest_market_price(tcg_data)
                    if latest_market:
                        logging.info(f"  -> TCGGO Latest Market Price: ${latest_market}")
                        metrics['tcggo_latest_market_usd'] = latest_market
                    else:
                        data_keys = list((tcg_data or {}).get('data', {}).keys())[:3]
                        logging.warning(f"  -> TCGGO returned data (sample dates: {data_keys}) but no valid tcg_player_market found")
                        
                except Exception as e:
                    logging.error(f"  -> TCGGO History API Failed: {e}")
                
                # --- 2. TCGGO eBay Sold Prices ---
                try:
                    logging.info(f"  -> Fetching TCGGO eBay sold prices for tcggo_id={tcggo_id}")
                    ebay_sold = fetch_tcggo_ebay_sold(tcggo_id, tcgpro_key)
                    sold_data = ebay_sold.get('data', []) if ebay_sold else []
                    
                    if sold_data:
                        logging.info(f"  -> TCGGO eBay: {len(sold_data)} grading entries found")
                        metrics['tcggo_ebay_sold_prices'] = sold_data
                        metrics['tcggo_ebay_sold_sync_iso'] = today_iso
                        updates_made = True
                    else:
                        logging.info(f"  -> TCGGO eBay: No graded sold data available")
                except Exception as e:
                    logging.error(f"  -> TCGGO eBay Sold API Failed: {e}")
            elif skip_tcggo:
                pass
            else:
                if not tcggo_id:
                    logging.info("  -> Skipping TCGGO: No tcggo_id found in metrics.")
                elif not tcgpro_key:
                    logging.error("  -> Skipping TCGGO: TCGPRO_API_KEY is empty or missing from environment!")

            # --- 3. eBay Buy Browse (active listings only) ---
            # Finding API findCompletedItems is legacy; many apps/GitHub IPs see empty results.
            # Browse returns total active matches + item summaries (OAuth: APP_ID + CERT_ID).
            if ebay_app_id and ebay_cert_id:
                row_for_q = {"set_code": set_code, "name": name, "number": card.get("number"), "metrics": metrics}
                query = build_ebay_active_search_query(row_for_q)
                logging.info(f"  -> eBay Browse (active) q={query[:120]!r}")
                try:
                    browse_lpp = max(1, min(int(os.environ.get("EBAY_BROWSE_LIMIT_PER_PAGE", "200")), 200))
                    browse_pages = max(1, min(int(os.environ.get("EBAY_BROWSE_MAX_PAGES", "5")), 50))
                    snap = fetch_ebay_active_listing_snapshot(
                        query,
                        app_id=ebay_app_id,
                        cert_id=ebay_cert_id,
                        limit_per_page=browse_lpp,
                        max_pages=browse_pages,
                    )
                    st = snap.get("http_status")
                    tot = snap.get("total")
                    n_sn = len(snap.get("snapshots") or [])
                    n_coh = len(snap.get("anonymous_cohort") or [])
                    pf = int(snap.get("pages_fetched") or 0)
                    n_items = int(snap.get("items_fetched") or 0)
                    got_data = n_sn > 0 or n_coh > 0
                    if snap.get("partial_error"):
                        logging.warning(
                            f"  -> eBay Browse: partial after page {pf} (using {n_items} items): "
                            f"{str(snap.get('partial_error'))[:180]!r}"
                        )
                    if got_data:
                        logging.info(
                            f"  -> eBay Browse: HTTP {st} total={tot!r} pages={pf} items={n_items} "
                            f"snapshots={n_sn} cohort={n_coh}"
                        )
                        metrics["ebay_active_total"] = tot
                        metrics["ebay_active_snapshots"] = snap.get("snapshots") or []
                        metrics["ebay_active_search_url"] = snap.get("search_url")
                        metrics["ebay_active_sync_iso"] = today_iso
                        metrics["ebay_active_source"] = "buy_browse"
                        updates_made = True
                        snaps = metrics.get("ebay_active_snapshots") or []
                        append_ebay_anonymous_cohort_daily(
                            price_history,
                            today_d=today_iso[:10],
                            total_api=tot,
                            cohort=snap.get("anonymous_cohort") or [],
                        )
                        prices_bn = []
                        for s in snaps:
                            if not isinstance(s, dict):
                                continue
                            pv = s.get("price_value")
                            try:
                                v = float(pv)
                            except (TypeError, ValueError):
                                continue
                            if v > 0:
                                prices_bn.append(v)
                        if prices_bn:
                            d_key = today_iso[:10]
                            hist = list(price_history.get("ebay_active_price_history") or [])
                            hist = [x for x in hist if not (isinstance(x, dict) and str(x.get("date") or "")[:10] == d_key)]
                            hist.append(
                                {
                                    "date": d_key,
                                    "median_usd": float(median(prices_bn)),
                                    "low_usd": float(min(prices_bn)),
                                    "high_usd": float(max(prices_bn)),
                                    "n": len(prices_bn),
                                }
                            )
                            hist.sort(key=lambda r: str(r.get("date") or ""))
                            keep = max(31, int(os.environ.get("EBAY_ACTIVE_PRICE_HISTORY_MAX", "400")))
                            hist = hist[-keep:]
                            price_history["ebay_active_price_history"] = hist
                    elif not got_data:
                        logging.warning(
                            f"  -> eBay Browse: HTTP {st} total={tot!r} err={str(snap.get('raw_error'))[:200]!r}"
                        )
                except Exception as e:
                    logging.error(f"  -> eBay Browse failed: {e}")
            elif ebay_app_id and not ebay_cert_id:
                logging.warning(
                    "  -> Skipping eBay Browse: EBAY_CERT_ID missing (OAuth client_secret required for active listings)."
                )

            # --- 3. Update Supabase ---
            current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
            
            update_payload = {'last_synced_at': current_time}
            if updates_made:
                update_payload['metrics'] = metrics
                update_payload['price_history'] = price_history
                
            supabase.table('pokemon_cards') \
                .update(update_payload) \
                .eq('unique_card_id', card_id) \
                .execute()
                
            success_count += 1
            
        except Exception as e:
            logging.error(f"Failed to process {card_id}: {e}")
            error_count += 1

    logging.info("==========================================")
    logging.info("Daily Sync Complete!")
    logging.info(f"Cards updated successfully: {success_count}")
    logging.info(f"Errors encountered: {error_count}")
    logging.info("==========================================")

if __name__ == '__main__':
    BATCH_SIZE = 760
    run_queue()
