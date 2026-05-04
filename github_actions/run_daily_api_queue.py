import os
import datetime
import logging
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

BATCH_SIZE = 1150

def run_queue():
    logging.info("==========================================")
    logging.info(f"Starting Daily Sync Queue (Batch Size: {BATCH_SIZE})")
    
    # 1. Fetch the oldest/un-synced cards
    logging.info("Querying Supabase for the most urgent cards...")
    
    # Order by last_synced_at ascending. Nulls first means cards never synced go first.
    try:
        response = supabase.table('pokemon_cards') \
            .select('unique_card_id, set_code, name, last_synced_at') \
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
    from tcggo_api_fetcher import fetch_tcggo_price_history, extract_latest_market_price
    from ebay_api_fetcher import fetch_ebay_sold_listings
    
    tcgpro_key = os.environ.get("TCGPRO_API_KEY")
    ebay_app_id = os.environ.get("EBAY_APP_ID")
    
    for index, card in enumerate(cards):
        card_id = card['unique_card_id']
        name = card['name']
        set_code = card['set_code']
        metrics = card.get('metrics') or {}
        price_history = card.get('price_history') or {}
        last_sync = card['last_synced_at'] or "NEVER"
        
        logging.info(f"[{index+1}/{len(cards)}] Processing: {name} ({set_code}) (Last sync: {last_sync})")
        
        try:
            updates_made = False
            today_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
            
            # --- 1. TCGGO API (TCGPro) ---
            tcgplayer_id = metrics.get('tcgtracking_product_id')
            if tcgplayer_id and tcgpro_key:
                logging.info(f"  -> Fetching TCGGO history for tcgplayer_id={tcgplayer_id}")
                tcg_data = fetch_tcggo_price_history(tcgplayer_id, tcgpro_key, days=2)
                latest_market = extract_latest_market_price(tcg_data)
                
                if latest_market:
                    logging.info(f"  -> TCGGO Market Price: ${latest_market}")
                    if 'tcggo_market_history' not in price_history:
                        price_history['tcggo_market_history'] = []
                        
                    # Prevent duplicates for today
                    history_list = [h for h in price_history['tcggo_market_history'] if h.get('date') != today_iso]
                    history_list.append({"date": today_iso, "price_usd": float(latest_market)})
                    price_history['tcggo_market_history'] = sorted(history_list, key=lambda x: x['date'])
                    updates_made = True
            else:
                logging.info("  -> Skipping TCGGO: No tcgtracking_product_id found in metrics.")

            # --- 2. eBay Finding API ---
            if ebay_app_id:
                # Build a simple search query (e.g. "Base Set Alakazam 1/102")
                query = f"{set_code} {name}" # Simplified for this example
                logging.info(f"  -> Fetching eBay sold history for '{query}'")
                sales = fetch_ebay_sold_listings(query, ebay_app_id, days=14)
                
                if sales['graded'] or sales['ungraded']:
                    logging.info(f"  -> Found {len(sales['graded'])} graded, {len(sales['ungraded'])} ungraded sales.")
                    metrics['ebay_sold_history_graded'] = sales['graded']
                    metrics['ebay_sold_history_ungraded'] = sales['ungraded']
                    metrics['ebay_sold_sync_iso'] = today_iso
                    updates_made = True

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
    BATCH_SIZE = 1150 
    run_queue()
