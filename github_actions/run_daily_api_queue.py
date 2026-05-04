import os
import datetime
import logging
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables (from local file if it exists, otherwise rely on GitHub Action Secrets)
try:
    load_dotenv('../scrape/ebay_listing_checker.env')
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
    
    # 2. Process each card (Placeholder for actual API integration)
    for index, card in enumerate(cards):
        card_id = card['unique_card_id']
        name = card['name']
        last_sync = card['last_synced_at'] or "NEVER"
        
        logging.info(f"[{index+1}/{len(cards)}] Processing: {name} (Last sync: {last_sync})")
        
        try:
            # TODO: Call TCGPro API for this card
            # TODO: Call Pokemon Wizard Scraper for this card
            # TODO: Call eBay API for this card
            
            # 3. Update the last_synced_at timestamp to push it to the back of the queue
            current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
            
            supabase.table('pokemon_cards') \
                .update({'last_synced_at': current_time}) \
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
    # For testing purposes, let's artificially limit the batch to 5 so we don't accidentally update 1150 rows yet.
    BATCH_SIZE = 5 
    run_queue()
