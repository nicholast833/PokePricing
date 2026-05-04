import json
import os
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv('ebay_listing_checker.env')

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("Error: SUPABASE_URL or SUPABASE_KEY not found in ebay_listing_checker.env")
    exit(1)

supabase: Client = create_client(url, key)

def sync_data():
    print("Loading JSON data...")
    try:
        with open('../pokemon_sets_data.json', 'r', encoding='utf-8') as f:
            sets_data = json.load(f)
    except Exception as e:
        print(f"Failed to load JSON: {e}")
        return

    print(f"Found {len(sets_data)} sets. Beginning sync...")

    for s in sets_data:
        set_code = s.get('set_code')
        if not set_code:
            continue
            
        print(f"Processing set: {s.get('set_name')} ({set_code})")
        
        # 1. Prepare Set Data
        set_record = {
            'set_code': set_code,
            'set_name': s.get('set_name'),
            'series': s.get('series'),
            'release_date': s.get('release_date'),
            'total_cards': s.get('total_cards'),
            'logo_url': s.get('logo_url'),
            'metadata': {
                'rarity_counts': s.get('rarity_counts', {}),
                'booster_pack_ev': s.get('booster_pack_ev'),
                'booster_box_ev': s.get('booster_box_ev'),
                'tcgplayer_pack_price': s.get('tcgplayer_pack_price'),
                'rarity_pull_rates': s.get('rarity_pull_rates', {})
            }
        }
        
        # Upsert Set
        try:
            supabase.table('pokemon_sets').upsert(set_record).execute()
        except Exception as e:
            print(f"Error upserting set {set_code}: {e}")
            continue
            
        # 2. Prepare Cards Data
        cards = s.get('top_25_cards', [])
        if not cards:
            continue
            
        card_records = []
        for c in cards:
            # Generate a consistent ID string for the unique constraint
            card_id_str = f"{set_code}_{c.get('number')}_{c.get('name')}".replace(' ', '_').lower()
            
            # Extract standard fields
            c_name = c.get('name')
            c_number = str(c.get('number')) if c.get('number') is not None else '?'
            c_rarity = c.get('rarity')
            c_market_price = c.get('market_price')
            c_artist = c.get('artist')
            c_image_url = c.get('image_url')
            
            # Everything else goes into metrics and price_history
            price_history = {
                'pokemon_wizard_price_history': c.get('pokemon_wizard_price_history'),
                'collectrics_price_history': c.get('collectrics_price_history'),
                'collectrics_history_justtcg': c.get('collectrics_history_justtcg'),
                'collectrics_history_ebay': c.get('collectrics_history_ebay')
            }
            
            metrics = {k: v for k, v in c.items() if k not in [
                'name', 'number', 'rarity', 'market_price', 'artist', 'image_url',
                'pokemon_wizard_price_history', 'collectrics_price_history', 
                'collectrics_history_justtcg', 'collectrics_history_ebay'
            ]}
            
            card_records.append({
                'unique_card_id': card_id_str,
                'set_code': set_code,
                'name': c_name,
                'number': c_number,
                'rarity': c_rarity,
                'market_price': c_market_price,
                'artist': c_artist,
                'image_url': c_image_url,
                'metrics': metrics,
                'price_history': price_history
            })
            
        # Upsert Cards in batch
        try:
            supabase.table('pokemon_cards').upsert(card_records).execute()
        except Exception as e:
            print(f"Error upserting cards for {set_code}: {e}")

    print("Sync complete!")

if __name__ == '__main__':
    sync_data()
