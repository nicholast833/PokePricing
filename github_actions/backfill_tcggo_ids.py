import os
import json
import urllib.request
import urllib.parse
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
import unicodedata
import time

try:
    load_dotenv('scrape/ebay_listing_checker.env')
except:
    pass

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
api_key: str = os.environ.get("TCGPRO_API_KEY") or os.environ.get("RAPIDAPI_KEY_TCGGO") or os.environ.get("RAPIDAPI_KEY")

if not url or not key or not api_key:
    print("Error: SUPABASE_URL, SUPABASE_KEY, or TCGPRO_API_KEY missing")
    exit(1)

supabase: Client = create_client(url, key)

HOST = "pokemon-tcg-api.p.rapidapi.com"

def norm_str(s):
    return unicodedata.normalize("NFC", str(s).strip()).casefold()

def fetch_episode_cards(episode_id: int):
    headers = {"X-RapidAPI-Host": HOST, "Accept": "application/json"}
    if api_key.startswith("tcggo_"):
        url = f"https://{HOST}/episodes/{episode_id}/cards?rapidapi-key={api_key}&per_page=300"
    else:
        headers["X-RapidAPI-Key"] = api_key
        url = f"https://{HOST}/episodes/{episode_id}/cards?per_page=300"

    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8")).get("data", [])
    except Exception as e:
        print(f"Error fetching episode {episode_id}: {e}")
        return []

def run_backfill():
    # 1. Load Episode Cache
    episodes_file = Path('scrape/tcggo_episodes_all.json')
    if not episodes_file.exists():
        print("Cannot find tcggo_episodes_all.json")
        return

    episodes_data = json.loads(episodes_file.read_text(encoding="utf-8")).get("data", [])
    episodes_by_name = {norm_str(e['name']): e['id'] for e in episodes_data if e.get('name')}
    
    # Manually map promo sets if needed
    episodes_by_name[norm_str('Wizards Black Star Promos')] = episodes_by_name.get(norm_str('Wizards Black Star Promos'), 125)
    episodes_by_name[norm_str('Nintendo Black Star Promos')] = episodes_by_name.get(norm_str('Nintendo Black Star Promos'), 113)

    # 2. Fetch Sets from Supabase
    sets_response = supabase.table('pokemon_sets').select('set_code, set_name').execute()
    set_name_map = {s['set_code']: s['set_name'] for s in sets_response.data}

    # 3. Fetch Cards missing IDs
    print("Fetching cards from Supabase...")
    cards_response = supabase.table('pokemon_cards').select('unique_card_id, set_code, name, number, metrics').execute()
    cards = cards_response.data

    missing_cards = []
    for c in cards:
        metrics = c.get('metrics') or {}
        if not metrics.get('tcgtracking_product_id'):
            missing_cards.append(c)

    print(f"Found {len(missing_cards)} cards missing TCGTracking/TCGPlayer Product IDs.")

    # 4. Process matches
    episode_cache = {}
    matched_updates = []

    for idx, card in enumerate(missing_cards):
        set_name = set_name_map.get(card['set_code'], card['set_code'])
        norm_set = norm_str(set_name)
        
        # Try to find episode
        episode_id = episodes_by_name.get(norm_set)
        
        # Fallback mappings for promos
        if not episode_id:
            if 'promo' in norm_set:
                if 'wizards' in norm_set or card['set_code'] == 'basep':
                    episode_id = episodes_by_name.get(norm_str('Wizards Black Star Promos'))
                elif 'nintendo' in norm_set or card['set_code'] == 'np':
                    episode_id = episodes_by_name.get(norm_str('Nintendo Black Star Promos'))
                elif 'ex' in norm_set or card['set_code'] == 'ex5':
                    episode_id = episodes_by_name.get(norm_str('EX Promos')) # Just guessing here
                
        if not episode_id:
            print(f"[{idx}] No episode found for set: {set_name}")
            continue

        if episode_id not in episode_cache:
            print(f"Downloading cards for episode {episode_id} ({set_name})...")
            episode_cache[episode_id] = fetch_episode_cards(episode_id)
            time.sleep(1)

        api_cards = episode_cache[episode_id]
        
        # Fuzzy match card by name and number
        norm_card_name = norm_str(card['name'])
        norm_num = norm_str(card['number'])
        
        match = None
        for ac in api_cards:
            if norm_str(ac.get('name')) == norm_card_name and norm_str(ac.get('card_number')) == norm_num:
                match = ac
                break
                
        if match:
            tcg_id = match.get('tcgplayer_id')
            if tcg_id:
                print(f"[{idx}] MATCHED: {card['name']} -> TCGPlayer ID: {tcg_id}")
                metrics = card.get('metrics') or {}
                metrics['tcgtracking_product_id'] = tcg_id
                matched_updates.append({
                    'unique_card_id': card['unique_card_id'],
                    'metrics': metrics
                })
            else:
                print(f"[{idx}] Matched {card['name']}, but API card has no tcgplayer_id")
        else:
            print(f"[{idx}] No card match found for {card['name']} #{card['number']} in episode")

    # 5. Push Updates
    if matched_updates:
        print(f"\nPushing {len(matched_updates)} updates to Supabase...")
        for batch in [matched_updates[i:i+100] for i in range(0, len(matched_updates), 100)]:
            try:
                supabase.table('pokemon_cards').upsert(batch).execute()
                print(f"Upserted batch of {len(batch)}")
            except Exception as e:
                print(f"Failed to upsert batch: {e}")
                
        print("Backfill complete!")
    else:
        print("\nNo cards were successfully matched.")

if __name__ == '__main__':
    run_backfill()
