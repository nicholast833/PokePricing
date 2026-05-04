import requests
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
import time
import os
import statistics

BASE_URL = "https://www.thepricedex.com"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}

def get_next_data(html):
    """Extracts the __NEXT_DATA__ json from the page HTML."""
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html)
    if match:
        return json.loads(match.group(1))
    return None

def get_set_list():
    """Fetches the master list of all sets."""
    print("Fetching master set list...", flush=True)
    req = requests.get(f"{BASE_URL}/sets", headers=HEADERS)
    data = get_next_data(req.text)
    if not data:
        print("Failed to find __NEXT_DATA__ on sets page.", flush=True)
        return []
    
    initialSets = data.get('props', {}).get('pageProps', {}).get('initialSets', [])
    return initialSets

def get_tcgplayer_pack_price(set_name):
    """Attempts to fetch the booster pack price from TCGPlayer for the set."""
    search_term = f"{set_name} Booster Pack".replace(' ', '+').replace('&', '%26')
    url = f"https://www.tcgplayer.com/search/pokemon/product?productLineName=pokemon&q={search_term}&view=grid"
    try:
        req = requests.get(url, headers=HEADERS, timeout=10)
        # TCGPlayer hydrates its state into window.__PRELOADED_STATE__ if not blocked by Cloudflare
        match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*({.*?});</script>', req.text)
        if match:
            state = json.loads(match.group(1))
            # navigate to find the first result price
            # Due to nested structure, this might just serve as an example placeholder.
            # Usually the price is in the search results
            # Lets try finding a simple regex for market price to avoid robust reverse engineering here
            price_match = re.search(r'"marketPrice":([\d\.]+)', match.group(1))
            if price_match:
                return float(price_match.group(1))
        
        # If we couldn't parse state, maybe we can find a raw string:
        price_match = re.search(r'\$?(\d+\.\d{2})', req.text)
        # fallback simple scrape not very accurate, return None to be safe
    except Exception as e:
        print(f"Error fetching TCGPlayer pack price for {set_name}: {e}")
    return None


def _dex_variant_slug_to_label(slug):
    """Turn PriceDex variant slug (e.g. firstEditionHolofoil) into short human text."""
    if not slug or not isinstance(slug, str):
        return ''
    s = re.sub(r'_', ' ', slug.strip())
    s = re.sub(r'([a-z])([A-Z])', r'\1 \2', s)
    return ' '.join(s.title().split())


def _classify_promo_and_reprint_subset(card, set_name):
    """Heuristic flags for product shape (Dex card row + set title)."""
    rarity = (card.get('rarity') or '').strip()
    rl = rarity.lower()
    sn = (set_name or '').lower()
    code = str(card.get('rarityCode') or '').strip().upper()
    is_promo = (
        rl == 'promo'
        or code == 'PROMO'
        or 'black star promos' in sn
        or 'pop series' in sn
        or re.search(r"\bmcdonald'?s collection\b", sn)
    )
    is_reprint_subset = (
        'classic collection' in rl
        or 'trainer gallery' in rl
        or 'galarian gallery' in rl
        or 'shiny vault' in sn
    )
    return is_promo, is_reprint_subset


def _card_row_key_for_analytics(row):
    n = row.get('number')
    return (row.get('name') or '', str(n) if n is not None else '')


def _is_chase_rarity_for_analytics(rarity):
    """
    Chase / trophy rarities that are often outside top-N-by-$ (e.g. second LEGEND half, cheap Prism)
    but should still appear on print-class and driver scatters after re-scrape.
    """
    if not rarity or not isinstance(rarity, str):
        return False
    r = rarity.lower()
    needles = (
        'legend',
        'illustration rare',
        'special illustration',
        'hyper rare',
        'secret rare',
        'rainbow rare',
        'gold star',
        'holo star',
        'shining',
        'amazing rare',
        'radiant rare',
        'ace spec',
        'prism star',
        'rare break',
        'rare prime',
        'lv.x',
        'lv x',
        'level x',
        'delta species',
        'crystal',
        'rare secret',
    )
    if any(t in r for t in needles):
        return True
    if 'rare holo ex' in r or ('holo ex' in r and 'rare' in r):
        return True
    return False


def process_set(set_obj):
    set_id = set_obj.get('id')
    set_slug = set_obj.get('slug')
    set_name = set_obj.get('name')
    
    print(f"Processing set: {set_name} ({set_id}/{set_slug})", flush=True)
    
    result = {
        'set_name': set_name,
        'set_code': set_id,
        'release_date': set_obj.get('releaseDate'),
        'total_cards': set_obj.get('total'),
        'series': set_obj.get('series'),
        'logo_url': set_obj.get('logo'),
        'top_25_cards': [],
        'chase_artist_cards': [],
        'rarity_counts': {},
        'booster_pack_ev': None,
        'booster_box_ev': None,
        'packs_per_box': None,
        'cards_per_pack': None,
        'tcgplayer_pack_price': None,
    }
    
    # 1. Fetch Set Cards and Rarities
    try:
        req = requests.get(f"{BASE_URL}/set/{set_id}/{set_slug}", headers=HEADERS)
        data = get_next_data(req.text)
        if data:
            props = data.get('props', {}).get('pageProps', {})
            initial_cards = props.get('initialCards', [])
            
            rarity_counts = {}
            card_market_prices = []
            chase_artist_cards = []
            basic_rarities = {'Common', 'Uncommon', 'Rare', 'Rare Holo', 'Double Rare', 'Promo', 'Unknown', 'None', '', 'Classic Collection', 'LEGEND', 'Amazing Rare', 'Radiant Rare', 'ACE SPEC Rare', 'Rare BREAK', 'Rare Prime', 'Rare Prism Star', 'Trainer Gallery Rare Holo'}
            
            for card in initial_cards:
                rarity = card.get('rarity', 'Unknown')
                rarity_counts[rarity] = rarity_counts.get(rarity, 0) + 1
                
                # Get max market price across variants; record which variant drove chart $
                max_price = 0.0
                primary_variant = None
                variant_slugs = []
                variants = card.get('variants', [])
                if variants:
                    for variant in variants:
                        vn = variant.get('name')
                        if isinstance(vn, str) and vn.strip():
                            variant_slugs.append(vn.strip())
                        prices = variant.get('prices', [])
                        if prices:
                            for p in prices:
                                mkt = p.get('market')
                                if mkt is not None and mkt > max_price:
                                    max_price = float(mkt)
                                    primary_variant = vn.strip() if isinstance(vn, str) else primary_variant

                    # Preserve first-seen order, unique
                    seen_v = set()
                    variant_keys = []
                    for v in variant_slugs:
                        if v not in seen_v:
                            seen_v.add(v)
                            variant_keys.append(v)

                    card_pull_rate = ''
                    if card.get('pullRatesByVariant'):
                        card_pull_rate = list(card['pullRatesByVariant'].values())[0]

                    is_promo, is_reprint_subset = _classify_promo_and_reprint_subset(card, set_name)
                    reg = card.get('regulationMark')
                    regulation_mark = reg.strip() if isinstance(reg, str) and reg.strip() else None

                    # Map artist cards over baseline rarities
                    artist = card.get('artist', 'Unknown Artist')
                    if max_price > 0 and rarity not in basic_rarities and artist != 'Unknown Artist':
                        chase_artist_cards.append({
                            'artist': artist,
                            'card_name': card.get('name'),
                            'set_name': set_name,
                            'rarity': rarity,
                            'market_price': max_price
                        })

                    row = {
                        'name': card.get('name'),
                        'number': card.get('number'),
                        'rarity': rarity,
                        'market_price': max_price,
                        'artist': artist,
                        'supertype': card.get('supertype'),
                        'subtypes': card.get('subtypes'),
                        'image_url': card.get('images')[0].get('large', '') if card.get('images') and isinstance(card.get('images'), list) and len(card.get('images')) > 0 else '',
                        'card_pull_rate': card_pull_rate,
                        'is_promo': bool(is_promo),
                        'is_reprint_subset': bool(is_reprint_subset),
                        'variant_primary': primary_variant,
                        'variant_keys': variant_keys,
                        'variant_primary_label': _dex_variant_slug_to_label(primary_variant) if primary_variant else None,
                    }
                    if regulation_mark:
                        row['regulation_mark'] = regulation_mark
                    card_market_prices.append(row)
                
            result['rarity_counts'] = rarity_counts
            result['chase_artist_cards'] = chase_artist_cards
            
            # Sort by market price; keep top N for analytics, then add chase rarities not in that slice
            # (LEGEND halves, cheap trophy rarities, etc.) so print-class / composite pools match user expectations.
            TOP_CHARTABLE_CARDS = 100
            CHASE_EXTRA_CAP = 28
            card_market_prices.sort(key=lambda x: x['market_price'], reverse=True)
            top_slice = card_market_prices[:TOP_CHARTABLE_CARDS]
            seen_keys = {_card_row_key_for_analytics(c) for c in top_slice}
            extras = []
            for c in card_market_prices:
                key = _card_row_key_for_analytics(c)
                if key in seen_keys:
                    continue
                if not _is_chase_rarity_for_analytics(c.get('rarity')):
                    continue
                extras.append(c)
                seen_keys.add(key)
            extras.sort(key=lambda x: x['market_price'], reverse=True)
            result['top_25_cards'] = top_slice + extras[:CHASE_EXTRA_CAP]
            
    except Exception as e:
        print(f"  Error fetching set data: {e}")
        
    time.sleep(1) # Be a good citizen
    
    # 2. Fetch Pull Rates / EV
    try:
        req2 = requests.get(f"{BASE_URL}/set/{set_id}/{set_slug}/pull-rates", headers=HEADERS)
        soup = BeautifulSoup(req2.text, 'html.parser')
        
        rarity_pull_rates = {}
        tbody = soup.find('tbody')
        if tbody:
            for tr in tbody.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) >= 2:
                    rarity_name = tds[0].text.strip()
                    rarity_pull_rate = tds[1].text.strip()
                    rarity_pull_rates[rarity_name] = rarity_pull_rate
        result['rarity_pull_rates'] = rarity_pull_rates
        
        for p in soup.find_all('p'):
            t = p.text.strip()
            val = p.find_next_sibling(lambda tag: tag.name not in ['style', 'script'])
            if not val:
                continue
                
            val_text = val.text.strip()
            
            if 'Booster Pack EV' in t:
                result['booster_pack_ev'] = val_text
            elif 'Booster Box EV' in t:
                result['booster_box_ev'] = val_text
            elif 'Packs Per Booster Box' in t:
                try: result['packs_per_box'] = int(val_text)
                except: pass
            elif 'Cards Per ' in t:  # Matches "Cards Per Booster Pack"
                try: result['cards_per_pack'] = int(val_text)
                except: pass
                
    except Exception as e:
        print(f"  Error fetching pull rates: {e}")
        
    time.sleep(1)
    
    # 3. TCGPlayer pack price (mocked/fetched minimally to avoid block)
    result['tcgplayer_pack_price'] = get_tcgplayer_pack_price(set_name)
    
    return result

def compile_dataset(max_sets=0):
    sets = get_set_list()
    print(f"Found {len(sets)} total sets.", flush=True)
    
    if max_sets > 0:
        sets = sets[:max_sets]
        
    results = []
    
    for s in sets:
        res = process_set(s)
        results.append(res)
        
    # Inject PriceCharting pack prices mapping locally
    try:
        if os.path.exists('pricecharting_packs.csv'):
            pc_df = pd.read_csv('pricecharting_packs.csv')
            pc_df['ProductNorm'] = pc_df['Product'].str.lower()
            pc_df['ProductNorm'] = pc_df['ProductNorm'].apply(lambda x: re.sub(r'\s+', ' ', str(x)))
            
            for s in results:
                s_name = s['set_name'].lower().strip()
                match = pc_df[pc_df['ProductNorm'].str.contains(f"pokemon {s_name}", na=False, regex=False)]
                if not match.empty:
                    exact = match[match['ProductNorm'].str.contains(f"pokemon {s_name}$", na=False, regex=True)]
                    hit = exact.iloc[0] if not exact.empty else match.iloc[0]
                    price_str = str(hit['Price']).replace('$', '').replace(',', '')
                    try:
                        s['tcgplayer_pack_price'] = float(price_str)
                    except: pass
    except Exception as e:
        print("Merge failed:", e)
        
    # Skip writing JSON here as we will clean up 'chase_artist_cards' at the end 
    # to avoid double-writing heavily bloated raw datasets
        
    # Flatten inner structures for CSV
    flat_results = []
    for r in results:
        flat = r.copy()
        
        # turn rarity_counts dict into string for csv
        flat['rarity_counts'] = json.dumps(r['rarity_counts'])
        
        # Top card name + price for easy CSV viewing
        if r['top_25_cards']:
            top1 = r['top_25_cards'][0]
            flat['most_expensive_card'] = f"{top1['name']} (${top1['market_price']})"
        else:
            flat['most_expensive_card'] = None
            
        del flat['top_25_cards']
        if 'chase_artist_cards' in flat:
            del flat['chase_artist_cards'] 
        flat_results.append(flat)
        
    df = pd.DataFrame(flat_results)
    try:
        df.to_csv('pokemon_sets_data.csv', index=False, encoding='utf-8')
    except Exception as e:
        print("Warning: CSV write failed due to lock.", e)
        
    # Process Global Artist Scores
    global_artist_hits = {}
    for r in results:
        for chase in r.get('chase_artist_cards', []):
            artist = chase['artist']
            global_artist_hits.setdefault(artist, []).append(chase)

    artist_scores = []
    for artist, hits in global_artist_hits.items():
        prices = sorted([h['market_price'] for h in hits])
        if len(prices) > 0:
            artist_scores.append({
                'Artist': artist,
                'Total_Chase_Cards': len(prices),
                'Median_Market_Price': round(statistics.median(prices), 2),
                'Average_Market_Price': round(sum(prices) / len(prices), 2),
                'Highest_Priced_Card': max(prices)
            })
            
    # Filter those with at least 1 hit (or just overall sort)
    artist_scores.sort(key=lambda x: x['Median_Market_Price'], reverse=True)
    
    with open('artist_scores.json', 'w', encoding='utf-8') as f:
        json.dump(artist_scores, f, indent=4)
        
    try:
        pd.DataFrame(artist_scores).to_csv('artist_scores.csv', index=False, encoding='utf-8')
    except Exception as e:
        print("Warning: Artist CSV write failed.", e)

    # Clean the primary json payload so it doesn't inflate massively in megabytes
    for r in results:
        if 'chase_artist_cards' in r:
            del r['chase_artist_cards']
            
    print("Writing pokemon_sets_data.json...", flush=True)
    with open('pokemon_sets_data.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)

    print("\nData compilation complete! Output saved to pokemon_sets_data, and artist_scores datasets.", flush=True)

if __name__ == "__main__":
    compile_dataset(max_sets=0)
