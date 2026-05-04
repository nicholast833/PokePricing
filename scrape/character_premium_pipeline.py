import json
import re
import requests
import pandas as pd
import time
import os

GRAPHQL_URL = "https://api.tcgdex.net/v2/graphql"
BASIC_RARITIES = {'Common', 'Uncommon', 'Rare', 'Rare Holo', 'Double Rare', 'Unknown', 'None', '', 'Classic Collection', 'LEGEND', 'Amazing Rare', 'Radiant Rare', 'ACE SPEC Rare', 'Trainer Gallery Rare Holo'}

# Keywords/Names associated with high-premium human Trainer cards (The "Waifu/Husbando" identity variable)
WAIFU_KEYWORDS = {
    'lillie', 'marnie', 'serena', 'iono', 'cynthia', 'erika', 'misty', 'brock', 'blue', 'red', 'leaf',
    'gardenia', 'mallow', 'lana', 'bea', 'nessa', 'elesa', 'lisia', 'skyla', 'rosa', 'acerola', 'kahili',
    'mina', 'selene', 'elaine', 'klara', 'avery', 'melony', 'peony', 'grusha', 'nemona', 'penny', 'arven',
    'riam', 'professors research', 'boss orders', 'miriam', 'sada', 'turo', 'roxanne', 'candice', 'irida',
    'adaman', 'volkner', 'morty', 'jasmine', 'clair', 'winona', 'flannery', 'may', 'dawn', 'hilda',
    'bianca', 'korrina', 'shauna', 'sycamore', 'birch', 'oak', 'elm', 'rowan', 'juniper', 
    'lusamine', 'gladion', 'gusmo', 'plumeria', 'faba', 'wicke', 'sonia', 'oleana', 'rose', 'pierce',
    'geeta', 'larry', 'tulip', 'lip', 'brassius'
}

# Training/Competitive utility trainers (less aesthetic premium, more playability driven)
UTILITY_KEYWORDS = {
    'professor', 'research', 'orders', 'boss', 'switch', 'stadium', 'ball', 'candy', 'search'
}

def get_trainer_archetype(name):
    name_lower = name.lower()
    if any(k in name_lower for k in WAIFU_KEYWORDS):
        return 'Waifu'
    if any(k in name_lower for k in UTILITY_KEYWORDS):
        return 'Utility'
    return 'Generic'

def is_human_trainer(name):
    name_lower = name.lower()
    if any(k in name_lower for k in WAIFU_KEYWORDS):
        return True
    if any(title in name_lower for title in ['prof.', 'professor', 'lady', 'gentleman', 'student', 'hiker']):
        return True
    return False

_FORM_PREFIX_RES = [
    re.compile(r'^mega\s+', re.I),
    re.compile(r'^primal\s+', re.I),
    re.compile(r'^galarian\s+', re.I),
    re.compile(r'^alolan\s+', re.I),
    re.compile(r'^hisuian\s+', re.I),
    re.compile(r'^paldean\s+', re.I),
    re.compile(r'^radiant\s+', re.I),
    re.compile(r'^rapid\s+strike\s+', re.I),
    re.compile(r'^single\s+strike\s+', re.I),
]


def _canonicalize_character_chunk(chunk):
    s = re.sub(r'\s+', ' ', chunk.strip())
    if len(s) <= 2:
        return s
    prev = None
    for _ in range(8):
        if s == prev:
            break
        prev = s
        for rx in _FORM_PREFIX_RES:
            s = rx.sub('', s)
        s = re.sub(r'\s+', ' ', s).strip()
    return s


def clean_character_name(raw_name):
    # Strip common competitive/mechanic suffixes
    suffixes = [r'\bVMAX\b', r'\bVSTAR\b', r'\bV\b', r'\bEX\b', r'\bex\b', r'\bGX\b', 
                r'\bBREAK\b', r'\bPrime\b', r'\bLV\.X\b', r'\bLEGEND\b', r'\bSP\b', r'\bStar\b']
    
    name = raw_name
    for suf in suffixes:
        name = re.sub(suf, '', name, flags=re.IGNORECASE)
        
    # Isolate Tag Teams
    chars = []
    for chunk in name.split('&'):
        # Strip all punctuation except spaces
        chunk = re.sub(r'[^a-zA-Z\s]', '', chunk).strip()
        chunk = re.sub(r'\s+', ' ', chunk)
        chunk = _canonicalize_character_chunk(chunk)
        if chunk and len(chunk) > 2: # Ignore things like "of"
            chars.append(chunk)

    return chars

def fetch_premium_volume(character_name):
    query = f'''
    query {{
      cards(filters: {{name: "{character_name}"}}) {{
        name
        rarity
      }}
    }}
    '''
    try:
        res = requests.post(GRAPHQL_URL, json={'query': query}, timeout=10)
        data = res.json()
        
        cards = data.get('data', {}).get('cards', [])
        if not cards: return 0
        
        premium_count = 0
        for c in cards:
            cname = c.get('name', '')
            c_rarity = c.get('rarity') or 'None'
            
            # Substring safety check: if we search "Mew", ignore "Mewtwo"
            # It must be a distinct word in the card name.
            words = set(re.findall(r'\b\w+\b', cname.lower()))
            target = character_name.lower()
            
            if target in words or target in cname.lower(): # Basic inclusion checks
                # Exclude Mewtwo if we searched Mew
                if target == "mew" and "mewtwo" in cname.lower():
                    continue

                if c_rarity not in BASIC_RARITIES:
                    premium_count += 1
                    
        return premium_count
    except Exception as e:
        print(f"Error fetching {character_name}: {e}")
        return 0

def run_pipeline():
    if not os.path.exists('pokemon_sets_data.json'):
        print("Data file not found. Ensure scraper has run first.")
        return
        
    with open('pokemon_sets_data.json', 'r', encoding='utf-8') as f:
        datasets = json.load(f)
        
    unique_chars = set()
    
    print("Isolating unique character identities and classifying from Set Metadata...")
    char_map = {} # char -> {is_human: bool}
    
    for s in datasets:
        for card in s.get('top_25_cards', []):
            identities = clean_character_name(card['name'])
            is_trainer = card.get('supertype') == 'Trainer'
            for i in identities:
                if i not in char_map:
                    char_map[i] = {'is_human': is_trainer}
                elif is_trainer: # prioritize trainer classification if ever found
                    char_map[i]['is_human'] = True
                
    char_list = list(char_map.keys())
    print(f"Discovered {len(char_list)} foundational character identities.")
    
    results = []
    
    # Sort for predictability during fetching
    char_list.sort()
    
    # Process dynamically
    for i, char in enumerate(char_list):
        print(f"[{i+1}/{len(char_list)}] Resolving Premium Prints for: {char}")
        volume = fetch_premium_volume(char)
        
        results.append({
            'Character': char,
            'High_Tier_Print_Volume': volume,
            'Is_Human': char_map[char]['is_human'],
            'Trainer_Archetype': get_trainer_archetype(char) if char_map[char]['is_human'] else 'N/A'
        })
        time.sleep(0.5) # respect API
        
    # Sort by Most Valuable character logically
    results.sort(key=lambda x: x['High_Tier_Print_Volume'], reverse=True)
    
    print("\nExporting datasets to character_premium_scores...")
    with open('character_premium_scores.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)
        
    pd.DataFrame(results).to_csv('character_premium_scores.csv', index=False, encoding='utf-8')
    print("Success!")

if __name__ == "__main__":
    run_pipeline()
