import json
import os
import re

def clean_character_name(raw_name):
    suffixes = [r'\bVMAX\b', r'\bVSTAR\b', r'\bV\b', r'\bEX\b', r'\bex\b', r'\bGX\b', 
                r'\bBREAK\b', r'\bPrime\b', r'\bLV\.X\b', r'\bLEGEND\b', r'\bSP\b', r'\bStar\b']
    name = raw_name
    for suf in suffixes:
        name = re.sub(suf, '', name, flags=re.IGNORECASE)
    chars = []
    for chunk in name.split('&'):
        chunk = re.sub(r'[^a-zA-Z\s]', '', chunk).strip()
        chunk = re.sub(r'\s+', ' ', chunk)
        if chunk and len(chunk) > 2:
            chars.append(chunk)
    return chars

def build_nostalgia_index():
    data_path = 'pokemon_sets_data.json'
    if not os.path.exists(data_path):
        print("Error: pokemon_sets_data.json not found.")
        return

    with open(data_path, 'r', encoding='utf-8') as f:
        datasets = json.load(f)

    species_prices = {} # species -> list of prices

    for s in datasets:
        for card in s.get('top_25_cards', []):
            price = card.get('market_price') or card.get('pricedex_market_usd') or 0
            if price <= 0: continue
            
            identities = clean_character_name(card['name'])
            for i in identities:
                if i not in species_prices:
                    species_prices[i] = []
                species_prices[i].append(price)

    nostalgia_index = {}
    for species, prices in species_prices.items():
        # Sort prices descending and take top 3 to measure "Ceiling/Icon" factor
        prices.sort(reverse=True)
        top_3 = prices[:3]
        score = sum(top_3)
        nostalgia_index[species] = {
            'nostalgia_score': score,
            'top_3_sum': score,
            'sample_size': len(prices)
        }

    output_path = 'nostalgia_index.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(nostalgia_index, f, indent=4)
    print(f"Success: Created {output_path} with {len(nostalgia_index)} species.")

if __name__ == "__main__":
    build_nostalgia_index()
