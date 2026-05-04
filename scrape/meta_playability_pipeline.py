import json
import re
import pandas as pd
import os

# Scraped raw Limitless TCG Metagame usage percentages
RAW_META = [
    ("Dragapult ex", 18.49),
    ("Marnie's Grimmsnarl ex", 13.60),
    ("Gardevoir ex", 11.48),
    ("N's Zoroark ex", 9.66),
    ("Gholdengo ex", 9.35),
    ("Mega Absol Box", 5.90),
    ("Froslass Munkidori", 5.50),
    ("Charizard ex", 4.59),
    ("Crustle Mysterious Rock Inn", 4.20),
    ("Raging Bolt ex", 3.98),
    ("Joltik Box", 3.98),
    ("Tera Box", 1.23),
    ("Ceruledge ex", 1.14),
    ("Alakazam Powerful Hand", 1.09),
    ("Mega Kangaskhan ex", 0.97),
    ("Festival Lead", 0.92),
    ("Flareon ex", 0.47),
    ("Mega Lucario ex", 0.34),
    ("Rocket's Honchkrow", 0.34),
    ("Bloodmoon Ursaluna Mad Bite", 0.28),
    ("Ogerpon Meganium", 0.28),
    ("Slowking Seek Inspiration", 0.25),
    ("Ho-Oh Armarouge", 0.22),
    ("Mega Sharpedo ex", 0.22),
    ("Ethan's Typhlosion", 0.18),
    ("Mega Venusaur ex", 0.16),
    ("Greninja ex", 0.15),
    ("Farigiraf ex", 0.13),
    ("Cynthia's Garchomp ex", 0.12),
    ("Rocket's Mewtwo ex", 0.12),
    ("Okidogi Adrena-Power", 0.10)
]

def build_meta_dynamics():
    # Load foundational character lists to match strictly against Target Pokemon
    if not os.path.exists('pokemon_sets_data.json'):
        print("pokemon_sets_data.json not found!")
        return

    with open('pokemon_sets_data.json', 'r', encoding='utf-8') as f:
        datasets = json.load(f)

    # Collect known pure characters
    known_characters = set()
    for s in datasets:
        for card in s.get('top_25_cards', []):
            name = card['name']
            suffixes = [r'\bVMAX\b', r'\bVSTAR\b', r'\bV\b', r'\bEX\b', r'\bex\b', r'\bGX\b', 
                        r'\bBREAK\b', r'\bPrime\b', r'\bLV\.X\b', r'\bLEGEND\b', r'\bSP\b', r'\bStar\b']
            for suf in suffixes:
                name = re.sub(suf, '', name, flags=re.IGNORECASE)
            for chunk in name.split('&'):
                chunk = re.sub(r'[^a-zA-Z\s]', '', chunk).strip()
                if chunk and len(chunk) > 2:
                    known_characters.add(chunk)

    meta_tallies = {}
    
    # Identify overlaps from Limitless TCG into known character mappings
    for raw_deck, raw_percent in RAW_META:
        clean_deck = re.sub(r'\b(ex|Mega|Box|VMAX|VSTAR|V|Lead)\b', '', raw_deck, flags=re.IGNORECASE).strip()
        
        # Simple token split to find Pokemon names inside deck titles (e.g. "Froslass Munkidori")
        tokens = clean_deck.split()
        for token in tokens:
            if token in known_characters:
                meta_tallies[token] = meta_tallies.get(token, 0) + raw_percent
            elif token.endswith("'s"): # "Marnie's Grimmsnarl"
                stripped = token[:-2]
                if stripped in known_characters:
                    meta_tallies[stripped] = meta_tallies.get(stripped, 0) + raw_percent
                    
    # Format and Output JSON
    results = []
    
    # We apply the dataset map to ALL extracted characters from the workspace, evaluating non-meta characters as 0% implicitly
    for char in known_characters:
         percent = meta_tallies.get(char, 0.0)
         results.append({
             "Character": char,
             "Limitless_Meta_Share_Percentage": round(percent, 2),
             "Is_Meta": bool(percent >= 5.0) # Using Boolean constraint defined during planning
         })
         
    results.sort(key=lambda x: x['Limitless_Meta_Share_Percentage'], reverse=True)

    with open('meta_playability_momentum.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)
        
    pd.DataFrame(results).to_csv('meta_playability_momentum.csv', index=False, encoding='utf-8')
    print(f"Successfully processed Playability metrics for {len(results)} distinct characters!")

if __name__ == "__main__":
    build_meta_dynamics()
