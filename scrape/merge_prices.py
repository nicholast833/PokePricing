import json
import pandas as pd
import re

def merge():
    try:
        with open('pokemon_sets_data.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        pc_df = pd.read_csv('pricecharting_packs.csv')
        pc_df['ProductNorm'] = pc_df['Product'].str.lower()
        pc_df['ProductNorm'] = pc_df['ProductNorm'].apply(lambda x: re.sub(r'\s+', ' ', x))
        
        matches_found = 0
        for s in data:
            s_name = s['set_name'].lower().strip()
            # Construct optimal search strings
            target1 = f"booster pack pokemon {s_name}"
            target2 = f"pokemon {s_name} booster pack"
            
            # Find in DF that contains the exact sequence
            # we'll look for strings that contain "pokemon <setname>"
            match = pc_df[pc_df['ProductNorm'].str.contains(f"pokemon {s_name}", na=False, regex=False)]
            
            if not match.empty:
                # filter out things like "base set 2" if looking for "base set"
                exact = match[match['ProductNorm'].str.contains(f"pokemon {s_name}$", na=False, regex=True)] # match at end
                if not exact.empty:
                    hit = exact.iloc[0]
                else:
                    hit = match.iloc[0]
                    
                price_str = hit['Price'].replace('$', '').replace(',', '')
                try:
                    s['tcgplayer_pack_price'] = float(price_str)
                    matches_found += 1
                except:
                    pass
        
        with open('pokemon_sets_data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
            
        print(f"Successfully mapped {matches_found} new pack prices from PriceCharting!")
                
    except Exception as e:
        print("Merge failed:", e)

if __name__ == '__main__':
    merge()
