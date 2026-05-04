import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def scrape_pricecharting():
    base_url = "https://www.pricecharting.com/search-products"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/114.0.0.0 Safari/537.36'
    }
    
    all_packs = []
    
    # We will loop through pages arbitrarily until no more results. 
    # PriceCharting doesn't use standard `&page=X` always, but let's try it.
    for page in range(1, 20):
        print(f"Scraping PriceCharting page {page}...")
        params = {
            'q': 'booster pack pokemon',
            'type': 'prices',
            'page': page
        }
        try:
            res = requests.get(base_url, headers=headers, params=params, timeout=10)
            if res.status_code != 200:
                print(f"Failed to fetch page {page}. Status Code: {res.status_code}")
                break
                
            soup = BeautifulSoup(res.text, 'html.parser')
            tbody = soup.find('tbody')
            if not tbody:
                print("No tbody found. End of results or blocked.")
                break
                
            rows = tbody.find_all('tr')
            if not rows:
                print("No rows found. End of results.")
                break
                
            added_on_page = 0
            for row in rows:
                title_elem = row.find('td', class_='title')
                price_elem = row.find('td', class_='price')
                
                if title_elem and price_elem:
                    title = title_elem.text.strip().replace('\n', ' ')
                    
                    price_text = price_elem.text.strip()
                    # Clean up multiple spacing if any
                    price_text = " ".join(price_text.split())
                    
                    all_packs.append({
                        'Product': title,
                        'Price': price_text
                    })
                    added_on_page += 1
            
            if added_on_page == 0:
                print("No valid elements parsed. End of results.")
                break
                
        except Exception as e:
            print(f"Error occurred: {e}")
            break
            
        time.sleep(2) # rate limit evasion
        
    print(f"Scraped {len(all_packs)} pack prices.")
    
    if all_packs:
        df = pd.DataFrame(all_packs)
        df.to_csv('pricecharting_packs.csv', index=False)
        print("Dataset saved to pricecharting_packs.csv")

if __name__ == "__main__":
    scrape_pricecharting()
