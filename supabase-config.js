// supabase-config.js
// This file is loaded by the frontend HTML files to configure the Supabase client.

const SUPABASE_URL = "https://gjvamuavqruirrjajefj.supabase.co";
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdqdmFtdWF2cXJ1aXJyamFqZWZqIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc4Mzc5MjQsImV4cCI6MjA5MzQxMzkyNH0.JiAS1mPtxo-1qj9S7c2phFzCqZ3tXYCY8cNM8EsNTU0";

// Helper function to fetch Sets and Cards from Supabase REST API (excluding heavy price history)
async function fetchPokemonSetsFromSupabase() {
    // We explicitly select the columns, omitting the massive `price_history` JSONB column.
    const url = `${SUPABASE_URL}/rest/v1/pokemon_sets?select=*,pokemon_cards(unique_card_id,set_code,name,number,rarity,market_price,artist,image_url,metrics)`;
    
    const response = await fetch(url, {
        headers: {
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": `Bearer ${SUPABASE_ANON_KEY}`
        }
    });

    if (!response.ok) {
        throw new Error(`Supabase fetch failed: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    
    // The frontend expects the JSON structure from `pokemon_sets_data.json`.
    // We need to map `pokemon_cards` back to `top_25_cards`, and restore the flattened metrics.
    return data.map(set => {
        // Map pokemon_cards -> top_25_cards
        const cards = set.pokemon_cards || [];
        const top_25_cards = cards.map(c => {
            // Reconstruct the original card object structure (without history arrays initially)
            return {
                ...c,
                ...(c.metrics || {}) // Spread metrics back into the top level
            };
        });

        // The JS currently filters / searches based on specific keys. We need to ensure
        // the original JSON structure is perfectly recreated.
        return {
            ...set,
            ...(set.metadata || {}), // Spread set metadata (ev, rarity_counts) back into top level
            top_25_cards: top_25_cards
        };
    });
}

// Fetches the heavy price_history JSONB column for a specific card on-demand
async function fetchCardPriceHistory(uniqueCardId) {
    // In PostgREST, we filter by unique_card_id using `?unique_card_id=eq.YOUR_ID`
    // We only select the price_history column.
    const url = `${SUPABASE_URL}/rest/v1/pokemon_cards?unique_card_id=eq.${encodeURIComponent(uniqueCardId)}&select=price_history`;
    
    const response = await fetch(url, {
        headers: {
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": `Bearer ${SUPABASE_ANON_KEY}`
        }
    });

    if (!response.ok) {
        console.error(`Failed to fetch history for ${uniqueCardId}: ${response.status}`);
        return null;
    }

    const data = await response.json();
    if (!data || data.length === 0) return null;
    
    // The data is an array of 1 row, and we want the `price_history` object inside it.
    return data[0].price_history;
}
