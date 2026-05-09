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
        const merged = {
            ...set,
            ...(set.metadata || {}), // Spread set metadata (ev, rarity_counts) back into top level
            top_25_cards: top_25_cards
        };
        if (typeof SHARED_UTILS !== 'undefined' && SHARED_UTILS.hydrateSetPackCostPipelineFields) {
            SHARED_UTILS.hydrateSetPackCostPipelineFields(merged);
        }
        return merged;
    });
}

/**
 * Fetches live columns written by GitHub Actions / scripts: `price_history`, `metrics`,
 * plus scalar fields used by the Explorer / Predictor UI.
 */
async function fetchCardLiveRowFromSupabase(uniqueCardId) {
    const cols = [
        'price_history',
        'metrics',
        'market_price',
        'last_synced_at',
    ].join(',');
    const url = `${SUPABASE_URL}/rest/v1/pokemon_cards?unique_card_id=eq.${encodeURIComponent(uniqueCardId)}&select=${cols}`;

    const response = await fetch(url, {
        headers: {
            apikey: SUPABASE_ANON_KEY,
            Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
        },
    });

    if (!response.ok) {
        console.error(`Failed to fetch live row for ${uniqueCardId}: ${response.status}`);
        return null;
    }

    const data = await response.json();
    if (!data || data.length === 0) return null;
    return data[0];
}

/** Merges a `pokemon_cards` REST row onto the in-memory card object (flat + nested helpers). */
function mergeLivePokemonCardRow(card, row) {
    if (!card || !row || typeof row !== 'object') return;

    if (row.last_synced_at != null && row.last_synced_at !== '') {
        card.last_synced_at = row.last_synced_at;
    }
    if (row.market_price != null && row.market_price !== '') {
        const n = Number(row.market_price);
        if (Number.isFinite(n)) card.market_price = n;
    }

    const ph = row.price_history;
    if (ph && typeof ph === 'object') {
        Object.keys(ph).forEach((k) => {
            card[k] = ph[k];
        });
    }

    const m = row.metrics;
    if (m && typeof m === 'object') {
        Object.keys(m).forEach((k) => {
            card[k] = m[k];
        });
    }
}

/** @deprecated Use fetchCardLiveRowFromSupabase + mergeLivePokemonCardRow */
async function fetchCardPriceHistory(uniqueCardId) {
    const row = await fetchCardLiveRowFromSupabase(uniqueCardId);
    return row && row.price_history ? row.price_history : null;
}
