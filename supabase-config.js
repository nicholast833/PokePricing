// supabase-config.js
// This file is loaded by the frontend HTML files to configure the Supabase client.

const SUPABASE_URL = "https://gjvamuavqruirrjajefj.supabase.co";
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdqdmFtdWF2cXJ1aXJyamFqZWZqIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc4Mzc5MjQsImV4cCI6MjA5MzQxMzkyNH0.JiAS1mPtxo-1qj9S7c2phFzCqZ3tXYCY8cNM8EsNTU0";

function _supabaseRestHeaders() {
    return {
        apikey: SUPABASE_ANON_KEY,
        Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
    };
}

/** Optional: dated pack history lives on ``pokemon_set_pack_pricing``, not in ``metadata`` (keeps list queries fast). */
async function mergePackCostHistoryFromPackPricingTable(sets) {
    if (!Array.isArray(sets) || !sets.length) return;
    const headers = _supabaseRestHeaders();
    const codes = [
        ...new Set(
            sets
                .map((s) => String(s.set_code || '').trim().toLowerCase())
                .filter(Boolean)
        ),
    ];
    const chunkSize = 50;
    const byCode = {};
    for (let i = 0; i < codes.length; i += chunkSize) {
        const part = codes.slice(i, i + chunkSize);
        const url = `${SUPABASE_URL}/rest/v1/pokemon_set_pack_pricing?select=set_code,pack_cost_price_history,pack_cost_price_history_en&set_code=in.(${part.join(
            ','
        )})`;
        const r = await fetch(url, { headers });
        if (!r.ok) return;
        const rows = await r.json();
        if (!Array.isArray(rows)) return;
        rows.forEach((row) => {
            const sc = String(row.set_code || '').trim().toLowerCase();
            if (sc) byCode[sc] = row;
        });
    }
    sets.forEach((set) => {
        const sc = String(set.set_code || '').trim().toLowerCase();
        const row = byCode[sc];
        if (!row) return;
        if (Array.isArray(row.pack_cost_price_history) && row.pack_cost_price_history.length) {
            set.pack_cost_price_history = row.pack_cost_price_history;
        }
        if (row.pack_cost_price_history_en && typeof row.pack_cost_price_history_en === 'object') {
            set.pack_cost_price_history_en = row.pack_cost_price_history_en;
        }
    });
}

// Helper function to fetch Sets and Cards from Supabase REST API (excluding heavy price history)
async function fetchPokemonSetsFromSupabase() {
    // One request for all rows + embedded cards can exceed Postgres `statement_timeout` (HTTP 500 / code 57014).
    // Page by set rows; order is stable for offset pagination.
    const select =
        '*,pokemon_cards(unique_card_id,set_code,name,number,rarity,market_price,artist,image_url,metrics)';
    const pageSize = 25;
    const headers = _supabaseRestHeaders();
    const data = [];
    for (let offset = 0; ; offset += pageSize) {
        const url = `${SUPABASE_URL}/rest/v1/pokemon_sets?select=${encodeURIComponent(
            select
        )}&order=set_code.asc&limit=${pageSize}&offset=${offset}`;
        const response = await fetch(url, { headers });
        if (!response.ok) {
            let detail = '';
            try {
                const errBody = await response.json();
                if (errBody && errBody.message) detail = `: ${errBody.message}`;
            } catch (e) {
                /* ignore */
            }
            throw new Error(`Supabase fetch failed: ${response.status}${detail}`);
        }
        const batch = await response.json();
        if (!Array.isArray(batch) || batch.length === 0) break;
        data.push(...batch);
        if (batch.length < pageSize) break;
    }
    
    // The frontend expects the JSON structure from `pokemon_sets_data.json`.
    // We need to map `pokemon_cards` back to `top_25_cards`, and restore the flattened metrics.
    const mergedRows = data.map((set) => {
        // Map pokemon_cards -> top_25_cards
        const cards = set.pokemon_cards || [];
        const top_25_cards = cards.map((c) => ({
            ...c,
            ...(c.metrics || {}),
        }));

        return {
            ...set,
            ...(set.metadata || {}),
            top_25_cards,
        };
    });
    try {
        await mergePackCostHistoryFromPackPricingTable(mergedRows);
    } catch (e) {
        console.warn('mergePackCostHistoryFromPackPricingTable skipped:', e);
    }
    mergedRows.forEach((m) => {
        if (typeof SHARED_UTILS !== 'undefined' && SHARED_UTILS.hydrateSetPackCostPipelineFields) {
            SHARED_UTILS.hydrateSetPackCostPipelineFields(m);
        }
    });
    return mergedRows;
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

    const response = await fetch(url, { headers: _supabaseRestHeaders() });

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

/**
 * Predictor / Analytics sidecar JSON from ``predictor_analytics_assets`` (same shapes as ``data/assets/*.json``).
 * @returns {Promise<Record<string, unknown>|null>} map asset_key → payload, or null if table missing / error / empty.
 */
async function fetchPredictorAnalyticsAssetsFromSupabase() {
    const url = `${SUPABASE_URL}/rest/v1/predictor_analytics_assets?select=asset_key,payload`;
    const r = await fetch(url, { headers: _supabaseRestHeaders() });
    if (!r.ok) return null;
    const rows = await r.json();
    if (!Array.isArray(rows) || rows.length === 0) return null;
    const out = {};
    rows.forEach((row) => {
        if (row && row.asset_key != null) out[String(row.asset_key)] = row.payload;
    });
    return Object.keys(out).length ? out : null;
}
