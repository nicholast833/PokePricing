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

const _EXPLORER_SLIM_CARD_COLS =
    'unique_card_id,set_code,name,number,rarity,market_price,artist,image_url,tracked_priority';
const _EXPLORER_FULL_CARD_COLS = `${_EXPLORER_SLIM_CARD_COLS},metrics`;

/**
 * @param {unknown[]} rawSets
 * @param {{ mergeCardMetrics?: boolean }} [opts]
 */
function mapRawSetsToExplorerShape(rawSets, opts) {
    const mergeCardMetrics = opts && opts.mergeCardMetrics !== false;
    return (rawSets || []).map((set) => {
        const cards = set.pokemon_cards || [];
        const top_25_cards = cards.map((c) => {
            const flat = mergeCardMetrics ? { ...c, ...(c.metrics || {}) } : { ...c };
            if (typeof SHARED_UTILS !== 'undefined' && SHARED_UTILS.deriveExplorerSpeciesKeyFromCardName) {
                const sk = SHARED_UTILS.deriveExplorerSpeciesKeyFromCardName(flat.name);
                if (sk) flat.species = sk;
            }
            return flat;
        });
        return {
            ...set,
            ...(set.metadata || {}),
            top_25_cards,
        };
    });
}

async function _fetchPokemonSetsPagedSelect(select, pageSize) {
    const headers = _supabaseRestHeaders();
    const out = [];
    for (let offset = 0; ; offset += pageSize) {
        const url = `${SUPABASE_URL}/rest/v1/pokemon_sets?select=${encodeURIComponent(
            select,
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
        out.push(...batch);
        if (batch.length < pageSize) break;
    }
    return out;
}

/** Set metadata only (no embedded cards) — small pages for Explorer catalog + filters. */
async function fetchPokemonSetsCatalogOnly() {
    const pageSize = 60;
    const raw = await _fetchPokemonSetsPagedSelect('*', pageSize);
    const merged = mapRawSetsToExplorerShape(raw, { mergeCardMetrics: false });
    merged.forEach((s) => {
        s.top_25_cards = [];
    });
    try {
        await mergePackCostHistoryFromPackPricingTable(merged);
    } catch (e) {
        console.warn('mergePackCostHistoryFromPackPricingTable skipped:', e);
    }
    merged.forEach((m) => {
        if (typeof SHARED_UTILS !== 'undefined' && SHARED_UTILS.hydrateSetPackCostPipelineFields) {
            SHARED_UTILS.hydrateSetPackCostPipelineFields(m);
        }
    });
    return merged;
}

/**
 * Fetch specific sets with embedded cards (slim or full). ``setCodes`` length should stay small (URL limits).
 * @param {string[]} setCodes
 * @param {{ withMetrics?: boolean }} [opts]
 */
async function fetchPokemonSetsCardSliceByCodes(setCodes, opts) {
    const withMetrics = Boolean(opts && opts.withMetrics);
    const cardCols = withMetrics ? _EXPLORER_FULL_CARD_COLS : _EXPLORER_SLIM_CARD_COLS;
    const selectTracked = `*,pokemon_cards(${cardCols})&pokemon_cards.order=tracked_priority.asc&pokemon_cards.tracked_priority=gte.1`;
    const selectLegacy = `*,pokemon_cards(${cardCols})`;
    const clean = [...new Set((setCodes || []).map((c) => String(c || '').trim().toLowerCase()).filter(Boolean))];
    if (!clean.length) return [];
    const inList = clean.map((c) => encodeURIComponent(c)).join(',');
    const headers = _supabaseRestHeaders();
    const tryFetch = async (sel) => {
        const url = `${SUPABASE_URL}/rest/v1/pokemon_sets?set_code=in.(${inList})&select=${encodeURIComponent(
            sel,
        )}&order=set_code.asc`;
        const r = await fetch(url, { headers });
        if (!r.ok) throw new Error(`Supabase fetch failed: ${r.status}`);
        return r.json();
    };
    let data;
    try {
        data = await tryFetch(selectTracked);
    } catch (e) {
        console.warn('tracked card slice failed; legacy slice.', e);
        data = await tryFetch(selectLegacy);
    }
    if (!Array.isArray(data)) return [];
    const nTracked = data.reduce(
        (n, s) => n + (Array.isArray(s.pokemon_cards) ? s.pokemon_cards.length : 0),
        0,
    );
    if (nTracked === 0 && clean.length) {
        data = await tryFetch(selectLegacy);
    }
    const merged = mapRawSetsToExplorerShape(data, { mergeCardMetrics: withMetrics });
    try {
        await mergePackCostHistoryFromPackPricingTable(merged);
    } catch (e) {
        console.warn('mergePackCostHistoryFromPackPricingTable skipped:', e);
    }
    merged.forEach((m) => {
        if (typeof SHARED_UTILS !== 'undefined' && SHARED_UTILS.hydrateSetPackCostPipelineFields) {
            SHARED_UTILS.hydrateSetPackCostPipelineFields(m);
        }
    });
    return merged;
}

/** One set with full ``metrics`` merge (Explorer expand / deep link). */
async function fetchPokemonSetWithMetricsByCode(setCode) {
    const sc = String(setCode || '').trim().toLowerCase();
    if (!sc) return null;
    const selectTracked = `*,pokemon_cards(${_EXPLORER_FULL_CARD_COLS})&pokemon_cards.order=tracked_priority.asc&pokemon_cards.tracked_priority=gte.1`;
    const selectLegacy = `*,pokemon_cards(${_EXPLORER_FULL_CARD_COLS})`;
    const headers = _supabaseRestHeaders();
    const urlBase = `${SUPABASE_URL}/rest/v1/pokemon_sets?set_code=eq.${encodeURIComponent(sc)}&select=`;
    let data;
    try {
        const r = await fetch(urlBase + encodeURIComponent(selectTracked), { headers });
        if (!r.ok) throw new Error(String(r.status));
        data = await r.json();
    } catch (e) {
        const r = await fetch(urlBase + encodeURIComponent(selectLegacy), { headers });
        if (!r.ok) return null;
        data = await r.json();
    }
    if (!Array.isArray(data) || !data.length) return null;
    let merged = mapRawSetsToExplorerShape(data, { mergeCardMetrics: true });
    const n0 = (merged[0].top_25_cards || []).length;
    if (n0 === 0) {
        const r2 = await fetch(urlBase + encodeURIComponent(selectLegacy), { headers });
        if (!r2.ok) return null;
        const data2 = await r2.json();
        if (!Array.isArray(data2) || !data2.length) return null;
        merged = mapRawSetsToExplorerShape(data2, { mergeCardMetrics: true });
    }
    try {
        await mergePackCostHistoryFromPackPricingTable(merged);
    } catch (e) {
        console.warn('mergePackCostHistoryFromPackPricingTable skipped:', e);
    }
    merged.forEach((m) => {
        if (typeof SHARED_UTILS !== 'undefined' && SHARED_UTILS.hydrateSetPackCostPipelineFields) {
            SHARED_UTILS.hydrateSetPackCostPipelineFields(m);
        }
    });
    return merged[0] || null;
}

// Helper function to fetch Sets and Cards from Supabase REST API (excluding heavy price history)
async function fetchPokemonSetsFromSupabase() {
    const selectTracked = `*,pokemon_cards(${_EXPLORER_FULL_CARD_COLS})&pokemon_cards.order=tracked_priority.asc&pokemon_cards.tracked_priority=gte.1`;
    const selectLegacy = `*,pokemon_cards(${_EXPLORER_FULL_CARD_COLS})`;
    const pageSize = 25;
    let data;
    try {
        data = await _fetchPokemonSetsPagedSelect(selectTracked, pageSize);
    } catch (e) {
        console.warn('tracked pokemon_cards embed failed (column missing or filter unsupported); using legacy fetch.', e);
        data = await _fetchPokemonSetsPagedSelect(selectLegacy, pageSize);
    }
    const nTracked = data.reduce(
        (n, s) => n + (Array.isArray(s.pokemon_cards) ? s.pokemon_cards.length : 0),
        0,
    );
    if (nTracked === 0) {
        data = await _fetchPokemonSetsPagedSelect(selectLegacy, pageSize);
    }
    const mergedRows = mapRawSetsToExplorerShape(data, { mergeCardMetrics: true });
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

    if (typeof SHARED_UTILS !== 'undefined' && SHARED_UTILS.deriveExplorerSpeciesKeyFromCardName) {
        const sk = SHARED_UTILS.deriveExplorerSpeciesKeyFromCardName(card.name);
        if (sk) card.species = sk;
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
 * @param {string[]|null} [assetKeys] When set, only those rows are fetched (smaller payload for Predictor).
 * @returns {Promise<Record<string, unknown>|null>} map asset_key → payload, or null if table missing / error / empty.
 */
async function fetchPredictorAnalyticsAssetsFromSupabase(assetKeys = null) {
    let url = `${SUPABASE_URL}/rest/v1/predictor_analytics_assets?select=asset_key,payload`;
    if (Array.isArray(assetKeys) && assetKeys.length) {
        const enc = assetKeys.map((k) => encodeURIComponent(String(k))).join(',');
        url += `&asset_key=in.(${enc})`;
    }
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

/**
 * Per-card predictor payload from ``predictor_card_precompute`` (written by GitHub Actions).
 * @param {string} uniqueCardId
 * @returns {Promise<Record<string, unknown>|null>}
 */
async function fetchPredictorCardPrecomputeFromSupabase(uniqueCardId) {
    if (!uniqueCardId || typeof SUPABASE_URL === 'undefined' || !SUPABASE_URL) return null;
    const url = `${SUPABASE_URL}/rest/v1/predictor_card_precompute?unique_card_id=eq.${encodeURIComponent(
        String(uniqueCardId),
    )}&select=payload`;
    const r = await fetch(url, { headers: _supabaseRestHeaders() });
    if (!r.ok) return null;
    const rows = await r.json();
    if (!Array.isArray(rows) || rows.length === 0) return null;
    const p = rows[0] && rows[0].payload;
    return p && typeof p === 'object' ? p : null;
}
