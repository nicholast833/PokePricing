/**
 * predictor.js - Price Predictor logic
 */

document.addEventListener('DOMContentLoaded', () => {
    const loadingEl = document.getElementById('loading');
    const containerEl = document.getElementById('predictor-container');
    const searchInput = document.getElementById('predictorSearch');
    const resultsEl = document.getElementById('predictorResults');
    const predictionView = document.getElementById('prediction-view');

    let allSetsData = [];
    let analyticsState = {
        characterData: [],
        trendsData: [],
        artistChaseLookup: {},
        tcgMacroInterest: {}
    };
    let globalSearchIndex = [];
    let globalModel = null;
    let globalRegression = null; // { b0, b1 } for PredPrice = 10^(b0 + b1*CompositeX)

    async function initPredictor() {
        const pg = typeof window.PTCG_PAGE_PROGRESS !== 'undefined' ? window.PTCG_PAGE_PROGRESS : null;
        if (pg) pg.begin();
        try {
            const sets = await fetchPokemonSetsFromSupabase();
            if (pg) pg.setDeterminate(0.32);
            const [characters, trends, artists, tcgMacro] = await Promise.all([
                fetch(SHARED_UTILS.resolveDataAssetUrl('character_premium_scores.json')).then(r => r.json()).catch(() => []),
                fetch(SHARED_UTILS.resolveDataAssetUrl('google_trends_momentum.json')).then(r => r.json()).catch(() => []),
                fetch(SHARED_UTILS.resolveDataAssetUrl('artist_scores.json')).then(r => r.json()).catch(() => []),
                fetch(SHARED_UTILS.resolveDataAssetUrl('tcg_macro_interest_by_year.json')).then(r => r.json()).catch(() => ({}))
            ]);
            if (pg) pg.setDeterminate(0.58);

            allSetsData = sets;
            analyticsState.characterData = characters;
            analyticsState.trendsData = trends;
            analyticsState.tcgMacroInterest = tcgMacro;
            
            // Build artist lookup
            artists.forEach(a => {
                if (a.artist && a.chase_median) analyticsState.artistChaseLookup[a.artist] = a.chase_median;
            });

            // 1. Build Search Index & Training Rows
            const trainingRows = [];
            allSetsData.forEach(set => {
                if (!set.top_25_cards) return;
                set.top_25_cards.forEach(card => {
                    const price = SHARED_UTILS.resolveExplorerChartUsd(card);
                    if (price && price > 0) {
                        const feat = REGRESSION_ENGINE.extractFeatures(card, set, analyticsState);
                        trainingRows.push({ card, set, feat, price });
                    }
                    globalSearchIndex.push({ card, set });
                });
            });

            // 2. Build Global Model
            buildGlobalModel(trainingRows);
            if (pg) pg.setDeterminate(0.92);

            loadingEl.style.display = 'none';
            containerEl.style.display = 'block';
            if (pg) pg.setDeterminate(1);

            wireSearch();
            tryOpenPredictorFromQuery();
        } catch (e) {
            console.error(e);
            loadingEl.innerHTML = `<p style="color:red;">Failed to load predictor engine: ${e.message}</p>`;
        } finally {
            if (pg) pg.end();
        }
    }

    function buildGlobalModel(rows) {
        // We use a simplified version of the analytics selector
        const keys = REGRESSION_ENGINE.COMPOSITE_KEYS;
        const means = {};
        const stds = {};
        const rBy = {};

        keys.forEach(k => {
            const xs = [];
            const ys = [];
            const ws = [];
            rows.forEach(r => {
                if (r.feat[k] != null) {
                    xs.push(r.feat[k]);
                    ys.push(Math.log10(r.price));
                    ws.push(1); // Equal weight for predictor baseline
                }
            });
            if (xs.length < 5) return;
            const ms = REGRESSION_ENGINE.weightedMeanStd(xs, ws);
            means[k] = ms.mean;
            stds[k] = ms.std;
            rBy[k] = REGRESSION_ENGINE.weightedPearsonR(xs, ys, ws);
        });

        // Filter to drivers with at least some correlation
        const finalKeys = keys.filter(k => means[k] != null && Math.abs(rBy[k]) > 0.1);
        
        globalModel = { keys: finalKeys, means, stds, r: rBy };

        // Fit the final regression: CompositeX vs LogPrice
        const xs = [];
        const ys = [];
        const ws = [];
        rows.forEach(r => {
            const cx = REGRESSION_ENGINE.compositeScoreFromRow(r.feat, globalModel);
            if (cx != null) {
                xs.push(cx);
                ys.push(Math.log10(r.price));
                ws.push(1);
            }
        });

        globalRegression = REGRESSION_ENGINE.fitWeightedLinearYOnX(xs, ys, ws);
        console.log('Global Model Fit:', globalRegression);
    }

    function wireSearch() {
        resultsEl.addEventListener('click', (e) => {
            const row = e.target.closest('.predictor-result-item');
            if (!row) return;
            const setCode = row.dataset.setCode;
            const num = row.dataset.cardNumber;
            const name = row.dataset.cardName || '';
            window.selectCard(setCode, num, name);
        });

        searchInput.addEventListener('input', () => {
            const term = searchInput.value.toLowerCase();
            if (term.length < 2) {
                resultsEl.hidden = true;
                return;
            }

            const matches = globalSearchIndex.filter(e => 
                e.card.name.toLowerCase().includes(term) || 
                (e.set.set_name && e.set.set_name.toLowerCase().includes(term))
            ).slice(0, 8);

            if (matches.length > 0) {
                resultsEl.innerHTML = matches.map(m => `
                    <div class="predictor-result-item" data-set-code="${SHARED_UTILS.escAttr(m.set.set_code)}" data-card-number="${SHARED_UTILS.escAttr(String(m.card.number))}" data-card-name="${SHARED_UTILS.escAttr(m.card.name || '')}">
                        <div style="display:flex; flex-direction:column;">
                            <span class="res-name">${SHARED_UTILS.escHtml(m.card.name)}</span>
                            <span class="res-set" style="font-size:0.65rem; opacity:0.8;">${SHARED_UTILS.escHtml(m.set.set_name)} · #${m.card.number}</span>
                        </div>
                        <span class="card-detail-tag" style="font-size:0.55rem; padding:1px 4px;">${SHARED_UTILS.escHtml(m.card.rarity || '')}</span>
                    </div>
                `).join('');
                resultsEl.hidden = false;
            } else {
                resultsEl.hidden = true;
            }
        });

        window.selectCard = (setCode, num, name) => {
            const cands = globalSearchIndex.filter((e) => e.set.set_code === setCode && String(e.card.number) === String(num));
            const hit = name ? cands.find((e) => e.card.name === name) || cands[0] : cands[0];
            if (hit) runPrediction(hit.card, hit.set);
            resultsEl.hidden = true;
            searchInput.value = '';
        };

        const tagBtn = document.getElementById('model-tag');
        if (tagBtn) {
            tagBtn.addEventListener('click', () => {
                if (!window._currentPredictorCard || !window._currentPredictorSet) return;
                window.location.href = `analytics.html?select_card_name=${encodeURIComponent(window._currentPredictorCard.name)}&select_card_set=${encodeURIComponent(window._currentPredictorSet.set_name)}`;
            });
        }
    }

    function syncPredictorCardUrl(card, set) {
        const u = new URL(window.location.href);
        u.searchParams.set('set', set.set_code);
        u.searchParams.set('num', String(card.number));
        u.searchParams.set('name', card.name || '');
        const qs = u.searchParams.toString();
        window.history.replaceState({}, '', `${u.pathname}${qs ? `?${qs}` : ''}${u.hash}`);
    }

    function tryOpenPredictorFromQuery() {
        if (!globalSearchIndex.length) return;
        const u = new URL(window.location.href);
        const sc = (u.searchParams.get('set') || '').trim();
        const num = (u.searchParams.get('num') || '').trim();
        if (!sc || num === '') return;
        const name = (u.searchParams.get('name') || '').trim();
        const cands = globalSearchIndex.filter((e) => e.set.set_code === sc && String(e.card.number) === String(num));
        if (!cands.length) return;
        const hit = name ? cands.find((e) => e.card.name === name) || cands[0] : cands[0];
        runPrediction(hit.card, hit.set);
    }

    let predictorCharts = [];

    function destroyPredictorCharts() {
        while (predictorCharts.length) {
            const ch = predictorCharts.pop();
            try { ch.destroy(); } catch (e) {}
        }
    }

    async function runPrediction(card, set) {
        predictionView.style.display = 'block';
        destroyPredictorCharts();
        
        const feat = REGRESSION_ENGINE.extractFeatures(card, set, analyticsState);
        const compX = REGRESSION_ENGINE.compositeScoreFromRow(feat, globalModel);
        const actualPrice = SHARED_UTILS.resolveExplorerChartUsd(card);
        
        let rawModelUsd = 0;
        if (compX != null && globalRegression) {
            const logP = globalRegression.b0 + globalRegression.b1 * compX;
            rawModelUsd = Math.pow(10, logP);
        }
        const cal = SHARED_UTILS.predictorCalibrateUsd(card, rawModelUsd);
        const predictedPrice = cal.final;

        renderScorecard(card, set, predictedPrice, actualPrice, feat, cal);
        renderCardHero(card, set);
        renderReasoning(card, predictedPrice, actualPrice, feat, cal);

        if (card.unique_card_id && typeof fetchCardLiveRowFromSupabase === 'function') {
            try {
                const row = await fetchCardLiveRowFromSupabase(card.unique_card_id);
                if (row && typeof mergeLivePokemonCardRow === 'function') {
                    mergeLivePokemonCardRow(card, row);
                }
            } catch (e) {
                console.warn('Failed to fetch Supabase live card row for predictor:', e);
            }
        }

        const detailGrid = document.getElementById('prediction-stats-grid');
        const chartsArea = document.getElementById('prediction-charts-area');
        const chartsEmpty = document.getElementById('prediction-charts-empty');
        const detailsSplit = document.querySelector('.prediction-details-split');

        detailGrid.innerHTML = SHARED_UTILS.buildCardDetailExplorerPanelHtml(card, set, {
            chartIdMode: 'dynamic',
            hidePredictorLink: true,
        });
        if (chartsArea) chartsArea.innerHTML = '';
        if (chartsEmpty) chartsEmpty.hidden = true;
        if (detailsSplit) detailsSplit.classList.add('prediction-details--full-card-data');

        requestAnimationFrame(() => {
            SHARED_UTILS.initCardDetailCharts(card, predictorCharts);
        });

        syncPredictorCardUrl(card, set);
    }

    function renderScorecard(card, set, pred, actual, feat, cal) {
        document.getElementById('val-predicted').innerText = SHARED_UTILS.fmtUsd(pred);
        const noteEl = document.getElementById('val-predicted-note');
        if (noteEl) {
            if (cal && cal.blended && cal.raw != null && Math.abs(cal.raw - pred) / Math.max(pred, 1) > 0.08) {
                noteEl.textContent = `Composite-only estimate was ${SHARED_UTILS.fmtUsd(cal.raw)} · blended toward PriceCharting sold comps (~${SHARED_UTILS.fmtUsd(cal.anchor)}).`;
                noteEl.hidden = false;
            } else {
                noteEl.textContent = '';
                noteEl.hidden = true;
            }
        }
        document.getElementById('val-actual').innerText = SHARED_UTILS.fmtUsd(actual);

        const disc = actual && pred ? (actual - pred) / pred : 0;
        const discText = (disc * 100).toFixed(1) + '%';
        const discEl = document.getElementById('val-discrepancy');
        discEl.innerText = (disc >= 0 ? '+' : '') + discText;
        discEl.style.color = disc > 0.3 ? '#f87171' : (disc < -0.3 ? '#10b981' : '#e2e8f0');

        // Pointer
        const pointer = document.getElementById('discrepancy-pointer');
        const pct = Math.min(100, Math.max(0, 50 + (disc * 50)));
        pointer.style.left = `${pct}%`;
    }

    function renderCardHero(card, set) {
        window._currentPredictorCard = card;
        window._currentPredictorSet = set;
        const container = document.getElementById('card-hero-container');
        container.innerHTML = `
            <img src="${card.image_url}" alt="${card.name}" class="predictor-hero-img">
            <div class="predictor-hero-meta">
                <h3 class="predictor-hero-meta__name">${SHARED_UTILS.escHtml(card.name)}</h3>
                <p class="predictor-hero-meta__set">${SHARED_UTILS.escHtml(set.set_name)} · #${card.number}</p>
                <div class="card-detail-tags predictor-hero-meta__tags">
                    <span class="card-detail-tag predictor-hero-meta__tag">${card.rarity || 'Common'}</span>
                    <span class="card-detail-tag predictor-hero-meta__tag">${card.supertype || 'Pokémon'}</span>
                </div>
            </div>
        `;
        
        const simC = document.getElementById('similar-cards-container');
        const simL = document.getElementById('similar-cards-list');
        if (simC && simL) {
            // Extract base species name (first word, stripping variant suffixes)
            const baseName = card.name.replace(/\s+(VMAX|VSTAR|V|ex|EX|GX|LV\.X|MEGA|BREAK|δ)\b.*$/i, '').split(' ')[0];
            
            if (baseName && baseName.length >= 3) {
                const matching = globalSearchIndex.filter(e => {
                    if (e.card === card) return false;
                    const price = SHARED_UTILS.resolveExplorerChartUsd(e.card);
                    if (!price || price <= 0) return false;
                    const eName = e.card.name.replace(/\s+(VMAX|VSTAR|V|ex|EX|GX|LV\.X|MEGA|BREAK|δ)\b.*$/i, '').split(' ')[0];
                    return eName === baseName;
                });
                matching.sort((a, b) => SHARED_UTILS.resolveExplorerChartUsd(b.card) - SHARED_UTILS.resolveExplorerChartUsd(a.card));
                const topSims = matching.slice(0, 12);
                
                if (topSims.length > 0) {
                    simL.innerHTML = topSims.map(e => {
                        const price = SHARED_UTILS.resolveExplorerChartUsd(e.card);
                        const trend = e.card.pokemon_wizard_current_trend_pct;
                        const pcTrend = SHARED_UTILS.pricechartingUsedShortTrendPct(e.card);
                        const trendNum = trend != null && Number.isFinite(Number(trend))
                            ? Number(trend)
                            : (pcTrend != null && Number.isFinite(Number(pcTrend)) ? Number(pcTrend) : null);
                        const trendStr = trendNum != null
                            ? `<span class="similar-card__trend" style="color:${trendNum >= 0 ? 'var(--success)' : 'var(--danger)'};">${trendNum >= 0 ? '+' : ''}${trendNum.toFixed(1)}%</span>`
                            : '';
                        return `
                        <a href="javascript:void(0)" class="similar-card-item" onclick="window.selectCard('${e.set.set_code}', '${e.card.number}', '${SHARED_UTILS.escAttr(e.card.name)}')">
                            <div class="similar-card__info">
                                <div class="similar-card__name">${SHARED_UTILS.escHtml(e.card.name)} #${e.card.number}</div>
                                <div class="similar-card__set">${SHARED_UTILS.escHtml(e.set.set_name)} · ${e.card.rarity || '—'}</div>
                                <div class="similar-card__price-row">
                                    <span class="similar-card__price">${SHARED_UTILS.fmtUsd(price)}</span>
                                    ${trendStr}
                                </div>
                            </div>
                            <img src="${e.card.image_url}" alt="" class="similar-card__thumb" loading="lazy" />
                        </a>`;
                    }).join('');
                    simC.style.display = 'block';
                } else {
                    simC.style.display = 'none';
                }
            } else {
                simC.style.display = 'none';
            }
        }
    }

    window.addEventListener('ptcg-theme-changed', () => {
        const pv = document.getElementById('prediction-view');
        if (!pv || pv.style.display === 'none' || !window._currentPredictorCard) return;
        destroyPredictorCharts();
        requestAnimationFrame(() => {
            SHARED_UTILS.initCardDetailCharts(window._currentPredictorCard, predictorCharts);
        });
    });

    document.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-ptcg-history-months]');
        if (!btn || !btn.closest('#prediction-stats-grid')) return;
        const m = Number(btn.getAttribute('data-ptcg-history-months'));
        if (![1, 3, 6, 12].includes(m)) return;
        try {
            sessionStorage.setItem('ptcg-unified-history-months', String(m));
        } catch (err) { /* ignore */ }
        const pv = document.getElementById('prediction-view');
        if (!pv || pv.style.display === 'none' || !window._currentPredictorCard) return;
        destroyPredictorCharts();
        requestAnimationFrame(() => {
            SHARED_UTILS.initCardDetailCharts(window._currentPredictorCard, predictorCharts, { historyWindowMonths: m });
        });
    });

    function renderReasoning(card, pred, actual, feat, cal) {
        const reasoningEl = document.getElementById('prediction-reasoning');
        const disc = (actual - pred) / pred;
        const bits = [];

        if (cal && cal.blended) {
            bits.push(
                'The headline price blends the composite LSRL estimate with PriceCharting sold comps (and chase-tier slab rows when scrape/sync_pricecharting.py merged pricecharting_grade_prices) so extreme collector SKUs are not collapsed to booster-math alone.',
            );
        }

        if (disc > 0.4) {
            bits.push('Versus that headline, actual market still looks materially higher—often alt-art iconicity, grade liquidity, or a different venue than the median blend.');
        } else if (disc < -0.4) {
            bits.push('Versus that headline, actual market looks lower—thin listings, stale merge data, or a temporary dip vs sold history.');
        } else if (feat.setAge > 4 && disc > 0.2) {
            bits.push('Older sets can add a sealed-scarcity premium over pull odds alone.');
        } else if (feat.artistChase > 50 && disc > 0.2) {
            bits.push('Strong illustrator chase signal can sit on top of pull math.');
        } else if (!bits.length) {
            bits.push('Trading roughly in line with the headline estimate for this blend of drivers and comps.');
        }

        reasoningEl.innerText = bits.join(' ');
    }

    initPredictor();
});
