document.addEventListener('DOMContentLoaded', () => {
    const loadingEl = document.getElementById('loading');
    const containerEl = document.getElementById('sets-container');
    const explorerLayoutEl = document.getElementById('explorer-layout');
    const trendingRoot = document.getElementById('explorer-trending-root');
    const searchInput = document.getElementById('searchInput');
    const seriesFilter = document.getElementById('seriesFilter');

    const modalEl = document.getElementById('imageModal');
    const modalImage = document.getElementById('modalImage');
    const modalCardPanel = document.getElementById('modalCardPanel');
    const closeModal = document.getElementById('closeModal');

    let allSets = [];
    let explorerListCursor = 0;
    let explorerScrollObserver = null;
    let explorerSearchDebounce = null;
    const EXPLORER_INITIAL_SLICE = 12;
    const EXPLORER_SCROLL_SLICE = 10;
    const EXPLORER_SEARCH_PREFETCH = 16;

    function mergeExplorerCardSliceInPlace(catalogSets, slice, slim) {
        const map = new Map((slice || []).map((s) => [String(s.set_code || '').trim().toLowerCase(), s]));
        catalogSets.forEach((set) => {
            const k = String(set.set_code || '').trim().toLowerCase();
            const hit = map.get(k);
            if (!hit || !Array.isArray(hit.top_25_cards)) return;
            set.top_25_cards = hit.top_25_cards;
            if (slim) {
                set._explorerSlim = true;
                set._explorerMetricsLoaded = false;
            } else {
                set._explorerSlim = false;
                set._explorerMetricsLoaded = true;
            }
        });
    }

    async function maybeHydrateExplorerFullMetrics(set) {
        if (!set || set._explorerHydrating) return;
        if (set._explorerMetricsLoaded) return;
        if (typeof fetchPokemonSetWithMetricsByCode !== 'function') return;
        const need =
            set._explorerSlim === true || !Array.isArray(set.top_25_cards) || set.top_25_cards.length === 0;
        if (!need) return;
        set._explorerHydrating = true;
        try {
            const full = await fetchPokemonSetWithMetricsByCode(set.set_code);
            if (!full || !Array.isArray(full.top_25_cards)) return;
            set.top_25_cards = full.top_25_cards;
            set._explorerSlim = false;
            set._explorerMetricsLoaded = true;
        } catch (e) {
            console.warn('Explorer full set fetch', e);
        } finally {
            set._explorerHydrating = false;
        }
    }

    function computeMainCardsCountFromSet(set) {
        let mainCardsCount = 0;
        const secretRarities = [
            'mega hyper rare',
            'special illustration rare',
            'ultra rare',
            'illustration rare',
        ];
        if (set.rarity_counts) {
            Object.entries(set.rarity_counts).forEach(([rarity, count]) => {
                if (!secretRarities.includes(rarity.toLowerCase())) {
                    mainCardsCount += count;
                }
            });
        }
        return mainCardsCount;
    }

    function buildTopCardsListHtml(set) {
        const setCodeAttr = escAttr(String(set.set_code || ''));
        const mainCardsCount = computeMainCardsCountFromSet(set);
        if (set._explorerHydrating) {
            return '<li class="card-row"><span style="color:#94a3b8;">Loading card rows…</span></li>';
        }
        if (!set.top_25_cards || set.top_25_cards.length === 0) {
            return '<li class="card-row"><span style="color:#64748b;font-size:0.85rem;">Open this set to load full pricing fields…</span></li>';
        }
        return set.top_25_cards
            .map((card) => {
                const crate = (card.card_pull_rate || 'N/A').replace('1 in ', '1/').split(' (')[0];
                const rrate = (set.rarity_pull_rates && set.rarity_pull_rates[card.rarity]
                    ? set.rarity_pull_rates[card.rarity]
                    : 'N/A').replace('1 in ', '1/').split(' (')[0];
                const cn = escHtml(String(card.name || ''));
                const numDisp = escHtml(String(card.number != null ? card.number : '?'));
                const numAttr = escAttr(String(card.number != null ? card.number : ''));
                const nameAttr = escAttr(String(card.name || ''));
                const liqHint = formatExplorerEbayLiquiditySuffix(card);
                const imgUrl = card.image_url ? escAttr(String(card.image_url)) : '';
                const thumbInner = imgUrl
                    ? `<img src="${imgUrl}" alt="" loading="lazy">`
                    : '<span style="font-size:0.55rem;color:#64748b;padding:2px;">—</span>';
                const hasPx = explorerCardHasPositiveMarketUsd(card);
                const dispUsd = explorerPrimaryDisplayUsd(card);
                const artistTop = hasPx
                    ? `<div>Artist: ${escHtml(String(card.artist || 'Unknown'))}</div>`
                    : '<div class="card-pricing-unavailable">No market pricing data</div>';
                return `
                    <li class="card-row">
                        <div class="card-row-thumb">${thumbInner}</div>
                        <div class="card-row-main">
                            <div class="card-name">${cn}</div>
                            <div class="card-rarity">${escHtml(String(card.rarity || 'Unknown'))} - #${numDisp}/${mainCardsCount}/${set.total_cards || '?'}</div>
                            <div class="card-artist" style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 2px;">
                                ${artistTop}
                                <div style="margin-top:2px;">Card: ${escHtml(crate)} &nbsp;&nbsp; Rarity: ${escHtml(rrate)}</div>
                            </div>
                        </div>
                        <div class="card-row-actions" style="min-width:110px; max-width:140px; text-align:right;">
                            <div class="card-price" style="font-size:0.95rem;">${hasPx && dispUsd != null ? `$${dispUsd.toFixed(2)}` : '<span class="card-pricing-unavailable">—</span>'}</div>
                            ${Number(card.collectrics_ebay_listings) > 0 ? `<div style="font-size:0.68rem; color:#94a3b8;">Listings: ${Number(card.collectrics_ebay_listings).toLocaleString()}</div>` : ''}
                            ${Number(card.collectrics_ebay_sold_volume) > 0 ? `<div style="font-size:0.68rem; color:#94a3b8;">Volume: ${Number(card.collectrics_ebay_sold_volume).toLocaleString()}</div>` : ''}
                            <button type="button" class="show-art-btn card-detail-btn" data-set-code="${setCodeAttr}" data-card-number="${numAttr}" data-card-name="${nameAttr}" style="margin-top:4px;">Details</button>
                        </div>
                    </li>
                    `;
            })
            .join('');
    }

    function wireExplorerInfiniteScroll() {
        const sen = document.getElementById('explorer-load-sentinel');
        if (!sen || explorerScrollObserver) return;
        let busy = false;
        explorerScrollObserver = new IntersectionObserver(
            async (entries) => {
                if (!entries[0] || !entries[0].isIntersecting || busy) return;
                if (explorerListCursor >= allSets.length) {
                    sen.hidden = true;
                    sen.textContent = '';
                    return;
                }
                if (typeof fetchPokemonSetsCardSliceByCodes !== 'function') return;
                busy = true;
                sen.hidden = false;
                sen.textContent = 'Loading more sets…';
                const next = allSets
                    .slice(explorerListCursor, explorerListCursor + EXPLORER_SCROLL_SLICE)
                    .map((s) => s.set_code)
                    .filter(Boolean);
                if (!next.length) {
                    explorerListCursor = allSets.length;
                    sen.hidden = true;
                    sen.textContent = '';
                    busy = false;
                    return;
                }
                try {
                    const sl = await fetchPokemonSetsCardSliceByCodes(next, { withMetrics: false });
                    mergeExplorerCardSliceInPlace(allSets, sl, true);
                    explorerListCursor += next.length;
                    const y = window.scrollY;
                    runFilters();
                    window.scrollTo(0, y);
                } catch (e) {
                    console.warn('Explorer scroll batch', e);
                } finally {
                    busy = false;
                    sen.textContent = '';
                    sen.hidden = true;
                    if (explorerListCursor >= allSets.length) {
                        sen.textContent = '';
                    }
                }
            },
            { root: null, rootMargin: '280px', threshold: 0 },
        );
        explorerScrollObserver.observe(sen);
    }

    function escHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    function escAttr(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;');
    }

    function fmtUsd(x) {
        const v = Number(x);
        if (!Number.isFinite(v)) return '—';
        return `$${v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    }

    const explorerCardCharts = [];

    function destroyCardDetailCharts() {
        while (explorerCardCharts.length) {
            const ch = explorerCardCharts.pop();
            try {
                if (ch && typeof ch.destroy === 'function') ch.destroy();
            } catch (e) { /* ignore */ }
        }
    }

    function initCardDetailCharts(card, chartOpts) {
        if (typeof SHARED_UTILS !== 'undefined' && SHARED_UTILS.hydrateCardPipelineFields) {
            SHARED_UTILS.hydrateCardPipelineFields(card);
        }
        if (typeof Chart === 'undefined') return;
        const co = chartOpts && typeof chartOpts === 'object' ? chartOpts : {};
        let sessionMo = NaN;
        try {
            sessionMo = parseInt(sessionStorage.getItem('ptcg-unified-history-months'), 10);
        } catch (e) {
            sessionMo = NaN;
        }
        const { histMo } = SHARED_UTILS.resolveUnifiedHistoryWindowMonths(
            card,
            co.historyWindowMonths,
            sessionMo,
        );
        const unifiedPackOpts = histMo != null && Number.isFinite(histMo) ? { historyWindowMonths: histMo } : {};

        const { tickColor, gridColor } = SHARED_UTILS.getChartAxisColors();
        const baseScale = {
            ticks: { color: tickColor },
            grid: { color: gridColor },
        };

        const ceBlend = Array.isArray(card.collectrics_price_history) ? card.collectrics_price_history : [];
        const ceJust = Array.isArray(card.collectrics_history_justtcg) ? card.collectrics_history_justtcg : [];
        const ceEbayHist = Array.isArray(card.collectrics_history_ebay) ? card.collectrics_history_ebay : [];
        const collectricsSplit = ceJust.length > 1 && ceEbayHist.length > 1;
        const collectricsJustOnly = ceJust.length > 1 && !collectricsSplit;
        const collectricsEbayHistOnly = ceEbayHist.length > 1 && !collectricsSplit;
        const collectricsBlendOnly = ceBlend.length > 1 && !collectricsSplit && !collectricsJustOnly && !collectricsEbayHistOnly;
        const unifiedMarketPack = SHARED_UTILS.buildExplorerUnifiedPriceChartPack(card, unifiedPackOpts);
        const canvasUnifiedExplorer = document.getElementById('explorerChartCeUnified');

        if (unifiedMarketPack && canvasUnifiedExplorer) {
            const prevEx = typeof Chart !== 'undefined' && typeof Chart.getChart === 'function' ? Chart.getChart(canvasUnifiedExplorer) : null;
            if (prevEx) {
                try {
                    prevEx.destroy();
                } catch (e) { /* ignore */ }
            }
            const scalesUnified = {
                x: { ...baseScale, ticks: { ...baseScale.ticks, maxRotation: 45, autoSkip: true } },
                y: {
                    ...baseScale,
                    grace: '22%',
                    title: { display: true, text: 'USD', color: tickColor },
                },
            };
            if (unifiedMarketPack.hasY1Axis) {
                scalesUnified.y1 = {
                    position: 'right',
                    beginAtZero: true,
                    grid: { drawOnChartArea: false },
                    title: { display: true, text: 'Sold listings / day', color: tickColor },
                    ticks: { color: tickColor, precision: 0 },
                };
            }
            const legendCompact = {
                position: 'bottom',
                align: 'center',
                labels: {
                    color: tickColor,
                    usePointStyle: true,
                    pointStyle: 'circle',
                    boxWidth: 5,
                    boxHeight: 5,
                    padding: 5,
                    font: { size: 9, family: 'system-ui, -apple-system, sans-serif' },
                },
            };
            explorerCardCharts.push(new Chart(canvasUnifiedExplorer, {
                type: 'line',
                data: { labels: unifiedMarketPack.labels, datasets: unifiedMarketPack.datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    layout: { padding: { bottom: 2 } },
                    plugins: {
                        title: { display: false },
                        legend: legendCompact,
                        tooltip: { mode: 'index', intersect: false },
                    },
                    scales: scalesUnified,
                },
            }));
        } else if (collectricsBlendOnly) {
            const canvas = document.getElementById('explorerChartCeRaw');
            if (canvas) {
                const sorted = ceBlend.slice().sort((a, b) => String(a.date || '').localeCompare(String(b.date || '')));
                const labels = sorted.map((r) => String(r.date || ''));
                const raw = sorted.map((r) => (r.raw_price != null && Number.isFinite(Number(r.raw_price)) ? Number(r.raw_price) : null));
                const vol = sorted.map((r) => (r.sales_volume != null && Number.isFinite(Number(r.sales_volume)) ? Number(r.sales_volume) : null));
                const hasVol = vol.some((v) => v != null);
                const datasets = [
                    {
                        type: 'line',
                        label: 'Blend',
                        data: raw,
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59,130,246,0.12)',
                        tension: 0.2,
                        spanGaps: true,
                        yAxisID: 'y',
                    },
                ];
                if (hasVol) {
                    datasets.push({
                        type: 'bar',
                        label: 'Volume',
                        data: vol,
                        backgroundColor: 'rgba(16,185,129,0.35)',
                        borderColor: 'rgba(16,185,129,0.85)',
                        borderWidth: 1,
                        yAxisID: 'y1',
                    });
                }
                const scales = {
                    x: { ...baseScale, ticks: { ...baseScale.ticks, maxRotation: 40, autoSkip: true } },
                    y: {
                        ...baseScale,
                        position: 'left',
                        title: { display: true, text: 'USD', color: tickColor },
                    },
                };
                if (hasVol) {
                    scales.y1 = {
                        position: 'right',
                        ticks: { color: '#6ee7b7' },
                        grid: { drawOnChartArea: false },
                        title: { display: true, text: 'Volume', color: '#6ee7b7' },
                    };
                }
                explorerCardCharts.push(new Chart(canvas, {
                    type: 'line',
                    data: { labels, datasets },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { labels: { color: tickColor } } },
                        scales,
                    },
                }));
            }
        }

        const mktRowsExplorer = SHARED_UTILS.collectricsEbayMarketHistorySorted(card);
        if (mktRowsExplorer.length > 1) {
            SHARED_UTILS.mountCollectricsEbayMarketVolumeChart(
                document.getElementById('explorerChartCeEbayMarketVol'),
                mktRowsExplorer,
                tickColor,
                baseScale,
                explorerCardCharts,
            );
        }

        if (!(unifiedMarketPack && unifiedMarketPack.skipStandaloneWizard)) {
            SHARED_UTILS.mountPokemonWizardRetailTrendChart(
                document.getElementById('explorerChartWizPrice'),
                card,
                explorerCardCharts,
            );
        }

        SHARED_UTILS.syncUnifiedHistoryRangeUi(modalCardPanel, histMo);

        if (typeof SHARED_UTILS.mountTcggoEbaySoldGradesBarChart === 'function') {
            SHARED_UTILS.mountTcggoEbaySoldGradesBarChart(
                document.getElementById('explorerChartTcggoEbaySold'),
                card,
                tickColor,
                baseScale,
                explorerCardCharts,
            );
        }
    }

    function hasCollectricsEbay(card) {
        if (!card) return false;
        const a = Number(card.collectrics_ebay_listings);
        const b = Number(card.collectrics_ebay_sold_volume);
        return (Number.isFinite(a) && a > 0) || (Number.isFinite(b) && b > 0);
    }

    function medianSorted(sorted) {
        if (!sorted.length) return null;
        const m = Math.floor(sorted.length / 2);
        return sorted.length % 2 ? sorted[m] : (sorted[m - 1] + sorted[m]) / 2;
    }

    function medianArray(arr) {
        if (!arr.length) return null;
        const s = arr.slice().sort((a, b) => a - b);
        return medianSorted(s);
    }

    function priceDedupForMedian(values, relEps) {
        const eps = relEps != null ? relEps : 0.006;
        const ok = values.filter((v) => Number.isFinite(v) && v > 0).sort((a, b) => a - b);
        const out = [];
        for (const v of ok) {
            if (!out.some((u) => Math.abs(u - v) <= eps * Math.max(u, v, 1))) out.push(v);
        }
        return out;
    }

    function wizardHistoryPositiveUsdMedian(card) {
        const hist = card && card.pokemon_wizard_price_history;
        if (!Array.isArray(hist) || hist.length === 0) return null;
        const vals = [];
        for (let i = 0; i < hist.length; i++) {
            const n = Number(hist[i] && hist[i].price_usd);
            if (Number.isFinite(n) && n > 0) vals.push(n);
        }
        return vals.length ? medianArray(vals) : null;
    }

    function cardExplorerLiquidityCount(card) {
        if (!card) return null;
        const li = Number(card.collectrics_ebay_listings);
        if (Number.isFinite(li) && li > 0) return li;
        const sv = Number(card.collectrics_ebay_sold_volume);
        if (Number.isFinite(sv) && sv > 0) return sv;
        const tcg = Number(card.tcgtracking_listings_nm_en);
        if (Number.isFinite(tcg) && tcg > 0) return tcg;
        return null;
    }

    function collectDedupedPositiveUsdPrices(card) {
        if (!card) return [];
        const vals = [];
        const push = (v) => {
            const n = Number(v);
            if (Number.isFinite(n) && n > 0) vals.push(n);
        };
        push(card.market_price);
        push(card.pricedex_market_usd);
        push(card.tcgtracking_market_usd);
        push(card.tcgapi_market_usd);
        push(card.pokemon_wizard_current_price_usd);
        const wh = wizardHistoryPositiveUsdMedian(card);
        if (wh != null) push(wh);
        push(card.pricecharting_used_price_usd);
        push(card.pricecharting_graded_price_usd);
        const pch = SHARED_UTILS.pricechartingHistoryPositiveUsdMedian(card);
        if (pch != null) push(pch);
        return priceDedupForMedian(vals);
    }

    /** Same median blend as the Analytics tab (list / Dex / Track / tcgapi / Wizard / Wizard history). */
    function resolveExplorerChartUsd(card) {
        if (!card) return null;
        const dedup = collectDedupedPositiveUsdPrices(card);
        if (dedup.length >= 2) return medianArray(dedup);
        if (dedup.length === 1) {
            const m = dedup[0];
            const wc = Number(card.pokemon_wizard_current_price_usd);
            if (Number.isFinite(wc) && wc > 0 && wc < m * 0.48) {
                const liq = cardExplorerLiquidityCount(card);
                const thin = liq == null || !Number.isFinite(liq) || liq <= 15;
                if (thin) {
                    const hm = wizardHistoryPositiveUsdMedian(card);
                    const pool = [m, wc];
                    if (hm != null && Number.isFinite(hm)) pool.push(hm);
                    const md = medianArray(priceDedupForMedian(pool));
                    return md != null ? md : m;
                }
            }
            return m;
        }
        const mp = Number(card.market_price);
        return Number.isFinite(mp) && mp > 0 ? mp : null;
    }

    /** Inline suffix after price on the set list (Collectrics preferred over capped TCG counts). */
    function formatExplorerEbayLiquiditySuffix(card) {
        if (!card) return '';
        const cl = Number(card.collectrics_ebay_listings);
        const sv = Number(card.collectrics_ebay_sold_volume);
        const bits = [];
        if (Number.isFinite(cl) && cl > 0) {
            bits.push(`eBay listings <span style="color:#94a3b8;">${cl.toLocaleString()}</span>`);
        }
        if (Number.isFinite(sv) && sv > 0) {
            bits.push(`eBay volume <span style="color:#94a3b8;">${sv.toLocaleString()}</span>`);
        }
        if (bits.length) {
            return ` <span style="color:#64748b;">·</span> ${bits.join(' <span style="color:#64748b;">·</span> ')}`;
        }
        const tcg = Number(card.tcgtracking_listings_nm_en);
        if (Number.isFinite(tcg) && tcg > 0) {
            const cap = tcg === 25 ? ' <span style="color:#64748b;">(TCG NM EN cap)</span>' : '';
            return ` <span style="color:#64748b;">·</span> <span style="color:#94a3b8;">${tcg}</span> NM EN${cap}`;
        }
        return '';
    }

    /** True if any merged list / Dex / Track / tcgapi / Wizard field has a positive USD value (Explorer + deferred-set heuristic). */
    function explorerCardHasPositiveMarketUsd(card) {
        if (!card) return false;
        const candidates = [
            card.market_price,
            card.pricedex_market_usd,
            card.tcgtracking_market_usd,
            card.tcgapi_market_usd,
            card.pokemon_wizard_current_price_usd,
            card.pricecharting_used_price_usd,
            card.pricecharting_graded_price_usd,
        ];
        if (candidates.some((v) => {
            const n = Number(v);
            return Number.isFinite(n) && n > 0;
        })) return true;
        if (wizardHistoryPositiveUsdMedian(card) != null) return true;
        return SHARED_UTILS.pricechartingHistoryPositiveUsdMedian(card) != null;
    }

    /** Display price for the row (median blend when several anchors disagree); null if none. */
    function explorerPrimaryDisplayUsd(card) {
        return resolveExplorerChartUsd(card);
    }

    /** ≥ half of top-list cards lack any positive merged market USD — group at bottom of Explorer. */
    function setHasPredominantlyMissingExplorerPricing(set) {
        const cards = set && set.top_25_cards;
        if (!Array.isArray(cards) || cards.length === 0) return false;
        let missing = 0;
        cards.forEach((c) => {
            if (!explorerCardHasPositiveMarketUsd(c)) missing += 1;
        });
        return missing / cards.length >= 0.5;
    }

    function getCardGradedPopTotal(card) {
        if (!card) return null;
        if (card.gemrate && card.gemrate.total != null) return card.gemrate.total;
        const psa = Number(card.psa_graded_pop_total);
        if (Number.isFinite(psa) && psa >= 0) return psa;
        return null;
    }

    function findSetAndCard(setCode, cardNumber, cardName) {
        const code = String(setCode || '').trim();
        const num = String(cardNumber || '').trim();
        const nm = String(cardName || '').trim();
        const set = allSets.find((s) => String(s.set_code || '').trim() === code);
        if (!set || !Array.isArray(set.top_25_cards)) return { set: null, card: null };
        const exact = set.top_25_cards.find((c) =>
            String(c.number || '').trim() === num && String(c.name || '').trim() === nm);
        if (exact) return { set, card: exact };
        const byNum = set.top_25_cards.find((c) => String(c.number || '').trim() === num);
        return { set, card: byNum || null };
    }

    function buildCardDetailPanelHtml(card, set) {
        return SHARED_UTILS.buildCardDetailExplorerPanelHtml(card, set);
    }

    function syncExplorerCardDetailUrl(set, card) {
        const u = new URL(window.location.href);
        u.searchParams.set('detail', '1');
        u.searchParams.set('set', set.set_code);
        u.searchParams.set('num', String(card.number));
        u.searchParams.set('name', card.name || '');
        const qs = u.searchParams.toString();
        window.history.replaceState({}, '', `${u.pathname}${qs ? `?${qs}` : ''}${u.hash}`);
    }

    function clearExplorerCardDetailUrl() {
        const u = new URL(window.location.href);
        ['detail', 'set', 'num', 'name'].forEach((k) => u.searchParams.delete(k));
        const qs = u.searchParams.toString();
        window.history.replaceState({}, '', `${u.pathname}${qs ? `?${qs}` : ''}${u.hash}`);
    }

    async function openCardDetailModal(setCode, cardNumber, cardName) {
        let { set, card } = findSetAndCard(setCode, cardNumber, cardName);
        if (!set && typeof fetchPokemonSetWithMetricsByCode === 'function') {
            try {
                const fetched = await fetchPokemonSetWithMetricsByCode(setCode);
                if (fetched) {
                    const lc = String(setCode || '').trim().toLowerCase();
                    const idx = allSets.findIndex((s) => String(s.set_code || '').trim().toLowerCase() === lc);
                    if (idx >= 0) {
                        allSets[idx] = { ...allSets[idx], ...fetched };
                        set = allSets[idx];
                    } else {
                        set = fetched;
                    }
                }
            } catch (e) {
                console.warn('Explorer fetch set for detail', e);
            }
        }
        if (set && !card && Array.isArray(set.top_25_cards)) {
            const num = String(cardNumber || '').trim();
            const nm = String(cardName || '').trim();
            card = set.top_25_cards.find((c) =>
                String(c.number || '').trim() === num && String(c.name || '').trim() === nm)
                || set.top_25_cards.find((c) => String(c.number || '').trim() === num) || null;
        }
        if (set && (!card || !Array.isArray(set.top_25_cards) || set.top_25_cards.length === 0)) {
            await maybeHydrateExplorerFullMetrics(set);
            ({ card } = findSetAndCard(setCode, cardNumber, cardName));
        }
        if (!card || !set) return;
        destroyCardDetailCharts();
        const src = card.image_url || '';
        modalImage.src = src;
        modalImage.alt = card.name ? `${card.name} · ${set.set_name || ''}` : 'Card';
        
        // Show loading state while fetching history
        modalCardPanel.innerHTML = '<div style="padding: 2rem; text-align: center; color: #94a3b8; font-size: 0.9rem;">Fetching live market charts...</div>';
        modalEl.style.display = 'flex';
        
        if (card.unique_card_id && typeof fetchCardLiveRowFromSupabase === 'function') {
            try {
                const row = await fetchCardLiveRowFromSupabase(card.unique_card_id);
                if (row && typeof mergeLivePokemonCardRow === 'function') {
                    mergeLivePokemonCardRow(card, row);
                }
            } catch (e) {
                console.error('Failed to fetch Supabase live card row:', e);
            }
        }
        
        modalCardPanel.innerHTML = buildCardDetailPanelHtml(card, set);
        window._ptcgExplorerDetailCard = card;
        syncExplorerCardDetailUrl(set, card);
        requestAnimationFrame(() => initCardDetailCharts(card));
    }

    function tryOpenCardDetailFromQuery() {
        if (!Array.isArray(allSets) || allSets.length === 0) return;
        const u = new URL(window.location.href);
        if (u.searchParams.get('detail') !== '1') return;
        const sc = u.searchParams.get('set') || '';
        const num = u.searchParams.get('num') || '';
        const name = u.searchParams.get('name') || '';
        openCardDetailModal(sc, num, name);
    }

    function closeCardDetailModal() {
        destroyCardDetailCharts();
        modalEl.style.display = 'none';
        modalImage.src = '';
        modalCardPanel.innerHTML = '';
        window._ptcgExplorerDetailCard = null;
        clearExplorerCardDetailUrl();
    }

    window.addEventListener('ptcg-theme-changed', () => {
        if (modalEl.style.display === 'flex' && window._ptcgExplorerDetailCard) {
            destroyCardDetailCharts();
            requestAnimationFrame(() => initCardDetailCharts(window._ptcgExplorerDetailCard));
        }
    });

    modalCardPanel.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-ptcg-history-months]');
        if (!btn) return;
        const m = Number(btn.getAttribute('data-ptcg-history-months'));
        if (![1, 3, 6, 12].includes(m)) return;
        const avail = window._ptcgExplorerDetailCard
            ? SHARED_UTILS.getUnifiedPriceHistoryAvailableWindowMonths(window._ptcgExplorerDetailCard)
            : [];
        if (avail.length && !avail.includes(m)) return;
        try {
            sessionStorage.setItem('ptcg-unified-history-months', String(m));
        } catch (err) { /* ignore */ }
        if (window._ptcgExplorerDetailCard) {
            destroyCardDetailCharts();
            requestAnimationFrame(() => initCardDetailCharts(window._ptcgExplorerDetailCard, { historyWindowMonths: m }));
        }
    });

    closeModal.addEventListener('click', () => { closeCardDetailModal(); });
    window.addEventListener('click', (e) => {
        if (e.target === modalEl) closeCardDetailModal();
    });
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && modalEl.style.display === 'flex') closeCardDetailModal();
    });

    document.addEventListener('click', (e) => {
        const btn = e.target.closest('.card-detail-btn');
        if (!btn) return;
        e.stopPropagation();
        openCardDetailModal(btn.dataset.setCode, btn.dataset.cardNumber, btn.dataset.cardName || '');
    });

    document.addEventListener('click', (e) => {
        const setRow = e.target.closest('.explorer-trending-root .explorer-trend-row--set');
        if (setRow) {
            e.preventDefault();
            e.stopPropagation();
            scrollExplorerToSetFromTrending(setRow.getAttribute('data-set-code') || '');
            return;
        }
        const row = e.target.closest('.explorer-trending-root .explorer-trend-row:not(.explorer-trend-row--set)');
        if (!row) return;
        const sc = row.getAttribute('data-set-code') || '';
        const num = row.getAttribute('data-card-number') || '';
        const nm = row.getAttribute('data-card-name') || '';
        openCardDetailModal(sc, num, nm);
    });

    async function scrollExplorerToSetFromTrending(setCode) {
        const lc = String(setCode || '').trim().toLowerCase();
        if (!lc || !containerEl) return;
        const el = containerEl.querySelector(`.set-item[data-set-code="${lc}"]`);
        if (!el) return;
        const set = allSets.find((s) => String(s.set_code || '').trim().toLowerCase() === lc);
        document.querySelectorAll('#sets-container .set-item').forEach((x) => x.classList.remove('expanded'));
        el.classList.add('expanded');
        if (set) {
            await maybeHydrateExplorerFullMetrics(set);
            const ul = el.querySelector('.top-cards-list');
            if (ul) ul.innerHTML = buildTopCardsListHtml(set);
        }
        el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    function renderExplorerTrendingPanel(payload) {
        if (!trendingRoot) return;
        const fmt = (x) => (typeof SHARED_UTILS !== 'undefined' && SHARED_UTILS.fmtUsd
            ? SHARED_UTILS.fmtUsd(x)
            : (Number.isFinite(Number(x)) ? `$${Number(x).toFixed(2)}` : '—'));
        const rowBtn = (r, statHtml, statClass) => {
            const sc = escAttr(String(r.set_code || ''));
            const num = escAttr(String(r.number != null ? r.number : ''));
            const nm = escAttr(String(r.name || ''));
            const img = r.image_url ? escAttr(String(r.image_url)) : '';
            const thumb = img
                ? `<div class="explorer-trend-row__thumb"><img src="${img}" alt="" loading="lazy"></div>`
                : '<div class="explorer-trend-row__thumb"></div>';
            return `<button type="button" class="explorer-trend-row" data-set-code="${sc}" data-card-number="${num}" data-card-name="${nm}">
                ${thumb}
                <div class="explorer-trend-row__main">
                    <div class="explorer-trend-row__name">${escHtml(String(r.name || '—'))}</div>
                    <div class="explorer-trend-row__meta">${escHtml(String(r.set_code || ''))} · #${escHtml(String(r.number != null ? r.number : '?'))}</div>
                </div>
                <div class="explorer-trend-row__stat ${statClass || ''}">${statHtml}</div>
            </button>`;
        };
        const trendDetails = (title, count, bodyHtml, { open = false } = {}) => {
            const oc = open ? ' open' : '';
            const badge = Number.isFinite(count) ? `<span class="explorer-trend-count">${count}</span>` : '';
            return `<details class="explorer-trend-details"${oc}><summary class="explorer-trend-summary"><span class="explorer-trend-summary__title">${escHtml(title)}</span>${badge}</summary><div class="explorer-trend-details__body">${bodyHtml}</div></details>`;
        };
        const cardBlockBody = (rows, kind) => {
            if (!rows.length) {
                return '<p class="explorer-trending-empty">No qualifying cards in the latest scan.</p>';
            }
            return `<div class="explorer-trend-list">${rows.map((r) => {
                if (kind === 'prior') {
                    const d = Number(r.delta_usd);
                    const cls = d >= 0 ? 'explorer-trend-row__stat--up' : 'explorer-trend-row__stat--down';
                    const pct = r.delta_pct != null ? ` (${d >= 0 ? '+' : ''}${Number(r.delta_pct).toFixed(1)}%)` : '';
                    return rowBtn(r, `${d >= 0 ? '+' : ''}${fmt(d)}${pct}`, cls);
                }
                if (kind === 'week') {
                    const p = Number(r.pct_change);
                    const cls = p >= 0 ? 'explorer-trend-row__stat--up' : 'explorer-trend-row__stat--down';
                    return rowBtn(r, `${p >= 0 ? '+' : ''}${p.toFixed(1)}%`, cls);
                }
                const ratio = Number(r.psa10_vs_raw_ratio);
                const pct = r.psa10_vs_raw_pct != null ? `+${Number(r.psa10_vs_raw_pct).toFixed(0)}%` : `×${ratio.toFixed(2)}`;
                return rowBtn(r, pct, 'explorer-trend-row__stat--up');
            }).join('')}</div>`;
        };
        if (!payload || typeof payload !== 'object') {
            trendingRoot.innerHTML = '<p class="explorer-trending-empty">Trending leaderboards are not available yet. Add the <code>explorer_trending_daily</code> asset (GitHub Action <code>Explorer trending daily</code>).</p>';
            return;
        }
        const prior = Array.isArray(payload.prior_day_dollar_movers) ? payload.prior_day_dollar_movers : [];
        const week = Array.isArray(payload.week_pct_movers) ? payload.week_pct_movers : [];
        const grade = Array.isArray(payload.psa10_vs_raw_leaders) ? payload.psa10_vs_raw_leaders : [];
        const chase = Array.isArray(payload.set_chase_by_tracked) ? payload.set_chase_by_tracked : [];
        const packMv = Array.isArray(payload.set_pack_trend_pct) ? payload.set_pack_trend_pct : [];
        const density = Array.isArray(payload.set_most_tracked) ? payload.set_most_tracked : [];
        const packDays = Number(payload.pack_trend_days) || 14;
        const when = payload.computed_at ? `<p class="explorer-trending-placeholder" style="margin:0 0 0.75rem;">Updated ${escHtml(String(payload.computed_at).slice(0, 16).replace('T', ' '))} UTC</p>` : '';

        const maxChase = chase.reduce((m, r) => Math.max(m, Number(r.chase_sum_usd) || 0), 0);
        const maxPackAbs = packMv.reduce((m, r) => Math.max(m, Math.abs(Number(r.pct_change) || 0)), 0);
        const maxDen = density.reduce((m, r) => Math.max(m, Number(r.n_tracked) || 0), 0);

        const setRowChase = (r) => {
            const sc = escAttr(String(r.set_code || ''));
            const v = Number(r.chase_sum_usd);
            const w = maxChase > 0 ? Math.round((v / maxChase) * 100) : 0;
            return `<button type="button" class="explorer-trend-row explorer-trend-row--set" data-set-code="${sc}">
                <div class="explorer-trend-row__main">
                    <div class="explorer-trend-row__name">${escHtml(String(r.set_name || r.set_code || '—'))}</div>
                    <div class="explorer-trend-mini-bar-track" aria-hidden="true"><div class="explorer-trend-mini-bar-fill" style="width:${w}%"></div></div>
                </div>
                <div class="explorer-trend-row__stat">${fmt(v)}</div>
            </button>`;
        };
        const setRowPack = (r) => {
            const sc = escAttr(String(r.set_code || ''));
            const p = Number(r.pct_change);
            const w = maxPackAbs > 0 ? Math.round((Math.abs(p) / maxPackAbs) * 100) : 0;
            const cls = p >= 0 ? 'explorer-trend-row__stat--up' : 'explorer-trend-row__stat--down';
            return `<button type="button" class="explorer-trend-row explorer-trend-row--set" data-set-code="${sc}">
                <div class="explorer-trend-row__main">
                    <div class="explorer-trend-row__name">${escHtml(String(r.set_name || r.set_code || '—'))}</div>
                    <div class="explorer-trend-mini-bar-track" aria-hidden="true"><div class="explorer-trend-mini-bar-fill explorer-trend-mini-bar-fill--accent" style="width:${w}%"></div></div>
                </div>
                <div class="explorer-trend-row__stat ${cls}">${p >= 0 ? '+' : ''}${p.toFixed(1)}%</div>
            </button>`;
        };
        const setRowDen = (r) => {
            const sc = escAttr(String(r.set_code || ''));
            const n = Number(r.n_tracked);
            const w = maxDen > 0 ? Math.round((n / maxDen) * 100) : 0;
            return `<button type="button" class="explorer-trend-row explorer-trend-row--set" data-set-code="${sc}">
                <div class="explorer-trend-row__main">
                    <div class="explorer-trend-row__name">${escHtml(String(r.set_name || r.set_code || '—'))}</div>
                    <div class="explorer-trend-mini-bar-track" aria-hidden="true"><div class="explorer-trend-mini-bar-fill explorer-trend-mini-bar-fill--muted" style="width:${w}%"></div></div>
                </div>
                <div class="explorer-trend-row__stat">${n}</div>
            </button>`;
        };

        const setsPackParts = [];
        if (chase.length) {
            setsPackParts.push(`<h4 class="explorer-trend-subh">Chase value (sum of top 10 tracked $)</h4><div class="explorer-trend-list">${chase.map(setRowChase).join('')}</div>`);
        }
        if (packMv.length) {
            setsPackParts.push(`<h4 class="explorer-trend-subh">Pack cost Δ% (${packDays}d)</h4><div class="explorer-trend-list">${packMv.map(setRowPack).join('')}</div>`);
        }
        if (density.length) {
            setsPackParts.push(`<h4 class="explorer-trend-subh">Tracked cards per set</h4><div class="explorer-trend-list">${density.map(setRowDen).join('')}</div>`);
        }
        const setsPackBody = setsPackParts.length
            ? setsPackParts.join('')
            : '<p class="explorer-trending-empty">No set-level pack or chase aggregates yet (run pack-cost sync + Explorer trending).</p>';
        const setsPackCount = chase.length + packMv.length + density.length;

        trendingRoot.innerHTML = when
            + trendDetails('Sets & packs', setsPackCount, setsPackBody, { open: true })
            + trendDetails('Prior-day movers', prior.length, cardBlockBody(prior, 'prior'), { open: false })
            + trendDetails('7-day % movers', week.length, cardBlockBody(week, 'week'), { open: false })
            + trendDetails('PSA 10 vs raw lift', grade.length, cardBlockBody(grade, 'grade'), { open: false });
    }

    async function loadAndRenderExplorerTrending() {
        if (!trendingRoot || typeof fetchPredictorAnalyticsAssetsFromSupabase !== 'function') return;
        try {
            const map = await fetchPredictorAnalyticsAssetsFromSupabase(['explorer_trending_daily']);
            renderExplorerTrendingPanel(map && map.explorer_trending_daily ? map.explorer_trending_daily : null);
        } catch (e) {
            console.warn('Explorer trending fetch', e);
            trendingRoot.innerHTML = '<p class="explorer-trending-empty">Could not load trending data.</p>';
        }
    }

    const setsJsonUrl = SHARED_UTILS.resolveDataAssetUrl('pokemon_sets_data.json');
    const pg = typeof window.PTCG_PAGE_PROGRESS !== 'undefined' ? window.PTCG_PAGE_PROGRESS : null;
    if (pg) pg.begin();

    async function bootstrapExplorerDataset() {
        if (typeof fetchPokemonSetsCatalogOnly === 'function' && typeof fetchPokemonSetsCardSliceByCodes === 'function') {
            if (pg) pg.setDeterminate(0.32);
            const catalog = await fetchPokemonSetsCatalogOnly();
            if (pg) pg.setDeterminate(0.52);
            allSets = catalog;
            if (typeof SHARED_UTILS !== 'undefined' && SHARED_UTILS.sortExplorerSetsByRecency) {
                SHARED_UTILS.sortExplorerSetsByRecency(allSets);
            }
            explorerListCursor = 0;
            const firstCodes = allSets
                .slice(0, EXPLORER_INITIAL_SLICE)
                .map((s) => String(s.set_code || '').trim())
                .filter(Boolean);
            const slice = await fetchPokemonSetsCardSliceByCodes(firstCodes, { withMetrics: false });
            mergeExplorerCardSliceInPlace(allSets, slice, true);
            explorerListCursor = Math.min(EXPLORER_INITIAL_SLICE, allSets.length);
            if (pg) pg.setDeterminate(0.82);
            return;
        }
        const data = await fetchPokemonSetsFromSupabase();
        if (!Array.isArray(data)) throw new Error('Expected an array of sets in JSON');
        allSets = data;
        if (typeof SHARED_UTILS !== 'undefined' && SHARED_UTILS.sortExplorerSetsByRecency) {
            SHARED_UTILS.sortExplorerSetsByRecency(allSets);
        }
        explorerListCursor = allSets.length;
        if (pg) pg.setDeterminate(0.82);
    }

    bootstrapExplorerDataset()
        .then(() => {
            if (pg) pg.setDeterminate(0.92);
            loadingEl.style.display = 'none';
            if (explorerLayoutEl) explorerLayoutEl.hidden = false;

            const seriesSet = new Set(allSets.map((s) => s.series).filter(Boolean));
            [...seriesSet].sort().forEach((s) => {
                const opt = document.createElement('option');
                opt.value = opt.innerText = s;
                seriesFilter.appendChild(opt);
            });

            renderSets(allSets);
            tryOpenCardDetailFromQuery();
            wireExplorerInfiniteScroll();
            loadAndRenderExplorerTrending();
            if (pg) pg.setDeterminate(1);
        })
        .catch((error) => {
            console.error('Error loading dataset:', error);
            const supa = error && error.message && String(error.message).includes('Supabase');
            const hint = supa
                ? `The live Supabase request failed (${error.message}). After deploying a fix, hard-refresh the page.`
                : `Failed to load dataset from <code style="word-break:break-all;">${setsJsonUrl}</code>. Put <code>pokemon_sets_data.json</code> next to this page on the server and open the site over <strong>http(s)://</strong> (not file://).`;
            loadingEl.innerHTML = `<p style="color: var(--warning);">${hint} Check the browser console for details.</p>`;
        })
        .finally(() => {
            if (pg) pg.end();
        });

    function runFilters() {
        const term = searchInput.value.toLowerCase();
        const series = seriesFilter.value;
        const filtered = allSets.filter((s) => {
            const matchesSearch = (s.set_name && s.set_name.toLowerCase().includes(term))
                || (s.set_code && s.set_code.toLowerCase().includes(term));
            const matchesSeries = !series || s.series === series;
            return matchesSearch && matchesSeries;
        });
        renderSets(filtered);
    }

    searchInput.addEventListener('input', () => {
        clearTimeout(explorerSearchDebounce);
        explorerSearchDebounce = setTimeout(async () => {
            const term = searchInput.value.toLowerCase();
            const series = seriesFilter.value;
            const filtered = allSets.filter((s) => {
                const matchesSearch = (s.set_name && s.set_name.toLowerCase().includes(term))
                    || (s.set_code && s.set_code.toLowerCase().includes(term));
                const matchesSeries = !series || s.series === series;
                return matchesSearch && matchesSeries;
            });
            const need = filtered
                .filter((s) => !Array.isArray(s.top_25_cards) || s.top_25_cards.length === 0)
                .slice(0, EXPLORER_SEARCH_PREFETCH)
                .map((s) => s.set_code)
                .filter(Boolean);
            if (need.length && typeof fetchPokemonSetsCardSliceByCodes === 'function') {
                try {
                    const sl = await fetchPokemonSetsCardSliceByCodes(need, { withMetrics: false });
                    mergeExplorerCardSliceInPlace(allSets, sl, true);
                } catch (e) {
                    console.warn('Explorer search prefetch', e);
                }
            }
            renderSets(filtered);
        }, 280);
    });
    seriesFilter.addEventListener('change', runFilters);

    function renderSets(sets) {
        containerEl.innerHTML = '';
        const senTop = document.getElementById('explorer-load-sentinel');
        if (senTop) senTop.hidden = true;
        if (sets.length === 0) {
            containerEl.innerHTML = '<p style="text-align: center; color: var(--text-secondary);">No sets found matching your search/filter.</p>';
            return;
        }

        const primarySets = [];
        const deferredSets = [];
        sets.forEach((s) => {
            if (setHasPredominantlyMissingExplorerPricing(s)) deferredSets.push(s);
            else primarySets.push(s);
        });

        const appendSetAccordion = (set) => {
            const setEl = document.createElement('div');
            setEl.className = 'set-item';
            setEl.dataset.setCode = String(set.set_code || '').trim().toLowerCase();

            const topCardsHtml = buildTopCardsListHtml(set);

            let raritiesHtml = '';
            if (set.rarity_counts && Object.keys(set.rarity_counts).length > 0) {
                raritiesHtml = Object.entries(set.rarity_counts)
                    .sort((a, b) => b[1] - a[1])
                    .map(([rarity, count]) => `
                        <div style="display: flex; justify-content: space-between; font-size: 0.9rem; margin-bottom: 0.25rem;">
                            <span style="color: var(--text-secondary);">${escHtml(rarity)}</span>
                            <span>${count}</span>
                        </div>
                    `).join('');
            } else {
                raritiesHtml = '<span style="color: var(--text-secondary);">N/A</span>';
            }

            const packEV = set.booster_pack_ev || 'N/A';
            const boxEV = set.booster_box_ev || 'N/A';

            if (set.set_name === 'Perfect Order' && !set.tcgplayer_pack_price) {
                set.tcgplayer_pack_price = 9.62;
            }

            const packPrice = set.tcgplayer_pack_price ? `$${Number(set.tcgplayer_pack_price).toFixed(2)}` : 'N/A';
            const tcgFresh = set.tcgtracking_price_updated
                ? `<div style="font-size:0.75rem;color:#64748b;margin-top:0.35rem;">TCGTracking prices: <code>${escHtml(String(set.tcgtracking_price_updated))}</code></div>`
                : '';

            setEl.innerHTML = `
                <div class="set-header">
                    <div class="set-identity">
                        ${set.logo_url ? `<img src="${escHtml(set.logo_url)}" alt="" class="set-logo" loading="lazy">` : `<div class="set-logo" style="background:#fff;border-radius:4px;display:flex;align-items:center;justify-content:center;color:#000;font-size:0.7rem;">No Logo</div>`}
                        <div>
                            <div class="set-title">${escHtml(String(set.set_name || 'Unknown Set'))}</div>
                            <div class="set-meta">
                                <span>${escHtml(String(set.set_code || ''))}</span>
                                <span>&bull;</span>
                                <span class="series-tag">${escHtml(String(set.series || 'Other'))}</span>
                                <span>&bull;</span>
                                <span>${escHtml(String(set.release_date || 'Unknown Date'))}</span>
                                <span>&bull;</span>
                                <span>${set.total_cards || '?'} Cards</span>
                            </div>
                            ${tcgFresh}
                        </div>
                    </div>
                    <div class="toggle-icon">▼</div>
                </div>
                <div class="set-content">
                    <div class="content-inner">
                        <div class="content-wrapper">
                            <div style="display: flex; flex-direction: column; gap: 1rem;">
                                <div class="info-card">
                                    <h3>Set Info</h3>
                                    <div class="stat-grid">
                                        <div class="stat-item">
                                            <div class="label">Pack EV</div>
                                            <div class="value">${escHtml(String(packEV))}</div>
                                        </div>
                                        <div class="stat-item">
                                            <div class="label">Box EV</div>
                                            <div class="value">${escHtml(String(boxEV))}</div>
                                        </div>
                                        <div class="stat-item">
                                            <div class="label">Market Pack</div>
                                            <div class="value">${escHtml(String(packPrice))}</div>
                                        </div>
                                        <div class="stat-item">
                                            <div class="label">Graded in set</div>
                                            <div class="value">${(() => { let t = 0; if (set.top_25_cards) set.top_25_cards.forEach(c => { if (c.gemrate && c.gemrate.total) t += c.gemrate.total; }); return t > 0 ? t.toLocaleString() : 'N/A'; })()}</div>
                                        </div>
                                    </div>
                                </div>
                                <div class="info-card" style="flex: 1;">
                                    <h3>Rarity Distribution</h3>
                                    <div class="custom-scrollbar" style="padding-right: 0.5rem; max-height: 150px; overflow-y: auto;">
                                        ${raritiesHtml}
                                    </div>
                                </div>
                            </div>

                            <div class="info-card">
                                <h3>Most Expensive Cards</h3>
                                <ul class="top-cards-list custom-scrollbar">
                                    ${topCardsHtml}
                                </ul>
                            </div>
                        </div>
                    </div>
                </div>
            `;

            const header = setEl.querySelector('.set-header');
            header.addEventListener('click', async () => {
                const isExpanded = setEl.classList.contains('expanded');
                document.querySelectorAll('#sets-container .set-item').forEach((el) => el.classList.remove('expanded'));
                if (!isExpanded) {
                    setEl.classList.add('expanded');
                    await maybeHydrateExplorerFullMetrics(set);
                    const ul = setEl.querySelector('.top-cards-list');
                    if (ul) ul.innerHTML = buildTopCardsListHtml(set);
                }
            });

            return setEl;
        };

        primarySets.forEach((set) => {
            containerEl.appendChild(appendSetAccordion(set));
        });

        if (deferredSets.length) {
            const det = document.createElement('details');
            det.className = 'explorer-deferred-sets';
            const n = deferredSets.length;
            const sum = document.createElement('summary');
            sum.className = 'explorer-deferred-sets-summary';
            sum.textContent = `Limited market pricing — ${n} set${n === 1 ? '' : 's'} (≥ half of top cards lack list / Dex / Track USD)`;
            const inner = document.createElement('div');
            inner.className = 'explorer-deferred-sets-inner';
            deferredSets.forEach((set) => {
                inner.appendChild(appendSetAccordion(set));
            });
            det.appendChild(sum);
            det.appendChild(inner);
            containerEl.appendChild(det);
        }

        const sen = document.getElementById('explorer-load-sentinel');
        if (sen) {
            if (explorerListCursor < allSets.length) {
                sen.hidden = false;
            } else {
                sen.hidden = true;
                sen.textContent = '';
            }
        }
    }
});
