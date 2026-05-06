/** 
 * shared.js - common utilities for Predictor, Analytics, and Explorer 
 */

const SHARED_UTILS = {
    fmtUsd(x) {
        const v = Number(x);
        if (!Number.isFinite(v)) return '—';
        return `$${v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    },

    escHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    },

    escAttr(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;');
    },

    /** Release year from `set.release_date` (YYYY/… or YYYY-…), or ''. */
    gemrateReleaseYear(set) {
        const rd = set && set.release_date;
        if (rd == null) return '';
        const s = String(rd).trim();
        if (s.length < 4) return '';
        const y = parseInt(s.slice(0, 4), 10);
        return Number.isFinite(y) && y >= 1970 && y <= 2100 ? String(y) : '';
    },

    /**
     * GemRate Universal Pop checklist URL for this TCG set (not card search).
     * Uses `set.gemrate_set_link` when present (from scraper); else builds `/universal-pop-report/{id}-{slug}-TCG`.
     */
    getGemrateSetPopReportUrl(set) {
        if (!set) return '';
        const stored = set.gemrate_set_link;
        if (stored && typeof stored === 'string') {
            const t = stored.trim();
            if (/^https?:\/\//i.test(t)) return t;
        }
        const sid = (set.gemrate_id && String(set.gemrate_id).trim()) || '';
        if (!sid) return '';
        const setName = (set.set_name && String(set.set_name).trim()) || '';
        if (!setName) return '';
        const year = SHARED_UTILS.gemrateReleaseYear(set);
        const segment = year ? `${year} Pokemon ${setName}` : `Pokemon ${setName}`;
        return `https://www.gemrate.com/universal-pop-report/${sid}-${encodeURIComponent(segment)}-TCG`;
    },

    /** Legacy universal-search URL (card or set id when present). Prefer {@link SHARED_UTILS.getGemrateSetPopReportUrl} for set checklist. */
    getGemrateUrl(card, set) {
        if (!card) return '';
        const gid = card.gemrate_id || (set && set.gemrate_id);
        if (gid) {
            return `https://www.gemrate.com/universal-search?gemrate_id=${encodeURIComponent(gid)}`;
        }
        const q = `${set && set.set_name ? set.set_name : ''} ${card.name || ''} ${card.number || ''}`.trim();
        return `https://www.gemrate.com/universal-search?query=${encodeURIComponent(q)}`;
    },

    resolveDataAssetUrl(filename) {
        const u = new URL(window.location.href);
        let path = u.pathname || '/';
        if (!path.endsWith('/')) {
            const seg = path.slice(path.lastIndexOf('/') + 1);
            if (seg.includes('.')) {
                path = path.slice(0, path.lastIndexOf('/') + 1) || '/';
            } else {
                path = `${path}/`;
            }
        }
        const clean = String(filename).replace(/^\//, '');
        u.pathname = (path.endsWith('/') ? path : `${path}/`) + clean;
        return u.href;
    },

    medianSorted(sorted) {
        if (!sorted.length) return null;
        const m = Math.floor(sorted.length / 2);
        return sorted.length % 2 ? sorted[m] : (sorted[m - 1] + sorted[m]) / 2;
    },

    medianArray(arr) {
        if (!arr.length) return null;
        const s = arr.slice().sort((a, b) => a - b);
        return SHARED_UTILS.medianSorted(s);
    },

    priceDedupForMedian(values, relEps = 0.006) {
        const ok = values.filter((v) => Number.isFinite(v) && v > 0).sort((a, b) => a - b);
        const out = [];
        for (const v of ok) {
            if (!out.some((u) => Math.abs(u - v) <= relEps * Math.max(u, v, 1))) out.push(v);
        }
        return out;
    },

    wizardHistoryPositiveUsdMedian(card) {
        const hist = card && card.pokemon_wizard_price_history;
        if (!Array.isArray(hist) || hist.length === 0) return null;
        const vals = [];
        for (let i = 0; i < hist.length; i++) {
            const n = Number(hist[i] && hist[i].price_usd);
            if (Number.isFinite(n) && n > 0) vals.push(n);
        }
        return vals.length ? SHARED_UTILS.medianArray(vals) : null;
    },

    collectDedupedPositiveUsdPrices(card) {
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
        const wh = SHARED_UTILS.wizardHistoryPositiveUsdMedian(card);
        if (wh != null) push(wh);
        push(card.pricecharting_used_price_usd);
        push(card.pricecharting_graded_price_usd);
        const pch = SHARED_UTILS.pricechartingHistoryPositiveUsdMedian(card);
        if (pch != null) push(pch);
        return SHARED_UTILS.priceDedupForMedian(vals);
    },

    resolveExplorerChartUsd(card) {
        if (!card) return null;
        const dedup = SHARED_UTILS.collectDedupedPositiveUsdPrices(card);
        if (dedup.length >= 2) return SHARED_UTILS.medianArray(dedup);
        if (dedup.length === 1) return dedup[0];
        const mp = Number(card.market_price);
        return Number.isFinite(mp) && mp > 0 ? mp : null;
    },

    /* UI Logic - Shared between Explorer and Predictor */
    hasCollectricsEbay(card) {
        if (!card) return false;
        const a = Number(card.collectrics_ebay_listings);
        const b = Number(card.collectrics_ebay_sold_volume);
        return (Number.isFinite(a) && a > 0) || (Number.isFinite(b) && b > 0);
    },

    /** Graded population total — Gemrate Universal when present, else merged PSA sidecar (not PokéMetrics; API retired / unreliable). */
    getCardGradedPopTotal(card) {
        if (!card) return null;
        if (card.gemrate && card.gemrate.total != null) return card.gemrate.total;
        const psa = Number(card.psa_graded_pop_total);
        if (Number.isFinite(psa) && psa >= 0) return psa;
        return null;
    },

    /** Per-grader totals from Gemrate breakdown (gem-mint counts + universal total). */
    getGraderTotals(card) {
        if (!card || !card.gemrate) return null;
        const g = card.gemrate;
        const total = g.total || 0;
        if (total <= 0) return null;
        return {
            psa_gems: g.psa_gems || 0,
            bgs_gems: g.beckett_gems || 0,
            cgc_gems: g.cgc_gems || 0,
            sgc_gems: g.sgc_gems || 0,
            total_gem_mint: g.total_gem_mint || 0,
            total: total
        };
    },

    _nz(v) {
        const n = Number(v);
        return Number.isFinite(n) && n > 0 ? n : 0;
    },

    /** Graded slab counts by company (PSA/CGC/BGS/SGC) for the compact share bar in graded supply. */
    gemrateGraderPopulationSlices(card) {
        const g = card && card.gemrate;
        if (!g) return null;
        const psa = SHARED_UTILS._nz(g.psa_grades);
        const cgc = SHARED_UTILS._nz(g.cgc_grades);
        const bgs = SHARED_UTILS._nz(g.beckett_grades);
        const sgc = SHARED_UTILS._nz(g.sgc_grades);
        const sum = psa + cgc + bgs + sgc;
        if (sum <= 0) return null;
        const labels = [];
        const values = [];
        const colors = [];
        const push = (lbl, val, col) => {
            if (val <= 0) return;
            labels.push(lbl);
            values.push(val);
            colors.push(col);
        };
        push('PSA', psa, '#3b82f6');
        push('CGC', cgc, '#ef4444');
        push('BGS', bgs, '#d4a017');
        push('SGC', sgc, '#94a3b8');
        if (!labels.length) return null;
        return { labels, values, colors, sum };
    },

    /** Format GemRate ratio (0–1 or 0–100) as a percentage string for display. */
    fmtGemRatePct(v) {
        if (v == null || !Number.isFinite(Number(v))) return '—';
        const x = Number(v);
        const pct = x <= 1 && x >= 0 ? x * 100 : x;
        return `${pct.toFixed(2)}%`;
    },

    /**
     * Graded population block: universal total, per-company graded counts, gem counts, gem rates,
     * optional gem-mint share bar and graded-volume-by-company share bar (same compact style).
     */
    buildGemrateGradedPopulationSection(card, set, sourceLabel) {
        const popTotal = SHARED_UTILS.getCardGradedPopTotal(card);
        if (popTotal == null || !Number.isFinite(Number(popTotal))) return '';
        const esc = SHARED_UTILS.escHtml;
        const hasGemData = card.gemrate != null;
        const g = hasGemData ? card.gemrate : {};
        const gs = Number(popTotal).toLocaleString();
        const gemSetUrl = SHARED_UTILS.getGemrateSetPopReportUrl(set) || '';
        const setLinkName = set && set.set_name ? String(set.set_name) : 'This set';

        const fmtRate = SHARED_UTILS.fmtGemRatePct;
        const row = (lbl, grades, gems, rate) => {
            const gn = SHARED_UTILS._nz(grades);
            const mn = SHARED_UTILS._nz(gems);
            if (gn <= 0 && mn <= 0) return '';
            const rateStr = rate != null && Number.isFinite(Number(rate)) ? fmtRate(rate) : '—';
            return `<div class="card-detail-stat"><span class="lbl">${esc(lbl)}</span><span class="val" style="font-size:0.78rem;">Graded <strong>${gn.toLocaleString()}</strong> · Gems <strong>${mn.toLocaleString()}</strong> · Gem rate <strong>${esc(rateStr)}</strong></span></div>`;
        };

        let popStats = `<div class="card-detail-stat"><span class="lbl">Universal total</span><span class="val">${gs}</span></div>`;
        if (hasGemData) {
            popStats += row('PSA', g.psa_grades, g.psa_gems, g.psa_gem_rate);
            popStats += row('CGC', g.cgc_grades, g.cgc_gems, g.cgc_gem_rate);
            popStats += row('BGS', g.beckett_grades, g.beckett_gems, g.beckett_gem_rate);
            popStats += row('SGC', g.sgc_grades, g.sgc_gems, g.sgc_gem_rate);
            if (g.total_gem_mint > 0) {
                popStats += `<div class="card-detail-stat"><span class="lbl">Gem-mint (all)</span><span class="val" style="color:#6ee7b7;">${Number(g.total_gem_mint).toLocaleString()}</span></div>`;
            }
            if (g.total_gem_rate != null && Number.isFinite(Number(g.total_gem_rate))) {
                popStats += `<div class="card-detail-stat"><span class="lbl">Overall gem rate</span><span class="val">${esc(fmtRate(g.total_gem_rate))}</span></div>`;
            }
        }

        let barHtml = '';
        if (hasGemData && g.total_gem_mint > 0) {
            const psaPct = ((g.psa_gems || 0) / g.total_gem_mint * 100).toFixed(1);
            const bgsPct = ((g.beckett_gems || 0) / g.total_gem_mint * 100).toFixed(1);
            const cgcPct = ((g.cgc_gems || 0) / g.total_gem_mint * 100).toFixed(1);
            const sgcPct = ((g.sgc_gems || 0) / g.total_gem_mint * 100).toFixed(1);
            const gemMixRow = (color, name, pct) => (
                `<div class="gemrate-compact-row"><span class="gemrate-compact-row__name"><span class="gemrate-compact-dot" style="background:${color};"></span>${esc(name)}</span><span class="gemrate-compact-row__pct">${pct}%</span></div>`
            );
            barHtml = `
                <div class="gemrate-compact-block gemrate-compact-block--stretch">
                    <div class="gemrate-compact-block__title">Gem-mint mix (by grader)</div>
                    <div class="gemrate-compact-bar gemrate-compact-bar--thin">
                        ${g.psa_gems > 0 ? `<div title="PSA: ${psaPct}%" style="width:${psaPct}%; background:#3b82f6;"></div>` : ''}
                        ${g.beckett_gems > 0 ? `<div title="BGS: ${bgsPct}%" style="width:${bgsPct}%; background:#d4a017;"></div>` : ''}
                        ${g.cgc_gems > 0 ? `<div title="CGC: ${cgcPct}%" style="width:${cgcPct}%; background:#ef4444;"></div>` : ''}
                        ${g.sgc_gems > 0 ? `<div title="SGC: ${sgcPct}%" style="width:${sgcPct}%; background:#e2e8f0;"></div>` : ''}
                    </div>
                    <div class="gemrate-compact-rows">
                        ${g.psa_gems > 0 ? gemMixRow('#3b82f6', 'PSA', psaPct) : ''}
                        ${g.beckett_gems > 0 ? gemMixRow('#d4a017', 'BGS', bgsPct) : ''}
                        ${g.cgc_gems > 0 ? gemMixRow('#ef4444', 'CGC', cgcPct) : ''}
                        ${g.sgc_gems > 0 ? gemMixRow('#e2e8f0', 'SGC', sgcPct) : ''}
                    </div>
                </div>`;
        }

        let slabBarHtml = '';
        const slabSlices = SHARED_UTILS.gemrateGraderPopulationSlices(card);
        if (slabSlices && slabSlices.labels.length) {
            const { labels: slabLabels, values: slabVals, colors: slabCols, sum: slabSum } = slabSlices;
            const topMargin = barHtml ? '0.85rem' : '0.65rem';
            const segs = slabLabels.map((lbl, i) => {
                const v = slabVals[i];
                const pct = ((v / slabSum) * 100).toFixed(1);
                const tti = `${lbl}: ${v.toLocaleString()} (${pct}%)`;
                return `<div title="${SHARED_UTILS.escAttr(tti)}" style="width:${pct}%; background:${slabCols[i]};"></div>`;
            }).join('');
            const legRows = slabLabels.map((lbl, i) => {
                const v = slabVals[i];
                const pct = ((v / slabSum) * 100).toFixed(1);
                const c = slabCols[i];
                return `<div class="gemrate-compact-row"><span class="gemrate-compact-row__name"><span class="gemrate-compact-dot" style="background:${c};"></span>${esc(lbl)}</span><span class="gemrate-compact-row__pct">${pct}% <span class="gemrate-compact-row__n">(${v.toLocaleString()})</span></span></div>`;
            }).join('');
            slabBarHtml = `
                <div class="gemrate-compact-block gemrate-compact-block--stretch" style="margin-top:${topMargin};">
                    <div class="gemrate-compact-block__title">GemRate · graded volume by company (${slabSum.toLocaleString()} slabs)</div>
                    <div class="gemrate-compact-bar gemrate-compact-bar--thin">
                        ${segs}
                    </div>
                    <div class="gemrate-compact-rows">
                        ${legRows}
                    </div>
                </div>`;
        }

        const linkRow = gemSetUrl
            ? `<div class="card-detail-stat card-detail-stat--full-span"><span class="lbl" style="color:#94a3b8;">GemRate</span><span class="val" style="font-size:0.72rem;"><a class="card-detail-link" href="${SHARED_UTILS.escAttr(gemSetUrl)}" target="_blank" rel="noopener noreferrer">${esc(setLinkName)} — checklist &amp; Universal Pop</a></span></div>`
            : '';

        const src = sourceLabel || (hasGemData ? 'GemRate Universal Pop' : 'Merged population');
        return `
            <div class="card-detail-section card-detail-section--graded-supply">
                <h4>Graded supply <span style="font-weight:400;color:#94a3b8;">(${esc(src)})</span></h4>
                <div class="card-detail-stat-grid card-detail-stat-grid--graded-supply">
                    ${popStats}
                    ${linkRow}
                </div>
                ${barHtml}${slabBarHtml}
            </div>`;
    },

    filterWizardPriceHistoryRows(ph) {
        if (!Array.isArray(ph)) return [];
        return ph.filter((row) => {
            const l = String(row.label || '').trim().toLowerCase();
            if (['date', 'price', 'trend', 'when', 'label', 'sort_key'].includes(l)) return false;
            const sk = String(row.sort_key || '').trim().toLowerCase();
            if (sk === 'date' || sk === 'price' || sk === 'trend') return false;
            return true;
        });
    },

    /** Spot EUR→USD for Cardmarket lows in charts (override with `window.PTCG_EUR_USD_RATE`). */
    getEurUsdRate() {
        const w = typeof window !== 'undefined' && window.PTCG_EUR_USD_RATE;
        const n = Number(w);
        if (Number.isFinite(n) && n > 0.5 && n < 3) return n;
        return 1.085;
    },

    /** Chart.js axis / legend tick colors from CSS (`--chart-axis-tick`, `--chart-axis-grid` on :root). */
    getChartAxisColors() {
        if (typeof document === 'undefined' || typeof getComputedStyle === 'undefined') {
            return { tickColor: '#94a3b8', gridColor: 'rgba(148,163,184,0.12)' };
        }
        const cs = getComputedStyle(document.documentElement);
        const tick = (cs.getPropertyValue('--chart-axis-tick') || '').trim();
        const grid = (cs.getPropertyValue('--chart-axis-grid') || '').trim();
        return {
            tickColor: tick || '#94a3b8',
            gridColor: grid || 'rgba(148,163,184,0.12)',
        };
    },

    eurToUsd(eur) {
        const n = Number(eur);
        if (!Number.isFinite(n)) return null;
        return n * SHARED_UTILS.getEurUsdRate();
    },

    /**
     * Trim shared category labels to the min…max index where any series has a finite value.
     * Avoids a long empty x-axis when Wizard/Collectrics imply years of labels but TCGGO only has recent days.
     */
    trimUnifiedPriceChartRangeToData(labels, datasets) {
        if (!labels || !labels.length || !datasets || !datasets.length) return { labels, datasets };
        let lo = labels.length;
        let hi = -1;
        for (let i = 0; i < labels.length; i++) {
            let any = false;
            for (let di = 0; di < datasets.length; di++) {
                const arr = datasets[di].data;
                if (!Array.isArray(arr)) continue;
                const v = arr[i];
                if (v != null && Number.isFinite(Number(v))) {
                    any = true;
                    break;
                }
            }
            if (any) {
                if (i < lo) lo = i;
                if (i > hi) hi = i;
            }
        }
        if (hi < lo) return { labels, datasets };
        const tLabels = labels.slice(lo, hi + 1);
        const tDatasets = datasets.map((ds) => {
            const out = { ...ds };
            if (Array.isArray(ds.data)) out.data = ds.data.slice(lo, hi + 1);
            return out;
        });
        return { labels: tLabels, datasets: tDatasets };
    },

    /**
     * Sync active state on unified price history range buttons (1M / 3M / 6M / 1Y).
     */
    syncUnifiedHistoryRangeUi(rootEl, months) {
        if (!rootEl || !rootEl.querySelector) return;
        const wrap = rootEl.querySelector('.card-detail-unified-range');
        if (!wrap) return;
        const m = Number(months);
        wrap.querySelectorAll('[data-ptcg-history-months]').forEach((b) => {
            const raw = b.getAttribute('data-ptcg-history-months');
            const bm = Number(raw);
            b.classList.toggle('is-active', Number.isFinite(bm) && bm === m);
        });
    },

    /**
     * One USD chart: Collectrics (TCG / eBay), TCGGO (TCGPlayer + Cardmarket EUR→USD), Pokémon Wizard,
     * plus optional eBay sold medians (ungraded / graded) and daily sold count on a secondary axis.
     * @param {object} [opts]
     * @param {1|3|6|12|null} [opts.historyWindowMonths] — slice to last N calendar months from newest label; omit/null = full span (then trim-to-data).
     * @returns {{ labels: string[], datasets: object[], titleNote: string, skipStandaloneWizard: boolean, hasY1Axis: boolean } | null}
     */
    buildExplorerUnifiedPriceChartPack(card, opts) {
        if (!card) return null;
        const o = opts && typeof opts === 'object' ? opts : {};
        const ceJust = Array.isArray(card.collectrics_history_justtcg) ? card.collectrics_history_justtcg : [];
        const ceEbayHist = Array.isArray(card.collectrics_history_ebay) ? card.collectrics_history_ebay : [];
        const rng = SHARED_UTILS.collectricsEbayEndedRangeSeries(card);

        const allLabelsSet = new Set();
        if (ceJust.length > 1) {
            ceJust.forEach((r) => { if (r.date) allLabelsSet.add(String(r.date).slice(0, 10)); });
        }
        if (ceEbayHist.length > 1) {
            ceEbayHist.forEach((r) => { if (r.date) allLabelsSet.add(String(r.date).slice(0, 10)); });
        } else if (rng && rng.labels && rng.labels.length > 1) {
            rng.labels.forEach((l) => allLabelsSet.add(String(l).slice(0, 10)));
        }

        const tg = card.tcggo;
        const ph = tg && tg.price_history_en;
        const daily = ph && ph.daily && typeof ph.daily === 'object' ? ph.daily : null;
        if (daily) {
            Object.keys(daily).forEach((k) => allLabelsSet.add(String(k).slice(0, 10)));
        }

        const wizRows = SHARED_UTILS.filterWizardPriceHistoryRows(card.pokemon_wizard_price_history || []);
        wizRows.forEach((r) => {
            const sk = String(r.sort_key || '').trim();
            if (sk.length >= 10) allLabelsSet.add(sk.slice(0, 10));
        });

        const soldRowsPre = SHARED_UTILS.ebaySoldRowsDeduped(card);
        soldRowsPre.forEach((r) => {
            const dk = SHARED_UTILS.normalizeEbaySoldDateKey(r.date) || String(r.date || '').trim().slice(0, 10);
            if (dk.length >= 10) allLabelsSet.add(dk.slice(0, 10));
        });

        // New API pipeline: tcggo_market_history array
        const tcggoApiHistPre = Array.isArray(card.tcggo_market_history) ? card.tcggo_market_history : [];
        tcggoApiHistPre.forEach((r) => {
            const dk = String(r.date || '').slice(0, 10);
            if (dk.length >= 10) allLabelsSet.add(dk);
        });

        let labels = Array.from(allLabelsSet).filter(Boolean).sort();
        if (labels.length < 2) return null;

        const datasets = [];
        const rate = SHARED_UTILS.getEurUsdRate();

        if (ceJust.length > 1) {
            const map = new Map(ceJust.map((r) => [String(r.date).slice(0, 10), Number(r.j_raw_price)]));
            datasets.push({
                type: 'line',
                label: 'TCGPlayer retail (Collectrics)',
                data: labels.map((l) => {
                    const v = map.get(l);
                    return Number.isFinite(v) ? v : null;
                }),
                borderColor: '#3b82f6',
                backgroundColor: 'rgba(59,130,246,0.1)',
                fill: false,
                tension: 0.3,
                spanGaps: true,
            });
        }

        if (rng && rng.labels && rng.labels.length > 1) {
            const mapHigh = new Map(rng.labels.map((l, i) => [String(l).slice(0, 10), rng.high[i]]));
            const mapLow = new Map(rng.labels.map((l, i) => [String(l).slice(0, 10), rng.low[i]]));
            datasets.push(
                {
                    type: 'line',
                    label: 'eBay sold high (Collectrics)',
                    data: labels.map((l) => (mapHigh.get(l) != null ? mapHigh.get(l) : null)),
                    borderColor: '#10b981',
                    backgroundColor: 'rgba(16,185,129,0.06)',
                    fill: false,
                    tension: 0.3,
                    spanGaps: true,
                    pointRadius: 0,
                },
                {
                    type: 'line',
                    label: 'eBay sold low (Collectrics)',
                    data: labels.map((l) => (mapLow.get(l) != null ? mapLow.get(l) : null)),
                    borderColor: '#059669',
                    backgroundColor: 'rgba(5,150,105,0.15)',
                    fill: '-1',
                    tension: 0.3,
                    spanGaps: true,
                    pointRadius: 0,
                },
            );
        } else if (ceEbayHist.length > 1) {
            const map = new Map(ceEbayHist.map((r) => [String(r.date).slice(0, 10), Number(r.ended_avg_raw_price)]));
            datasets.push({
                type: 'line',
                label: 'eBay sold avg (Collectrics)',
                data: labels.map((l) => {
                    const v = map.get(l);
                    return Number.isFinite(v) ? v : null;
                }),
                borderColor: '#10b981',
                backgroundColor: 'rgba(16,185,129,0.15)',
                fill: true,
                tension: 0.3,
                spanGaps: true,
            });
        }

        let anyTcggoTcg = false;
        let anyTcggoCm = false;
        let anyProxyDay = false;
        if (daily) {
            const tcgData = labels.map((l) => {
                const row = daily[l];
                const v = row && row.tcg_player_market != null ? Number(row.tcg_player_market) : null;
                if (v != null && Number.isFinite(v)) anyTcggoTcg = true;
                return Number.isFinite(v) ? v : null;
            });
            const cmData = labels.map((l) => {
                const row = daily[l];
                const eur = row && row.cardmarket_low_eur != null ? Number(row.cardmarket_low_eur) : null;
                if (eur != null && Number.isFinite(eur)) anyTcggoCm = true;
                return SHARED_UTILS.eurToUsd(eur);
            });
            labels.forEach((l) => {
                const row = daily[l];
                const hasTcg = row && row.tcg_player_market != null && Number.isFinite(Number(row.tcg_player_market));
                const hasCm = row && row.cardmarket_low_eur != null && Number.isFinite(Number(row.cardmarket_low_eur));
                if (!hasTcg && hasCm) anyProxyDay = true;
            });
            if (anyTcggoTcg) {
                datasets.push({
                    type: 'line',
                    label: 'TCGGO · TCGPlayer (USD)',
                    data: tcgData,
                    borderColor: '#f97316',
                    backgroundColor: 'rgba(249,115,22,0.1)',
                    fill: false,
                    tension: 0.25,
                    spanGaps: true,
                });
            }
            if (anyTcggoCm) {
                datasets.push({
                    type: 'line',
                    label: `TCGGO · Cardmarket low (EUR→USD ×${rate.toFixed(3)})`,
                    data: cmData,
                    borderColor: '#ec4899',
                    borderDash: [6, 4],
                    backgroundColor: 'rgba(236,72,153,0.06)',
                    fill: false,
                    tension: 0.25,
                    spanGaps: true,
                });
            }
        }

        // --- New API Pipeline: tcggo_market_history (array of {date, price_usd, cm_low}) ---
        const tcggoApiHist = Array.isArray(card.tcggo_market_history) ? card.tcggo_market_history : [];
        if (tcggoApiHist.length > 1 && !anyTcggoTcg) {
            // Only add if the old daily format didn't already provide TCGGO data
            const apiMap = new Map();
            const cmMap = new Map();
            tcggoApiHist.forEach((r) => {
                const dk = String(r.date || '').slice(0, 10);
                if (dk.length >= 10) {
                    allLabelsSet.add(dk);
                    if (r.price_usd != null && Number.isFinite(Number(r.price_usd))) {
                        apiMap.set(dk, Number(r.price_usd));
                    }
                    if (r.cm_low != null && Number.isFinite(Number(r.cm_low))) {
                        cmMap.set(dk, Number(r.cm_low));
                    }
                }
            });
            // Rebuild labels with the new dates added
            labels = Array.from(allLabelsSet).filter(Boolean).sort();

            if (apiMap.size > 0) {
                datasets.push({
                    type: 'line',
                    label: 'TCG Pro · TCGPlayer Market (USD)',
                    data: labels.map((l) => apiMap.get(l) ?? null),
                    borderColor: '#f97316',
                    backgroundColor: 'rgba(249,115,22,0.1)',
                    fill: false,
                    tension: 0.25,
                    spanGaps: true,
                });
            }
            if (cmMap.size > 0) {
                datasets.push({
                    type: 'line',
                    label: 'TCG Pro · Cardmarket Low (EUR)',
                    data: labels.map((l) => cmMap.get(l) ?? null),
                    borderColor: '#ec4899',
                    borderDash: [6, 4],
                    backgroundColor: 'rgba(236,72,153,0.06)',
                    fill: false,
                    tension: 0.25,
                    spanGaps: true,
                });
            }
        }

        let wizPts = 0;
        const wizMap = new Map();
        wizRows.forEach((r) => {
            const sk = String(r.sort_key || '').trim();
            if (sk.length < 10) return;
            const dk = sk.slice(0, 10);
            const p = Number(r.price_usd);
            if (Number.isFinite(p)) {
                wizMap.set(dk, p);
                wizPts += 1;
            }
        });
        if (wizPts > 0) {
            datasets.push({
                type: 'line',
                label: 'Pokémon Wizard (USD)',
                data: labels.map((l) => (wizMap.has(l) ? wizMap.get(l) : null)),
                borderColor: '#a78bfa',
                backgroundColor: 'rgba(167,139,250,0.08)',
                fill: false,
                tension: 0.3,
                spanGaps: true,
                order: 3,
            });
        }

        const soldRows = SHARED_UTILS.ebaySoldRowsDeduped(card);
        if (soldRows.length) {
            const dayKey = (r) => SHARED_UTILS.normalizeEbaySoldDateKey(r.date) || String(r.date || '').trim().slice(0, 10);
            const byDay = new Map();
            soldRows.forEach((r) => {
                const dk = dayKey(r);
                if (!dk || dk.length < 10) return;
                const dks = dk.slice(0, 10);
                const p = Number(r.price);
                if (!Number.isFinite(p) || p <= 0) return;
                if (!byDay.has(dks)) byDay.set(dks, { ug: [], gr: [] });
                const b = byDay.get(dks);
                if (SHARED_UTILS.isEbaySoldListingGradedTitle(r.title)) b.gr.push(p);
                else b.ug.push(p);
            });
            const ugMed = labels.map((l) => {
                const b = byDay.get(l);
                if (!b || !b.ug.length) return null;
                return SHARED_UTILS._medianSortedNums(b.ug);
            });
            const grMed = labels.map((l) => {
                const b = byDay.get(l);
                if (!b || !b.gr.length) return null;
                return SHARED_UTILS._medianSortedNums(b.gr);
            });
            const volCt = labels.map((l) => {
                const b = byDay.get(l);
                if (!b) return null;
                const n = b.ug.length + b.gr.length;
                return n > 0 ? n : null;
            });
            const hasUg = ugMed.some((v) => v != null);
            const hasGr = grMed.some((v) => v != null);
            const hasVol = volCt.some((v) => v != null && v > 0);
            if (hasUg) {
                datasets.push({
                    type: 'line',
                    label: 'eBay sold median (ungraded)',
                    data: ugMed,
                    borderColor: '#eab4cf',
                    backgroundColor: 'rgba(234,180,207,0.1)',
                    fill: false,
                    tension: 0.25,
                    spanGaps: true,
                    borderWidth: 2,
                    pointRadius: 2,
                    yAxisID: 'y',
                    order: 3,
                });
            }
            if (hasGr) {
                datasets.push({
                    type: 'line',
                    label: 'eBay sold median (graded)',
                    data: grMed,
                    borderColor: '#e8c4a4',
                    backgroundColor: 'rgba(232,196,164,0.1)',
                    fill: false,
                    tension: 0.25,
                    spanGaps: true,
                    borderWidth: 2,
                    pointRadius: 2,
                    yAxisID: 'y',
                    order: 3,
                });
            }
            if (hasVol) {
                datasets.push({
                    type: 'bar',
                    label: 'eBay sold / day (count)',
                    data: volCt,
                    yAxisID: 'y1',
                    order: -1,
                    borderRadius: 6,
                    borderSkipped: false,
                    backgroundColor: 'rgba(16,185,129,0.38)',
                    borderColor: 'rgba(52,211,153,0.75)',
                    borderWidth: 1,
                    maxBarThickness: 28,
                });
            }
        }

        const winM = o.historyWindowMonths != null ? Number(o.historyWindowMonths) : NaN;
        if (Number.isFinite(winM) && winM > 0 && labels.length >= 2) {
            const finiteTs = labels
                .map((lab) => Date.parse(String(lab).slice(0, 10)))
                .filter((t) => Number.isFinite(t));
            if (finiteTs.length) {
                const lastMs = Math.max(...finiteTs);
                const cut = lastMs - winM * 30.4375 * 86400000;
                let startIdx = 0;
                for (let i = 0; i < labels.length; i++) {
                    const t = Date.parse(String(labels[i]).slice(0, 10));
                    if (Number.isFinite(t) && t >= cut) {
                        startIdx = i;
                        break;
                    }
                }
                if (labels.length - startIdx < 2 && startIdx > 0) {
                    startIdx = Math.max(0, startIdx - 1);
                }
                if (labels.length - startIdx >= 2) {
                    labels = labels.slice(startIdx);
                    datasets.forEach((ds) => {
                        if (Array.isArray(ds.data)) ds.data = ds.data.slice(startIdx);
                    });
                }
            }
        }

        if (datasets.length === 0) return null;

        const trimmed = SHARED_UTILS.trimUnifiedPriceChartRangeToData(labels, datasets);
        const outLabels = trimmed.labels.length >= 2 ? trimmed.labels : labels;
        const outDatasets = trimmed.labels.length >= 2 ? trimmed.datasets : datasets;

        const parts = [`EUR→USD ×${rate.toFixed(3)} (set window.PTCG_EUR_USD_RATE to override).`];
        if (anyProxyDay) {
            parts.push('Some dates lack TCGPlayer in TCGGO; pink dashed = Cardmarket EU low converted to USD—use orange TCGPlayer when both exist.');
        }
        const hasY1Out = outDatasets.some((ds) => ds && ds.yAxisID === 'y1');
        if (hasY1Out) {
            parts.push('Green bars (right axis): eBay sold listings per day; soft pink / peach lines: median sold USD (ungraded / graded).');
        }
        const titleNote = parts.join(' ');

        return {
            labels: outLabels,
            datasets: outDatasets,
            titleNote,
            skipStandaloneWizard: wizPts > 1,
            hasY1Axis: hasY1Out,
        };
    },

    /** 1–2–5 × 10^n tick step (readable 0/5-style decades on the axis). */
    _wizardNiceStepPositive(span, targetTicks = 5) {
        if (!Number.isFinite(span) || span <= 0) return 1;
        const raw = span / Math.max(2, targetTicks);
        const pow10 = Math.pow(10, Math.floor(Math.log10(raw)));
        const m = raw / pow10;
        let f = 10;
        if (m <= 1) f = 1;
        else if (m <= 2) f = 2;
        else if (m <= 5) f = 5;
        return f * pow10;
    },

    /**
     * Tight axis range: small padding past data, min/max snapped to nice steps.
     * @param {number} lo
     * @param {number} hi
     * @param {{ padFrac?: number, floorZero?: boolean }} [opts]
     */
    _wizardNiceAxisEnds(lo, hi, opts) {
        const padFrac = opts && opts.padFrac != null ? opts.padFrac : 0.05;
        const floorZero = Boolean(opts && opts.floorZero);
        if (!Number.isFinite(lo) || !Number.isFinite(hi)) return null;
        if (lo > hi) [lo, hi] = [hi, lo];
        const spanData = hi - lo;
        const mag = Math.max(Math.abs(lo), Math.abs(hi), 1e-9);
        const coreSpan = Math.max(spanData, mag * 0.012, spanData === 0 ? mag * 0.035 : 0);
        const pad = Math.max(coreSpan * padFrac, spanData === 0 ? mag * 0.018 : 0);
        let a = lo - pad;
        let b = hi + pad;
        if (floorZero && lo >= 0) a = Math.max(0, a);
        const step = SHARED_UTILS._wizardNiceStepPositive(Math.max(b - a, coreSpan * 0.08), 6);
        let min = Math.floor(a / step) * step;
        let max = Math.ceil(b / step) * step;
        if (floorZero && lo >= 0 && min < 0) min = 0;
        if (!(max > min)) max = min + step;
        return { min, max, step };
    },

    /** VGPC.chart_data on card JSON: [timestampMs, valueCents][] */
    pricechartingChartData(card) {
        const d = card && card.pricecharting_chart_data;
        return d && typeof d === 'object' ? d : null;
    },

    pricechartingCentsToUsd(cents) {
        const n = Number(cents);
        if (!Number.isFinite(n) || n <= 0) return null;
        return n / 100;
    },

    /** Median of positive “used” (ungraded) snapshots from PriceCharting chart_data, in USD. */
    pricechartingHistoryPositiveUsdMedian(card) {
        const d = SHARED_UTILS.pricechartingChartData(card);
        const used = d && Array.isArray(d.used) ? d.used : [];
        const vals = [];
        for (let i = 0; i < used.length; i++) {
            const pt = used[i];
            if (!Array.isArray(pt) || pt.length < 2) continue;
            const u = SHARED_UTILS.pricechartingCentsToUsd(pt[1]);
            if (u != null) vals.push(u);
        }
        return vals.length ? SHARED_UTILS.medianArray(vals) : null;
    },

    /** Highest chase-tier slab quote from `pricecharting_grade_prices` (excludes BGS 10 Black / “black label” rows). */
    pricechartingChaseGradeUsd(card) {
        const gp = card && card.pricecharting_grade_prices;
        if (!gp || typeof gp !== 'object') return null;
        let best = null;
        for (const [label, raw] of Object.entries(gp)) {
            const lab = String(label || '').trim();
            if (!lab) continue;
            const low = lab.toLowerCase();
            if (/\bblack\b/.test(low)) continue;
            const n = Number(raw);
            if (!Number.isFinite(n) || n <= 0) continue;
            const ok =
                /^psa\s*10$/i.test(lab) ||
                /^bgs\s*10$/i.test(lab) ||
                /^cgc\s*10$/i.test(lab) ||
                /^tag\s*10$/i.test(lab) ||
                /^grade\s*9\.5$/i.test(lab) ||
                /^ace\s*10$/i.test(lab) ||
                /^sgc\s*10$/i.test(lab) ||
                (/cgc/i.test(lab) && /pristine/i.test(lab));
            if (!ok) continue;
            best = best == null ? n : Math.max(best, n);
        }
        return best;
    },

    /**
     * Sold-comp anchor for calibrating predictor NM$: PriceCharting used + history median, reconciled with
     * the same Explorer-style median (`resolveExplorerChartUsd`) when PC and listings violently disagree.
     * Chase-tier slab quotes only lift the anchor modestly above that NM base — PSA10÷2.05 must not
     * override a $175 market row when PC “used” is stale at ~$14.
     */
    predictorPcAnchorUsd(card) {
        if (!card) return null;
        const used = Number(card.pricecharting_used_price_usd);
        const pch = SHARED_UTILS.pricechartingHistoryPositiveUsdMedian(card);
        const nmParts = [];
        if (Number.isFinite(used) && used > 0) nmParts.push(used);
        if (pch != null && pch > 0) nmParts.push(pch);
        let pcNm = nmParts.length
            ? SHARED_UTILS.medianArray(SHARED_UTILS.priceDedupForMedian(nmParts))
            : null;

        const blendHint = SHARED_UTILS.resolveExplorerChartUsd(card);
        let base = pcNm;
        if (blendHint != null && blendHint > 0) {
            if (base == null || base <= 0) {
                base = blendHint;
            } else {
                const lo = Math.min(base, blendHint);
                const hi = Math.max(base, blendHint);
                if (hi / lo > 4) base = blendHint;
                else base = SHARED_UTILS.medianArray([base, blendHint]);
            }
        }

        if (base == null || base <= 0 || !Number.isFinite(base)) return null;

        const chase = SHARED_UTILS.pricechartingChaseGradeUsd(card);
        const fromSlabs = chase != null && chase > 0 ? chase / 2.05 : null;
        if (fromSlabs == null || !Number.isFinite(fromSlabs) || fromSlabs <= 0) return base;

        const maxSlabLift = base * 2.25;
        if (fromSlabs <= maxSlabLift) return Math.max(base, fromSlabs);
        return base;
    },

    /**
     * When composite LSRL collapses far below PriceCharting sold comps, blend in log space toward `predictorPcAnchorUsd`.
     * Does not replace the regression long-term — rescues obvious collector / thin-listing misses on this page.
     */
    predictorCalibrateUsd(card, rawModelUsd) {
        const anchor = SHARED_UTILS.predictorPcAnchorUsd(card);
        if (anchor == null || !Number.isFinite(rawModelUsd) || rawModelUsd <= 0) {
            return { final: rawModelUsd, raw: rawModelUsd, blended: false };
        }
        const r = anchor / rawModelUsd;
        if (r < 1.55) return { final: rawModelUsd, raw: rawModelUsd, blended: false, anchor };
        const t = Math.min(0.94, Math.log10(r) / 1.48);
        const lf = Math.log10(rawModelUsd) * (1 - t) + Math.log10(anchor) * t;
        return { final: Math.pow(10, lf), raw: rawModelUsd, blended: true, anchor, t };
    },

    /** Approx. % change from first to last positive “used” point in chart_data (sold comps). */
    pricechartingUsedShortTrendPct(card) {
        const d = SHARED_UTILS.pricechartingChartData(card);
        if (!d || !Array.isArray(d.used) || d.used.length < 2) return null;
        const pts = d.used
            .filter((pt) => Array.isArray(pt) && pt.length >= 2 && Number(pt[1]) > 0)
            .slice()
            .sort((a, b) => Number(a[0]) - Number(b[0]));
        if (pts.length < 2) return null;
        const a = SHARED_UTILS.pricechartingCentsToUsd(pts[0][1]);
        const b = SHARED_UTILS.pricechartingCentsToUsd(pts[pts.length - 1][1]);
        if (a == null || b == null || a <= 0) return null;
        return ((b - a) / a) * 100;
    },

    /** Sorted timestamps (ms) that have at least one positive cents sample in selected series. */
    pricechartingChartSortedTimestamps(card) {
        const d = SHARED_UTILS.pricechartingChartData(card);
        if (!d) return [];
        const keys = ['used', 'graded', 'new', 'complete', 'boxonly', 'cib'];
        const ts = new Set();
        keys.forEach((k) => {
            const arr = d[k];
            if (!Array.isArray(arr)) return;
            arr.forEach((pt) => {
                if (!Array.isArray(pt) || pt.length < 2) return;
                const c = Number(pt[1]);
                if (Number.isFinite(c) && c > 0) ts.add(Number(pt[0]));
            });
        });
        return [...ts].sort((a, b) => a - b);
    },

    isEbaySoldListingGradedTitle(title) {
        const lower = String(title || '').toLowerCase();
        return ['psa', 'cgc', 'bgs', 'beckett', 'ace', 'pca', 'graded', 'gem mint'].some((kw) => lower.includes(kw));
    },

    /** Merge HTML scrape / Finding rows with optional `ebay_sold_observations` (e.g. ScrapeChain ingest); de-dupe. */
    ebaySoldRowsDeduped(card) {
        if (!card || typeof card !== 'object') return [];
        const ug = Array.isArray(card.ebay_sold_history_ungraded) ? card.ebay_sold_history_ungraded : [];
        const gr = Array.isArray(card.ebay_sold_history_graded) ? card.ebay_sold_history_graded : [];
        const obs = Array.isArray(card.ebay_sold_observations) ? card.ebay_sold_observations : [];
        const rows = [...ug, ...gr, ...obs].filter((r) => r && typeof r === 'object');
        const seen = new Set();
        const out = [];
        rows.forEach((r) => {
            const dk = SHARED_UTILS.normalizeEbaySoldDateKey(r.date) || String(r.date || '').trim().slice(0, 10);
            const p = Number(r.price);
            const key = `${dk}|${Number.isFinite(p) ? p : 'x'}|${String(r.title || '').slice(0, 140)}`;
            if (seen.has(key)) return;
            seen.add(key);
            out.push(r);
        });
        return out;
    },

    buildEbaySoldChartJs(card, opts) {
        const all = SHARED_UTILS.ebaySoldRowsDeduped(card);
        const ungraded = all.filter((r) => !SHARED_UTILS.isEbaySoldListingGradedTitle(r.title));
        const graded = all.filter((r) => SHARED_UTILS.isEbaySoldListingGradedTitle(r.title));
        if (!ungraded.length && !graded.length) return null;

        const rowDay = (r) => SHARED_UTILS.normalizeEbaySoldDateKey(r.date) || String(r.date || '').trim().slice(0, 10);

        // Collect all unique dates
        const dateSet = new Set();
        ungraded.forEach((r) => dateSet.add(rowDay(r)));
        graded.forEach((r) => dateSet.add(rowDay(r)));
        
        let sortedDates = Array.from(dateSet).sort();
        
        // Apply maxHistoryMs filter if provided
        const maxMs = opts && opts.maxHistoryMs != null ? Number(opts.maxHistoryMs) : 62 * 86400000;
        if (sortedDates.length >= 2 && Number.isFinite(maxMs) && maxMs > 0) {
            const newestStr = sortedDates[sortedDates.length - 1];
            const newestMs = new Date(newestStr).getTime();
            const cutoff = newestMs - maxMs;
            const filtered = sortedDates.filter(d => new Date(d).getTime() >= cutoff);
            if (filtered.length >= 2) sortedDates = filtered;
        }

        const labels = sortedDates.map(d => {
            // "2026-03-30" -> "Mar 30, 2026"
            const dt = new Date(d);
            return dt.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
        });

        // Average prices if multiple sales on the same date
        const mapAvg = (arr) => {
            const m = new Map();
            const counts = new Map();
            arr.forEach((r) => {
                const d = rowDay(r);
                const prev = m.get(d) || 0;
                m.set(d, prev + r.price);
                counts.set(d, (counts.get(d) || 0) + 1);
            });
            const avgMap = new Map();
            for (const [date, total] of m.entries()) {
                avgMap.set(date, Number((total / counts.get(date)).toFixed(2)));
            }
            return avgMap;
        };

        const uMap = mapAvg(ungraded);
        const gMap = mapAvg(graded);

        const datasets = [];
        
        const uData = sortedDates.map(d => uMap.has(d) ? uMap.get(d) : null);
        if (uData.some(v => v != null)) {
            datasets.push({
                label: 'Ungraded (used)',
                data: uData,
                borderColor: '#f472b6',
                backgroundColor: 'rgba(244,114,182,0.12)',
                tension: 0.25,
                spanGaps: true,
            });
        }
        
        const gData = sortedDates.map(d => gMap.has(d) ? gMap.get(d) : null);
        if (gData.some(v => v != null)) {
            datasets.push({
                label: 'Graded',
                data: gData,
                borderColor: '#fb923c',
                backgroundColor: 'rgba(251,146,60,0.12)',
                tension: 0.25,
                spanGaps: true,
            });
        }
        
        if (!datasets.length) return null;
        return { labels, datasets };
    },

    /** Normalize eBay sold row `date` to YYYY-MM-DD for daily bucketing. */
    normalizeEbaySoldDateKey(d) {
        if (d == null) return '';
        const s = String(d).trim();
        if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
        const t = Date.parse(s);
        if (Number.isFinite(t)) return new Date(t).toISOString().slice(0, 10);
        return '';
    },

    /** @param {number[]} arr */
    _medianSortedNums(arr) {
        const a = arr.filter((x) => Number.isFinite(x)).slice().sort((x, y) => x - y);
        if (!a.length) return null;
        const mid = Math.floor(a.length / 2);
        return a.length % 2 === 1 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
    },

    /**
     * Daily eBay sold volume (count) + median USD price for mixed bar/line chart.
     * @param {object} [opts]
     * @param {number} [opts.maxHistoryMs] Default ~62 days from newest sale.
     * @returns {{ labels: string[], volumes: number[], medians: (number|null)[], dateKeys: string[] }|null}
     */
    buildEbaySoldDailyVolumeMedianChartData(card, opts) {
        const rows = SHARED_UTILS.ebaySoldRowsDeduped(card);
        if (!rows.length) return null;

        const byDay = new Map();
        rows.forEach((r) => {
            const dk = SHARED_UTILS.normalizeEbaySoldDateKey(r.date);
            if (!dk) return;
            const p = Number(r.price);
            if (!Number.isFinite(p) || p <= 0) return;
            if (!byDay.has(dk)) byDay.set(dk, []);
            byDay.get(dk).push(p);
        });
        if (!byDay.size) return null;

        let sortedKeys = [...byDay.keys()].sort();
        const maxMs = opts && opts.maxHistoryMs != null ? Number(opts.maxHistoryMs) : 62 * 86400000;
        if (sortedKeys.length >= 2 && Number.isFinite(maxMs) && maxMs > 0) {
            const newestMs = Date.parse(sortedKeys[sortedKeys.length - 1]);
            if (Number.isFinite(newestMs)) {
                const cutoff = newestMs - maxMs;
                const filtered = sortedKeys.filter((k) => Date.parse(k) >= cutoff);
                if (filtered.length >= 1) sortedKeys = filtered;
            }
        }

        const labels = sortedKeys.map((d) => {
            const dt = new Date(d + 'T12:00:00');
            return dt.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
        });
        const volumes = sortedKeys.map((k) => (byDay.get(k) || []).length);
        const medians = sortedKeys.map((k) => SHARED_UTILS._medianSortedNums(byDay.get(k) || []));
        if (!volumes.some((v) => v > 0)) return null;
        return { labels, volumes, medians, dateKeys: sortedKeys };
    },

    /**
     * Mixed Chart.js: rounded bars (sales/day) + line (median USD / day).
     * @param {HTMLCanvasElement|null} canvasEl
     * @param {object[]} containerCharts Push `new Chart(...)` here for teardown.
     */
    mountEbaySoldDailyVolumeMedianChart(canvasEl, card, containerCharts) {
        if (!canvasEl || typeof Chart === 'undefined') return;
        const orphan = typeof Chart !== 'undefined' && typeof Chart.getChart === 'function' ? Chart.getChart(canvasEl) : null;
        if (orphan) {
            try {
                orphan.destroy();
            } catch (e) { /* ignore */ }
        }
        const pack = SHARED_UTILS.buildEbaySoldDailyVolumeMedianChartData(card, { maxHistoryMs: 62 * 86400000 });
        if (!pack || !pack.labels.length) return;
        const { tickColor, gridColor } = SHARED_UTILS.getChartAxisColors();
        const baseScale = { ticks: { color: tickColor }, grid: { color: gridColor } };
        containerCharts.push(new Chart(canvasEl, {
            type: 'bar',
            data: {
                labels: pack.labels,
                datasets: [
                    {
                        type: 'bar',
                        label: 'Sales / day',
                        data: pack.volumes,
                        yAxisID: 'y',
                        order: 2,
                        borderRadius: 8,
                        borderSkipped: false,
                        backgroundColor: 'rgba(16,185,129,0.45)',
                        borderColor: 'rgba(52,211,153,0.9)',
                        borderWidth: 1,
                        maxBarThickness: 36,
                    },
                    {
                        type: 'line',
                        label: 'Median price (USD)',
                        data: pack.medians,
                        yAxisID: 'y1',
                        order: 1,
                        borderColor: '#fbbf24',
                        backgroundColor: 'rgba(251,191,36,0.08)',
                        borderWidth: 2,
                        tension: 0.25,
                        spanGaps: true,
                        pointRadius: 3,
                        pointBackgroundColor: '#fde68a',
                        pointBorderColor: '#f59e0b',
                        fill: false,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        labels: { color: tickColor, usePointStyle: true, boxWidth: 8 },
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                    },
                },
                scales: {
                    x: {
                        ...baseScale,
                        ticks: { ...baseScale.ticks, maxRotation: 45, autoSkip: true, maxTicksLimit: 18 },
                    },
                    y: {
                        ...baseScale,
                        position: 'left',
                        beginAtZero: true,
                        title: { display: true, text: 'Sales count', color: tickColor },
                        ticks: { ...baseScale.ticks, precision: 0 },
                    },
                    y1: {
                        position: 'right',
                        ticks: { color: '#fcd34d' },
                        grid: { drawOnChartArea: false },
                        title: { display: true, text: 'Median USD', color: '#fcd34d' },
                    },
                },
            },
        }));
    },

    /**
     * Pokémon Wizard: one canvas — retail price (line) and summary % (bars) share the x-axis when both exist.
     * @param {HTMLCanvasElement|null} canvasEl
     * @param {object} card
     * @param {object[]} containerCharts Push `new Chart(...)` for teardown.
     */
    mountPokemonWizardRetailTrendChart(canvasEl, card, containerCharts) {
        if (!canvasEl || typeof Chart === 'undefined') return;
        const { tickColor, gridColor } = SHARED_UTILS.getChartAxisColors();
        const baseScale = { ticks: { color: tickColor }, grid: { color: gridColor } };
        const wizLayout = { padding: { left: 2, right: 4, top: 4, bottom: 0 } };
        const fmtUsdTick = (v) => {
            if (!Number.isFinite(v)) return '';
            const n = Number(v);
            if (Math.abs(n) >= 100) return `$${Math.round(n)}`;
            if (Math.abs(n) >= 10) return `$${n.toFixed(n % 1 === 0 ? 0 : 1)}`;
            return `$${n.toFixed(2)}`;
        };
        const fmtPctTick = (v) => {
            if (!Number.isFinite(v)) return '';
            const n = Number(v);
            if (Math.abs(n) >= 10 || Math.abs(n - Math.round(n)) < 1e-6) return `${Math.round(n)}%`;
            return `${n % 1 === 0 ? n.toFixed(0) : n.toFixed(1)}%`;
        };

        const wiz = SHARED_UTILS.filterWizardPriceHistoryRows(card.pokemon_wizard_price_history || []).slice().reverse();
        const { labels: barLabs, vals: barVals } = SHARED_UTILS.wizardSummaryPctBars(card);
        const hasWizUrl = Boolean(card && card.pokemon_wizard_url);
        const hasPct = hasWizUrl && barLabs.length > 0;
        const priceLabels = wiz.map((r) => String(r.label || r.sort_key || '').trim());
        const prices = wiz.map((r) => (r.price_usd != null && Number.isFinite(Number(r.price_usd)) ? Number(r.price_usd) : null));
        const finitePrices = prices.filter((p) => p != null && Number.isFinite(p));
        const hasAnyPrice = finitePrices.length > 0;
        const hasMultiPrice = wiz.length > 1;
        const allUsdNonNeg = finitePrices.length > 0 && finitePrices.every((p) => p >= 0);

        if (hasMultiPrice && !hasPct) {
            if (!finitePrices.length) return;
            const lo = Math.min(...finitePrices);
            const hi = Math.max(...finitePrices);
            const usdAx = SHARED_UTILS._wizardNiceAxisEnds(lo, hi, { padFrac: 0.055, floorZero: allUsdNonNeg });
            const yUsd = usdAx
                ? {
                    ...baseScale,
                    min: usdAx.min,
                    max: usdAx.max,
                    title: { display: true, text: 'USD', color: tickColor },
                    ticks: {
                        ...baseScale.ticks,
                        stepSize: usdAx.step,
                        callback: (v) => fmtUsdTick(v),
                    },
                }
                : { ...baseScale, title: { display: true, text: 'USD', color: tickColor } };

            containerCharts.push(new Chart(canvasEl, {
                type: 'line',
                data: {
                    labels: priceLabels,
                    datasets: [{
                        label: 'Wizard price (USD)',
                        data: prices,
                        borderColor: '#a78bfa',
                        backgroundColor: 'rgba(167,139,250,0.15)',
                        fill: true,
                        tension: 0.3,
                        spanGaps: true,
                    }],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    layout: wizLayout,
                    plugins: {
                        legend: { labels: { color: tickColor, usePointStyle: true, boxWidth: 8 } },
                        tooltip: { intersect: false, mode: 'index' },
                    },
                    scales: {
                        x: {
                            ...baseScale,
                            offset: false,
                            ticks: { ...baseScale.ticks, maxRotation: 45, autoSkip: true, maxTicksLimit: 20 },
                        },
                        y: yUsd,
                    },
                },
            }));
            return;
        }

        if (!hasPct) return;

        if (!hasAnyPrice) {
            const lo = Math.min(...barVals);
            const hi = Math.max(...barVals);
            const pctAx = SHARED_UTILS._wizardNiceAxisEnds(lo, hi, { padFrac: 0.06, floorZero: false });
            const yPct = pctAx
                ? {
                    ...baseScale,
                    min: pctAx.min,
                    max: pctAx.max,
                    title: { display: true, text: '%', color: tickColor },
                    ticks: {
                        ...baseScale.ticks,
                        stepSize: pctAx.step,
                        callback: (v) => fmtPctTick(v),
                    },
                }
                : { ...baseScale, title: { display: true, text: '%', color: tickColor } };

            containerCharts.push(new Chart(canvasEl, {
                type: 'bar',
                data: {
                    labels: barLabs,
                    datasets: [{
                        label: '% change',
                        data: barVals,
                        backgroundColor: barVals.map((v) => (v >= 0 ? 'rgba(16,185,129,0.65)' : 'rgba(248,113,113,0.65)')),
                        borderColor: barVals.map((v) => (v >= 0 ? '#10b981' : '#f87171')),
                        borderWidth: 1,
                        maxBarThickness: 34,
                    }],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    layout: wizLayout,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: {
                            ...baseScale,
                            offset: false,
                            ticks: { ...baseScale.ticks, maxRotation: 40, autoSkip: true, maxTicksLimit: 12 },
                        },
                        y: yPct,
                    },
                },
            }));
            return;
        }

        const combinedLabels = [...priceLabels, ...barLabs];
        const priceData = [...prices, ...barLabs.map(() => null)];
        const pctData = [...wiz.map(() => null), ...barVals];
        const barBg = pctData.map((v) => (v == null || !Number.isFinite(v) ? 'transparent' : (v >= 0 ? 'rgba(16,185,129,0.58)' : 'rgba(248,113,113,0.58)')));
        const barBorder = pctData.map((v) => (v == null || !Number.isFinite(v) ? 'transparent' : (v >= 0 ? '#10b981' : '#f87171')));

        const loP = Math.min(...finitePrices);
        const hiP = Math.max(...finitePrices);
        const usdAx = SHARED_UTILS._wizardNiceAxisEnds(loP, hiP, { padFrac: 0.055, floorZero: allUsdNonNeg });

        const pctAll = barVals.filter((v) => Number.isFinite(v));
        const lo1 = Math.min(...pctAll);
        const hi1 = Math.max(...pctAll);
        const pctAx = SHARED_UTILS._wizardNiceAxisEnds(lo1, hi1, { padFrac: 0.055, floorZero: false });

        const yUsdMixed = usdAx
            ? {
                ...baseScale,
                position: 'left',
                min: usdAx.min,
                max: usdAx.max,
                title: { display: true, text: 'USD', color: tickColor },
                ticks: {
                    ...baseScale.ticks,
                    stepSize: usdAx.step,
                    callback: (v) => fmtUsdTick(v),
                },
            }
            : {
                ...baseScale,
                position: 'left',
                title: { display: true, text: 'USD', color: tickColor },
            };

        const y1Mixed = pctAx
            ? {
                position: 'right',
                min: pctAx.min,
                max: pctAx.max,
                ticks: {
                    color: '#86efac',
                    stepSize: pctAx.step,
                    callback: (v) => fmtPctTick(v),
                },
                grid: { drawOnChartArea: false },
                title: { display: true, text: '% change', color: '#86efac' },
            }
            : {
                position: 'right',
                ticks: { color: '#86efac' },
                grid: { drawOnChartArea: false },
                title: { display: true, text: '% change', color: '#86efac' },
            };

        containerCharts.push(new Chart(canvasEl, {
            type: 'bar',
            data: {
                labels: combinedLabels,
                datasets: [
                    {
                        type: 'line',
                        label: 'Retail (USD)',
                        data: priceData,
                        yAxisID: 'y',
                        borderColor: '#a78bfa',
                        backgroundColor: 'rgba(167,139,250,0.12)',
                        fill: true,
                        tension: 0.3,
                        spanGaps: false,
                        order: 1,
                    },
                    {
                        type: 'bar',
                        label: '% vs prior',
                        data: pctData,
                        yAxisID: 'y1',
                        backgroundColor: barBg,
                        borderColor: barBorder,
                        borderWidth: 1,
                        maxBarThickness: 34,
                        order: 2,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: wizLayout,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { labels: { color: tickColor, usePointStyle: true, boxWidth: 8 } },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        filter: (item) => item.raw != null && Number.isFinite(item.raw),
                        callbacks: {
                            label(ctx) {
                                const v = ctx.raw;
                                if (ctx.dataset.label === 'Retail (USD)') {
                                    return `Retail: $${v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
                                }
                                return `% vs prior: ${v >= 0 ? '+' : ''}${v.toFixed(1)}%`;
                            },
                        },
                    },
                },
                scales: {
                    x: {
                        ...baseScale,
                        offset: false,
                        ticks: { ...baseScale.ticks, maxRotation: 40, autoSkip: true, maxTicksLimit: 22 },
                    },
                    y: yUsdMixed,
                    y1: y1Mixed,
                },
            },
        }));
    },

    /** Labels + datasets for Chart.js (timestamps aligned). @param {object} [opts] @param {number} [opts.maxHistoryMs] Default ~62 days of snapshots. */
    buildPricechartingChartJs(card, opts) {
        const d = SHARED_UTILS.pricechartingChartData(card);
        if (!d) return null;
        let sortedTs = SHARED_UTILS.pricechartingChartSortedTimestamps(card);
        if (!sortedTs.length) return null;
        const maxMs = opts && opts.maxHistoryMs != null ? Number(opts.maxHistoryMs) : 62 * 86400000;
        if (sortedTs.length >= 2 && Number.isFinite(maxMs) && maxMs > 0) {
            const newest = sortedTs[sortedTs.length - 1];
            const cutoff = newest - maxMs;
            const filtered = sortedTs.filter((t) => t >= cutoff);
            if (filtered.length >= 2) {
                sortedTs = filtered;
            }
        }
        const labels = sortedTs.map((t) =>
            new Date(t).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' }),
        );
        const seriesDefs = [
            { key: 'used', label: 'Ungraded (used)', color: '#f472b6', fill: 'rgba(244,114,182,0.12)' },
            { key: 'graded', label: 'Graded', color: '#fb923c', fill: 'rgba(251,146,60,0.12)' },
            { key: 'boxonly', label: 'Box only', color: '#94a3b8', fill: 'rgba(148,163,184,0.1)' },
        ];
        const mapFrom = (arr) => {
            const m = new Map();
            if (!Array.isArray(arr)) return m;
            arr.forEach((pt) => {
                if (!Array.isArray(pt) || pt.length < 2) return;
                const u = SHARED_UTILS.pricechartingCentsToUsd(pt[1]);
                if (u == null) return;
                m.set(Number(pt[0]), u);
            });
            return m;
        };
        const datasets = [];
        seriesDefs.forEach(({ key, label, color, fill }) => {
            const m = mapFrom(d[key]);
            const data = sortedTs.map((t) => (m.has(t) ? m.get(t) : null));
            if (!data.some((v) => v != null)) return;
            datasets.push({
                label,
                data,
                borderColor: color,
                backgroundColor: fill,
                tension: 0.25,
                spanGaps: true,
            });
        });
        if (!datasets.length) return null;
        return { labels, datasets };
    },

    /** Scrollable mini-table: snapshot dates vs ungraded / graded / box-only (USD). */
    buildPricechartingSnapshotTableHtml(card) {
        if (!card || !card.pricecharting_url) return '';
        const d = SHARED_UTILS.pricechartingChartData(card);
        if (!d) return '';
        const pcEsc = SHARED_UTILS.escHtml;
        const pcFmt = SHARED_UTILS.fmtUsd;
        const valAt = (key, tms) => {
            if (!Array.isArray(d[key])) return null;
            for (let i = 0; i < d[key].length; i++) {
                const pt = d[key][i];
                if (Array.isArray(pt) && pt.length >= 2 && Number(pt[0]) === tms) {
                    return SHARED_UTILS.pricechartingCentsToUsd(pt[1]);
                }
            }
            return null;
        };
        let histRowsPc = '';
        const tsList = SHARED_UTILS.pricechartingChartSortedTimestamps(card);
        tsList.forEach((tms) => {
            const ds = new Date(tms).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
            const vu = valAt('used', tms);
            const vg = valAt('graded', tms);
            const vb = valAt('boxonly', tms);
            histRowsPc += `<tr><td>${pcEsc(ds)}</td><td>${vu != null ? pcFmt(vu) : '—'}</td><td>${vg != null ? pcFmt(vg) : '—'}</td><td>${vb != null ? pcFmt(vb) : '—'}</td></tr>`;
        });
        if (!histRowsPc) return '';
        return `<div class="card-detail-table-scroll"><table class="card-detail-mini-table"><thead><tr><th>Snapshot</th><th>Ungraded</th><th>Graded</th><th>Box only</th></tr></thead><tbody>${histRowsPc}</tbody></table></div>`;
    },

    wizardSummaryPctBars(card) {
        const labels = [];
        const vals = [];
        if (!card) return { labels, vals };
        const push = (lbl, v) => {
            if (v == null || !Number.isFinite(Number(v))) return;
            labels.push(lbl);
            vals.push(Number(v));
        };
        push('Current', card.pokemon_wizard_current_trend_pct);
        push('7d', card.pokemon_wizard_last_7d_pct);
        push('30d', card.pokemon_wizard_last_30d_pct);
        push('YTD', card.pokemon_wizard_ytd_pct);
        return { labels, vals };
    },

    /** eBay web search URL for the same Browse query (all hits), from sync or rebuilt from `ebay_browse_query`. */
    ebaySchSearchUrlFromCard(card) {
        if (!card) return '';
        const su = card.ebay_browse_search_url;
        if (su && String(su).trim()) return String(su).trim();
        const q = card.ebay_browse_query;
        if (q && String(q).trim()) {
            return `https://www.ebay.com/sch/i.html?_nkw=${encodeURIComponent(String(q).trim())}`;
        }
        return '';
    },

    /** Numeric Collectrics `ended_avg_*` fields on one history row (sold comps by grade). */
    collectricsEbayRowEndedPriceNums(row) {
        if (!row || typeof row !== 'object') return [];
        const nums = [];
        Object.keys(row).forEach((k) => {
            if (!/^ended_avg_/i.test(k)) return;
            const n = Number(row[k]);
            if (Number.isFinite(n) && n > 0) nums.push(n);
        });
        return nums;
    },

    _compressCollectricsEbayByWeekChunks(points) {
        if (!Array.isArray(points) || points.length <= 40) return points;
        const out = [];
        for (let i = 0; i < points.length; i += 7) {
            const chunk = points.slice(i, i + 7);
            if (!chunk.length) continue;
            const lo = Math.min(...chunk.map((c) => c.lo));
            const hi = Math.max(...chunk.map((c) => c.hi));
            const lastLabel = chunk[chunk.length - 1].label;
            out.push({ label: lastLabel, lo, hi });
        }
        return out.length >= 2 ? out : points;
    },

    /**
     * Daily (or 7-day chunked) high/low band from Collectrics `collectrics_history_ebay` ended averages.
     * Last ~62 calendar days when dates parse; else last 62 rows.
     */
    collectricsEbayEndedRangeSeries(card) {
        const rows = Array.isArray(card.collectrics_history_ebay) ? card.collectrics_history_ebay : [];
        if (rows.length < 2) return null;
        const sortedAll = rows
            .filter((r) => r && r.date)
            .slice()
            .sort((a, b) => String(a.date).localeCompare(String(b.date)));
        if (sortedAll.length < 2) return null;
        const parse = (s) => {
            const t = Date.parse(String(s || '').slice(0, 10));
            return Number.isFinite(t) ? t : null;
        };
        const tLast = parse(sortedAll[sortedAll.length - 1].date);
        const maxDays = 62;
        const cutoff = tLast != null ? tLast - maxDays * 86400000 : null;
        let sorted = sortedAll;
        if (cutoff != null) {
            const filtered = sortedAll.filter((r) => {
                const t = parse(r.date);
                return t == null || t >= cutoff;
            });
            if (filtered.length >= 2) sorted = filtered;
            else sorted = sortedAll.slice(-Math.min(62, sortedAll.length));
        }
        const points = [];
        sorted.forEach((r) => {
            let nums = SHARED_UTILS.collectricsEbayRowEndedPriceNums(r);
            if (!nums.length) {
                const v = Number(r.ended_avg_raw_price);
                if (Number.isFinite(v) && v > 0) nums = [v];
            }
            if (!nums.length) return;
            const lo = Math.min(...nums);
            const hi = Math.max(...nums);
            points.push({ label: String(r.date || '').slice(0, 10), lo, hi });
        });
        if (points.length < 2) return null;
        const compressed = SHARED_UTILS._compressCollectricsEbayByWeekChunks(points);
        if (compressed.length < 2) return null;
        return {
            labels: compressed.map((p) => p.label),
            low: compressed.map((p) => p.lo),
            high: compressed.map((p) => p.hi),
        };
    },

    /** First numeric Collectrics eBay-market field among candidate keys (API-normalized snake_case). */
    collectricsEbayMarketNum(row, ...keys) {
        if (!row || typeof row !== 'object') return null;
        for (let i = 0; i < keys.length; i++) {
            const k = keys[i];
            if (k == null || !(k in row) || row[k] == null) continue;
            const n = Number(row[k]);
            if (Number.isFinite(n)) return n;
        }
        return null;
    },

    /** Sorted `collectrics_history_ebay_market` rows, last ~`maxDays` calendar days (fallback: tail rows). */
    collectricsEbayMarketHistorySorted(card, maxDays = 62, minRows = 2) {
        const rows = Array.isArray(card.collectrics_history_ebay_market) ? card.collectrics_history_ebay_market : [];
        if (rows.length < minRows) return [];
        const sortedAll = rows
            .filter((r) => r && r.date)
            .slice()
            .sort((a, b) => String(a.date).localeCompare(String(b.date)));
        if (sortedAll.length < minRows) return [];
        const parse = (s) => {
            const t = Date.parse(String(s || '').slice(0, 10));
            return Number.isFinite(t) ? t : null;
        };
        const tLast = parse(sortedAll[sortedAll.length - 1].date);
        const cutoff = tLast != null ? tLast - maxDays * 86400000 : null;
        let sorted = sortedAll;
        if (cutoff != null) {
            const filtered = sortedAll.filter((r) => {
                const t = parse(r.date);
                return t == null || t >= cutoff;
            });
            if (filtered.length >= minRows) sorted = filtered;
            else sorted = sortedAll.slice(-Math.min(maxDays, sortedAll.length));
        }
        return sorted;
    },

    collectricsEbayMarketVolumeUsable(rows) {
        if (!Array.isArray(rows) || rows.length < 2) return false;
        let hasAct = false;
        let hasEnd = false;
        rows.forEach((r) => {
            const a = SHARED_UTILS.collectricsEbayMarketNum(r, 'active_to', 'activeTo');
            const e = SHARED_UTILS.collectricsEbayMarketNum(r, 'ended');
            if (a != null && a >= 0) hasAct = true;
            if (e != null && e >= 0) hasEnd = true;
        });
        return hasAct || hasEnd;
    },

    collectricsEbayMarketDynamicsUsable(rows) {
        if (!Array.isArray(rows) || rows.length < 1) return false;
        return rows.some((r) => {
            const a = SHARED_UTILS.collectricsEbayMarketNum(r, 'demand_pressure_observed', 'demandPressureObserved');
            const b = SHARED_UTILS.collectricsEbayMarketNum(r, 'demand_pressure_est', 'demandPressureEst');
            const c = SHARED_UTILS.collectricsEbayMarketNum(r, 'sold_rate_est', 'soldRateEst');
            return (a != null && a >= 0) || (b != null && b >= 0) || (c != null && c >= 0);
        });
    },

    /** Snapshot or history row for KPI / gauge panel. */
    collectricsEbayMarketDynamicsPanelUsable(card) {
        if (!card) return false;
        const snap = card.collectrics_ebay_market_snapshot;
        if (snap && typeof snap === 'object') {
            const a = SHARED_UTILS.collectricsEbayMarketNum(snap, 'active_to', 'activeTo', 'active-to');
            if (a != null && a >= 0) return true;
        }
        const rows = SHARED_UTILS.collectricsEbayMarketHistorySorted(card, 62, 1);
        return SHARED_UTILS.collectricsEbayMarketDynamicsUsable(rows);
    },

    collectricsEbayMarketFindRowNearDays(rows, daysBack) {
        if (!Array.isArray(rows) || rows.length < 2) return null;
        const last = rows[rows.length - 1];
        const tLast = Date.parse(String(last.date || '').slice(0, 10));
        if (!Number.isFinite(tLast)) return null;
        const target = tLast - daysBack * 86400000;
        let best = null;
        let bestD = Infinity;
        rows.forEach((r) => {
            const t = Date.parse(String(r.date || '').slice(0, 10));
            if (!Number.isFinite(t)) return;
            const d = Math.abs(t - target);
            if (d < bestD) {
                bestD = d;
                best = r;
            }
        });
        return best;
    },

    collectricsEbayMarketPctChip(now, prev) {
        const n = Number(now);
        const p = Number(prev);
        if (!Number.isFinite(n) || !Number.isFinite(p) || p === 0) return '';
        const pct = ((n - p) / p) * 100;
        const s = `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}% vs 30d`;
        const cls = pct >= 0 ? 'ce-dyn-kpi__chip ce-dyn-kpi__chip--up' : 'ce-dyn-kpi__chip ce-dyn-kpi__chip--down';
        return `<span class="${cls}">${SHARED_UTILS.escHtml(s)}</span>`;
    },

    /** Collectrics-style KPI row + linear gauges (HTML). */
    collectricsEbayMarketDynamicsPanelHtml(card) {
        if (!card) return '';
        const esc = SHARED_UTILS.escHtml;
        const rows = SHARED_UTILS.collectricsEbayMarketHistorySorted(card, 62, 1);
        const snap = card.collectrics_ebay_market_snapshot && typeof card.collectrics_ebay_market_snapshot === 'object'
            ? card.collectrics_ebay_market_snapshot
            : null;
        const last = (rows.length && rows[rows.length - 1]) || snap;
        if (!last) return '';
        const row30 = rows.length >= 2 ? SHARED_UTILS.collectricsEbayMarketFindRowNearDays(rows, 28) : null;

        const active = SHARED_UTILS.collectricsEbayMarketNum(last, 'active_to', 'activeTo', 'active-to');
        const neu = SHARED_UTILS.collectricsEbayMarketNum(last, 'new');
        const soldE = SHARED_UTILS.collectricsEbayMarketNum(last, 'sold_est', 'soldEst');
        const ended = SHARED_UTILS.collectricsEbayMarketNum(last, 'ended');
        const demand = SHARED_UTILS.collectricsEbayMarketNum(last, 'demand_pressure_observed', 'demandPressureObserved')
            ?? SHARED_UTILS.collectricsEbayMarketNum(last, 'demand_pressure_est', 'demandPressureEst');
        const soldRate = SHARED_UTILS.collectricsEbayMarketNum(last, 'sold_rate_est', 'soldRateEst');

        const fmtN = (v) => {
            if (!Number.isFinite(v)) return '—';
            const d = Math.abs(v - Math.trunc(v)) < 1e-6 ? Math.trunc(v) : Number(v.toFixed(1));
            return esc(String(d));
        };
        const act30 = row30 ? SHARED_UTILS.collectricsEbayMarketNum(row30, 'active_to', 'activeTo', 'active-to') : null;
        const new30 = row30 ? SHARED_UTILS.collectricsEbayMarketNum(row30, 'new') : null;
        const sold30 = row30 ? (SHARED_UTILS.collectricsEbayMarketNum(row30, 'sold_est', 'soldEst') ?? SHARED_UTILS.collectricsEbayMarketNum(row30, 'ended')) : null;

        const kpi = (lbl, val, chip) => `
            <div class="ce-dyn-kpi">
                <div class="ce-dyn-kpi__lbl">${esc(lbl)}</div>
                <div class="ce-dyn-kpi__val">${val}</div>
                ${chip || ''}
            </div>`;

        const needlePct = (v) => {
            const n = Number(v);
            if (!Number.isFinite(n) || n < 0) return 0;
            const x = n <= 1 ? n * 100 : Math.min(100, n * 50);
            return Math.max(0, Math.min(100, x));
        };
        const dPct = needlePct(demand);

        let gauges = '';
        if (Number.isFinite(demand) && demand >= 0) {
            gauges += `
            <div class="ce-gauge">
                <div class="ce-gauge__hdr">demand pressure</div>
                <div class="ce-gauge__track" role="presentation">
                    <div class="ce-gauge__fill"></div>
                    <div class="ce-gauge__needle" style="left:${dPct}%"></div>
                </div>
                <div class="ce-gauge__ticks"><span>heavy supply</span><span>${esc(`${(demand * 100).toFixed(1)}%`)}</span><span>very tight</span></div>
            </div>`;
        }
        let supplySatPct = null;
        let supplyShiftVal = null;
        if (row30 && Number.isFinite(ended) && ended > 0 && soldE != null && soldE >= 0) {
            const e30 = SHARED_UTILS.collectricsEbayMarketNum(row30, 'ended');
            const s30 = SHARED_UTILS.collectricsEbayMarketNum(row30, 'sold_est', 'soldEst');
            if (Number.isFinite(e30) && e30 > 0 && s30 != null && s30 >= 0) {
                const uNow = Math.max(0, ended - soldE);
                const u30 = Math.max(0, e30 - s30);
                const rNow = uNow / ended;
                const r30 = u30 / e30;
                supplyShiftVal = rNow - r30;
                supplySatPct = Math.max(0, Math.min(100, 50 + supplyShiftVal * 100));
            }
        }
        if (supplySatPct != null && supplyShiftVal != null) {
            gauges += `
            <div class="ce-gauge ce-gauge--muted">
                <div class="ce-gauge__hdr">supply saturation shift (vs 30d)</div>
                <div class="ce-gauge__track ce-gauge__track--slate" role="presentation">
                    <div class="ce-gauge__fill ce-gauge__fill--slate"></div>
                    <div class="ce-gauge__needle ce-gauge__needle--slate" style="left:${supplySatPct}%"></div>
                </div>
                <div class="ce-gauge__ticks"><span>tightening</span><span>${esc(supplyShiftVal.toFixed(2))}</span><span>loosening</span></div>
            </div>`;
        } else if (Number.isFinite(soldRate) && soldRate >= 0) {
            gauges += `
            <div class="ce-gauge ce-gauge--muted">
                <div class="ce-gauge__hdr">sold rate (est.)</div>
                <div class="ce-gauge__track ce-gauge__track--slate" role="presentation">
                    <div class="ce-gauge__fill ce-gauge__fill--slate"></div>
                    <div class="ce-gauge__needle ce-gauge__needle--slate" style="left:${needlePct(soldRate)}%"></div>
                </div>
                <div class="ce-gauge__ticks"><span>low</span><span>${esc(String(soldRate.toFixed(2)))}</span><span>high</span></div>
            </div>`;
        }

        if (!Number.isFinite(active) && !Number.isFinite(neu) && !Number.isFinite(soldE) && !gauges) return '';

        return `<div class="ce-ebay-dyn">
            <div class="ce-dyn-kpis">
                ${Number.isFinite(active) ? kpi('active listings', fmtN(active), row30 ? SHARED_UTILS.collectricsEbayMarketPctChip(active, act30) : '') : ''}
                ${Number.isFinite(neu) ? kpi('new listings', fmtN(neu), row30 ? SHARED_UTILS.collectricsEbayMarketPctChip(neu, new30) : '') : ''}
                ${Number.isFinite(soldE) ? kpi('sold est.', fmtN(soldE), row30 ? SHARED_UTILS.collectricsEbayMarketPctChip(soldE, sold30) : '') : ''}
            </div>
            ${gauges}
            <p class="ce-ebay-dyn__note"><em>Demand pressure</em> reflects sold activity vs the active listing pool (Collectrics-style estimate). <em>Supply saturation shift</em> compares the share of listings that ended without a matched sale vs ~30d baseline (tightening = fewer non-sale exits).</p>
        </div>`;
    },

    /** Min/max from current Browse price sample (≥2 prices). Not a time series. */
    ebayActiveSnapshotPriceSpan(card) {
        if (!card) return null;
        const vals = [];
        const rows = Array.isArray(card.ebay_browse_item_summaries) ? card.ebay_browse_item_summaries : [];
        rows.forEach((row) => {
            const v = Number(row && row.price_value);
            if (Number.isFinite(v) && v > 0) vals.push(v);
        });
        if (vals.length < 2) {
            const one = Number(card.ebay_browse_first_item_price_value);
            if (Number.isFinite(one) && one > 0) vals.push(one);
        }
        if (vals.length < 2) return null;
        const low = Math.min(...vals);
        const high = Math.max(...vals);
        if (!(high > low)) return null;
        return { low, high, n: vals.length };
    },

    ebayBrowseSnippetsListHtml(card) {
        const esc = SHARED_UTILS.escHtml;
        const escA = SHARED_UTILS.escAttr;
        const fmt = SHARED_UTILS.fmtUsd;
        const rows = Array.isArray(card.ebay_browse_item_summaries) ? card.ebay_browse_item_summaries : [];
        const items = [];
        rows.forEach((row, i) => {
            if (!row || typeof row !== 'object') return;
            const title = row.title != null ? String(row.title) : `Listing ${i + 1}`;
            const url = row.url ? String(row.url) : '';
            const pv = Number(row.price_value);
            let priceTxt = '—';
            if (Number.isFinite(pv) && pv > 0) {
                const cur = row.price_currency ? String(row.price_currency) : 'USD';
                priceTxt = cur === 'USD' ? fmt(pv) : `${pv.toFixed(2)} ${esc(cur)}`;
            }
            const titleHtml = url
                ? `<a class="card-detail-link" href="${escA(url)}" target="_blank" rel="noopener noreferrer">${esc(title)}</a>`
                : `<span>${esc(title)}</span>`;
            items.push(`<li class="card-detail-ebay-snippet"><span class="card-detail-ebay-snippet__title">${titleHtml}</span><span class="card-detail-ebay-snippet__price">${priceTxt}</span></li>`);
        });
        if (!items.length && (card.ebay_browse_first_item_url || card.ebay_browse_first_item_title)) {
            const url = card.ebay_browse_first_item_url ? String(card.ebay_browse_first_item_url) : '';
            const title = card.ebay_browse_first_item_title ? String(card.ebay_browse_first_item_title) : 'Top search hit';
            const pv = Number(card.ebay_browse_first_item_price_value);
            const cur = card.ebay_browse_first_item_price_currency ? String(card.ebay_browse_first_item_price_currency) : 'USD';
            let priceTxt = '—';
            if (Number.isFinite(pv) && pv > 0) {
                priceTxt = cur === 'USD' ? fmt(pv) : `${pv.toFixed(2)} ${esc(cur)}`;
            }
            const titleHtml = url
                ? `<a class="card-detail-link" href="${escA(url)}" target="_blank" rel="noopener noreferrer">${esc(title)}</a>`
                : esc(title);
            items.push(`<li class="card-detail-ebay-snippet"><span class="card-detail-ebay-snippet__title">${titleHtml}</span><span class="card-detail-ebay-snippet__price">${priceTxt}</span></li>`);
        }
        if (!items.length) return '';
        return `<ul class="card-detail-ebay-snippets">${items.join('')}</ul>`;
    },

    /** Canvas ids for chart blocks: Explorer modal uses legacy fixed ids; elsewhere card.id or `shared`. */
    cardChartCanvasIds(card, chartIdMode) {
        if (chartIdMode === 'explorer') {
            return {
                ceRaw: 'explorerChartCeRaw',
                ceUnified: 'explorerChartCeUnified',
                ceJusttcg: 'explorerChartCeJusttcg',
                ceEbay: 'explorerChartCeEbay',
                ceEbayMarketVol: 'explorerChartCeEbayMarketVol',
                ceEbayMarketDyn: 'explorerChartCeEbayMarketDyn',
                wizPrice: 'explorerChartWizPrice',
                pcPrice: 'explorerChartPcPrice',
                ebayBrowse: 'explorerChartEbayBrowse',
                ebaySoldDaily: 'explorerChartEbaySoldDaily',
            };
        }
        const idPrefix = card && card.id ? String(card.id).replace(/\W/g, '_') : 'shared';
        return {
            ceRaw: `${idPrefix}_ceRaw`,
            ceUnified: `${idPrefix}_ceUnified`,
            ceJusttcg: `${idPrefix}_ceJusttcg`,
            ceEbay: `${idPrefix}_ceEbay`,
            ceEbayMarketVol: `${idPrefix}_ceEbayMarketVol`,
            ceEbayMarketDyn: `${idPrefix}_ceEbayMarketDyn`,
            wizPrice: `${idPrefix}_wizPrice`,
            pcPrice: `${idPrefix}_pcPrice`,
            ebayBrowse: `${idPrefix}_ebayBrowse`,
            ebaySoldDaily: `${idPrefix}_ebaySoldDaily`,
        };
    },

    /**
     * Compact market score panel. RSI/vol/trend from `price_history_en`;
     * ATH/ATL/spread from merged USD anchors (`collectDedupedPositiveUsdPrices`); PSA lines from PriceCharting.
     */
    buildTcggoStyleScorecardHtml(card) {
        const esc = SHARED_UTILS.escHtml;
        const fmt = SHARED_UTILS.fmtUsd;
        const tg = card && card.tcggo;
        if (!tg || typeof tg !== 'object') return '';

        const ph = tg.price_history_en;
        const daily = ph && ph.daily && typeof ph.daily === 'object' ? ph.daily : null;
        const prices = tg.prices && typeof tg.prices === 'object' ? tg.prices : {};
        const cm = prices.cardmarket && typeof prices.cardmarket === 'object' ? prices.cardmarket : {};
        const tcp = prices.tcg_player && typeof prices.tcg_player === 'object' ? prices.tcg_player : {};

        const closeKeys = daily ? Object.keys(daily).sort() : [];
        const closes = [];
        closeKeys.forEach((k) => {
            const row = daily[k];
            const v = row && typeof row === 'object' ? Number(row.tcg_player_market) : NaN;
            if (Number.isFinite(v) && v > 0) closes.push(v);
        });
        const hasCm = Number.isFinite(Number(cm.lowest_near_mint)) || Number.isFinite(Number(cm['30d_average']));
        if (!closes.length && !hasCm && !Number.isFinite(Number(tcp.market_price))) return '';

        const usdPool = SHARED_UTILS.collectDedupedPositiveUsdPrices(card);
        let athUsd = null;
        let atlUsd = null;
        let spreadUsd = null;
        if (usdPool.length) {
            athUsd = Math.max(...usdPool);
            atlUsd = Math.min(...usdPool);
            spreadUsd = athUsd > atlUsd ? athUsd - atlUsd : 0;
        }

        let volPct = null;
        if (closes.length >= 3) {
            let s = 0;
            let c = 0;
            for (let i = 1; i < closes.length; i += 1) {
                const a = closes[i - 1];
                const b = closes[i];
                if (a > 0) {
                    s += Math.abs((b - a) / a);
                    c += 1;
                }
            }
            if (c) volPct = (s / c) * 100;
        }

        let rsi = null;
        let rsiLabel = 'Neutral';
        if (closes.length >= 15) {
            const seg = closes.slice(-15);
            let g = 0;
            let l = 0;
            for (let i = 1; i < seg.length; i += 1) {
                const d = seg[i] - seg[i - 1];
                if (d >= 0) g += d;
                else l -= d;
            }
            const rs = l === 0 ? 99 : g / l;
            rsi = 100 - 100 / (1 + rs);
            rsiLabel = rsi >= 70 ? 'Overbought' : rsi <= 30 ? 'Oversold' : 'Neutral';
        }

        let momentum = 50;
        if (closes.length >= 8) {
            const a = closes[closes.length - 1];
            const b = closes[closes.length - 8];
            if (b > 0) {
                const p = ((a - b) / b) * 100;
                momentum = Math.max(0, Math.min(100, Math.round(50 + p * 1.5)));
            }
        }

        let stability = 50;
        if (volPct != null) stability = Math.max(0, Math.min(100, Math.round(100 - Math.min(volPct * 10, 95))));

        const nDays = closeKeys.length;
        let liquidity = Math.min(100, Math.round((nDays / 35) * 100));
        const sellers = Number(card.tcgplayer_sellers_count);
        if (Number.isFinite(sellers) && sellers > 0) {
            liquidity = Math.max(liquidity, Math.min(100, Math.round(55 + Math.log10(sellers + 1) * 15)));
        }

        let gradePrem = 50;
        const blendU = SHARED_UTILS.resolveExplorerChartUsd(card);
        const rawNm = Number.isFinite(blendU) && blendU > 0 ? blendU : Number(card.market_price);
        const gp = card.pricecharting_grade_prices;
        let psa10v = null;
        let psa9v = null;
        if (gp && typeof gp === 'object') {
            const norm = (s) => String(s).trim().toLowerCase().replace(/\s+/g, ' ');
            Object.keys(gp).forEach((k) => {
                const nk = norm(k);
                const val = Number(gp[k]);
                if (!Number.isFinite(val) || val <= 0) return;
                if (nk === 'psa 10' || nk === 'psa10') psa10v = val;
                if (nk === 'psa 9' || nk === 'psa9') psa9v = val;
            });
        }
        if (Number.isFinite(rawNm) && rawNm > 0 && psa10v) {
            gradePrem = Math.max(0, Math.min(100, Math.round(Math.min(4.5, psa10v / rawNm - 1) * 35)));
        }

        let demand = 60;
        const ceVol = Number(card.collectrics_ebay_sold_volume);
        if (Number.isFinite(ceVol) && ceVol > 0) {
            demand = Math.min(100, Math.round(52 + Math.log10(ceVol + 1) * 18));
        }

        let marketDepth = 0;
        const lnm = Number(cm.lowest_near_mint);
        const av7 = Number(cm['7d_average']);
        const av30 = Number(cm['30d_average']);
        if (Number.isFinite(lnm) && (Number.isFinite(av30) || Number.isFinite(av7))) {
            const ref = Number.isFinite(av30) ? av30 : av7;
            if (ref > 0) {
                const sp = Math.abs(lnm - ref) / ref;
                marketDepth = Math.max(0, Math.min(100, Math.round((1 - Math.min(sp, 0.95)) * 100)));
            }
        }

        const subscores = [momentum, stability, liquidity, gradePrem, demand, marketDepth];
        const composite = Math.round(subscores.reduce((a, b) => a + b, 0) / subscores.length);
        const tier = composite >= 75 ? 'Strong' : composite >= 55 ? 'Moderate' : 'Light';
        const tierClass = composite >= 75 ? 'card-detail-tcggo-score--tier-pro' : composite >= 55 ? 'card-detail-tcggo-score--tier-plus' : 'card-detail-tcggo-score--tier-free';

        let syncAge = '';
        if (ph && ph.sync_iso) {
            const t = Date.parse(String(ph.sync_iso));
            if (Number.isFinite(t)) {
                const days = Math.max(0, Math.floor((Date.now() - t) / 86400000));
                syncAge = days === 0 ? 'today' : `${days} day${days === 1 ? '' : 's'} ago`;
            }
        }

        const bar = (label, val) => {
            const v = Math.max(0, Math.min(100, Math.round(val)));
            const w = `${v}%`;
            return `<div class="card-detail-tcggo-score__row"><span class="card-detail-tcggo-score__lbl">${esc(label)}</span><div class="card-detail-tcggo-score__track"><div class="card-detail-tcggo-score__fill" style="width:${w}"></div></div><span class="card-detail-tcggo-score__num">${v}</span></div>`;
        };

        let psRows = '';
        if (Number.isFinite(rawNm) && rawNm > 0 && psa10v) {
            const x10 = (psa10v / rawNm).toFixed(1);
            psRows += `<div class="card-detail-tcggo-score__psa"><span class="card-detail-tcggo-score__psa-l">PSA 10</span><span class="card-detail-tcggo-score__psa-m">${fmt(psa10v)}</span><span class="card-detail-tcggo-score__psa-x">${x10}x raw</span></div>`;
        }
        if (Number.isFinite(rawNm) && rawNm > 0 && psa9v) {
            const x9 = (psa9v / rawNm).toFixed(1);
            psRows += `<div class="card-detail-tcggo-score__psa"><span class="card-detail-tcggo-score__psa-l">PSA 9</span><span class="card-detail-tcggo-score__psa-m">${fmt(psa9v)}</span><span class="card-detail-tcggo-score__psa-x">${x9}x raw</span></div>`;
        }

        const rsiPart = rsi != null
            ? `RSI ${Math.round(rsi)} <span class="card-detail-tcggo-score__muted">${esc(rsiLabel)}</span> · Vol ${volPct != null ? volPct.toFixed(1) : '—'}%`
            : `<span class="card-detail-tcggo-score__muted">RSI —</span> · Vol ${volPct != null ? volPct.toFixed(1) : '—'}%`;

        const usdPart = athUsd != null && atlUsd != null
            ? ` · ATH ${fmt(athUsd)} · ATL ${fmt(atlUsd)} · Spread ${fmt(spreadUsd != null ? spreadUsd : 0)}`
            : '';

        return `
<div class="card-detail-section card-detail-tcggo-score">
  <div class="card-detail-tcggo-score__head">
    <h4 class="card-detail-tcggo-score__title">Market score</h4>
    ${syncAge ? `<div class="card-detail-tcggo-score__age">${esc(syncAge)}</div>` : ''}
  </div>
  <div class="card-detail-tcggo-score__hero">
    <div class="card-detail-tcggo-score__badge ${tierClass}">
      <span class="card-detail-tcggo-score__badge-num">${composite}</span>
    </div>
    <div class="card-detail-tcggo-score__hero-main">
      <div class="card-detail-tcggo-score__tier ${tierClass}">${esc(tier)}</div>
      <div class="card-detail-tcggo-score__metrics">${rsiPart}${usdPart}</div>
    </div>
  </div>
  <div class="card-detail-tcggo-score__cols">
    <div class="card-detail-tcggo-score__col">
      ${bar('Trend', momentum)}
      ${bar('Steadiness', stability)}
      ${bar('Availability', liquidity)}
    </div>
    <div class="card-detail-tcggo-score__col">
      ${bar('Grade value', gradePrem)}
      ${bar('Sales', demand)}
      ${bar('Price tightness', marketDepth)}
    </div>
  </div>
  ${psRows ? `<div class="card-detail-tcggo-score__psa-wrap">${psRows}</div>` : ''}
</div>`;
    },

    cardChartsSectionHtml(card, chartIdMode) {
        const mode = chartIdMode === 'explorer' ? 'explorer' : 'dynamic';
        const ceBlend = Array.isArray(card.collectrics_price_history) ? card.collectrics_price_history : [];
        const ceJust = Array.isArray(card.collectrics_history_justtcg) ? card.collectrics_history_justtcg : [];
        const ceSoldEbay = Array.isArray(card.collectrics_history_ebay) ? card.collectrics_history_ebay : [];
        const wiz = SHARED_UTILS.filterWizardPriceHistoryRows(card.pokemon_wizard_price_history || []);
        const { labels: barLabs, vals: barVals } = SHARED_UTILS.wizardSummaryPctBars(card);
        const parts = [];
        const ids = SHARED_UTILS.cardChartCanvasIds(card, mode);

        const unifiedMarketPack = SHARED_UTILS.buildExplorerUnifiedPriceChartPack(card);
        if (unifiedMarketPack) {
            const fx = SHARED_UTILS.getEurUsdRate().toFixed(3);
            const rangeBar = `<div class="card-detail-unified-range" role="group" aria-label="Price history window">
                <span class="card-detail-unified-range__lbl">Window</span>
                <div class="card-detail-unified-range__btns">
                    <button type="button" class="card-detail-unified-range__btn" data-ptcg-history-months="1">1M</button>
                    <button type="button" class="card-detail-unified-range__btn" data-ptcg-history-months="3">3M</button>
                    <button type="button" class="card-detail-unified-range__btn" data-ptcg-history-months="6">6M</button>
                    <button type="button" class="card-detail-unified-range__btn" data-ptcg-history-months="12">1Y</button>
                </div>
            </div>`;
            const escU = SHARED_UTILS.escHtml;
            const noteTech = unifiedMarketPack.titleNote
                ? `<p class="card-detail-unified-help__p">${escU(String(unifiedMarketPack.titleNote))}</p>`
                : `<p class="card-detail-unified-help__p">Override EUR→USD with <code class="card-detail-unified-help__code">window.PTCG_EUR_USD_RATE</code> (console).</p>`;
            const helpBlock = `
                <details class="card-detail-unified-help">
                    <summary class="card-detail-unified-help__summary">Line &amp; series descriptions</summary>
                    <div class="card-detail-unified-help__body">
                        <p class="card-detail-unified-help__p">Orange: TCGGO TCGPlayer (USD). Pink dashed: Cardmarket EU low at <strong>×${fx}</strong> EUR→USD. Purple: Pokémon Wizard. Soft pink / peach lines: eBay sold median (ungraded / graded). Green bars (right axis): sold count per day.</p>
                        ${noteTech}
                    </div>
                </details>`;
            parts.push(`<div class="card-detail-chart-block card-detail-chart-block--unified-market-span"><div class="card-detail-chart-title">Price history · USD <span style="font-weight:400;font-size:0.75rem;color:#94a3b8;">(Collectrics · TCGGO · Wizard · eBay sold)</span></div>${rangeBar}<div class="card-detail-chart-canvas card-detail-chart-canvas--unified-market"><canvas id="${ids.ceUnified}"></canvas></div>${helpBlock}</div>`);
        }

        const collectricsEbayRange = SHARED_UTILS.collectricsEbayEndedRangeSeries(card);
        const collectricsEbayRangeOk = Boolean(collectricsEbayRange && collectricsEbayRange.labels.length > 1);
        const hasTcg = ceJust.length > 1;
        const hasEbay = ceSoldEbay.length > 1 || collectricsEbayRangeOk;
        const ceBlendOnly = ceBlend.length > 1 && !hasTcg && !hasEbay;

        if (ceBlendOnly) {
            parts.push(`<div class="card-detail-chart-block"><div class="card-detail-chart-title">Price blend &amp; volume</div><div class="card-detail-chart-canvas"><canvas id="${ids.ceRaw}"></canvas></div></div>`);
        }

        const ceMarketRows = SHARED_UTILS.collectricsEbayMarketHistorySorted(card);
        const ceMarketVolOk = SHARED_UTILS.collectricsEbayMarketVolumeUsable(ceMarketRows);
        const ceMarketDynHtml = SHARED_UTILS.collectricsEbayMarketDynamicsPanelHtml(card);
        const ceMarketDynOk = Boolean(ceMarketDynHtml);
        if (ceMarketVolOk || ceMarketDynOk) {
            const volTitle = 'eBay listing volume';
            const dynTitle = 'eBay market dynamics';
            const dynBlock = ceMarketDynOk
                ? `<div class="card-detail-chart-block"><div class="card-detail-chart-title">${dynTitle}</div><div id="${ids.ceEbayMarketDyn}" class="card-detail-chart-canvas card-detail-chart-canvas--ebay-dyn">${ceMarketDynHtml}</div></div>`
                : '';
            if (ceMarketVolOk && ceMarketDynOk) {
                parts.push(`<div class="card-detail-charts-split"><div class="card-detail-chart-block"><div class="card-detail-chart-title">${volTitle}</div><div class="card-detail-chart-canvas card-detail-chart-canvas--ebay-vol"><canvas id="${ids.ceEbayMarketVol}"></canvas></div></div>${dynBlock}</div>`);
            } else if (ceMarketVolOk) {
                parts.push(`<div class="card-detail-chart-block"><div class="card-detail-chart-title">${volTitle}</div><div class="card-detail-chart-canvas card-detail-chart-canvas--ebay-vol"><canvas id="${ids.ceEbayMarketVol}"></canvas></div></div>`);
            } else {
                parts.push(dynBlock);
            }
        }

        const hasWizUrl = Boolean(card.pokemon_wizard_url);
        const hasPct = hasWizUrl && barLabs.length > 0;
        const hasAnyFinitePrice = wiz.some((r) => r.price_usd != null && Number.isFinite(Number(r.price_usd)));
        const hasMultiPrice = wiz.length > 1;
        const skipWizCanvas = Boolean(unifiedMarketPack && unifiedMarketPack.skipStandaloneWizard);
        if (!skipWizCanvas) {
            if (hasMultiPrice && !hasPct) {
                const wizTitle = card.pokemon_wizard_url
                    ? `<a class="card-detail-link" style="color:#60a5fa;" href="${SHARED_UTILS.escAttr(String(card.pokemon_wizard_url))}" target="_blank" rel="noopener noreferrer">Pokémon Wizard · TCG retail price</a>`
                    : 'Pokémon Wizard · TCG retail price';
                parts.push(`<div class="card-detail-chart-block card-detail-chart-block--wiz-span"><div class="card-detail-chart-title">${wizTitle}</div><div class="card-detail-chart-canvas"><canvas id="${ids.wizPrice}"></canvas></div></div>`);
            } else if (hasPct) {
                const useRetailTrendTitle = hasMultiPrice || (wiz.length >= 1 && hasAnyFinitePrice);
                const pctHeading = useRetailTrendTitle ? 'retail &amp; trend %' : 'summary % changes';
                const wizTitle = card.pokemon_wizard_url
                    ? `<a class="card-detail-link" style="color:#60a5fa;" href="${SHARED_UTILS.escAttr(String(card.pokemon_wizard_url))}" target="_blank" rel="noopener noreferrer">Pokémon Wizard · ${pctHeading}</a>`
                    : `Pokémon Wizard · ${pctHeading}`;
                parts.push(`<div class="card-detail-chart-block card-detail-chart-block--wiz-span"><div class="card-detail-chart-title">${wizTitle}</div><div class="card-detail-chart-canvas card-detail-chart-canvas--wiz-overlay"><canvas id="${ids.wizPrice}"></canvas></div></div>`);
            }
        }
        if (!parts.length) return '';
        return `
            <div class="card-detail-section card-detail-section--charts-grid">
                ${parts.join('')}
            </div>`;
    },

    /** Chart.js plugin: vertical dashed markers at ~7d and ~30d before last label date. */
    _ceEbayMarketVolumeMarkerPlugin(labels) {
        const lastLab = labels.length ? String(labels[labels.length - 1]).slice(0, 10) : '';
        const findIdx = (daysBack) => {
            const tLast = Date.parse(lastLab);
            if (!Number.isFinite(tLast)) return null;
            const target = tLast - daysBack * 86400000;
            let best = null;
            let bestD = Infinity;
            labels.forEach((lab, i) => {
                const t = Date.parse(String(lab).slice(0, 10));
                if (!Number.isFinite(t)) return;
                const d = Math.abs(t - target);
                if (d < bestD) {
                    bestD = d;
                    best = i;
                }
            });
            return best;
        };
        const i7 = findIdx(7);
        const i30 = findIdx(30);
        return {
            id: 'ce_ebay_vol_markers',
            afterDatasetsDraw(chart) {
                const { ctx, chartArea } = chart;
                const meta0 = chart.getDatasetMeta(0);
                const drawLine = (idx, tag) => {
                    if (idx == null || !meta0.data[idx]) return;
                    const x = meta0.data[idx].x;
                    ctx.save();
                    ctx.strokeStyle = 'rgba(220,38,38,0.72)';
                    ctx.lineWidth = 1;
                    ctx.setLineDash([4, 4]);
                    ctx.beginPath();
                    ctx.moveTo(x, chartArea.top);
                    ctx.lineTo(x, chartArea.bottom);
                    ctx.stroke();
                    ctx.setLineDash([]);
                    ctx.fillStyle = 'rgba(185,28,28,0.95)';
                    ctx.font = '600 10px system-ui,-apple-system,sans-serif';
                    ctx.textAlign = 'center';
                    ctx.fillText(tag, x, chartArea.top + 12);
                    ctx.restore();
                };
                drawLine(i30, '30d');
                drawLine(i7, '7d');
            },
        };
    },

    /** Collectrics-style mirrored stacked bars: existing active + new (up), sold + unsold est (down). */
    mountCollectricsEbayMarketVolumeChart(canvasEl, rows, tickColor, baseScale, chartList) {
        if (typeof Chart === 'undefined' || !canvasEl || !Array.isArray(rows) || rows.length < 2) return;
        if (!SHARED_UTILS.collectricsEbayMarketVolumeUsable(rows)) return;
        const labels = rows.map((r) => String(r.date || '').slice(0, 10));
        const existing = [];
        const neu = [];
        const soldSeg = [];
        const unsoldSeg = [];
        rows.forEach((r) => {
            const aTo = SHARED_UTILS.collectricsEbayMarketNum(r, 'active_to', 'activeTo');
            const aFrom = SHARED_UTILS.collectricsEbayMarketNum(r, 'active_from', 'activeFrom');
            const n = SHARED_UTILS.collectricsEbayMarketNum(r, 'new');
            const ended = SHARED_UTILS.collectricsEbayMarketNum(r, 'ended');
            const soldE = SHARED_UTILS.collectricsEbayMarketNum(r, 'sold_est', 'soldEst');
            let ex = null;
            if (aFrom != null && aFrom >= 0) ex = aFrom;
            else if (aTo != null && n != null && aTo >= 0 && n >= 0) ex = Math.max(0, aTo - n);
            else if (aTo != null && aTo >= 0) ex = aTo;
            const nn = (n != null && n >= 0) ? n : 0;
            if (ex == null) existing.push(null);
            else existing.push(ex);
            neu.push(nn);
            let soldV = 0;
            if (soldE != null && soldE >= 0) soldV = soldE;
            let unsoldV = 0;
            if (ended != null && ended >= 0) {
                unsoldV = Math.max(0, ended - soldV);
            }
            soldSeg.push(-soldV);
            unsoldSeg.push(-unsoldV);
        });
        const posSum = labels.map((_, i) => {
            const a = existing[i];
            const b = neu[i];
            const x = (a != null && Number.isFinite(a) ? a : 0) + (Number.isFinite(b) ? b : 0);
            return x;
        });
        const negAbs = labels.map((_, i) => Math.abs(soldSeg[i]) + Math.abs(unsoldSeg[i]));
        const m = Math.max(1, ...posSum, ...negAbs);
        const pad = Math.ceil(m * 0.08);
        const yMax = m + pad;
        const plugin = SHARED_UTILS._ceEbayMarketVolumeMarkerPlugin(labels);
        chartList.push(new Chart(canvasEl, {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    {
                        label: 'existing active',
                        data: existing.map((v) => (v != null && Number.isFinite(v) ? v : 0)),
                        backgroundColor: 'rgba(226,232,240,0.92)',
                        borderColor: 'rgba(148,163,184,0.35)',
                        borderWidth: 0,
                        stack: 'ebayVol',
                        borderRadius: { topLeft: 3, topRight: 3, bottomLeft: 0, bottomRight: 0 },
                    },
                    {
                        label: 'new',
                        data: neu,
                        backgroundColor: 'rgba(147,197,253,0.88)',
                        borderColor: 'rgba(96,165,250,0.4)',
                        borderWidth: 0,
                        stack: 'ebayVol',
                        borderRadius: { topLeft: 3, topRight: 3, bottomLeft: 0, bottomRight: 0 },
                    },
                    {
                        label: 'sold (est.)',
                        data: soldSeg,
                        backgroundColor: 'rgba(251,146,60,0.9)',
                        borderColor: 'rgba(234,88,12,0.35)',
                        borderWidth: 0,
                        stack: 'ebayVol',
                        borderRadius: { bottomLeft: 3, bottomRight: 3, topLeft: 0, topRight: 0 },
                    },
                    {
                        label: 'unsold (est.)',
                        data: unsoldSeg,
                        backgroundColor: 'rgba(30,41,59,0.9)',
                        borderColor: 'rgba(15,23,42,0.5)',
                        borderWidth: 0,
                        stack: 'ebayVol',
                        borderRadius: { bottomLeft: 3, bottomRight: 3, topLeft: 0, topRight: 0 },
                    },
                ],
            },
            plugins: [plugin],
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                        labels: { color: tickColor, boxWidth: 10, padding: 10, font: { size: 11 } },
                    },
                },
                scales: {
                    x: {
                        ...baseScale,
                        stacked: true,
                        ticks: { ...baseScale.ticks, maxRotation: 0, autoSkip: true, maxTicksLimit: 10 },
                        grid: { display: false },
                    },
                    y: {
                        ...baseScale,
                        stacked: true,
                        min: -yMax,
                        max: yMax,
                        title: { display: true, text: 'Listings', color: tickColor },
                        ticks: { ...baseScale.ticks, callback(v) { return Math.abs(v); } },
                    },
                },
            },
        }));
    },

    /**
     * Full Explorer-style card detail body (market, eBay, Wizard, Collectrics, charts, pull model, graded).
     * @param {object} opts.chartIdMode `'explorer'` (fixed canvas ids for index.html modal) or `'dynamic'` (predictor / shared.initCardDetailCharts).
     */
    buildCardDetailExplorerPanelHtml(card, set, opts) {
        const chartIdMode = opts && opts.chartIdMode === 'dynamic' ? 'dynamic' : 'explorer';
        const hidePredictorLink = Boolean(opts && opts.hidePredictorLink);
        const esc = SHARED_UTILS.escHtml;
        const fmt = SHARED_UTILS.fmtUsd;
        const setTitle = set && set.set_name ? String(set.set_name) : 'Set';
        const setCode = set && set.set_code ? String(set.set_code) : '';
        const release = set && set.release_date ? String(set.release_date) : '';
        const num = card.number != null ? String(card.number) : '?';
        const nm = card.name ? String(card.name) : 'Card';
        const rr = set && set.rarity_pull_rates && card.rarity ? set.rarity_pull_rates[card.rarity] : null;
        const subtypes = Array.isArray(card.subtypes) ? card.subtypes.filter(Boolean).join(' · ') : '';

        const tags = [];
        if (card.rarity) tags.push(String(card.rarity));
        if (card.supertype) tags.push(String(card.supertype));
        if (subtypes) tags.push(subtypes);
        const tagHtml = tags.map((t) => `<span class="card-detail-tag">${esc(t)}</span>`).join('');

        const blendUsd = SHARED_UTILS.resolveExplorerChartUsd(card);
        const listUsd = Number(card.market_price);
        const statRows = [];
        statRows.push({ lbl: 'List / scraper price', val: fmt(card.market_price) });
        if (blendUsd != null && Number.isFinite(blendUsd)
            && (!Number.isFinite(listUsd) || listUsd <= 0 || Math.abs(blendUsd - listUsd) / Math.max(listUsd, blendUsd, 1) > 0.04)) {
            statRows.push({ lbl: 'Blended median', val: fmt(blendUsd) });
        }
        if (card.pricedex_market_usd != null && Number.isFinite(Number(card.pricedex_market_usd))) {
            statRows.push({ lbl: 'PriceDex', val: fmt(card.pricedex_market_usd) });
        }
        if (card.tcgtracking_market_usd != null && Number.isFinite(Number(card.tcgtracking_market_usd))) {
            statRows.push({ lbl: 'TCGTracker (Mkt)', val: fmt(card.tcgtracking_market_usd) });
        }
        if (card.tcgtracking_low_usd != null && Number.isFinite(Number(card.tcgtracking_low_usd))) {
            statRows.push({ lbl: 'TCGTracker (Low)', val: fmt(card.tcgtracking_low_usd) });
        }
        const ceList = Number(card.collectrics_ebay_listings);
        const ceVol = Number(card.collectrics_ebay_sold_volume);
        if (Number.isFinite(ceList) && ceList > 0) {
            statRows.push({ lbl: 'eBay list (Coll.)', val: esc(ceList.toLocaleString()) });
        }
        if (Number.isFinite(ceVol) && ceVol > 0) {
            statRows.push({ lbl: 'eBay vol (Coll.)', val: esc(ceVol.toLocaleString()) });
        }
        if (card.tcgtracking_price_subtype) {
            statRows.push({ lbl: 'TCG price subtype', val: esc(String(card.tcgtracking_price_subtype)) });
        }
        if (card.tcgtracking_match && String(card.tcgtracking_match) !== 'ok') {
            statRows.push({ lbl: 'TCG match', val: esc(String(card.tcgtracking_match)) });
        }
        // eBay Sold prices (last sale) — merge scrape / Finding + optional ScrapeChain `ebay_sold_observations`
        const allEbaySold = SHARED_UTILS.ebaySoldRowsDeduped(card);
        const bySoldTime = (a, b) => {
            const ta = Date.parse(SHARED_UTILS.normalizeEbaySoldDateKey(a.date) || String(a.date || ''));
            const tb = Date.parse(SHARED_UTILS.normalizeEbaySoldDateKey(b.date) || String(b.date || ''));
            return (Number.isFinite(ta) ? ta : 0) - (Number.isFinite(tb) ? tb : 0);
        };
        const ugOnly = allEbaySold.filter((r) => !SHARED_UTILS.isEbaySoldListingGradedTitle(r.title)).slice().sort(bySoldTime);
        const grOnly = allEbaySold.filter((r) => SHARED_UTILS.isEbaySoldListingGradedTitle(r.title)).slice().sort(bySoldTime);
        const ebSoldUg = ugOnly.length ? ugOnly[ugOnly.length - 1] : null;
        const ebSoldGr = grOnly.length ? grOnly[grOnly.length - 1] : null;

        // Horizontal KPI strip — each stat is a mini chip
        const statsStrip = statRows.map((s) => `
            <div class="card-detail-liquidity-chip">
                <span class="card-detail-liquidity-chip__lbl">${esc(s.lbl)}</span>
                <span class="card-detail-liquidity-chip__val">${s.val}</span>
            </div>`).join('');

        let collectricsSnapBlock = '';
        const ceSnap = card.collectrics_ebay_market_snapshot;
        if (ceSnap && typeof ceSnap === 'object') {
            const snapRows = [];
            const addSnap = (k, lbl) => {
                const v = ceSnap[k];
                if (v == null || v === '') return;
                snapRows.push(`<div class="card-detail-stat"><span class="lbl">${esc(lbl)}</span><span class="val">${esc(String(v))}</span></div>`);
            };
            addSnap('date', 'eBay market row (date)');
            addSnap('active_to', 'Active listings');
            addSnap('new', 'New listings');
            addSnap('ended', 'Ended listings');
            addSnap('demand_pressure_est', 'Demand pressure (est.)');
            addSnap('demand_pressure_observed', 'Demand pressure (obs.)');
            addSnap('sold_est', 'Sold (est.)');
            addSnap('sold_rate_est', 'Sold rate (est.)');
            if (snapRows.length) {
                collectricsSnapBlock = `
            <div class="card-detail-section">
                <h4>eBay market snapshot</h4>
                <div class="card-detail-stat-grid">${snapRows.join('')}</div>
            </div>`;
            }
        }

        let collectricsTrendBlock = '';
        const c30 = card.collectrics_raw_price_change_30d_pct;
        const c60 = card.collectrics_raw_price_change_60d_pct;
        if (c30 != null || c60 != null) {
            const t30 = c30 != null && Number.isFinite(Number(c30)) ? `${Number(c30).toFixed(2)}%` : '—';
            const t60 = c60 != null && Number.isFinite(Number(c60)) ? `${Number(c60).toFixed(2)}%` : '—';
            collectricsTrendBlock = `
            <div class="card-detail-section">
                <h4>Blended price trend</h4>
                <div class="card-detail-stat-grid">
                    <div class="card-detail-stat"><span class="lbl">Δ vs ~30d</span><span class="val">${esc(t30)}</span></div>
                    <div class="card-detail-stat"><span class="lbl">Δ vs ~60d</span><span class="val">${esc(t60)}</span></div>
                </div>
            </div>`;
        }

        const chartsBlock = SHARED_UTILS.cardChartsSectionHtml(card, chartIdMode);

        let wizardBlock = '';
        const topPeers = set && Array.isArray(set.top_25_cards) ? set.top_25_cards : [];
        const setHasAnyWizard = topPeers.some((c) => c && c.pokemon_wizard_url);
        if (!card.pokemon_wizard_url && setHasAnyWizard) {
            console.warn(
                '[PokemonTCG] Missing pokemon_wizard_url for',
                setCode,
                '#' + num,
                nm,
                '— other top-list cards in this set have Wizard data.',
            );
        }
        if (card.pokemon_wizard_url) {
            const wcur = card.pokemon_wizard_current_price_usd != null && Number.isFinite(Number(card.pokemon_wizard_current_price_usd))
                ? fmt(card.pokemon_wizard_current_price_usd) : '—';
            const wt = card.pokemon_wizard_current_trend_pct != null && Number.isFinite(Number(card.pokemon_wizard_current_trend_pct))
                ? `${Number(card.pokemon_wizard_current_trend_pct).toFixed(2)}%` : '—';
            const w7 = card.pokemon_wizard_last_7d_pct != null && Number.isFinite(Number(card.pokemon_wizard_last_7d_pct))
                ? `${Number(card.pokemon_wizard_last_7d_pct).toFixed(2)}%` : '—';
            const w30 = card.pokemon_wizard_last_30d_pct != null && Number.isFinite(Number(card.pokemon_wizard_last_30d_pct))
                ? `${Number(card.pokemon_wizard_last_30d_pct).toFixed(2)}%` : '—';
            const wy = card.pokemon_wizard_ytd_pct != null && Number.isFinite(Number(card.pokemon_wizard_ytd_pct))
                ? `${Number(card.pokemon_wizard_ytd_pct).toFixed(2)}%` : '—';
            let histRows = '';
            const ph = SHARED_UTILS.filterWizardPriceHistoryRows(card.pokemon_wizard_price_history || []);
            ph.forEach((row) => {
                const pr = row.price_usd != null && Number.isFinite(Number(row.price_usd)) ? fmt(row.price_usd) : '—';
                const tr = row.trend != null ? esc(String(row.trend)) : '—';
                histRows += `<tr><td>${esc(String(row.label || row.sort_key || ''))}</td><td>${pr}</td><td>${tr}</td></tr>`;
            });
            const histTable = histRows
                ? `<div class="card-detail-table-scroll"><table class="card-detail-mini-table"><thead><tr><th>When</th><th>Price</th><th>Trend</th></tr></thead><tbody>${histRows}</tbody></table></div>`
                : '';
            wizardBlock = `
            <div class="card-detail-section">
                <h4>Pokémon Wizard <span style="font-weight:400;color:#94a3b8;">(TCGPlayer retail proxy)</span></h4>
                <p class="card-detail-sub" style="margin-bottom:0.75rem;"><a class="card-detail-link" href="${SHARED_UTILS.escAttr(String(card.pokemon_wizard_url))}" target="_blank" rel="noopener noreferrer">Open on Pokémon Wizard</a></p>
                <div class="card-detail-stat-grid">
                    <div class="card-detail-stat"><span class="lbl">Current</span><span class="val">${wcur} <span style="color:#94a3b8;">(${esc(wt)} trend)</span></span></div>
                    <div class="card-detail-stat"><span class="lbl">Last 7d</span><span class="val">${esc(w7)}</span></div>
                    <div class="card-detail-stat"><span class="lbl">Last 30d</span><span class="val">${esc(w30)}</span></div>
                    <div class="card-detail-stat"><span class="lbl">YTD</span><span class="val">${esc(wy)}</span></div>
                </div>
                ${histTable}
            </div>`;
        }


        let ebayBrowseBlock = '';
        if (card.ebay_browse_sync_iso) {
            const ebAttr = SHARED_UTILS.escAttr;
            const schUrl = SHARED_UTILS.ebaySchSearchUrlFromCard(card);
            const ebN = Number(card.ebay_browse_result_total);
            const totStr = Number.isFinite(ebN) ? ebN.toLocaleString() : '—';
            const snapSpanBrowse = SHARED_UTILS.ebayActiveSnapshotPriceSpan(card);
            const fmt = SHARED_UTILS.fmtUsd;
            const escA = SHARED_UTILS.escAttr;

            const kpiSegs = [];
            if (snapSpanBrowse) {
                kpiSegs.push(`<span class="card-detail-ebay-kpi-seg"><span class="card-detail-ebay-kpi-k">Price</span><span class="card-detail-ebay-kpi-v">${fmt(snapSpanBrowse.low)}–${fmt(snapSpanBrowse.high)}</span></span>`);
            }
            if (ebSoldUg) {
                kpiSegs.push(`<span class="card-detail-ebay-kpi-seg card-detail-ebay-kpi-seg--ug"><span class="card-detail-ebay-kpi-k">Raw</span><span class="card-detail-ebay-kpi-v">${fmt(ebSoldUg.price)}</span></span>`);
            }
            if (ebSoldGr) {
                kpiSegs.push(`<span class="card-detail-ebay-kpi-seg card-detail-ebay-kpi-seg--gr"><span class="card-detail-ebay-kpi-k">Graded</span><span class="card-detail-ebay-kpi-v">${fmt(ebSoldGr.price)}</span></span>`);
            }
            const kpiRow = kpiSegs.length
                ? `<div class="card-detail-ebay-kpi-row" role="group" aria-label="eBay price range, raw and graded last sold">${kpiSegs.join('<span class="card-detail-ebay-kpi-dot" aria-hidden="true">·</span>')}</div>`
                : '';

            // Sample listings as premium cards
            const rows = Array.isArray(card.ebay_browse_item_summaries) ? card.ebay_browse_item_summaries : [];
            const listingCards = [];
            rows.forEach((row, i) => {
                if (!row || typeof row !== 'object') return;
                const title = row.title != null ? String(row.title) : `Listing ${i + 1}`;
                const url = row.url ? String(row.url) : '';
                const pv = Number(row.price_value);
                const priceTxt = (Number.isFinite(pv) && pv > 0)
                    ? fmt(pv)
                    : '—';
                const titleHtml = url
                    ? `<a class="card-detail-link" style="color:#e2e8f0;text-decoration:none;" href="${escA(url)}" target="_blank" rel="noopener noreferrer">${esc(title)}</a>`
                    : `<span style="color:#e2e8f0;">${esc(title)}</span>`;
                listingCards.push(`
                    <div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.07);border-radius:6px;padding:0.45rem 0.65rem;display:flex;justify-content:space-between;align-items:flex-start;gap:0.5rem;">
                        <div style="font-size:0.78rem;line-height:1.35;flex:1;min-width:0;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;">${titleHtml}</div>
                        <div style="font-size:0.88rem;font-weight:600;color:#34d399;white-space:nowrap;flex-shrink:0;">${esc(priceTxt)}</div>
                    </div>`);
            });
            // Fallback to first item if no summaries
            if (!listingCards.length && (card.ebay_browse_first_item_url || card.ebay_browse_first_item_title)) {
                const url = card.ebay_browse_first_item_url ? String(card.ebay_browse_first_item_url) : '';
                const title = card.ebay_browse_first_item_title ? String(card.ebay_browse_first_item_title) : 'Top search hit';
                const pv = Number(card.ebay_browse_first_item_price_value);
                const priceTxt = (Number.isFinite(pv) && pv > 0) ? fmt(pv) : '—';
                const titleHtml = url
                    ? `<a class="card-detail-link" style="color:#e2e8f0;text-decoration:none;" href="${escA(url)}" target="_blank" rel="noopener noreferrer">${esc(title)}</a>`
                    : `<span style="color:#e2e8f0;">${esc(title)}</span>`;
                listingCards.push(`
                    <div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.07);border-radius:6px;padding:0.45rem 0.65rem;display:flex;justify-content:space-between;align-items:flex-start;gap:0.5rem;">
                        <div style="font-size:0.78rem;line-height:1.35;flex:1;min-width:0;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;">${titleHtml}</div>
                        <div style="font-size:0.88rem;font-weight:600;color:#34d399;white-space:nowrap;flex-shrink:0;">${esc(priceTxt)}</div>
                    </div>`);
            }

            // Mix in up to 2 recent sold listings
            const recentSold = allEbaySold.slice().sort((a, b) => {
                const db = SHARED_UTILS.normalizeEbaySoldDateKey(b.date) || String(b.date || '');
                const da = SHARED_UTILS.normalizeEbaySoldDateKey(a.date) || String(a.date || '');
                return db.localeCompare(da);
            }).slice(0, 2);

            recentSold.forEach(sale => {
                const priceTxt = fmt(sale.price);
                const shortTitle = sale.title.length > 70 ? sale.title.slice(0, 68) + '\u2026' : sale.title;
                listingCards.push(`
                    <div style="background:rgba(16,185,129,0.06);border:1px solid rgba(16,185,129,0.2);border-radius:6px;padding:0.45rem 0.65rem;display:flex;justify-content:space-between;align-items:flex-start;gap:0.5rem;">
                        <div style="font-size:0.78rem;line-height:1.35;flex:1;min-width:0;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;color:#d1fae5;">
                            <span style="font-size:0.6rem;background:rgba(16,185,129,0.2);color:#6ee7b7;border-radius:3px;padding:0.1rem 0.3rem;margin-right:0.35rem;vertical-align:middle;">SOLD ${esc(sale.date)}</span>${esc(shortTitle)}
                        </div>
                        <div style="font-size:0.88rem;font-weight:600;color:#6ee7b7;white-space:nowrap;flex-shrink:0;">${esc(priceTxt)}</div>
                    </div>`);
            });

            const viewAllLink = schUrl && Number.isFinite(ebN)
                ? `<a class="card-detail-link" style="color:#34d399;font-size:0.78rem;" href="${ebAttr(schUrl)}" target="_blank" rel="noopener noreferrer">${esc(totStr)} eBay listings&#8599;</a>`
                : (schUrl
                    ? `<a class="card-detail-link" style="color:#34d399;font-size:0.78rem;" href="${ebAttr(schUrl)}" target="_blank" rel="noopener noreferrer">eBay listings&#8599;</a>`
                    : '');

            ebayBrowseBlock = `
            <div class="card-detail-section">
                <h4 style="display:flex;align-items:center;justify-content:space-between;gap:1rem;">
                    <span>eBay Active Listings</span>
                    ${viewAllLink}
                </h4>
                ${kpiRow ? `<div class="card-detail-ebay-kpi-wrap">${kpiRow}</div>` : ''}
                ${listingCards.length ? `
                <div class="card-detail-ebay-listings-scroll" style="max-height:261px;overflow-y:auto;display:flex;flex-direction:column;gap:0.4rem;padding-right:3px;margin-top:0.35rem;">
                        ${listingCards.join('')}
                    </div>` : ''}
                <p style="margin:0.65rem 0 0;font-size:0.72rem;color:#475569;">Active listing snapshot · sold medians &amp; daily counts are in the unified <strong>Price history</strong> chart when sold data exists · query: <code style="font-size:0.85em;color:#64748b;">${esc(card.ebay_browse_query ? String(card.ebay_browse_query) : '—')}</code></p>
            </div>`;
        }

        const gradedSrc = card.gemrate != null ? 'GemRate Universal Pop' : 'Legacy / merged population';
        const gradedBlock = SHARED_UTILS.buildGemrateGradedPopulationSection(card, set, gradedSrc);

        let flagsBlock = '';
        const flags = [];
        if (card.is_promo === true) flags.push('Promo');
        if (card.is_reprint_subset === true) flags.push('Subset / reprint-style');
        if (flags.length) {
            flagsBlock = `<div class="card-detail-section"><h4>Flags</h4><div class="card-detail-tags">${flags.map((f) => `<span class="card-detail-tag">${esc(f)}</span>`).join('')}</div></div>`;
        }

        const priceUpdated = set && set.tcgtracking_price_updated != null
            ? esc(String(set.tcgtracking_price_updated))
            : '';
        const foot = priceUpdated
            ? `<p class="card-detail-footnote">Set-level TCG price refresh: <strong>${priceUpdated}</strong></p>`
            : '';

        const predictCta = hidePredictorLink
            ? ''
            : ` <a href="predictor.html?set=${encodeURIComponent(setCode)}&num=${encodeURIComponent(num)}&name=${encodeURIComponent(nm)}" class="card-detail-tag" style="background: rgba(59, 130, 246, 0.2); color: #60a5fa; text-decoration: none; border: 1px solid rgba(59, 130, 246, 0.4); margin-left: 0.5rem; cursor: pointer;">🔮 Predict Value</a>`;
        const artistFrag = card.artist ? ` · <span class="card-detail-hero-meta-art">${esc(String(card.artist))}</span>` : '';
        const pullOddsStr = card.card_pull_rate ? esc(String(card.card_pull_rate)) : '';
        const pullSlotStr = rr ? esc(String(rr)) : '';
        const pullHeroLine = [pullOddsStr && `Dex odds: ${pullOddsStr}`, pullSlotStr && `Slot: ${pullSlotStr}`].filter(Boolean).join(' · ');
        const pullHeroHtml = pullHeroLine
            ? `<div class="card-detail-hero-compact-pull">${pullHeroLine}</div>`
            : '';

        const marketLiquidityBlock = statRows.length ? `
            <div class="card-detail-liquidity-strip">
                ${statsStrip}
            </div>` : '';
        const tcggoScoreBlock = SHARED_UTILS.buildTcggoStyleScorecardHtml(card);

        const marketRowHtml = wizardBlock || ebayBrowseBlock
            ? `<div class="card-detail-market-row">${wizardBlock}${ebayBrowseBlock}</div>`
            : '';

        let collectricsPairRow = '';
        if (collectricsSnapBlock && collectricsTrendBlock) {
            collectricsPairRow = `<div class="card-detail-dense-row card-detail-dense-row--collectrics-pair">${collectricsSnapBlock}${collectricsTrendBlock}</div>`;
        } else {
            collectricsPairRow = collectricsSnapBlock + collectricsTrendBlock;
        }

        const flagsSlotHtml = flagsBlock
            ? `<div class="card-detail-dense-flags-slot">${flagsBlock}</div>`
            : '';
        const contextColumnHtml = `${collectricsPairRow}${marketRowHtml}${flagsSlotHtml}`;
        const chartsTrim = chartsBlock.trim();
        const chartsColumnHtml = chartsTrim ? `${chartsBlock}${gradedBlock}` : '';
        const bodyMainHtml = chartsTrim
            ? `<div class="card-detail-dense-main-split">
                <div class="card-detail-dense-main-split__charts">${chartsColumnHtml}</div>
                <div class="card-detail-dense-main-split__context">${contextColumnHtml}</div>
            </div>`
            : `<div class="card-detail-dense-main-split card-detail-dense-main-split--no-charts">${collectricsPairRow}${marketRowHtml}${gradedBlock}${flagsSlotHtml}</div>`;

        return `
            <div class="card-detail-body card-detail-body--dense">
            <div class="card-detail-dense-hero">
                <div class="card-detail-hero-compact">
                    <h2 class="card-detail-hero-compact-title" id="cardDetailTitle">${esc(nm)}</h2>
                    <div class="card-detail-hero-compact-meta">${esc(setTitle)} · <code>${esc(setCode)}</code> · #${esc(num)}${release ? ` · ${esc(release)}` : ''}${artistFrag}</div>
                    ${pullHeroHtml}
                    <div class="card-detail-tags card-detail-hero-compact-tags">${tagHtml}${predictCta}</div>
                </div>
            </div>
            ${marketLiquidityBlock}
            ${tcggoScoreBlock}
            ${bodyMainHtml}
            ${foot}
            </div>
        `;
    },

    buildCardDetailFactsHtml(card, set) {
        const esc = SHARED_UTILS.escHtml;
        const fmt = SHARED_UTILS.fmtUsd;
        const setTitle = set && set.set_name ? String(set.set_name) : 'Set';
        const setCode = set && set.set_code ? String(set.set_code) : '';
        const release = set && set.release_date ? String(set.release_date) : '';
        const num = card.number != null ? String(card.number) : '?';
        const nm = card.name ? String(card.name) : 'Card';
        const rr = set && set.rarity_pull_rates && card.rarity ? set.rarity_pull_rates[card.rarity] : null;

        const tags = [];
        if (card.rarity) tags.push(String(card.rarity));
        if (card.supertype) tags.push(String(card.supertype));
        if (card.subtypes) tags.push(Array.isArray(card.subtypes) ? card.subtypes.join(' · ') : String(card.subtypes));
        const tagHtml = tags.map((t) => `<span class="card-detail-tag">${esc(t)}</span>`).join('');

        const blendUsd = SHARED_UTILS.resolveExplorerChartUsd(card);
        const statRows = [];
        
        // --- Unified Market Data ---
        statRows.push({ lbl: 'Market / Scraper', val: fmt(card.market_price) });
        if (blendUsd != null && Number.isFinite(blendUsd)) {
            statRows.push({ lbl: 'Blended median', val: fmt(blendUsd) });
        }
        
        if (card.pokemon_wizard_url) {
            const wcur = card.pokemon_wizard_current_price_usd != null && Number.isFinite(Number(card.pokemon_wizard_current_price_usd))
                ? fmt(card.pokemon_wizard_current_price_usd) : '—';
            const wt = card.pokemon_wizard_current_trend_pct != null && Number.isFinite(Number(card.pokemon_wizard_current_trend_pct))
                ? `${Number(card.pokemon_wizard_current_trend_pct).toFixed(2)}%` : '—';
            statRows.push({ lbl: 'Wizard Retail', val: `${wcur} (${wt})` });
        }
        if (card.pricecharting_used_price_usd != null && Number.isFinite(Number(card.pricecharting_used_price_usd))) {
            statRows.push({ lbl: 'PC ungraded', val: fmt(card.pricecharting_used_price_usd) });
        }
        if (card.pricecharting_graded_price_usd != null && Number.isFinite(Number(card.pricecharting_graded_price_usd))) {
            statRows.push({ lbl: 'PC graded', val: fmt(card.pricecharting_graded_price_usd) });
        }

        if (card.pricedex_market_usd != null) statRows.push({ lbl: 'PriceDex', val: fmt(card.pricedex_market_usd) });
        if (card.tcgtracking_market_usd != null) statRows.push({ lbl: 'TCGTracking', val: fmt(card.tcgtracking_market_usd) });
        
        const ceList = Number(card.collectrics_ebay_listings);
        const ceVol = Number(card.collectrics_ebay_sold_volume);
        if (ceList > 0) statRows.push({ lbl: 'eBay list (Coll.)', val: ceList.toLocaleString() });
        if (ceVol > 0) statRows.push({ lbl: 'eBay vol (Coll.)', val: ceVol.toLocaleString() });

        const statsGrid = statRows.map((s) => `
            <div class="card-detail-stat">
                <span class="lbl">${esc(s.lbl)}</span>
                <span class="val">${s.val}</span>
            </div>`).join('');

        const gradedSrcFacts = card.gemrate != null ? 'GemRate Universal Pop' : 'Merged population';
        const gradedBlock = SHARED_UTILS.buildGemrateGradedPopulationSection(card, set, gradedSrcFacts);
        const pullFactsOdds = card.card_pull_rate ? esc(String(card.card_pull_rate)) : '';
        const pullFactsSlot = rr ? esc(String(rr)) : '';
        const pullFactsLine = [pullFactsOdds && `Dex odds: ${pullFactsOdds}`, pullFactsSlot && `Slot: ${pullFactsSlot}`].filter(Boolean).join(' · ');
        const pullFactsHtml = pullFactsLine
            ? `<div class="card-detail-sub card-detail-sub--spaced card-detail-hero-compact-pull">${pullFactsLine}</div>`
            : '';

        return `
            <div class="card-detail-hero-layout">
                <div class="card-detail-hero-identity">
                    <div class="card-detail-price-hero">${fmt(blendUsd || card.market_price)}</div>
                    <h2 class="card-detail-title">${esc(nm)}</h2>
                    <div class="card-detail-sub card-detail-sub--spaced">${esc(setTitle)} · #${esc(num)}</div>
                    <div class="card-detail-sub card-detail-sub--spaced">${release ? `Released: ${esc(release)}` : ''}</div>
                    ${pullFactsHtml}
                    <div class="card-detail-tags">${tagHtml} <a href="predictor.html?set=${encodeURIComponent(setCode)}&num=${encodeURIComponent(num)}&name=${encodeURIComponent(nm)}" class="card-detail-tag" style="background: rgba(59, 130, 246, 0.2); color: #60a5fa; text-decoration: none; border: 1px solid rgba(59, 130, 246, 0.4); margin-left: 0.5rem; cursor: pointer;">🔮 Predict Value</a></div>
                    ${card.artist ? `<div class="card-detail-sub card-detail-illustrator">Illustrator: <strong>${esc(card.artist)}</strong></div>` : ''}
                </div>
                <div class="card-detail-hero-market">
                    <div class="card-detail-section card-detail-section--no-border">
                        <h4>Market indicators <span style="font-weight:400;color:#94a3b8;">(Prices & Liquidity)</span></h4>
                        <div class="card-detail-stat-grid">${statsGrid}</div>
                    </div>
                </div>
            </div>

            ${gradedBlock}
        `;
    },

    buildPriceHistoryTableHtml(card) {
        if (!card) return '';
        const esc = SHARED_UTILS.escHtml;
        const fmt = SHARED_UTILS.fmtUsd;
        const parts = [];
        if (card.pokemon_wizard_url) {
            let histRows = '';
            const ph = SHARED_UTILS.filterWizardPriceHistoryRows(card.pokemon_wizard_price_history || []);
            ph.slice(0, 15).forEach((row) => {
                const pr = row.price_usd != null ? fmt(row.price_usd) : '—';
                histRows += `<tr><td>${esc(String(row.label || '—'))}</td><td>${pr}</td><td>${esc(String(row.trend || '—'))}</td></tr>`;
            });
            if (histRows) {
                parts.push(`
            <div class="card-detail-section card-detail-section--full-width">
                <h4>Price history <span style="font-weight:400;color:#94a3b8;">(Wizard Retail Trends)</span></h4>
                <div class="card-detail-table-scroll"><table class="card-detail-mini-table"><thead><tr><th>When</th><th>Price</th><th>Trend</th></tr></thead><tbody>${histRows}</tbody></table></div>
            </div>`);
            }
        }
        const pcTbl = SHARED_UTILS.buildPricechartingSnapshotTableHtml(card);
        if (pcTbl) {
            parts.push(`
            <div class="card-detail-section card-detail-section--full-width">
                <h4>Price history <span style="font-weight:400;color:#94a3b8;">(PriceCharting snapshots)</span></h4>
                ${pcTbl}
            </div>`);
        }
        return parts.join('');
    },

    buildCardDetailPanelHtml(card, set) {
        return SHARED_UTILS.buildCardDetailFactsHtml(card, set) + 
               SHARED_UTILS.buildPriceHistoryTableHtml(card) +
               SHARED_UTILS.cardChartsSectionHtml(card);
    },

    initCardDetailCharts(card, containerCharts, chartOpts) {
        if (typeof Chart === 'undefined') return;
        const co = chartOpts && typeof chartOpts === 'object' ? chartOpts : {};
        let histMo = co.historyWindowMonths != null ? Number(co.historyWindowMonths) : NaN;
        if (!Number.isFinite(histMo) || histMo <= 0) {
            try {
                histMo = parseInt(sessionStorage.getItem('ptcg-unified-history-months'), 10);
            } catch (e) {
                histMo = NaN;
            }
        }
        if (![1, 3, 6, 12].includes(histMo)) histMo = 3;

        const { tickColor, gridColor } = SHARED_UTILS.getChartAxisColors();
        const idPrefix = card.id ? card.id.replace(/\W/g, '_') : 'shared';
        const baseScale = { ticks: { color: tickColor }, grid: { color: gridColor } };

        const ceBlend = Array.isArray(card.collectrics_price_history) ? card.collectrics_price_history : [];
        const ceJust = Array.isArray(card.collectrics_history_justtcg) ? card.collectrics_history_justtcg : [];
        const ceEbayHist = Array.isArray(card.collectrics_history_ebay) ? card.collectrics_history_ebay : [];
        const collectricsSplit = ceJust.length > 1 && ceEbayHist.length > 1;
        const collectricsJustOnly = ceJust.length > 1 && !collectricsSplit;
        const collectricsEbayHistOnly = ceEbayHist.length > 1 && !collectricsSplit;
        const collectricsBlendOnly = ceBlend.length > 1 && !collectricsSplit && !collectricsJustOnly && !collectricsEbayHistOnly;
        const unifiedMarketPack = SHARED_UTILS.buildExplorerUnifiedPriceChartPack(card, { historyWindowMonths: histMo });
        const canvasUnifiedDyn = document.getElementById(`${idPrefix}_ceUnified`);

        if (unifiedMarketPack && canvasUnifiedDyn) {
            const prevU = typeof Chart !== 'undefined' && typeof Chart.getChart === 'function' ? Chart.getChart(canvasUnifiedDyn) : null;
            if (prevU) {
                try {
                    prevU.destroy();
                } catch (e) { /* ignore */ }
            }
            const scalesUnifiedDyn = {
                x: { ...baseScale, ticks: { ...baseScale.ticks, maxRotation: 45, autoSkip: true } },
                y: {
                    ...baseScale,
                    grace: '22%',
                    title: { display: true, text: 'USD', color: tickColor },
                },
            };
            if (unifiedMarketPack.hasY1Axis) {
                scalesUnifiedDyn.y1 = {
                    position: 'right',
                    beginAtZero: true,
                    grid: { drawOnChartArea: false },
                    title: { display: true, text: 'Sold / day', color: tickColor },
                    ticks: { color: tickColor, precision: 0 },
                };
            }
            const legendCompactDyn = {
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
            containerCharts.push(new Chart(canvasUnifiedDyn, {
                type: 'line',
                data: { labels: unifiedMarketPack.labels, datasets: unifiedMarketPack.datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    layout: { padding: { bottom: 2 } },
                    plugins: {
                        title: { display: false },
                        legend: legendCompactDyn,
                        tooltip: { mode: 'index', intersect: false },
                    },
                    scales: scalesUnifiedDyn,
                },
            }));
        } else if (collectricsBlendOnly) {
            const canvasCe = document.getElementById(`${idPrefix}_ceRaw`);
            if (canvasCe && ceBlend.length >= 1) {
                const sorted = ceBlend.slice().sort((a, b) => String(a.date || '').localeCompare(String(b.date || '')));
                containerCharts.push(new Chart(canvasCe, {
                    type: 'line',
                    data: {
                        labels: sorted.map((r) => String(r.date || '')),
                        datasets: [{
                            label: 'Blend',
                            data: sorted.map((r) => Number(r.raw_price)),
                            borderColor: '#3b82f6',
                            backgroundColor: 'rgba(59,130,246,0.12)',
                            tension: 0.2,
                        }],
                    },
                    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { color: tickColor } } }, scales: { x: baseScale, y: baseScale } },
                }));
            }
        }

        const mktRowsShared = SHARED_UTILS.collectricsEbayMarketHistorySorted(card);
        if (mktRowsShared.length > 1) {
            SHARED_UTILS.mountCollectricsEbayMarketVolumeChart(
                document.getElementById(`${idPrefix}_ceEbayMarketVol`),
                mktRowsShared,
                tickColor,
                baseScale,
                containerCharts,
            );
        }

        if (!(unifiedMarketPack && unifiedMarketPack.skipStandaloneWizard)) {
            SHARED_UTILS.mountPokemonWizardRetailTrendChart(
                document.getElementById(`${idPrefix}_wizPrice`),
                card,
                containerCharts,
            );
        }

        const unifiedCanvas = document.getElementById(`${idPrefix}_ceUnified`);
        if (unifiedCanvas) {
            const block = unifiedCanvas.closest('.card-detail-chart-block');
            if (block) SHARED_UTILS.syncUnifiedHistoryRangeUi(block, histMo);
        }
    }
};

window.SHARED_UTILS = SHARED_UTILS;

/**
 * Thin bottom progress bar: each page calls begin/setDeterminate/end around its own fetch / chart work.
 */
(function pageLoadProgressIife() {
    const CL_VISIBLE = 'page-load-progress--visible';
    const CL_IND = 'page-load-progress__fill--indeterminate';

    let active = 0;
    let bar = null;
    let fill = null;

    function mount() {
        if (bar) return;
        bar = document.createElement('div');
        bar.id = 'page-load-progress';
        bar.className = 'page-load-progress';
        bar.setAttribute('aria-hidden', 'true');
        fill = document.createElement('div');
        fill.className = 'page-load-progress__fill';
        bar.appendChild(fill);
        (document.body || document.documentElement).appendChild(bar);
    }

    function showIndeterminate() {
        mount();
        bar.classList.add(CL_VISIBLE);
        bar.setAttribute('aria-hidden', 'false');
        fill.classList.add(CL_IND);
        fill.style.width = '';
        fill.style.removeProperty('transform');
    }

    function showDeterminate(p) {
        mount();
        bar.classList.add(CL_VISIBLE);
        bar.setAttribute('aria-hidden', 'false');
        fill.classList.remove(CL_IND);
        const x = Math.max(0, Math.min(1, Number(p) || 0));
        fill.style.width = `${Math.round(x * 1000) / 10}%`;
    }

    function hideBar() {
        if (!bar) return;
        bar.classList.remove(CL_VISIBLE);
        bar.setAttribute('aria-hidden', 'true');
        fill.classList.remove(CL_IND);
        fill.style.width = '0%';
    }

    window.PTCG_PAGE_PROGRESS = {
        begin() {
            active++;
            if (active === 1) showIndeterminate();
        },
        end() {
            active = Math.max(0, active - 1);
            if (active === 0) hideBar();
        },
        /** @param {number} p 0..1 — switches from indeterminate to a filled segment */
        setDeterminate(p) {
            active = Math.max(active, 1);
            showDeterminate(p);
        },
        reset() {
            active = 0;
            hideBar();
        }
    };
})();
