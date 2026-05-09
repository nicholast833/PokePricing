document.addEventListener('DOMContentLoaded', () => {
    if (typeof Chart !== 'undefined' && typeof ChartZoom !== 'undefined') {
        try {
            Chart.unregister(ChartZoom);
        } catch (_e) {
            /* plugin not registered */
        }
    }

    const loadingEl = document.getElementById('loading');
    const busyEl = document.getElementById('analytics-busy');
    const containerEl = document.getElementById('analytics-container');
    const titleEl = document.getElementById('analytics-title');
    const metaEl = document.getElementById('analytics-meta');
    const addSetSelect = document.getElementById('addSetSelect');

    const COMPOSITE_CHART_KEY = 'composite';
    /** Non-composite panels; DOM order under #chart-dashboard-sub is sorted by |r| vs price after each rebuild. */
    const SECONDARY_CHART_KEYS = ['pullCost', 'character', 'artistChase', 'trends', 'rarityTier', 'setVintage', 'tcgMacroInterest', 'gradedPop', 'hypeScarcity', 'hypePullRatio'];
    let secondaryChartOrder = SECONDARY_CHART_KEYS.slice();
    /** X is ≥ 0 by construction (not z-blends); composite & pullCost are excluded here. */
    const SCATTER_X_NON_NEGATIVE_CHARTS = new Set([
        'rarityTier', 'setVintage', 'character', 'gradedPop',
        'tcgMacroInterest', 'artistChase', 'trends',
        'hypeScarcity', 'hypePullRatio'
    ]);

    function allChartKeys() {
        return [COMPOSITE_CHART_KEY, ...secondaryChartOrder];
    }

    /**
     * Species graded “stretch” maps slab share s∈[0,1] to rawStretch = floor + (1−floor)×s (endpoints: floor..1).
     * Axis multiplier = 1 + POPULARITY_GRADED_TO_AXIS_BLEND × (rawStretch − 1), so the signal is pulled toward 1×
     * file popularity and does not collapse X variance (full blend at weight 1 was crushing |r| vs price).
     */
    const POPULARITY_SPECIES_GRADED_FLOOR = 0.38;
    const POPULARITY_GRADED_TO_AXIS_BLEND = 0.28;
    /**
     * Popularity scatter X: mild extra stretch from this card’s log₁₀(1+graded) vs the set’s median among
     * charted rows (demand signal beyond export-wide species slab share).
     */
    const POPULARITY_SCATTER_SLAB_VS_SET_BLEND = 0.12;
    /** Hype×scarcity: weight on (set median log pop − card) and (modern-era median log pop − card). */
    const HYPE_SCARCITY_SLAB_VS_SET_WEIGHT = 0.24;
    const HYPE_SCARCITY_SLAB_VS_MODERN_ERA_WEIGHT = 0.18;
    /** LSRL fits log₁₀(price) vs log₁₀(max(x, ε)) — heavy-tailed positive X otherwise yields tiny |r| vs log y. */
    const LSRL_LOG_X_MIN_POS = 1e-9;

    const MEGA_HYPER_PULL_COST_FACTOR = 0.62;
    const GOLD_SECRET_PULL_COST_FACTOR = 0.58;
    /** Blend card-specific odds vs slot/K when both exist */
    const PULL_BLEND_CARD = 0.55;
    const PULL_BLEND_SLOT = 0.45;
    /**
     * POP / McDonald's / Trainer Kit product lines often carry Dex "1 in N packs" on each card that
     * reflects pooled catalog search, not opening a standard booster—blending that with slot odds
     * mis-states ln(price/pull) and composite drivers. We drop the per-card branch and down-weight LSRL.
     */
    const NON_BOOSTER_PULL_REGRESSION_WEIGHT = 0.4;
    /** Composite LSRL: downweight dense X≈0 stack before Huber (POP / imputed ln(p/u) neutralization). */
    const COMPOSITE_LSRL_LOW_X_WEIGHT_FLOOR = 0.42;
    const COMPOSITE_LSRL_LOW_X_BLEND_AT_ABS = 0.28;
    /** One Huber iteration on WLS residuals (log₁₀ y for composite). */
    const LSRL_HUBER_C = 1.345;
    const LSRL_HUBER_MIN_WEIGHT_MULT = 0.12;
    const LSRL_ROBUST_SECOND_PASS_MIN_N = 5;
    /** Median log₁₀(1+graded) on “modern-era” sets in the current scatter sample (see `buildScatterModernGradedMedianLog`). */
    let scatterModernGradedMedianLogPop = null;
    /**
     * LSRL dashed line + footer Pearson r use log₁₀(market $) vs X (same core convention as composite).
     * Pull-cost chart Y is ln(market/pull)—excluded here.
     */
    const CHART_LSRL_LOG10_MARKET_PRICE = new Set([
        'character', 'trends', 'rarityTier', 'setVintage', 'tcgMacroInterest', 'gradedPop',
        'hypeScarcity', 'hypePullRatio', 'artistChase'
    ]);
    /** LSRL x-axis uses log₁₀(display x) for these charts (y still log₁₀ price when in CHART_LSRL_LOG10_MARKET_PRICE). */
    const CHART_LSRL_LOG10_X_FIT = new Set(['trends', 'hypeScarcity', 'hypePullRatio', 'character']);

    /** X value fed to WLS / Pearson for LSRL (display scatter still uses linear x). */
    function lsrlDisplayXToFitX(chartId, xDisplay) {
        if (!CHART_LSRL_LOG10_X_FIT.has(chartId)) return Number(xDisplay);
        const xv = Number(xDisplay);
        if (!Number.isFinite(xv) || xv <= 0) return NaN;
        return Math.log10(Math.max(xv, LSRL_LOG_X_MIN_POS));
    }

    function isNonBoosterProductPullCatalogSet(setInfo) {
        if (!setInfo) return false;
        const sc = String(setInfo.set_code || '').trim().toLowerCase();
        if (/^pop\d+$/.test(sc) || /^mcd\d+$/.test(sc)) return true;
        const ser = String(setInfo.series || '').trim().toUpperCase();
        if (ser === 'POP') return true;
        const sn = String(setInfo.set_name || '').toLowerCase();
        if (/\btrainer kit\b/.test(sn)) return true;
        if (/\bmcdonald'?s collection\b/.test(sn)) return true;
        if (/\bpop series\b/.test(sn)) return true;
        if (/\btcg classic\b/.test(sn)) return true;
        if (sc === 'cel25c') return true; // Celebrations Classic Collection
        return false;
    }

    /** When false, Dex per-card pack odds are ignored (slot / rarity table only). */
    function usesBoosterStyleCardPullOdds(setInfo, card) {
        if (card && card.is_promo === true) return false;
        if (isNonBoosterProductPullCatalogSet(setInfo)) return false;
        return true;
    }

    function nonBoosterPullRegressionWeightMult(setInfo, card) {
        return usesBoosterStyleCardPullOdds(setInfo, card) ? 1.0 : NON_BOOSTER_PULL_REGRESSION_WEIGHT;
    }

    /**
     * Composite ln(price/pull): fixed-product rows are excluded from this driver when fitting μ, σ, and r
     * (booster-only pool). At scoring they use means.logPremium so z≈0 on that driver (pull chart unchanged).
     */
    function logPremiumForCompositeTraining(logPremium, setInfo, card) {
        if (logPremium == null || !Number.isFinite(logPremium)) return null;
        if (!usesBoosterStyleCardPullOdds(setInfo, card)) return null;
        return logPremium;
    }

    function logPremiumForCompositeScore(logPremium, setInfo, card, model) {
        if (logPremium == null || !Number.isFinite(logPremium)) return null;
        if (usesBoosterStyleCardPullOdds(setInfo, card)) return logPremium;
        const mu = model && model.means && model.means.logPremium;
        return Number.isFinite(mu) ? mu : null;
    }
    /** |Pearson r| vs price to include a driver in the composite X (moderate or stronger) */
    const COMPOSITE_MODERATE_ABS_R = 0.30;
    /** If no driver clears COMPOSITE_MODERATE_ABS_R, use the single strongest with at least this |r| */
    const COMPOSITE_FALLBACK_ABS_R = 0.15;
    /** Composite scatter: fixed z-blend X span and top of log price axis (per analytics framing). */
    const COMPOSITE_AXIS_X_MIN = -5;
    const COMPOSITE_AXIS_X_MAX = 3;
    const COMPOSITE_AXIS_Y_MAX_USD = 5000;
    /** Scatter default floor ($); sub-$5 can be included via checkbox (often helps composite |r|). */
    const MIN_CHART_MARKET_PRICE = 5;
    /** When true, `isChartableMarketPrice` accepts any finite price &gt; 0 (still excludes $0 / missing). */
    let analyticsIncludeSubFiveDollar = true;

    function chartScatterMinUsd() {
        return analyticsIncludeSubFiveDollar ? 1e-9 : MIN_CHART_MARKET_PRICE;
    }
    /** Uniform point styling; expansion/set identity appears in hover tooltips only. */
    const SCATTER_POINT_BG = '#0f172a';
    const SCATTER_POINT_BORDER = 'rgba(226, 232, 240, 0.92)';
    const SCATTER_THUMB_RING = 'rgba(100, 116, 139, 0.92)';
    /** Default scatter glyph size (−15% vs legacy 15 / 18). */
    const SCATTER_POINT_RADIUS = 15 * 0.85;
    const SCATTER_POINT_HOVER_RADIUS = 18 * 0.85;
    /**
     * Margin from data extrema to axis limits in plot pixels → data units.
     * Covers point radius, border, card-thumb overlay, and a buffer so art is not clipped.
     */
    const SCATTER_AXIS_EDGE_PAD_PX = Math.ceil(SCATTER_POINT_RADIUS + 38);
    /** Darken sampled card-art tint for circle fill (matches prior scatter thumb look). */
    const SCATTER_FILL_DARKEN_FACTOR = 0.46;
    /**
     * Base Set (base1) Charizard is omitted from every analytics scatter and from character-volume
     * calibration pairs — a single ultra-thin legacy listing dominates log-price geometry across decades.
     */
    function isExcludedLegacyBaseCharizardOutlier(setInfo, card) {
        if (!setInfo || !card) return false;
        if (String(setInfo.set_code || '').trim().toLowerCase() !== 'base1') return false;
        return String(card.name || '').trim().toLowerCase() === 'charizard';
    }
    /** Fixed “as of” instant for set-age in years (UTC); change when refreshing vintage semantics */
    const ANALYTICS_AS_OF_MS = Date.UTC(2026, 3, 11);
    /**
     * Set-age axis: sqrt(years) with a cap so 1999–2026 pools do not stretch mostly from WotC leverage alone.
     * X = sqrt(min(years, cap)) / sqrt(cap) in [0,1]; tooltips still show raw years.
     */
    const SET_VINTAGE_SQRT_CAP_YEARS = 28;
    /** Min points per rarity tier to join residual-based calibration (composite weights) */
    const RARITY_CALIB_MIN_BUCKET = 4;
    const RARITY_CALIB_CLAMP = [0.55, 1.85];
    const RARITY_CALIB_MIN_ROWS = 12;
    /** Optional quadratic volume on X when sample is large and |r| vs log(price) improves */
    const CHARACTER_QUADRATIC_MIN_N = 40;
    const CHARACTER_QUADRATIC_R_IMPROVE = 0.018;

    let allSetsData = [];
    let characterData = [];
    let nostalgiaData = {};
    let trendsData = [];
    /** `species_key` / normalized `display_name` → row from `species_popularity_list.json` (survey + Sparklez). */
    const speciesPopularityByKey = new Map();
    /** Sum of PSA-style graded totals per base species (whole `pokemon_sets_data.json` export); dual-name cards split the count. */
    const speciesGradedPopSumByKey = new Map();
    let speciesGradedPopMaxCached = 0;
    /** Rows from artist_scores.json (scraper); lookup by artist name (case-insensitive). */
    let artistChaseData = [];
    const artistChaseByNorm = new Map();
    /** Calendar year (set release) → hobby-wide interest index from tcg_macro_interest_by_year.json */
    let tcgMacroByYear = new Map();
    let latestTcgMacroSeriesLabel = '';
    const charts = {
        pullCost: null,
        composite: null,
        character: null,
        artistChase: null,
        trends: null,
        hypeScarcity: null,
        hypePullRatio: null,
        rarityTier: null,
        setVintage: null,
        tcgMacroInterest: null,
        gradedPop: null
    };

    /** Built from pooled cards before each rebuild; used by createDatasets for composite X */
    let latestCompositeModel = null;
    /** Last rebuild: which driver-screen + hype variant maximized |r|(composite X, log₁₀ price) on training rows. */
    let latestCompositeModelPickMeta = null;
    /** Tier → multiplier vs static rarity weights (from ln-price vs ln-pull residual medians); empty before first fit */
    let latestRarityTierCalibration = {};
    /** Character chart X: linear volume or v + v² / scale when it improves correlation */
    let latestCharacterVolumeTransform = { mode: 'linear', vScale: 1 };

    /** Chart.js may drop custom fields on datasets; keep LSRL identifiable without relying on lsrlLine alone */
    function isLsrlDataset(d) {
        if (!d) return false;
        if (d.lsrlLine === true) return true;
        return d.type === 'line' && d.label === 'LSRL';
    }

    let addedSetIndices = new Set();
    /** `${image_url}_${ring}` → decoded `HTMLImageElement` for plugin draw (no per-point off-DOM canvases). */
    const imageCache = new Map();
    const pendingScatterThumbLoads = new Set();
    /** `image_url` → `#rrggbb` from decoded card art (top half) for placeholder tint until overlay draws. */
    const thumbCenterColorByUrl = new Map();

    function urlHashPlaceholderColor(url) {
        const s = String(url || '');
        let h = 0;
        for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
        const hue = h % 360;
        return `hsl(${hue}, 32%, 26%)`;
    }

    function scatterPointFillFromHex(hex) {
        const m = /^#?([0-9a-f]{6})$/i.exec(String(hex || '').trim());
        if (!m) return SCATTER_POINT_BG;
        const n = parseInt(m[1], 16);
        const f = SCATTER_FILL_DARKEN_FACTOR;
        const r = Math.round(((n >> 16) & 255) * f);
        const g = Math.round(((n >> 8) & 255) * f);
        const b = Math.round((n & 255) * f);
        return `rgb(${r},${g},${b})`;
    }

    /** Average color of the top half of the card bitmap (art-heavy region vs name block at bottom). */
    function sampleImageTopHalfColor(img) {
        try {
            const w = img.naturalWidth || img.width;
            const h = img.naturalHeight || img.height;
            if (!w || !h) return null;
            const c = document.createElement('canvas');
            c.width = 1;
            c.height = 1;
            const ctx = c.getContext('2d', { willReadFrequently: true });
            if (!ctx) return null;
            const srcH = Math.max(1, Math.floor(h / 2));
            ctx.drawImage(img, 0, 0, w, srcH, 0, 0, 1, 1);
            const d = ctx.getImageData(0, 0, 1, 1).data;
            const rr = d[0];
            const gg = d[1];
            const bb = d[2];
            return `#${[rr, gg, bb].map((x) => x.toString(16).padStart(2, '0')).join('')}`;
        } catch (_e) {
            return null;
        }
    }

    /**
     * Cached images often fire `onload` synchronously; each `chart.update()` has a deep call stack.
     * Hundreds of nested updates in one rebuild overflows the stack. Batch updates to the next frame.
     */
    const pendingThumbChartIds = new Set();
    let pendingThumbChartRaf = null;

    let scatterThumbScrollBound = false;
    let scatterThumbScrollRaf = null;
    /** Debounced per-chart passes: pixel hit-test, viewport clip, priority order, staggered loads (no grid occlusion — coarse cells hid valid points). */
    const scatterThumbLayoutTimers = new Map();
    const SCATTER_THUMB_VIEW_MARGIN = 40;
    /** First N thumbs load in rAF chunks; remainder use idle queue (still only on-screen in chartArea ∩ viewport). */
    const SCATTER_THUMB_PRIORITY_CAP = 60;
    const SCATTER_THUMB_RAF_CHUNK = 12;
    const SCATTER_THUMB_LAYOUT_DEBOUNCE_MS = 30;
    /** First-visit warm cache: decode up to this many unique card images before leaving the loading overlay. */
    const INITIAL_SCATTER_THUMB_PRELOAD_MAX = 420;
    const INITIAL_SCATTER_THUMB_PRELOAD_CONCURRENCY = 8;

    function cancelPendingThumbnailUpdates() {
        if (pendingThumbChartRaf != null) {
            cancelAnimationFrame(pendingThumbChartRaf);
            pendingThumbChartRaf = null;
        }
        pendingThumbChartIds.clear();
        scatterThumbLayoutTimers.forEach((t) => clearTimeout(t));
        scatterThumbLayoutTimers.clear();
    }

    function scheduleScatterThumbLayout(chartId) {
        if (!chartId || !charts[chartId]) return;
        const prev = scatterThumbLayoutTimers.get(chartId);
        if (prev != null) clearTimeout(prev);
        const t = setTimeout(() => {
            scatterThumbLayoutTimers.delete(chartId);
            runScatterThumbLayout(chartId);
        }, SCATTER_THUMB_LAYOUT_DEBOUNCE_MS);
        scatterThumbLayoutTimers.set(chartId, t);
    }

    function thumbPointScreenCenter(chart, px, py) {
        const canvas = chart.canvas;
        const cr = canvas.getBoundingClientRect();
        const sx = cr.width / Math.max(canvas.offsetWidth || cr.width, 1);
        const sy = cr.height / Math.max(canvas.offsetHeight || cr.height, 1);
        return { x: cr.left + px * sx, y: cr.top + py * sy };
    }

    function pixelThumbApproximatelyVisible(chart, px, py) {
        const { x, y } = thumbPointScreenCenter(chart, px, py);
        const m = SCATTER_THUMB_VIEW_MARGIN;
        const vw = window.innerWidth || 800;
        const vh = window.innerHeight || 800;
        return y > -m && y < vh + m && x > -m && x < vw + m;
    }

    function gatherScatterThumbPixelCandidates(chart) {
        const ca = chart.chartArea;
        const sx = chart.scales && chart.scales.x;
        const sy = chart.scales && chart.scales.y;
        if (!ca || ca.width <= 0 || !sx || !sy) return [];

        const list = [];
        chart.data.datasets.forEach((ds, dsi) => {
            if (isLsrlDataset(ds) || !ds.data) return;
            ds.data.forEach((p, pi) => {
                if (!p || !p.card || !p.card.image_url) return;
                const rawX = Number(p.x);
                const rawY = Number(p.y);
                if (!Number.isFinite(rawX) || !Number.isFinite(rawY)) return;
                let px;
                let py;
                try {
                    px = sx.getPixelForValue(rawX);
                    py = sy.getPixelForValue(rawY);
                } catch (_e) {
                    return;
                }
                if (!Number.isFinite(px) || !Number.isFinite(py)) return;
                if (px < ca.left || px > ca.right || py < ca.top || py > ca.bottom) return;
                if (!pixelThumbApproximatelyVisible(chart, px, py)) return;
                list.push({ ds, dsi, pi, px, py, p });
            });
        });
        return list;
    }

    function scatterPinnedPixelCandidate(chart) {
        const pin = pinnedScatterSelection;
        if (!pin || pin.chart !== chart) return null;
        const ca = chart.chartArea;
        const sx = chart.scales && chart.scales.x;
        const sy = chart.scales && chart.scales.y;
        if (!ca || !sx || !sy) return null;
        const ds = chart.data.datasets[pin.datasetIndex];
        const p = ds && ds.data[pin.dataIndex];
        if (!p || !p.card || !p.card.image_url) return null;
        const rawX = Number(p.x);
        const rawY = Number(p.y);
        if (!Number.isFinite(rawX) || !Number.isFinite(rawY)) return null;
        try {
            return {
                ds,
                dsi: pin.datasetIndex,
                pi: pin.dataIndex,
                px: sx.getPixelForValue(rawX),
                py: sy.getPixelForValue(rawY),
                p
            };
        } catch (_e) {
            return null;
        }
    }

    function scheduleScatterThumbIdleQueue(chartId, queue) {
        if (!queue || !queue.length) return;
        const perSlice = 12;
        const run = () => {
            const ch = charts[chartId];
            if (!ch || !isChartCanvasMostlyVisibleForThumbs(ch)) return;
            for (let k = 0; k < perSlice && queue.length; k++) {
                const w = queue.shift();
                if (!w) break;
                const ps = w.ds.pointStyle[w.pi];
                if (ps != null && ps !== 'circle') continue;
                const ck = `${w.p.card.image_url}_${SCATTER_THUMB_RING}`;
                if (imageCache.has(ck) || pendingScatterThumbLoads.has(ck)) continue;
                if (!pixelThumbApproximatelyVisible(ch, w.px, w.py)) continue;
                startScatterPointThumbLoad(w.ds, w.pi, w.p.card, SCATTER_THUMB_RING, chartId);
            }
            if (queue.length === 0) return;
            if (typeof window.requestIdleCallback === 'function') {
                window.requestIdleCallback(run, { timeout: 400 });
            } else {
                setTimeout(run, 20);
            }
        };
        if (typeof window.requestIdleCallback === 'function') {
            window.requestIdleCallback(run, { timeout: 500 });
        } else {
            setTimeout(run, 60);
        }
    }

    function runScatterThumbLayout(chartId) {
        const ch = charts[chartId];
        if (!ch || !isChartCanvasMostlyVisibleForThumbs(ch)) {
            console.log(`[THUMB] runScatterThumbLayout(${chartId}): chart not visible or missing, skipping`);
            return;
        }

        let ordered = gatherScatterThumbPixelCandidates(ch);
        console.log(`[THUMB] runScatterThumbLayout(${chartId}): ${ordered.length} pixel candidates gathered`);
        const pinC = scatterPinnedPixelCandidate(ch);
        if (pinC && Number.isFinite(pinC.px) && Number.isFinite(pinC.py)) {
            ordered = ordered.filter((w) => !(w.dsi === pinC.dsi && w.pi === pinC.pi));
            ordered.unshift(pinC);
        }

        ordered.forEach((w) => {
            const scr = thumbPointScreenCenter(ch, w.px, w.py);
            w.scrY = scr.y;
            w.scrX = scr.x;
        });
        ordered.sort((a, b) => {
            if (a.dsi !== b.dsi) return b.dsi - a.dsi;
            if (a.scrY !== b.scrY) return a.scrY - b.scrY;
            return a.scrX - b.scrX;
        });

        const R = SCATTER_POINT_RADIUS;
        const keptPixels = [];
        const toLoad = [];
        let skippedNotCircle = 0, skippedCached = 0, skippedPending = 0, skippedOccluded = 0;

        for (let i = 0; i < ordered.length; i++) {
            const w = ordered[i];
            const ps = w.ds.pointStyle[w.pi];
            if (ps != null && ps !== 'circle') { skippedNotCircle++; continue; }
            const ck = `${w.p.card.image_url}_${SCATTER_THUMB_RING}`;
            if (imageCache.has(ck)) { skippedCached++; continue; }
            if (pendingScatterThumbLoads.has(ck)) { skippedPending++; continue; }

            let occluded = false;
            for (let j = 0; j < keptPixels.length; j++) {
                const dx = w.scrX - keptPixels[j].x;
                const dy = w.scrY - keptPixels[j].y;
                if (dx * dx + dy * dy < R * R) { occluded = true; break; }
            }
            if (occluded) { skippedOccluded++; continue; }

            keptPixels.push({ x: w.scrX, y: w.scrY });
            toLoad.push(w);
        }
        console.log(`[THUMB] runScatterThumbLayout(${chartId}): toLoad=${toLoad.length}, skippedNotCircle=${skippedNotCircle}, skippedCached=${skippedCached}, skippedPending=${skippedPending}, skippedOccluded=${skippedOccluded}`);

        const priority = toLoad.slice(0, SCATTER_THUMB_PRIORITY_CAP);
        const rest = toLoad.slice(SCATTER_THUMB_PRIORITY_CAP);

        let idx = 0;
        const pump = () => {
            const ch2 = charts[chartId];
            if (!ch2 || !isChartCanvasMostlyVisibleForThumbs(ch2)) return;
            const end = Math.min(idx + SCATTER_THUMB_RAF_CHUNK, priority.length);
            for (; idx < end; idx++) {
                const w = priority[idx];
                startScatterPointThumbLoad(w.ds, w.pi, w.p.card, SCATTER_THUMB_RING, chartId);
            }
            if (idx < priority.length) {
                requestAnimationFrame(pump);
            } else {
                scheduleScatterThumbIdleQueue(chartId, rest);
            }
        };
        if (priority.length) requestAnimationFrame(pump);
        else scheduleScatterThumbIdleQueue(chartId, rest);
    }

    function startScatterPointThumbLoad(dataset, dataIndex, card, ringColor, chartId) {
        if (!card || !card.image_url) return;
        const cacheKey = `${card.image_url}_${ringColor}`;
        if (imageCache.has(cacheKey)) {
            scheduleScatterOverlayRepaint(chartId);
            return;
        }
        if (pendingScatterThumbLoads.has(cacheKey)) return;
        pendingScatterThumbLoads.add(cacheKey);
        console.log(`[THUMB] startLoad: ${card.image_url.slice(-40)} for chart=${chartId}`);

        const img = new Image();
        img.decoding = 'async';
        img.crossOrigin = 'Anonymous';
        img.onload = () => {
            pendingScatterThumbLoads.delete(cacheKey);
            imageCache.set(cacheKey, img);
            const tint = sampleImageTopHalfColor(img);
            if (tint && dataset.data && dataIndex < dataset.data.length) {
                thumbCenterColorByUrl.set(card.image_url, tint);
                ensureScatterPointStyleArrays(dataset, dataIndex);
                dataset.pointBackgroundColor[dataIndex] = scatterPointFillFromHex(tint);
                dataset.pointBorderColor[dataIndex] = ringColor;
            }
            console.log(`[THUMB] LOADED: ${card.image_url.slice(-40)} (${img.naturalWidth}x${img.naturalHeight}), cacheSize=${imageCache.size}`);
            scheduleScatterOverlayRepaint(chartId);
        };
        img.onerror = (ev) => {
            pendingScatterThumbLoads.delete(cacheKey);
            console.warn(`[THUMB] ERROR loading: ${card.image_url}`, ev);
            ensureScatterPointStyleArrays(dataset, dataIndex);
            dataset.pointStyle[dataIndex] = 'circle';
            dataset.pointBackgroundColor[dataIndex] = urlHashPlaceholderColor(card.image_url);
            dataset.pointBorderColor[dataIndex] = ringColor;
            scheduleScatterOverlayRepaint(chartId);
        };
        img.src = card.image_url;
    }

    /**
     * Decode a card image into `imageCache` without touching Chart.js datasets (initial preload).
     * Skips repaint; does not duplicate in-flight loads for the same URL.
     */
    function primeScatterThumbImage(url) {
        const ringColor = SCATTER_THUMB_RING;
        const cacheKey = `${url}_${ringColor}`;
        if (imageCache.has(cacheKey)) return Promise.resolve();
        if (pendingScatterThumbLoads.has(cacheKey)) return Promise.resolve();
        pendingScatterThumbLoads.add(cacheKey);
        return new Promise((resolve) => {
            const img = new Image();
            img.decoding = 'async';
            img.crossOrigin = 'Anonymous';
            img.onload = () => {
                pendingScatterThumbLoads.delete(cacheKey);
                imageCache.set(cacheKey, img);
                const tint = sampleImageTopHalfColor(img);
                if (tint) thumbCenterColorByUrl.set(url, tint);
                resolve();
            };
            img.onerror = () => {
                pendingScatterThumbLoads.delete(cacheKey);
                resolve();
            };
            img.src = url;
        });
    }

    function collectScatterImageUrlsForPreload(chart, maxAdd, seen) {
        const out = [];
        if (!chart || !chart.data || !Array.isArray(chart.data.datasets)) return out;
        const datasets = chart.data.datasets;
        for (let dsi = datasets.length - 1; dsi >= 0 && out.length < maxAdd; dsi--) {
            const ds = datasets[dsi];
            if (isLsrlDataset(ds) || !ds.data) continue;
            const meta = chart.getDatasetMeta(dsi);
            if (meta && meta.hidden) continue;
            for (let i = ds.data.length - 1; i >= 0 && out.length < maxAdd; i--) {
                const p = ds.data[i];
                const u = p && p.card && p.card.image_url;
                if (!u || seen.has(u)) continue;
                seen.add(u);
                out.push(u);
            }
        }
        return out;
    }

    function reapplyScatterPointTintsFromPreloadCache() {
        allChartKeys().forEach((key) => {
            const ch = charts[key];
            if (!ch || !ch.data || !Array.isArray(ch.data.datasets)) return;
            ch.data.datasets.forEach((ds) => {
                if (isLsrlDataset(ds) || !ds.data) return;
                ds.data.forEach((p, i) => {
                    const url = p && p.card && p.card.image_url;
                    if (!url) return;
                    const rawTint = thumbCenterColorByUrl.get(url);
                    if (!rawTint) return;
                    ensureScatterPointStyleArrays(ds, i);
                    ds.pointBackgroundColor[i] = scatterPointFillFromHex(rawTint);
                    ds.pointBorderColor[i] = SCATTER_THUMB_RING;
                });
            });
            try {
                ch.update('none');
            } catch (_e) {
                /* ignore */
            }
        });
    }

    async function runInitialScatterThumbPreload() {
        const preloadEl = document.getElementById('analytics-loading-preload');
        const skipBtn = document.getElementById('analytics-skip-preload');
        let skip = false;
        const onSkip = () => { skip = true; };

        const seen = new Set();
        const urls = [];
        const cap = INITIAL_SCATTER_THUMB_PRELOAD_MAX;
        const take = (ch) => {
            if (urls.length >= cap || !ch) return;
            const chunk = collectScatterImageUrlsForPreload(ch, cap - urls.length, seen);
            chunk.forEach((u) => urls.push(u));
        };
        take(charts.composite);
        allChartKeys().forEach((k) => {
            if (urls.length >= cap) return;
            if (k === 'composite') return;
            take(charts[k]);
        });

        const total = urls.length;

        try {
            if (!total) return;

            if (preloadEl) {
                preloadEl.hidden = false;
                preloadEl.textContent = `Preloading up to ${total} card previews (top dataset layers first)…`;
            }
            if (skipBtn) {
                skipBtn.hidden = false;
                skipBtn.addEventListener('click', onSkip, { once: true });
            }

            let next = 0;
            let finished = 0;
            const bump = () => {
                finished++;
                if (preloadEl && !skip) {
                    preloadEl.textContent = `Preloading card previews (${finished} / ${total})…`;
                }
            };

            const worker = async () => {
                while (!skip) {
                    const idx = next++;
                    if (idx >= total) return;
                    await primeScatterThumbImage(urls[idx]);
                    bump();
                }
            };

            const nWorkers = Math.min(INITIAL_SCATTER_THUMB_PRELOAD_CONCURRENCY, total);
            await Promise.all(Array.from({ length: nWorkers }, () => worker()));
        } finally {
            if (skipBtn) {
                skipBtn.hidden = true;
                skipBtn.removeEventListener('click', onSkip);
            }
            if (preloadEl) {
                preloadEl.hidden = true;
                preloadEl.textContent = '';
            }
        }
    }

    function isChartCanvasMostlyVisibleForThumbs(chart) {
        const el = chart && chart.canvas && chart.canvas.closest('.chart-canvas-container');
        if (!el) return false;
        const r = el.getBoundingClientRect();
        const vh = window.innerHeight || 800;
        const margin = 140;
        return r.bottom > -margin && r.top < vh + margin;
    }

    function refreshScatterChartThumbVisibility() {
        allChartKeys().forEach((chartId) => {
            const ch = charts[chartId];
            if (!ch) return;
            if (!isChartCanvasMostlyVisibleForThumbs(ch)) return;
            scheduleScatterThumbLayout(chartId);
        });
    }

    function ensureScatterThumbScrollListener() {
        if (scatterThumbScrollBound) return;
        scatterThumbScrollBound = true;
        const onScroll = () => {
            if (scatterThumbScrollRaf != null) return;
            scatterThumbScrollRaf = requestAnimationFrame(() => {
                scatterThumbScrollRaf = null;
                refreshScatterChartThumbVisibility();
            });
        };
        window.addEventListener('scroll', onScroll, { passive: true, capture: true });
        window.addEventListener('resize', onScroll, { passive: true });
    }

    /** Re-run Chart draw so `scatterImageOverlayPlugin` paints decoded images (`update('none')` reliably runs plugin hooks). */
    function scheduleScatterOverlayRepaint(chartId) {
        pendingThumbChartIds.add(chartId);
        if (pendingThumbChartRaf != null) return;
        pendingThumbChartRaf = requestAnimationFrame(() => {
            pendingThumbChartRaf = null;
            const ids = [...pendingThumbChartIds];
            pendingThumbChartIds.clear();
            console.log(`[THUMB] scheduleScatterOverlayRepaint: repainting charts [${ids.join(', ')}]`);
            ids.forEach((id) => {
                setTimeout(() => {
                    const ch = charts[id];
                    if (!ch) {
                        console.warn(`[THUMB] repaint: chart '${id}' not found in charts object`);
                        return;
                    }
                    try {
                        ch.update('none');
                    } catch (e) {
                        console.error(`[THUMB] repaint error for chart '${id}':`, e);
                    }
                }, 0);
            });
        });
    }

    /** Calendar year from set `release_date` first segment (YYYY/... or YYYY-...). */
    let dataYearMinGlobal = 1990;
    let dataYearMaxGlobal = 2030;
    /** Slider endpoints (release years); cards use sets whose release year passes the filter. */
    let yearFilterLo = 1999;
    let yearFilterHi = 2026;
    /** Initial range after data loads (clamped to actual release years in JSON). */
    const DEFAULT_ANALYTICS_YEAR_FILTER_LO = 1999;
    const DEFAULT_ANALYTICS_YEAR_FILTER_HI = 2026;
    /** If true, only sets with release year in [lo,hi] contribute; if false, those sets are omitted. */
    let yearFilterIncludeInRange = true;
    /** “Add sets in range” adds every matching set (no low cap). */
    const MAX_BULK_ADD_SETS = 99999;

    let yearFilterDebounceTimer = null;
    /** Bumps on each rebuild so in-flight async chart passes exit if a newer rebuild started. */
    let scatterRebuildGen = 0;
    /** Flat index of plotted scatter points for card/species search (rebuilt after each scatter pass). */
    let scatterCardSearchIndex = [];
    /** After datasets are merged, weighted LSRL stats are computed once per rebuild and reused (footers, sort, tooltips). */
    let scatterRegressionCache = null;
    let scatterRegressionRMap = null;
    let scatterRegressionCacheGen = -1;
    let analyticsBusySessionId = 0;
    /** Chart.js updates per animation frame during refresh (was 1 chart / macrotask). */
    const CHART_REFRESH_PER_FRAME = 3;


    function setAnalyticsBusy(visible, message) {
        if (!busyEl) return;
        if (message) {
            const m = busyEl.querySelector('.analytics-busy-msg');
            if (m) m.textContent = message;
        }
        busyEl.hidden = !visible;
        document.body.style.overflow = visible ? 'hidden' : '';
        const pg = typeof window.PTCG_PAGE_PROGRESS !== 'undefined' ? window.PTCG_PAGE_PROGRESS : null;
        if (pg) {
            if (visible) pg.begin();
            else pg.end();
        }
    }

    function rebuildChartsWithBusy(message, onDone) {
        const sessionId = ++analyticsBusySessionId;
        setAnalyticsBusy(true, message);
        rebuildAllScatterDatasets(
            () => {
                if (sessionId !== analyticsBusySessionId) {
                    if (typeof onDone === 'function') onDone();
                    return;
                }
                setAnalyticsBusy(false);
                if (typeof onDone === 'function') onDone();
            },
            () => {
                if (sessionId !== analyticsBusySessionId) return;
                setAnalyticsBusy(false);
            }
        );
    }

    function fillYearDualRangeTrack() {
        const minEl = document.getElementById('yearFilterMin');
        const maxEl = document.getElementById('yearFilterMax');
        const fillEl = document.getElementById('yearDualRangeFill');
        if (!minEl || !maxEl) return;
        maxEl.style.removeProperty('background');
        const minR = parseInt(minEl.min, 10);
        const maxR = parseInt(minEl.max, 10);
        const span = maxR - minR;
        if (!Number.isFinite(span) || span <= 0) return;
        const lo = parseInt(minEl.value, 10);
        const hi = parseInt(maxEl.value, 10);
        const loC = Math.min(lo, hi);
        const hiC = Math.max(lo, hi);
        const p1 = ((loC - minR) / span) * 100;
        const p2 = ((hiC - minR) / span) * 100;
        if (fillEl) {
            fillEl.style.left = `${p1}%`;
            fillEl.style.width = `${Math.max(0, p2 - p1)}%`;
        }
    }

    function wireDualYearSliderZOrder(minEl, maxEl) {
        const bump = (which) => {
            if (which === 'min') {
                minEl.style.zIndex = '3';
                maxEl.style.zIndex = '2';
            } else {
                minEl.style.zIndex = '2';
                maxEl.style.zIndex = '3';
            }
        };
        minEl.addEventListener('pointerdown', () => bump('min'));
        maxEl.addEventListener('pointerdown', () => bump('max'));
    }

    /** Strip form words (Mega Gengar → Gengar) so lookups hit base species in character / trends files. */
    const CHARACTER_FORM_PREFIX_RES = [
        /^mega\s+/i,
        /^primal\s+/i,
        /^galarian\s+/i,
        /^alolan\s+/i,
        /^hisuian\s+/i,
        /^paldean\s+/i,
        /^radiant\s+/i,
        /^rapid\s+strike\s+/i,
        /^single\s+strike\s+/i,
        /* Neo / e-card / EX-era printed prefixes — Trends + character files use base species (e.g. Shining Charizard -> Charizard). */
        /^shining\s+/i,
        /^crystal\s+/i,
        /^dark\s+/i,
        /^light\s+/i,
        /^baby\s+/i
    ];

    function canonicalizeCharacterChunk(chunk) {
        let s = String(chunk || '').replace(/\s+/g, ' ').trim();
        if (s.length <= 2) return s;
        let prev = '';
        for (let guard = 0; guard < 8 && s !== prev; guard++) {
            prev = s;
            CHARACTER_FORM_PREFIX_RES.forEach((re) => {
                s = s.replace(re, '').replace(/\s+/g, ' ').trim();
            });
        }
        return s;
    }

    function cleanCharacterName(name) {
        if (!name) return [];
        const slogans = [/_____'s/gi];
        let cleaned = name;
        slogans.forEach(s => cleaned = cleaned.replace(s, ''));
        cleaned = String(cleaned).replace(/\u2019/g, "'");

        // Heuristic: Is this a Pokémon or a Trainer/Stadium?
        // If it contains "Professor", "Secret Base", "Hospitality", etc., we avoid stripping "Team X's".
        const lower = cleaned.toLowerCase();
        const isTrainerOrStadium = lower.includes('professor') || lower.includes('secret base') || 
                                   lower.includes('research') || lower.includes('hospitality') || 
                                   lower.includes('stadium') || lower.includes('supporter');

        if (!isTrainerOrStadium) {
            /* "Team Magma's Groudon-EX" -> Groudon-EX (trends/character files key on species, not team label). */
            cleaned = cleaned.replace(
                /\bteam\s+(?:magma|aqua|rocket|galactic|plasma|flare|skull|yell|star)\s*'s\s+/gi,
                ''
            );
        }

        const suffixes = [/\bVMAX\b/gi, /\bVSTAR\b/gi, /\bV\b/gi, /\bEX\b/gi, /\bex\b/gi, /\bGX\b/gi,
            /\bBREAK\b/gi, /\bPrime\b/gi, /\bLV\.X\b/gi, /\bLEGEND\b/gi, /\bSP\b/gi, /\bStar\b/gi];
        suffixes.forEach(suf => {
            cleaned = cleaned.replace(suf, '');
        });
        return cleaned.split('&').map((chunk) => {
            const stripped = chunk.replace(/[^a-zA-Z\s]/g, '').trim().replace(/\s+/g, ' ');
            // If it's a trainer/stadium, canonicalize less aggressively
            return isTrainerOrStadium ? stripped : canonicalizeCharacterChunk(stripped);
        }).filter(chunk => chunk.length > 2);
    }

    function isMegaHyperRare(rarity) {
        if (!rarity || typeof rarity !== 'string') return false;
        const r = rarity.toLowerCase();
        return r.includes('mega') && r.includes('hyper');
    }

    /** Gold / rainbow gold style buckets share a slot; shrink like mega (separate from mega hyper) */
    function isGoldSecretBucket(rarity) {
        if (!rarity || typeof rarity !== 'string') return false;
        if (isMegaHyperRare(rarity)) return false;
        const r = rarity.toLowerCase();
        if (r.includes('rainbow')) return true;
        if (r.includes('gold')) return r.includes('secret') || r.includes('star') || r.includes('rare');
        return false;
    }

    /** Coarse bucket for calibration (mirrors static rarity weight tiers). */
    function getRarityTierKey(rarity) {
        if (!rarity || typeof rarity !== 'string') return 'unknown';
        if (isMegaHyperRare(rarity)) return 'mega_hyper';
        if (isGoldSecretBucket(rarity)) return 'gold_secret';
        const r = rarity.toLowerCase();
        if (r.includes('special illustration') || r.includes('sir')) return 'sir';
        if (r.includes('illustration rare') || r.includes('ir')) return 'ir';
        if (r.includes('hyper rare') || r.includes('secret rare')) return 'hyper_secret';
        if (r.includes('rainbow rare')) return 'rainbow';
        if (r.includes('ultra rare') || r.includes('full art')) return 'ultra_fa';
        if (r.includes('shiny ultra rare') || r.includes('shiny rare')) return 'shiny';
        if (r.includes('amazing rare')) return 'amazing';
        if (r.includes('holo star') || r.includes('rare holo star')) return 'gold_star';
        if (/\bshining\b/.test(r)) return 'neo_shining';
        if (r.includes('legend')) return 'legend';
        if (r.includes('rare secret')) return 'ecard_secret';
        if (r.includes('crystal')) return 'crystal';
        if (r.includes('lv.x') || r.includes('lv x')) return 'lvx';
        if (r.includes('rare holo ex') || r.includes('holo ex')) return 'ex_holo';
        if (r.includes('rare prime') || r.includes('rare break')) return 'prime_break';
        if (r.includes('radiant')) return 'radiant';
        if (r.includes('rare holo')) return 'vintage_holo';
        return 'standard';
    }

    /** Double Rare and above: analytics may median-blend Dex / TCGTracking / optional tcgapi. */
    function isRarityDoubleRareOrHigher(rarity) {
        if (!rarity || typeof rarity !== 'string') return false;
        if (isMegaHyperRare(rarity) || isGoldSecretBucket(rarity)) return true;
        const r = rarity.toLowerCase();
        const keys = [
            'double rare', 'ultra rare', 'illustration rare', 'special illustration',
            'hyper rare', 'secret rare', 'rainbow rare', 'amazing rare',
            'shiny ultra rare', 'shiny rare', 'radiant rare', 'mega attack rare',
            'ace spec',
            'holo star', 'rare holo star', 'rare shining', 'rare secret', 'legend'
        ];
        return keys.some((k) => r.includes(k));
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

    function parseOneInXPacks(str) {
        if (!str || typeof str !== 'string') return null;
        const m = str.match(/1\s?(?:in|\/)\s*([\d,.]+)/i);
        if (!m || !m[1]) return null;
        const v = parseFloat(m[1].replace(/,/g, ''));
        return Number.isFinite(v) && v > 0 ? v : null;
    }

    /** Parse PriceDex-style currency ("$164.97") or empty/em dash to a positive USD float, else null. */
    function parseMoneyLikeField(val) {
        if (val == null) return null;
        if (typeof val === 'number' && Number.isFinite(val) && val > 0) return val;
        const s = String(val).replace(/\u2014/g, '').trim();
        if (!s || s === '-' || /^n\/?a$/i.test(s)) return null;
        const cleaned = s.replace(/[$,\s]/g, '');
        const n = parseFloat(cleaned);
        return Number.isFinite(n) && n > 0 ? n : null;
    }

    /**
     * Valid $/pack candidates for pull math + tooltips: sealed (TCGTracking), Dex box EV ÷ packs, TCGPlayer pack.
     * @returns {{ median: number, parts: Array<{ label: string, value: number }> }}
     */
    function getPackPriceMedianDetail(setInfo) {
        const parts = [];
        const sealedMkt = Number(setInfo.tcgtracking_implied_pack_usd_sealed_mkt);
        if (Number.isFinite(sealedMkt) && sealedMkt > 0) {
            parts.push({ label: 'TCG sealed box ÷ packs', value: sealedMkt });
        }
        const packsPer = Number(setInfo.packs_per_box);
        const boxUsd = parseMoneyLikeField(setInfo.booster_box_ev);
        if (Number.isFinite(packsPer) && packsPer > 0 && boxUsd != null) {
            parts.push({ label: 'Dex box EV ÷ packs', value: boxUsd / packsPer });
        }
        const tcg = Number(setInfo.tcgplayer_pack_price);
        if (Number.isFinite(tcg) && tcg > 0) {
            parts.push({ label: 'TCGPlayer pack', value: tcg });
        }
        const vals = parts.map((p) => p.value);
        const median = !vals.length ? 5.0 : vals.length === 1 ? vals[0] : medianArray(vals);
        return { median, parts };
    }

    function resolvePackPriceUsdForPull(setInfo) {
        return getPackPriceMedianDetail(setInfo).median;
    }

    /** Prefer Collectrics eBay counts (uncapped); else TCGTracking NM EN (capped at 25 in API). */
    function cardEbayLiquidityCountForWeights(card) {
        if (!card) return null;
        const li = Number(card.collectrics_ebay_listings);
        if (Number.isFinite(li) && li > 0) return li;
        const sv = Number(card.collectrics_ebay_sold_volume);
        if (Number.isFinite(sv) && sv > 0) return sv;
        const tcg = Number(card.tcgtracking_listings_nm_en);
        if (Number.isFinite(tcg) && tcg > 0) return tcg;
        return null;
    }

    function usesCollectricsEbayLiquidity(card) {
        if (!card) return false;
        const li = Number(card.collectrics_ebay_listings);
        const sv = Number(card.collectrics_ebay_sold_volume);
        return (Number.isFinite(li) && li > 0) || (Number.isFinite(sv) && sv > 0);
    }

    /** √(min(cnt, cap)/cap) for chase rarities in composite weighting; 1 if unknown. */
    function listingLiquidityFactor(card) {
        if (!card || !isRarityDoubleRareOrHigher(card.rarity)) return 1.0;
        const raw = cardEbayLiquidityCountForWeights(card);
        if (raw == null || !Number.isFinite(raw)) return 1.0;
        const cap = usesCollectricsEbayLiquidity(card) ? 1200 : 25;
        const c = Math.max(0, Math.min(Number(raw), cap));
        return Math.sqrt(c / cap);
    }

    function getRarityCohortCount(setInfo, rarity) {
        if (!rarity || !setInfo || !setInfo.rarity_counts) return 1;
        const rc = setInfo.rarity_counts;
        if (typeof rc[rarity] === 'number' && rc[rarity] > 0) return rc[rarity];
        const keys = Object.keys(rc);
        const hit = keys.find(k => k.toLowerCase() === rarity.toLowerCase());
        if (hit && typeof rc[hit] === 'number' && rc[hit] > 0) return rc[hit];
        return 1;
    }

    /**
     * Card odds when present are blended with slot odds divided by set rarity cohort size K.
     * Mega / gold-style rarities get extra shrink on each branch before blending.
     */
    function computeEffectivePullCost(card, setInfo, packPrice) {
        let fromCard = null;
        const cardOdds = card.card_pull_rate ? parseOneInXPacks(card.card_pull_rate) : null;
        if (cardOdds != null) {
            fromCard = Math.min(cardOdds * packPrice, packPrice * 36 * 3);
        }

        let fromSlot = null;
        if (setInfo.rarity_pull_rates && card.rarity && setInfo.rarity_pull_rates[card.rarity]) {
            const slotOdds = parseOneInXPacks(setInfo.rarity_pull_rates[card.rarity]);
            if (slotOdds != null) {
                const k = getRarityCohortCount(setInfo, card.rarity);
                fromSlot = (slotOdds * packPrice) / Math.max(1, k);
            }
        }

        const applyTierShrink = (v) => {
            if (v == null) return null;
            let x = v;
            if (isMegaHyperRare(card.rarity)) x *= MEGA_HYPER_PULL_COST_FACTOR;
            if (isGoldSecretBucket(card.rarity)) x *= GOLD_SECRET_PULL_COST_FACTOR;
            
            // Rational Rip Ceiling: For high-end cards, the cost to pull is often deeply decoupled from market liquidity.
            // Dampen values over $12,000 to prevent deeply negative ln(p/u) that drags predictions.
            if (x > 12000) {
                x = 12000 + Math.log(x / 12000) * 12000;
            }
            return x;
        };

        fromCard = applyTierShrink(fromCard);
        fromSlot = applyTierShrink(fromSlot);

        if (!usesBoosterStyleCardPullOdds(setInfo, card)) {
            fromCard = null;
        }

        let value = 0;
        if (fromCard != null && fromSlot != null) {
            value = PULL_BLEND_CARD * fromCard + PULL_BLEND_SLOT * fromSlot;
        } else if (fromCard != null) {
            value = fromCard;
        } else if (fromSlot != null) {
            value = fromSlot;
        }

        return { value, fromCard, fromSlot };
    }

    function speciesPopularityNormKey(chunk) {
        return String(chunk || '').replace(/[^a-z0-9]/gi, '').toLowerCase();
    }

    function ingestSpeciesPopularityList(doc) {
        speciesPopularityByKey.clear();
        if (!doc || !Array.isArray(doc.species)) return;
        doc.species.forEach((r) => {
            if (!r || typeof r !== 'object') return;
            const sk = String(r.species_key || '').toLowerCase().trim();
            if (sk) speciesPopularityByKey.set(sk, r);
            const nk = speciesPopularityNormKey(r.display_name);
            if (nk && nk !== sk) speciesPopularityByKey.set(nk, r);
        });
    }

    function lookupSpeciesPopularityRow(canonicalId) {
        if (!speciesPopularityByKey.size) return null;
        return speciesPopularityByKey.get(speciesPopularityNormKey(canonicalId)) || null;
    }

    /** Shape like `google_trends_momentum.json` rows so popularity / tooltip fields stay one code path. */
    function syntheticTrendsRowFromSpeciesList(spRow, characterLabel) {
        const pop = Number(spRow.popularity_index);
        const tr = Number(spRow.trend_index_average);
        return {
            Character: String(characterLabel || spRow.display_name || '').trim(),
            Popularity_Index: Number.isFinite(pop) && pop > 0 ? pop : null,
            Trend_Index_Average: Number.isFinite(tr) && tr > 0 ? tr : null,
            Survey_AdjustedRank_Mean: spRow.panel_survey_adj_mean,
            Survey_Poll_Count: spRow.panel_poll_count,
            Sparklez_MaxVotes: spRow.sparklez_vote_total,
            Sparklez_SyntheticAdj: spRow.sparklez_synthetic_adj,
            __fromSpeciesPopularityList: true
        };
    }

    /** Primary chart / composite hype X: `Popularity_Index` when present, else legacy `Trend_Index_Average`. */
    function popularityIndexFromTrendsRow(t) {
        if (!t || typeof t !== 'object') return 0;
        const p = Number(t.Popularity_Index);
        if (Number.isFinite(p) && p > 0) return p;
        const tr = Number(t.Trend_Index_Average);
        return Number.isFinite(tr) && tr > 0 ? tr : 0;
    }

    function getTrendsScoreDetail(cardName) {
        const identities = cleanCharacterName(cardName);
        if (identities.length === 0) {
            return {
                avg: 0,
                filePopularityAvg: 0,
                matchCount: 0,
                trendOnlyAvg: 0,
                surveyMeanAvg: null,
                surveyPollMax: 0,
                sparklezVotesAvg: null,
                sparklezSyntheticAvg: null,
                gradedSpeciesNorm: 0,
                gradedPopBlendScale: 1,
                hasGlobalGraded: false
            };
        }
        let totalPop = 0;
        let totalTrend = 0;
        let matchCount = 0;
        let surveySum = 0;
        let surveyParts = 0;
        let surveyPollMax = 0;
        let sparklezVotesSum = 0;
        let sparklezVotesParts = 0;
        let sparklezAdjSum = 0;
        let sparklezAdjParts = 0;
        identities.forEach((id) => {
            let found = trendsData.find((t) => t.Character === id);
            if (!found) {
                const sp = lookupSpeciesPopularityRow(id);
                if (sp != null && Number.isFinite(Number(sp.popularity_index)) && Number(sp.popularity_index) > 0) {
                    found = syntheticTrendsRowFromSpeciesList(sp, id);
                }
            }
            if (found) {
                totalPop += popularityIndexFromTrendsRow(found);
                const trOnly = Number(found.Trend_Index_Average);
                totalTrend += Number.isFinite(trOnly) && trOnly > 0 ? trOnly : 0;
                const sm = found.Survey_AdjustedRank_Mean;
                const pc = Number(found.Survey_Poll_Count);
                if (sm != null && Number.isFinite(Number(sm))) {
                    surveySum += Number(sm);
                    surveyParts += 1;
                }
                if (Number.isFinite(pc) && pc > surveyPollMax) surveyPollMax = pc;
                const szv = Number(found.Sparklez_MaxVotes);
                if (Number.isFinite(szv) && szv > 0) {
                    sparklezVotesSum += szv;
                    sparklezVotesParts += 1;
                }
                const szAdj = found.Sparklez_SyntheticAdj;
                if (szAdj != null && Number.isFinite(Number(szAdj))) {
                    sparklezAdjSum += Number(szAdj);
                    sparklezAdjParts += 1;
                }
                matchCount++;
            }
        });
        const gb = speciesPopularityGradedNormForCardName(cardName);
        if (matchCount === 0) {
            return {
                avg: 0,
                filePopularityAvg: 0,
                matchCount: 0,
                trendOnlyAvg: 0,
                surveyMeanAvg: null,
                surveyPollMax: 0,
                sparklezVotesAvg: null,
                sparklezSyntheticAvg: null,
                gradedSpeciesNorm: gb.normMax,
                gradedPopBlendScale: gb.scale,
                hasGlobalGraded: gb.hasGlobalGraded
            };
        }
        const fileAvg = totalPop / matchCount;
        return {
            avg: fileAvg * gb.scale,
            filePopularityAvg: fileAvg,
            matchCount,
            trendOnlyAvg: totalTrend / matchCount,
            surveyMeanAvg: surveyParts > 0 ? surveySum / surveyParts : null,
            surveyPollMax,
            sparklezVotesAvg: sparklezVotesParts > 0 ? sparklezVotesSum / sparklezVotesParts : null,
            sparklezSyntheticAvg: sparklezAdjParts > 0 ? sparklezAdjSum / sparklezAdjParts : null,
            gradedSpeciesNorm: gb.normMax,
            gradedPopBlendScale: gb.scale,
            hasGlobalGraded: gb.hasGlobalGraded
        };
    }

    function getTrendsScore(cardName) {
        return getTrendsScoreDetail(cardName).avg;
    }

    /** Deterministic 0.85–1.15 spread so imputed trend indices do not stack on one X. */
    function tinyTrendSpreadJitter(name) {
        const s = String(name || '');
        let h = 2166136261 >>> 0;
        for (let i = 0; i < s.length; i++) {
            h ^= s.charCodeAt(i);
            h = Math.imul(h, 16777619);
        }
        const u = (h >>> 0) / 0xffffffff;
        return 0.85 + 0.3 * u;
    }

    function buildArtistChaseLookup(rows) {
        artistChaseByNorm.clear();
        if (!Array.isArray(rows)) return;
        rows.forEach((row) => {
            const name = row && row.Artist != null ? String(row.Artist).trim() : '';
            if (!name) return;
            const med = Number(row.Median_Market_Price);
            if (!Number.isFinite(med) || med <= 0) return;
            const cnt = Number(row.Total_Chase_Cards);
            const key = name.toLowerCase();
            if (!artistChaseByNorm.has(key)) {
                artistChaseByNorm.set(key, {
                    displayArtist: name,
                    median: med,
                    count: Number.isFinite(cnt) ? cnt : 0
                });
            }
        });
    }

    function lookupArtistChase(artistRaw) {
        if (!artistRaw || typeof artistRaw !== 'string') return null;
        const t = artistRaw.trim();
        if (!t || /^unknown/i.test(t)) return null;
        return artistChaseByNorm.get(t.toLowerCase()) || null;
    }

    function getCharacterPremiumInfo(cardName) {
        const identities = cleanCharacterName(cardName);
        if (identities.length === 0) return { volume: 0, isHuman: false, speciesKeys: [], archetype: 'N/A' };
        let totalVol = 0;
        let humanCount = 0;
        let matchCount = 0;
        let archetype = 'Generic';
        identities.forEach(id => {
            const found = characterData.find(c => c.Character === id);
            if (found) {
                totalVol += found.High_Tier_Print_Volume;
                if (found.Is_Human === true) {
                    humanCount++;
                    if (found.Trainer_Archetype && found.Trainer_Archetype !== 'N/A') {
                        archetype = found.Trainer_Archetype;
                    }
                }
                matchCount++;
            }
        });
        return {
            volume: matchCount > 0 ? totalVol / matchCount : 0,
            isHuman: matchCount > 0 ? (humanCount / matchCount > 0.5) : false,
            speciesKeys: identities.slice(),
            archetype: archetype
        };
    }

    function parseSetReleaseYear(releaseDateStr) {
        if (!releaseDateStr || typeof releaseDateStr !== 'string') return null;
        const y = parseInt(String(releaseDateStr).trim().split(/[/-]/)[0], 10);
        return Number.isFinite(y) ? y : null;
    }

    function ingestTcgMacroInterestDoc(doc) {
        tcgMacroByYear = new Map();
        latestTcgMacroSeriesLabel = '';
        if (!doc || typeof doc !== 'object') return;
        const lab = doc.series_label || doc.query || doc.label;
        if (typeof lab === 'string' && lab.trim()) latestTcgMacroSeriesLabel = lab.trim();
        const raw = doc.by_year || doc.years;
        if (!raw || typeof raw !== 'object') return;
        Object.keys(raw).forEach((k) => {
            const v = raw[k];
            if (v == null) return;
            const n = Number(v);
            if (!Number.isFinite(n)) return;
            const y = parseInt(String(k).trim(), 10);
            if (!Number.isFinite(y)) return;
            tcgMacroByYear.set(String(y), n);
        });
    }

    function lookupTcgMacroInterestForReleaseYear(year) {
        if (year == null || !Number.isFinite(year)) return null;
        const k = String(Math.round(year));
        if (!tcgMacroByYear.has(k)) return null;
        const v = tcgMacroByYear.get(k);
        if (v == null || !Number.isFinite(v)) return null;
        
        /** 
         * Vintage Scarcity Factor: Macro interest for older sets is inherently understated 
         * because it doesn't account for the massive attrition of raw material. 
         * We boost the macro factor for sets > 10 years old.
         */
        const age = Math.max(0, 2026 - year);
        const prestige = age > 10 ? 1 + Math.log1p(age - 10) * 0.65 : 1.0;
        return v * prestige;
    }

    function computeGlobalReleaseYearRange() {
        let minY = 9999;
        let maxY = -9999;
        allSetsData.forEach((s) => {
            const y = parseSetReleaseYear(s && s.release_date);
            if (y == null) return;
            minY = Math.min(minY, y);
            maxY = Math.max(maxY, y);
        });
        if (maxY < minY) return { minY: 2000, maxY: new Date().getUTCFullYear() };
        return { minY, maxY };
    }

    function setIndexPassesYearFilter(setIdx) {
        const s = allSetsData[setIdx];
        if (!s) return false;
        const y = parseSetReleaseYear(s.release_date);
        const lo = Math.min(yearFilterLo, yearFilterHi);
        const hi = Math.max(yearFilterLo, yearFilterHi);
        if (y == null) return yearFilterIncludeInRange;
        const inR = y >= lo && y <= hi;
        return yearFilterIncludeInRange ? inR : !inR;
    }

    /** Sets selected in the UI that also pass the release-year filter (sorted by index for stable model fits). */
    function getEffectiveSetIndicesOrdered() {
        return [...addedSetIndices].filter(setIndexPassesYearFilter).sort((a, b) => a - b);
    }

    /**
     * After `sets.reverse()`, low indices are the newest English product—often without PokéMetrics rows yet.
     * Spread the default selection across the array so graded-pop and cross-era charts (e.g. character) are populated.
     */
    function pickInitialAnalyticsSetIndices(nSets, cap) {
        const c = Math.min(Math.max(0, cap), nSets);
        if (c === 0) return [];
        const out = new Set();
        const head = Math.ceil(c / 2);
        const tail = c - head;
        for (let i = 0; i < head; i++) out.add(i);
        for (let t = 0; t < tail; t++) {
            const idx = nSets - 1 - t;
            if (idx >= 0) out.add(idx);
        }
        let fill = 0;
        while (out.size < c && fill < nSets) {
            out.add(fill);
            fill++;
        }
        return [...out].sort((a, b) => a - b);
    }

    /**
     * Default comparison: prefer sets whose release year lies in [lo, hi] (e.g. 1999–2011), spread across that list.
     * Falls back to empty array if no sets match (caller uses pickInitialAnalyticsSetIndices).
     */
    function pickInitialSetIndicesForYearRange(loRaw, hiRaw, cap) {
        if (!Array.isArray(allSetsData) || allSetsData.length === 0) return [];
        const lo = Math.min(loRaw, hiRaw);
        const hi = Math.max(loRaw, hiRaw);
        const inRange = [];
        for (let i = 0; i < allSetsData.length; i++) {
            const y = parseSetReleaseYear(allSetsData[i] && allSetsData[i].release_date);
            if (y == null || y < lo || y > hi) continue;
            inRange.push(i);
        }
        if (inRange.length === 0) return [];
        const c = Math.min(Math.max(1, cap), inRange.length);
        if (inRange.length <= c) return inRange.slice();
        const chosen = new Set();
        for (let k = 0; k < c; k++) {
            const j = c === 1 ? 0 : Math.round((k / (c - 1)) * (inRange.length - 1));
            chosen.add(inRange[j]);
        }
        return [...chosen].sort((a, b) => a - b);
    }

    /** Every set index whose release calendar year lies in [lo, hi] (inclusive). */
    function pickAllSetIndicesInYearRange(loRaw, hiRaw) {
        if (!Array.isArray(allSetsData) || allSetsData.length === 0) return [];
        const lo = Math.min(loRaw, hiRaw);
        const hi = Math.max(loRaw, hiRaw);
        const out = [];
        for (let i = 0; i < allSetsData.length; i++) {
            const y = parseSetReleaseYear(allSetsData[i] && allSetsData[i].release_date);
            if (y == null || y < lo || y > hi) continue;
            out.push(i);
        }
        return out;
    }

    function updateAnalyticsDashboardMeta() {
        if (!metaEl) return;
        const priceRuleHtml = analyticsIncludeSubFiveDollar
            ? '<span>Top 25 cards per set · finite market price &gt; $0 (sub-$5 included; missing/$0 and some promos excluded)</span>'
            : `<span>Top 25 cards per set · market price ≥ $${MIN_CHART_MARKET_PRICE} (some promos excluded)</span>`;
        metaEl.innerHTML = `${priceRuleHtml} &bull; <span>Double-click a chart to refit axes</span>`;
        
        // Set count line inside Data drawer
        const setCountEl = document.getElementById('analytics-set-count-line');
        if (setCountEl) {
            setCountEl.textContent = `${getEffectiveSetIndicesOrdered().length} set(s) in charts (${addedSetIndices.size} selected; release-year filter in toolbar)`;
        }
    }

    function updateAnalyticsFilterSummary() {
        const el = document.getElementById('analytics-filter-summary');
        if (!el) return;
        const sel = addedSetIndices.size;
        const eff = getEffectiveSetIndicesOrdered().length;
        const lo = Math.min(yearFilterLo, yearFilterHi);
        const hi = Math.max(yearFilterLo, yearFilterHi);
        const modeLabel = yearFilterIncludeInRange ? 'Include' : 'Exclude';
        if (sel === 0) {
            el.textContent = 'No sets selected. Add sets from the list or use “Add sets in range”.';
            return;
        }
        el.textContent = `${eff} of ${sel} selected set(s) pass the release-year filter (${modeLabel} calendar years ${lo}–${hi}). Hover tooltips still show raw market price ($).`;
    }

    function syncYearSliderLabelElements() {
        const a = document.getElementById('yearFilterMinLabel');
        const b = document.getElementById('yearFilterMaxLabel');
        const lo = Math.min(yearFilterLo, yearFilterHi);
        const hi = Math.max(yearFilterLo, yearFilterHi);
        if (a) a.textContent = String(lo);
        if (b) b.textContent = String(hi);
        const minEl = document.getElementById('yearFilterMin');
        const maxEl = document.getElementById('yearFilterMax');
        const span = `${lo}–${hi}`;
        if (minEl) minEl.setAttribute('aria-valuetext', span);
        if (maxEl) maxEl.setAttribute('aria-valuetext', span);
    }

    let analyticsDataControlsWired = false;

    function resizeAllChartsAfterRebuild() {
        requestAnimationFrame(() => {
            allChartKeys().forEach((k) => {
                if (charts[k]) charts[k].resize();
            });
            requestAnimationFrame(() => {
                refreshScatterChartThumbVisibility();
            });
        });
    }

    function scheduleDebouncedYearFilterRebuild() {
        if (yearFilterDebounceTimer) clearTimeout(yearFilterDebounceTimer);
        yearFilterDebounceTimer = setTimeout(() => {
            yearFilterDebounceTimer = null;
            rebuildAllScatterDatasets(resizeAllChartsAfterRebuild);
        }, 140);
    }

    function initAnalyticsYearSlidersFromData() {
        const yr = computeGlobalReleaseYearRange();
        dataYearMinGlobal = yr.minY;
        dataYearMaxGlobal = yr.maxY;
        const clampY = (v) => Math.min(dataYearMaxGlobal, Math.max(dataYearMinGlobal, v));
        yearFilterLo = clampY(DEFAULT_ANALYTICS_YEAR_FILTER_LO);
        yearFilterHi = clampY(DEFAULT_ANALYTICS_YEAR_FILTER_HI);
        if (yearFilterLo > yearFilterHi) {
            const t = yearFilterLo;
            yearFilterLo = yearFilterHi;
            yearFilterHi = t;
        }
        yearFilterIncludeInRange = true;

        const minEl = document.getElementById('yearFilterMin');
        const maxEl = document.getElementById('yearFilterMax');
        const modeEl = document.getElementById('yearFilterMode');
        if (minEl && maxEl) {
            minEl.min = maxEl.min = String(dataYearMinGlobal);
            minEl.max = maxEl.max = String(dataYearMaxGlobal);
            minEl.step = '1';
            maxEl.step = '1';
            minEl.value = String(yearFilterLo);
            maxEl.value = String(yearFilterHi);
        }
        if (modeEl) modeEl.value = 'include';
        syncYearSliderLabelElements();
        fillYearDualRangeTrack();
    }

    function syncAnalyticsAddSetFieldVisibility() {
        const wrap = document.getElementById('analyticsAddSetFieldWrap');
        const hint = document.getElementById('analyticsAddSetEmptyHint');
        if (!wrap) return;
        if (!Array.isArray(allSetsData) || allSetsData.length === 0) {
            wrap.hidden = false;
            if (hint) hint.hidden = true;
            syncAddSetSelectDisplayMode();
            return;
        }
        const allIn = addedSetIndices.size >= allSetsData.length;
        wrap.hidden = allIn;
        if (hint) {
            hint.hidden = !allIn;
        }
        syncAddSetSelectDisplayMode();
    }

    /**
     * Add-set control always uses an inline listbox (size &gt; 1). On Windows, a normal dark-themed
     * &lt;select size="1"&gt; often opens a tall, apparently empty native menu; listbox mode draws options
     * in-page where our CSS applies. Row count scales up to a cap; extra sets scroll inside the control.
     */
    const ADD_SET_LISTBOX_MAX_ROWS = 10;

    function syncAddSetSelectDisplayMode() {
        if (!addSetSelect) return;
        addSetSelect.classList.add('analytics-add-set-select--listbox');
        const wrap = document.getElementById('analyticsAddSetFieldWrap');
        if (wrap && wrap.hidden) return;
        const enabled = Array.from(addSetSelect.options).filter((o) => o.value !== '' && !o.disabled).length;
        const placeholderRows = 1;
        if (enabled <= 0) {
            addSetSelect.size = Math.min(ADD_SET_LISTBOX_MAX_ROWS, Math.max(2, placeholderRows + 1));
            return;
        }
        const want = Math.min(ADD_SET_LISTBOX_MAX_ROWS, enabled + placeholderRows);
        addSetSelect.size = Math.max(2, want);
    }

    function renderSetChips() {
        const wrap = document.getElementById('analytics-set-chips');
        if (!wrap) return;
        wrap.innerHTML = '';
        const indices = [...addedSetIndices].sort((a, b) => a - b);
        indices.forEach((idx) => {
            const set = allSetsData[idx];
            const name = set && set.set_name ? set.set_name : `Set ${idx}`;
            const chip = document.createElement('span');
            chip.className = 'analytics-set-chip';
            chip.dataset.setIndex = String(idx);
            const label = document.createElement('span');
            label.className = 'analytics-set-chip-label';
            label.textContent = name;
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'analytics-set-chip-remove';
            btn.setAttribute('aria-label', `Remove ${name} from comparison`);
            btn.textContent = '×';
            btn.addEventListener('click', () => {
                removeSetFromComparison(idx);
            });
            chip.appendChild(label);
            chip.appendChild(btn);
            wrap.appendChild(chip);
        });
        syncAnalyticsAddSetFieldVisibility();
    }

    function removeSetFromComparison(idx) {
        if (!addedSetIndices.has(idx)) return;
        addedSetIndices.delete(idx);
        const option = Array.from(addSetSelect.options).find((o) => parseInt(o.value, 10) === idx);
        if (option) option.disabled = false;
        addSetSelect.value = '';
        rebuildChartsWithBusy('Updating charts…', resizeAllChartsAfterRebuild);
    }

    function wireAnalyticsDataControls() {
        const subFiveEl = document.getElementById('analyticsIncludeSubFiveDollar');
        if (subFiveEl && !subFiveEl.dataset.wired) {
            subFiveEl.dataset.wired = '1';
            analyticsIncludeSubFiveDollar = !!subFiveEl.checked;
            subFiveEl.addEventListener('change', () => {
                analyticsIncludeSubFiveDollar = !!subFiveEl.checked;
                rebuildChartsWithBusy('Updating chart sample…', () => {
                    resizeAllChartsAfterRebuild();
                    updateAnalyticsDashboardMeta();
                });
            });
        }

        if (analyticsDataControlsWired) return;
        const minEl = document.getElementById('yearFilterMin');
        const maxEl = document.getElementById('yearFilterMax');
        const modeEl = document.getElementById('yearFilterMode');
        const addRangeBtn = document.getElementById('addSetsInYearRangeBtn');
        const clearBtn = document.getElementById('clearComparisonSetsBtn');
        if (!minEl || !maxEl || !modeEl) return;
        analyticsDataControlsWired = true;

        const clampYear = (v) => {
            let n = parseInt(String(v), 10);
            if (!Number.isFinite(n)) n = dataYearMinGlobal;
            return Math.min(dataYearMaxGlobal, Math.max(dataYearMinGlobal, n));
        };

        const onYearSlidersInput = () => {
            let lo = clampYear(minEl.value);
            let hi = clampYear(maxEl.value);
            if (lo > hi) {
                const active = document.activeElement;
                if (active === minEl) lo = hi;
                else if (active === maxEl) hi = lo;
                else {
                    lo = Math.min(lo, hi);
                    hi = Math.max(lo, hi);
                }
            }
            yearFilterLo = lo;
            yearFilterHi = hi;
            minEl.value = String(lo);
            maxEl.value = String(hi);
            syncYearSliderLabelElements();
            fillYearDualRangeTrack();
            scheduleDebouncedYearFilterRebuild();
        };

        minEl.addEventListener('input', onYearSlidersInput);
        maxEl.addEventListener('input', onYearSlidersInput);
        wireDualYearSliderZOrder(minEl, maxEl);
        fillYearDualRangeTrack();

        modeEl.addEventListener('change', () => {
            yearFilterIncludeInRange = modeEl.value === 'include';
            rebuildAllScatterDatasets(resizeAllChartsAfterRebuild);
        });

        if (addRangeBtn) {
            addRangeBtn.addEventListener('click', () => {
                yearFilterLo = clampYear(minEl.value);
                yearFilterHi = clampYear(maxEl.value);
                const lo = Math.min(yearFilterLo, yearFilterHi);
                const hi = Math.max(yearFilterLo, yearFilterHi);
                let nNew = 0;
                for (let i = 0; i < allSetsData.length && nNew < MAX_BULK_ADD_SETS; i++) {
                    if (addedSetIndices.has(i)) continue;
                    const y = parseSetReleaseYear(allSetsData[i] && allSetsData[i].release_date);
                    if (y == null || y < lo || y > hi) continue;
                    addedSetIndices.add(i);
                    const option = Array.from(addSetSelect.options).find((o) => parseInt(o.value, 10) === i);
                    if (option) option.disabled = true;
                    nNew++;
                }
                addSetSelect.value = '';
                rebuildChartsWithBusy('Adding sets from year range…', resizeAllChartsAfterRebuild);
            });
        }

        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                addedSetIndices.clear();
                Array.from(addSetSelect.options).forEach((o) => {
                    if (o.value !== '') o.disabled = false;
                });
                addSetSelect.value = '';
                rebuildChartsWithBusy('Clearing comparison…', resizeAllChartsAfterRebuild);
            });
        }
    }

    /** Years from set `release_date` (YYYY/MM/DD or YYYY-MM-DD) to `asOfMs` (UTC). */
    function parseSetReleaseYearsSince(releaseDateStr, asOfMs) {
        if (!releaseDateStr || typeof releaseDateStr !== 'string') return null;
        const p = releaseDateStr.trim().split(/[/-]/);
        if (p.length < 3) return null;
        const y = parseInt(p[0], 10);
        const mo = parseInt(p[1], 10) - 1;
        const d = parseInt(p[2], 10);
        if (!Number.isFinite(y) || !Number.isFinite(mo) || !Number.isFinite(d)) return null;
        const t0 = Date.UTC(y, mo, d);
        if (!Number.isFinite(t0)) return null;
        const diff = asOfMs - t0;
        const years = diff / (365.25 * 24 * 60 * 60 * 1000);
        return years >= 0 ? years : 0;
    }

    /** Sqrt-scaled capped age: more horizontal spread for 0–15y sets, vintage tail compressed vs raw log. */
    function setVintageDisplayX(rawYears) {
        const y = Number(rawYears);
        if (!Number.isFinite(y) || y < 0) return null;
        const capped = Math.min(y, SET_VINTAGE_SQRT_CAP_YEARS);
        const den = Math.sqrt(SET_VINTAGE_SQRT_CAP_YEARS);
        if (den <= 1e-12) return 0;
        return Math.sqrt(Math.max(0, capped)) / den;
    }

    /** Ordinal “print class” score for collectors (same ladder as rarity LSRL weights—SIR &gt; IR &gt; UR…). */
    function getCollectorPrintClassScore(rarity) {
        return getStaticRarityWeight(rarity);
    }

    function pearsonR(xs, ys) {
        const n = Math.min(xs.length, ys.length);
        if (n < 3) return 0;
        let mx = 0;
        let my = 0;
        for (let i = 0; i < n; i++) {
            mx += xs[i];
            my += ys[i];
        }
        mx /= n;
        my /= n;
        let num = 0;
        let dx = 0;
        let dy = 0;
        for (let i = 0; i < n; i++) {
            const vx = xs[i] - mx;
            const vy = ys[i] - my;
            num += vx * vy;
            dx += vx * vx;
            dy += vy * vy;
        }
        const den = Math.sqrt(dx * dy);
        return den > 1e-12 ? num / den : 0;
    }

    function weightedPearsonR(xs, ys, ws) {
        const n = Math.min(xs.length, ys.length, ws.length);
        if (n < 3) return 0;
        let sw = 0;
        for (let i = 0; i < n; i++) {
            const w = ws[i] > 0 && Number.isFinite(ws[i]) ? ws[i] : 0;
            sw += w;
        }
        if (sw <= 1e-12) return 0;
        let mx = 0;
        let my = 0;
        for (let i = 0; i < n; i++) {
            const w = ws[i] > 0 && Number.isFinite(ws[i]) ? ws[i] : 0;
            mx += w * xs[i];
            my += w * ys[i];
        }
        mx /= sw;
        my /= sw;
        let cov = 0;
        let vx = 0;
        let vy = 0;
        for (let i = 0; i < n; i++) {
            const w = ws[i] > 0 && Number.isFinite(ws[i]) ? ws[i] : 0;
            const dx = xs[i] - mx;
            const dy = ys[i] - my;
            cov += w * dx * dy;
            vx += w * dx * dx;
            vy += w * dy * dy;
        }
        const den = Math.sqrt(vx * vy);
        return den > 1e-12 ? cov / den : 0;
    }

    function weightedMeanStd(xs, ws) {
        let sw = 0;
        let swx = 0;
        for (let i = 0; i < xs.length; i++) {
            const w = ws[i] > 0 && Number.isFinite(ws[i]) ? ws[i] : 0;
            sw += w;
            swx += w * xs[i];
        }
        if (sw <= 1e-12) return null;
        const mean = swx / sw;
        let sv = 0;
        for (let i = 0; i < xs.length; i++) {
            const w = ws[i] > 0 && Number.isFinite(ws[i]) ? ws[i] : 0;
            const d = xs[i] - mean;
            sv += w * d * d;
        }
        const std = Math.sqrt(sv / sw);
        return { mean, std: std > 1e-12 ? std : 1 };
    }

    /** Weighted LS y ≈ b0 + b1 x (de-trend hype vs popularity on training rows). */
    function fitWeightedLinearYOnX(xArr, yArr, wArr) {
        const n = Math.min(xArr.length, yArr.length, wArr.length);
        if (n < 3) return null;
        let S = 0;
        let Sx = 0;
        let Sy = 0;
        let Sxx = 0;
        let Sxy = 0;
        for (let i = 0; i < n; i++) {
            const wi = wArr[i] > 0 && Number.isFinite(wArr[i]) ? wArr[i] : 0;
            if (!(wi > 0)) continue;
            const x = xArr[i];
            const y = yArr[i];
            if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
            S += wi;
            Sx += wi * x;
            Sy += wi * y;
            Sxx += wi * x * x;
            Sxy += wi * x * y;
        }
        if (S <= 1e-12) return null;
        const det = S * Sxx - Sx * Sx;
        if (Math.abs(det) < 1e-18) return null;
        const b1 = (S * Sxy - Sx * Sy) / det;
        const b0 = (Sy - b1 * Sx) / S;
        if (!Number.isFinite(b0) || !Number.isFinite(b1)) return null;
        return { b0, b1 };
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

    /** Median of positive Wizard history snapshots (ignores null / $0 rows) — anchors chart $ when list prints are stale. */
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

    /**
     * Positive USD candidates for charting: list, Dex, TCGTracking, tcgapi, Wizard current,
     * and the median of Wizard positive history (one synthetic anchor so thin high asks do not dominate).
     */
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
        const pch = typeof SHARED_UTILS !== 'undefined' && SHARED_UTILS.pricechartingHistoryPositiveUsdMedian
            ? SHARED_UTILS.pricechartingHistoryPositiveUsdMedian(card)
            : null;
        if (pch != null) push(pch);
        return priceDedupForMedian(vals);
    }

    /**
     * Chart / composite dollar axis: **median** of every distinct positive signal on the card
     * (Dex / TCG / tcgapi / list / Wizard current / Wizard history median). When only one distinct value
     * remains but Wizard current is far below list and liquidity is thin, blend toward median([list, wizard, hist]).
     */
    function resolveChartMarketUsd(card) {
        if (!card) return NaN;
        const dedup = collectDedupedPositiveUsdPrices(card);
        if (dedup.length >= 2) return medianArray(dedup);
        if (dedup.length === 1) {
            const m = dedup[0];
            const wc = Number(card.pokemon_wizard_current_price_usd);
            if (Number.isFinite(wc) && wc > 0 && wc < m * 0.48) {
                const liq = cardEbayLiquidityCountForWeights(card);
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
        return Number(card.market_price);
    }

    /**
     * After a simple ln(price) ~ ln(pull) fit, shrink tiers whose residuals are wide vs the global median
     * (geometric-normalized multipliers, clamped). Keeps hand-tuned static weights but nudges them from the pooled sample.
     */
    function computeRarityTierCalibrationFactors(trainRows) {
        const rows = trainRows.filter((r) =>
            r.logPremium != null && Number.isFinite(r.logPremium) && r.price > 0 && Number.isFinite(r.price));
        if (rows.length < RARITY_CALIB_MIN_ROWS) return {};
        const ys = rows.map((r) => Math.log(r.price));
        const xs = rows.map((r) => r.logPremium);
        const n = xs.length;
        let sx = 0;
        let sy = 0;
        let sxx = 0;
        let sxy = 0;
        for (let i = 0; i < n; i++) {
            sx += xs[i];
            sy += ys[i];
            sxx += xs[i] * xs[i];
            sxy += xs[i] * ys[i];
        }
        const den = n * sxx - sx * sx;
        if (Math.abs(den) < 1e-12) return {};
        const b = (n * sxy - sx * sy) / den;
        const a = (sy - b * sx) / n;
        const absRes = rows.map((_, i) => Math.abs(ys[i] - (a + b * xs[i])));
        const globalMed = medianArray(absRes);
        if (globalMed == null || globalMed < 1e-12) return {};

        const byTier = {};
        rows.forEach((r, i) => {
            const t = r.tierKey || 'unknown';
            if (!byTier[t]) byTier[t] = [];
            byTier[t].push(absRes[i]);
        });
        const raw = {};
        Object.keys(byTier).sort().forEach((t) => {
            const arr = byTier[t];
            if (arr.length < RARITY_CALIB_MIN_BUCKET) return;
            const mt = medianArray(arr);
            if (mt == null || mt < 1e-12) return;
            raw[t] = Math.min(RARITY_CALIB_CLAMP[1], Math.max(RARITY_CALIB_CLAMP[0], globalMed / mt));
        });
        const vals = Object.values(raw);
        if (vals.length === 0) return {};
        const logMean = vals.reduce((s, v) => s + Math.log(v), 0) / vals.length;
        const scale = Math.exp(logMean);
        const out = {};
        Object.keys(raw).sort().forEach((t) => {
            out[t] = raw[t] / scale;
        });
        return out;
    }

    function geometricNormalizeCalibrationMap(map) {
        const vals = Object.values(map).filter((v) => v > 0 && Number.isFinite(v));
        if (!vals.length) return {};
        const logMean = vals.reduce((s, v) => s + Math.log(v), 0) / vals.length;
        const scale = Math.exp(logMean);
        const out = {};
        Object.keys(map).sort().forEach((t) => {
            const v = map[t];
            if (v > 0 && Number.isFinite(v)) out[t] = v / scale;
        });
        return out;
    }

    /**
     * When ≥2 sets contribute rows, median each tier's pooled calibration factor across leave-one-set-out fits,
     * then renormalize (same geometry as single pooled pass). Reduces sensitivity to one set driving tier spreads.
     */
    function computeRarityTierCalibrationFactorsJackknife(trainRows) {
        const pooled = computeRarityTierCalibrationFactors(trainRows);
        const setIds = [...new Set(trainRows.map((r) => r.setIdx).filter((x) => x != null && !Number.isNaN(Number(x))))]
            .sort((a, b) => a - b);
        if (setIds.length < 2) return pooled;

        const tierToList = {};
        setIds.forEach((held) => {
            const sub = trainRows.filter((r) => r.setIdx !== held);
            const fac = computeRarityTierCalibrationFactors(sub);
            Object.keys(fac).forEach((t) => {
                if (!tierToList[t]) tierToList[t] = [];
                tierToList[t].push(fac[t]);
            });
        });

        const merged = {};
        Object.keys(pooled).sort().forEach((t) => {
            const arr = tierToList[t];
            merged[t] = arr && arr.length >= 2 ? medianArray(arr) : pooled[t];
        });
        Object.keys(tierToList).sort().forEach((t) => {
            if (merged[t] != null) return;
            const arr = tierToList[t];
            if (!arr || !arr.length) return;
            merged[t] = arr.length >= 2 ? medianArray(arr) : arr[0];
        });
        const norm = geometricNormalizeCalibrationMap(merged);
        return Object.keys(norm).length > 0 ? norm : pooled;
    }

    function collectCharacterVolumePairs(setIndices) {
        const pairs = [];
        setIndices.forEach((setIdx) => {
            const setInfo = allSetsData[setIdx];
            if (!setInfo || !setInfo.top_25_cards) return;
            setInfo.top_25_cards.forEach((card) => {
                if (isExcludedLegacyBaseCharizardOutlier(setInfo, card)) return;
                const marketPrice = resolveChartMarketUsd(card);
                if (!isChartableMarketPrice(marketPrice)) return;
                const charInfo = getCharacterPremiumInfo(card.name);
                if (charInfo.volume <= 0) return;
                pairs.push({ vol: charInfo.volume, logPrice: Math.log(marketPrice) });
            });
        });
        return pairs;
    }

    function computeCharacterVolumeTransform(setIndices) {
        const pairs = collectCharacterVolumePairs(setIndices);
        const linearDefault = { mode: 'linear', vScale: 1 };
        if (pairs.length < CHARACTER_QUADRATIC_MIN_N) return linearDefault;
        const vols = pairs.map((p) => p.vol);
        const logs = pairs.map((p) => p.logPrice);
        const sortedV = vols.slice().sort((a, b) => a - b);
        const idx = Math.min(sortedV.length - 1, Math.max(0, Math.floor(0.92 * (sortedV.length - 1))));
        const vScale = Math.max(sortedV[idx] || sortedV[sortedV.length - 1] || 1, 1);
        const quadX = vols.map((v) => v + (v * v) / vScale);
        const rLin = pearsonR(vols, logs);
        const rQuad = pearsonR(quadX, logs);
        if (Math.abs(rQuad) > Math.abs(rLin) + CHARACTER_QUADRATIC_R_IMPROVE) {
            return { mode: 'quadratic', vScale };
        }
        return linearDefault;
    }

    function syncCharacterChartXAxisTitle() {
        const ch = charts.character;
        if (!ch || !ch.options.scales || !ch.options.scales.x || !ch.options.scales.x.title) return;
        const t = ch.options.scales.x.title;
        if (latestCharacterVolumeTransform.mode === 'quadratic') {
            const s = latestCharacterVolumeTransform.vScale;
            t.text = `Volume + volume² / s (s ≈ ${Number.isFinite(s) ? s.toFixed(0) : '1'})`;
        } else {
            t.text = 'High-tier print volume';
        }
    }

    function syncSetVintageChartXAxisTitle() {
        const ch = charts.setVintage;
        if (!ch || !ch.options.scales || !ch.options.scales.x || !ch.options.scales.x.title) return;
        ch.options.scales.x.title.text = `sqrt(years since release, cap ${SET_VINTAGE_SQRT_CAP_YEARS}y) ÷ sqrt(cap)`;
    }

    function syncTcgMacroInterestChartXAxisTitle() {
        const ch = charts.tcgMacroInterest;
        if (!ch || !ch.options.scales || !ch.options.scales.x || !ch.options.scales.x.title) return;
        const t = ch.options.scales.x.title;
        const lab = latestTcgMacroSeriesLabel.trim();
        t.text = lab.length > 70 ? `${lab.slice(0, 68)}…` : (lab || 'Hobby-wide index (set release year)');
    }

    function syncArtistChaseChartXAxisTitle() {
        const ch = charts.artistChase;
        if (!ch || !ch.options.scales || !ch.options.scales.x || !ch.options.scales.x.title) return;
        ch.options.scales.x.title.text = 'ln(1 + illustrator chase median $) from artist_scores.json';
    }

    /**
     * Composite drivers (each must clear |r| vs price or enter via fallback in `pickCompositeModel` / `buildCompositeModelFromRowsInner`).
     * `logGradedPop` is log₁₀(1 + graded count); same X as the graded-pop scatter when POP data exist.
     */
    const COMPOSITE_KEYS = [
        'logPremium', 'logCharVol', 'trends', 'hype', 'logGradedPop', 
        'logArtStyle', 'logNostalgia', 'logGemMintRate', 'trainerArchetype',
        'logVintageShowcase', 'logFixedProductCorrection', 'logSpeciesUtility',
        'logArtistChase', 'logIconicPremium', 'logPriceMomentum', 'logSetSaturation'
    ];

    /**
     * Ship contract: each composite driver must appear on ≥1 dedicated scatter (or the composite itself)
     * so users can inspect the raw relationship to price. When adding a COMPOSITE_KEYS entry, extend this
     * map, wire `createDatasets`, and add/update a panel in `analytics.html` (plus tooltip copy if needed).
     */
    const COMPOSITE_DRIVER_SCATTER_COVERAGE = {
        logPremium: { charts: ['pullCost', 'composite'] },
        logCharVol: { charts: ['character', 'composite'] },
        trends: { charts: ['trends', 'hypeScarcity', 'hypePullRatio', 'composite'] },
        hype: { charts: ['hypeScarcity', 'hypePullRatio', 'composite'] },
        logGradedPop: { charts: ['gradedPop', 'composite'] },
        logArtStyle: { charts: ['rarityTier', 'composite'] },
        logNostalgia: { charts: ['character', 'trends', 'composite'] },
        logGemMintRate: { charts: ['gradedPop', 'composite'] },
        trainerArchetype: { charts: ['character', 'composite'] },
        logVintageShowcase: { charts: ['setVintage', 'composite'] },
        logFixedProductCorrection: { charts: ['pullCost', 'composite'] },
        logSpeciesUtility: { charts: ['character', 'composite'] },
        logArtistChase: { charts: ['artistChase', 'composite'] },
        logIconicPremium: { charts: ['trends', 'composite'] },
        logPriceMomentum: { charts: ['pullCost', 'composite'] },
        logSetSaturation: { charts: ['character', 'composite'] }
    };

    function assertCompositeDriverScatterCoverage() {
        const specKeys = Object.keys(COMPOSITE_DRIVER_SCATTER_COVERAGE);
        const expected = new Set(COMPOSITE_KEYS);
        const got = new Set(specKeys);
        const missingInSpec = COMPOSITE_KEYS.filter((k) => !got.has(k));
        const orphanInSpec = specKeys.filter((k) => !expected.has(k));
        if (missingInSpec.length || orphanInSpec.length) {
            console.error('[analytics] COMPOSITE_KEYS out of sync with COMPOSITE_DRIVER_SCATTER_COVERAGE', {
                missingInSpec,
                orphanInSpec
            });
        }
        COMPOSITE_KEYS.forEach((k) => {
            const spec = COMPOSITE_DRIVER_SCATTER_COVERAGE[k];
            if (!spec || !Array.isArray(spec.charts) || spec.charts.length === 0) {
                console.error('[analytics] Composite driver has no scatter coverage entry:', k);
                return;
            }
            spec.charts.forEach((chartKey) => {
                if (!(chartKey in charts)) {
                    console.warn('[analytics] Coverage lists unknown chart key:', chartKey, 'for driver', k);
                }
            });
        });
    }

    function isChartableMarketPrice(p) {
        const n = Number(p);
        const lo = chartScatterMinUsd();
        return Number.isFinite(n) && n > 0 && n >= lo;
    }

    /** Count top-list cards in the full export that carry a positive graded total (Gemrate or sidecar). */
    function countCardsWithGradedPopInDataset(setsArr) {
        let n = 0;
        if (!Array.isArray(setsArr)) return 0;
        setsArr.forEach((s) => {
            const top = s && s.top_25_cards;
            if (!Array.isArray(top)) return;
            top.forEach((c) => {
                const t = getCardGradedPopTotal(c);
                if (t != null && Number.isFinite(t) && t > 0) n += 1;
            });
        });
        return n;
    }

    function updateAnalyticsGemrateStrip() {
        const el = document.getElementById('analytics-gemrate-line');
        if (!el || !Array.isArray(allSetsData)) return;
        const nCards = countCardsWithGradedPopInDataset(allSetsData);
        
        if (nCards === 0) {
            el.innerHTML = '<span class="analytics-pm-missing">No Gemrate graded totals in this <code>pokemon_sets_data.json</code> — run <code>python gemrate_scraper.py</code>.</span>';
            return;
        }
        el.innerHTML = `<span class="analytics-pm-ok">Gemrate Population</span> · <strong>${nCards.toLocaleString()}</strong> top-list card(s) with Universal pop totals · <code>gemrate.com</code>`;
    }

    /**
     * Graded population total for charting.
     */
    function getCardGradedPopTotal(card) {
        if (!card) return null;
        if (card.gemrate && card.gemrate.total != null) return card.gemrate.total;
        const psa = Number(card.psa_graded_pop_total);
        if (Number.isFinite(psa) && psa >= 0) return psa;
        return null;
    }

    function getCardGradedPopLog10X(card) {
        const t = getCardGradedPopTotal(card);
        if (t == null || !Number.isFinite(t) || t < 0) return null;
        return Math.log10(1 + t);
    }

    function clampFinite(n, lo, hi) {
        if (!Number.isFinite(n)) return lo;
        return Math.min(hi, Math.max(lo, n));
    }

    /**
     * Median log₁₀(1+graded pop) among chartable-$ top-list cards from sets in the “modern product” band
     * (calendar year ≥ 2015 or ≤12y since release). Used as a slab-density reference vs older / rarer runs.
     */
    function buildScatterModernGradedMedianLog(orderedSetIndices) {
        const vals = [];
        if (!Array.isArray(orderedSetIndices)) return null;
        orderedSetIndices.forEach((sIdx) => {
            const setInfo = allSetsData[sIdx];
            if (!setInfo || !Array.isArray(setInfo.top_25_cards)) return;
            const calY = parseSetReleaseYear(setInfo.release_date);
            const ySince = parseSetReleaseYearsSince(setInfo.release_date, ANALYTICS_AS_OF_MS);
            const modernEra = (calY != null && calY >= 2015) || (ySince != null && Number.isFinite(ySince) && ySince <= 12);
            if (!modernEra) return;
            setInfo.top_25_cards.forEach((card) => {
                if (isExcludedLegacyBaseCharizardOutlier(setInfo, card)) return;
                const marketPrice = resolveChartMarketUsd(card);
                if (!isChartableMarketPrice(marketPrice)) return;
                const lp = getCardGradedPopLog10X(card);
                if (lp != null && Number.isFinite(lp)) vals.push(lp);
            });
        });
        if (!vals.length) return null;
        return medianArray(vals);
    }

    /**
     * Popularity scatter X: file + species-graded index, then a small per-card slab vs set-median nudge.
     */
    function popularityScatterXWithSlabVsSet(trendsChart, logCardGradedPop, setMedianLogGradedPop) {
        if (trendsChart == null || !Number.isFinite(trendsChart) || trendsChart <= 0) return null;
        if (logCardGradedPop == null || !Number.isFinite(logCardGradedPop)
            || setMedianLogGradedPop == null || !Number.isFinite(setMedianLogGradedPop)) {
            return trendsChart;
        }
        const rel = logCardGradedPop - setMedianLogGradedPop;
        const mult = Math.exp(POPULARITY_SCATTER_SLAB_VS_SET_BLEND * clampFinite(rel, -1.25, 1.25));
        return trendsChart * mult;
    }

    /**
     * Scarcity for hype×price: pull difficulty (log₁₀) plus slab scarcity vs set peers and vs modern-era median.
     */
    function hypeScarcityMetricCombined(effectivePull, logCardGradedPop, setMedianLogGradedPop, eraModernMedianLogPop) {
        const ep = Number(effectivePull);
        if (!Number.isFinite(ep) || ep <= 0) return null;
        let s = Math.log10(ep + 1);
        if (logCardGradedPop != null && Number.isFinite(logCardGradedPop)
            && setMedianLogGradedPop != null && Number.isFinite(setMedianLogGradedPop)) {
            s += HYPE_SCARCITY_SLAB_VS_SET_WEIGHT * clampFinite(setMedianLogGradedPop - logCardGradedPop, -0.4, 2.8);
        }
        if (logCardGradedPop != null && Number.isFinite(logCardGradedPop)
            && eraModernMedianLogPop != null && Number.isFinite(eraModernMedianLogPop)) {
            s += HYPE_SCARCITY_SLAB_VS_MODERN_ERA_WEIGHT * clampFinite(eraModernMedianLogPop - logCardGradedPop, -0.4, 2.2);
        }
        return s;
    }

    function rebuildSpeciesGradedPopFromExport() {
        speciesGradedPopSumByKey.clear();
        speciesGradedPopMaxCached = 0;
        if (!Array.isArray(allSetsData)) return;
        allSetsData.forEach((setInfo) => {
            if (!setInfo || !Array.isArray(setInfo.top_25_cards)) return;
            setInfo.top_25_cards.forEach((card) => {
                const tot = getCardGradedPopTotal(card);
                if (tot == null || !Number.isFinite(tot) || tot <= 0) return;
                const ids = cleanCharacterName(card.name);
                if (!ids.length) return;
                const share = tot / ids.length;
                ids.forEach((id) => {
                    const k = String(id).trim();
                    if (!k) return;
                    speciesGradedPopSumByKey.set(k, (speciesGradedPopSumByKey.get(k) || 0) + share);
                });
            });
        });
        let m = 0;
        speciesGradedPopSumByKey.forEach((v) => {
            if (v > m) m = v;
        });
        speciesGradedPopMaxCached = m;
    }

    function gradedPopularityAxisMultiplierFromSpeciesNorm(normMax) {
        const s = Number(normMax);
        const share = Number.isFinite(s) && s > 0 ? Math.min(1, Math.max(0, s)) : 0;
        const rawStretch = POPULARITY_SPECIES_GRADED_FLOOR + (1 - POPULARITY_SPECIES_GRADED_FLOOR) * share;
        return 1 + POPULARITY_GRADED_TO_AXIS_BLEND * (rawStretch - 1);
    }

    /**
     * Export-wide species graded intensity in [0, 1] vs the top species (1 = highest total slabs in this JSON).
     * `scale` multiplies file popularity; graded is blended toward 1 so the axis stays comparable to file-only spreads.
     */
    function speciesPopularityGradedNormForCardName(cardName) {
        const gmax = speciesGradedPopMaxCached;
        if (gmax <= 0 || speciesGradedPopSumByKey.size === 0) {
            return { normMax: 0, scale: 1, hasGlobalGraded: false };
        }
        const ids = cleanCharacterName(cardName);
        if (!ids.length) {
            return { normMax: 0, scale: gradedPopularityAxisMultiplierFromSpeciesNorm(0), hasGlobalGraded: true };
        }
        let best = 0;
        ids.forEach((id) => {
            const v = speciesGradedPopSumByKey.get(String(id).trim()) || 0;
            best = Math.max(best, v / gmax);
        });
        const scale = gradedPopularityAxisMultiplierFromSpeciesNorm(best);
        return { normMax: best, scale, hasGlobalGraded: true };
    }

    function gatherCompositeTrainingRows() {
        const out = [];
        for (const setIdx of getEffectiveSetIndicesOrdered()) {
            const setInfo = allSetsData[setIdx];
            if (!setInfo || !setInfo.top_25_cards) continue;
            const packPrice = resolvePackPriceUsdForPull(setInfo);
            setInfo.top_25_cards.forEach((card) => {
                if (isExcludedLegacyBaseCharizardOutlier(setInfo, card)) return;
                const marketPrice = resolveChartMarketUsd(card);
                if (!isChartableMarketPrice(marketPrice)) return;
                const pull = computeEffectivePullCost(card, setInfo, packPrice);
                const charInfo = getCharacterPremiumInfo(card.name);
                const trendsScore = getTrendsScore(card.name);
                const ep = pull.value;
                const logPremium = ep > 0 ? Math.log(Math.max(marketPrice / Math.max(ep, 1e-9), 1e-9)) : null;
                const logPremiumComposite = logPremiumForCompositeTraining(logPremium, setInfo, card);
                const logCharVol = charInfo.volume > 0 ? Math.log1p(charInfo.volume) : null;
                const trends = trendsScore > 0 ? trendsScore : null;
                const hype = ep > 0 && trendsScore > 0 ? trendsScore * Math.log10(ep + 1) : null;
                const logGradedPop = getCardGradedPopLog10X(card);
                const gemRate = getCardGemMintRate(card);
                const logGemMintRate = Math.log1p(gemRate * 12); // Slightly higher weight for condition difficulty
                const artScore = getArtStyleScore(card.rarity);
                const logArtStyle = Math.log1p(artScore);

                let nostalgiaScore = 0;
                charInfo.speciesKeys.forEach(sk => {
                    if (nostalgiaData[sk]) nostalgiaScore = Math.max(nostalgiaScore, nostalgiaData[sk].nostalgia_score);
                });
                // Nostalgia Scaling: Bulk Pikachu/Charizard should not get the full grail premium.
                const nostalgiaScalar = Math.max(0.12, artScore / 10.0);
                const logNostalgia = nostalgiaScore > 0 ? Math.log10(1 + nostalgiaScore) * nostalgiaScalar : null;

                const trainerArchetype = charInfo.archetype === 'Waifu' ? 2.5 : (charInfo.archetype === 'Utility' ? 1.4 : 1.0);
                
                const logVintageShowcase = getVintageShowcaseScore(card, setInfo) || 0;
                const logFixedProductCorrection = usesBoosterStyleCardPullOdds(setInfo, card) ? 0 : -1.5;
                const logSpeciesUtility = charInfo.speciesKeys.length > 0 ? 0 : -1.2;

                const artChase = lookupArtistChase(card.artist);
                const logArtistChase = artChase != null ? Math.log(1 + artChase.median) : null;

                const histMed = wizardHistoryPositiveUsdMedian(card);
                let logPriceMomentum = (histMed != null && histMed > 0) ? Math.log(marketPrice / histMed) : 0;
                logPriceMomentum = Math.min(1.8, Math.max(-1.8, logPriceMomentum)); // Cap momentum to prevent skew

                const setSize = (setInfo.top_25_cards || []).length;
                const rarityCount = getRarityCohortCount(setInfo, card.rarity);
                const logSetSaturation = Math.log1p(setSize / Math.max(1, rarityCount)) * 0.5; // Slightly dampened

                const iconicMap = { 
                    'charizard': 2.2, 'pikachu': 1.8, 'mewtwo': 1.7, 'mew': 1.6, 'celebi': 1.5,
                    'lugia': 1.45, 'rayquaza': 1.45, 'umbreon': 1.35, 'gengar': 1.3,
                    'suicune': 1.25, 'raikou': 1.2, 'entei': 1.2, 'blastoise': 1.2, 'venusaur': 1.2,
                    'lucario': 1.1, 'eevee': 1.1, 'darkrai': 1.0, 'arceus': 1.0
                };
                let iconicV = 0;
                charInfo.speciesKeys.forEach(sk => {
                    const lowSk = String(sk).toLowerCase();
                    if (iconicMap[lowSk]) iconicV = Math.max(iconicV, iconicMap[lowSk]);
                });
                const logIconicPremium = iconicV > 0 ? Math.log1p(iconicV) : null;

                const tierKey = getRarityTierKey(card.rarity);
                const staticWeight = getStaticRarityWeight(card.rarity);
                const liq = listingLiquidityFactor(card);
                const pullCtx = nonBoosterPullRegressionWeightMult(setInfo, card);
                out.push({
                    setIdx,
                    price: marketPrice,
                    tierKey,
                    staticWeight,
                    weight: staticWeight * liq * pullCtx,
                    logPremium: logPremiumComposite,
                    logCharVol,
                    trends,
                    hype,
                    logGradedPop,
                    logArtStyle,
                    logNostalgia,
                    logGemMintRate,
                    trainerArchetype,
                    logVintageShowcase,
                    logFixedProductCorrection,
                    logSpeciesUtility,
                    logArtistChase,
                    logIconicPremium,
                    logPriceMomentum,
                    logSetSaturation
                });
            });
        }
        return out;
    }

    function computeHypeOrthTrendOnRows(rows) {
        const xs = [];
        const ys = [];
        const ws = [];
        rows.forEach((row) => {
            const h = row.hype;
            const t = row.trends;
            const w = row.weight;
            if (h == null || !Number.isFinite(h) || t == null || !Number.isFinite(t) || w == null || !(w > 0)) return;
            xs.push(t);
            ys.push(h);
            ws.push(w);
        });
        if (xs.length < 5) {
            rows.forEach((row) => { row.hypeOrthTrend = row.hype; });
            return null;
        }
        const coef = fitWeightedLinearYOnX(xs, ys, ws);
        if (!coef) {
            rows.forEach((row) => { row.hypeOrthTrend = row.hype; });
            return null;
        }
        rows.forEach((row) => {
            const h = row.hype;
            const t = row.trends;
            if (h == null || !Number.isFinite(h) || t == null || !Number.isFinite(t)) {
                row.hypeOrthTrend = h != null && Number.isFinite(h) ? h : null;
                return;
            }
            row.hypeOrthTrend = h - (coef.b0 + coef.b1 * t);
        });
        return coef;
    }

    function driverValueForCompositeRow(row, k, useHypeOrth) {
        if (k === 'hype' && useHypeOrth) {
            const v = row.hypeOrthTrend;
            return v != null && Number.isFinite(v) ? v : null;
        }
        const v = row[k];
        return v != null && Number.isFinite(v) ? v : null;
    }

    /** |weighted r| between composite X and log₁₀(price) on training rows (headline geometry for the composite chart). */
    function evaluateCompositeAbsRLogPrice(trainRows, model) {
        if (!model) return -1;
        const xs = [];
        const ys = [];
        const ws = [];
        const yFloor = chartScatterMinUsd();
        trainRows.forEach((row) => {
            const feat = compositeTrainingFeatForModel(row, model);
            const cx = compositeScoreFromRow(feat, model);
            if (cx == null || !Number.isFinite(cx) || !(row.price > 0)) return;
            xs.push(cx);
            ys.push(Math.log10(Math.max(row.price, yFloor)));
            const rw = row.weight != null && Number.isFinite(row.weight) && row.weight > 0 ? row.weight : 1;
            ws.push(rw);
        });
        if (xs.length < 3) return -1;
        return Math.abs(weightedPearsonR(xs, ys, ws));
    }

    function compositeTrainingFeatForModel(row, model) {
        if (!model) return null;
        const useOrth = !!model.trainFeatHypeUsesOrth;
        const hypeV = useOrth && row.hypeOrthTrend != null && Number.isFinite(row.hypeOrthTrend)
            ? row.hypeOrthTrend
            : row.hype;
            
        const feat = {};
        COMPOSITE_KEYS.forEach(k => {
            if (k === 'hype') feat[k] = hypeV;
            else feat[k] = row[k];
        });
        return feat;
    }

    /**
     * `priceRModeForR`: marginal driver–price Pearson uses linear USD or log₁₀(USD).
     * `trainFeatHypeUsesOrth`: hype driver uses WLS residual vs trends (same key `hype` in blend).
     */
    function buildCompositeModelFromRowsInner(trainRows, priceRModeForR, trainFeatHypeUsesOrth) {
        const rBy = {};
        const yFloor = chartScatterMinUsd();
        COMPOSITE_KEYS.forEach((k) => {
            const xs = [];
            const ys = [];
            const ws = [];
            trainRows.forEach((row) => {
                const v = driverValueForCompositeRow(row, k, trainFeatHypeUsesOrth);
                if (v == null || !Number.isFinite(v) || !(row.price > 0)) return;
                xs.push(v);
                const yp = priceRModeForR === 'log'
                    ? Math.log10(Math.max(row.price, yFloor))
                    : row.price;
                ys.push(yp);
                const rw = row.weight != null && Number.isFinite(row.weight) && row.weight > 0 ? row.weight : 1;
                ws.push(rw);
            });
            rBy[k] = xs.length >= 3 ? weightedPearsonR(xs, ys, ws) : 0;
        });
        let selected = COMPOSITE_KEYS.filter((k) => Math.abs(rBy[k]) >= COMPOSITE_MODERATE_ABS_R);
        if (selected.length === 0) {
            const best = COMPOSITE_KEYS.reduce((a, k) =>
                (Math.abs(rBy[k]) > Math.abs(rBy[a]) ? k : a), COMPOSITE_KEYS[0]);
            if (Math.abs(rBy[best]) >= COMPOSITE_FALLBACK_ABS_R) selected = [best];
            else return null;
        }
        const means = {};
        const stds = {};
        selected.forEach((k) => {
            const xs = [];
            const ws = [];
            trainRows.forEach((row) => {
                const v = driverValueForCompositeRow(row, k, trainFeatHypeUsesOrth);
                if (v == null || !Number.isFinite(v)) return;
                xs.push(v);
                const rw = row.weight != null && Number.isFinite(row.weight) && row.weight > 0 ? row.weight : 1;
                ws.push(rw);
            });
            if (xs.length < 2) return;
            const ms = weightedMeanStd(xs, ws);
            if (!ms) return;
            means[k] = ms.mean;
            stds[k] = ms.std;
        });
        const keys = selected.filter((k) => means[k] != null && stds[k] != null);
        if (!keys.length) return null;

        const baseModel = { keys, r: rBy, means, stds, priceRModeForR, trainFeatHypeUsesOrth, driverScreenYBasis: priceRModeForR === 'log' ? 'log' : 'linear' };
        let currentBestR = evaluateCompositeAbsRLogPrice(trainRows, baseModel);
        let finalKeys = [...keys];

        // Greedy Pruning Pass: Try adding other drivers that didn't hit COMPOSITE_MODERATE_ABS_R
        // but might improve the total R by explaining residuals.
        const candidates = COMPOSITE_KEYS.filter(k => !finalKeys.includes(k) && Math.abs(rBy[k]) >= COMPOSITE_FALLBACK_ABS_R);
        candidates.sort((a, b) => Math.abs(rBy[b]) - Math.abs(rBy[a]));

        for (const cand of candidates) {
            const trialKeys = [...finalKeys, cand];
            const trialMeans = {};
            const trialStds = {};
            let ok = true;
            trialKeys.forEach(k => {
                const xs = [];
                const ws = [];
                trainRows.forEach(row => {
                    const v = driverValueForCompositeRow(row, k, trainFeatHypeUsesOrth);
                    if (v == null || !Number.isFinite(v)) return;
                    xs.push(v);
                    const rw = row.weight != null && Number.isFinite(row.weight) && row.weight > 0 ? row.weight : 1;
                    ws.push(rw);
                });
                const ms = xs.length >= 2 ? weightedMeanStd(xs, ws) : null;
                if (!ms) ok = false; else { trialMeans[k] = ms.mean; trialStds[k] = ms.std; }
            });
            if (!ok) continue;

            const trialModel = { ...baseModel, keys: trialKeys, means: trialMeans, stds: trialStds };
            const trialR = evaluateCompositeAbsRLogPrice(trainRows, trialModel);
            if (trialR > currentBestR + 0.0005) { // Significant improvement threshold
                currentBestR = trialR;
                finalKeys = trialKeys;
                means[cand] = trialMeans[cand];
                stds[cand] = trialStds[cand];
            }
        }

        const label = finalKeys.map((k) => `${k} (r=${rBy[k].toFixed(2)})`).join(' · ')
            + (trainFeatHypeUsesOrth ? ' · hype⊥trends' : '');
        return {
            keys: finalKeys,
            r: rBy,
            means,
            stds,
            label,
            priceRModeForR,
            trainFeatHypeUsesOrth,
            driverScreenYBasis: baseModel.driverScreenYBasis,
            hypeOrthCoeffs: null
        };
    }

    function pickCompositeModel(trainRows) {
        const hypeCoef = computeHypeOrthTrendOnRows(trainRows);
        const mLin = buildCompositeModelFromRowsInner(trainRows, 'linear', false);
        const mLog = buildCompositeModelFromRowsInner(trainRows, 'log', false);
        const rLin = mLin ? evaluateCompositeAbsRLogPrice(trainRows, mLin) : -1;
        const rLog = mLog ? evaluateCompositeAbsRLogPrice(trainRows, mLog) : -1;
        let mOrth = null;
        let rOrth = -1;
        if (mLog && rLog < rLin) {
            mOrth = buildCompositeModelFromRowsInner(trainRows, 'log', true);
            rOrth = mOrth ? evaluateCompositeAbsRLogPrice(trainRows, mOrth) : -1;
        }
        const candidates = [];
        if (mLin) candidates.push({ m: mLin, tag: 'linearPriceDriverR', r: rLin });
        if (mLog) candidates.push({ m: mLog, tag: 'logPriceDriverR', r: rLog });
        if (mOrth) candidates.push({ m: mOrth, tag: 'logPriceDriverR+hypeOrthTrend', r: rOrth });
        let best = null;
        let bestTag = '';
        let bestR = Number.NEGATIVE_INFINITY;
        candidates.forEach(({ m, tag, r }) => {
            const rr = Number.isFinite(r) ? r : -1;
            if (rr > bestR) {
                bestR = rr;
                best = m;
                bestTag = tag;
            }
        });
        if (!best) {
            latestCompositeModelPickMeta = {
                tag: '',
                absRLogPriceVsX: null,
                absR_eval_linear: rLin > 0 ? rLin : null,
                absR_eval_log: rLog > 0 ? rLog : null,
                absR_eval_hypeOrth: rOrth > 0 ? rOrth : null
            };
            return null;
        }
        if (best.trainFeatHypeUsesOrth && hypeCoef) {
            best.hypeOrthCoeffs = hypeCoef;
        } else {
            best.hypeOrthCoeffs = null;
        }
        latestCompositeModelPickMeta = {
            tag: bestTag,
            absRLogPriceVsX: bestR > 0 ? bestR : null,
            absR_eval_linear: rLin > 0 ? rLin : null,
            absR_eval_log: rLog > 0 ? rLog : null,
            absR_eval_hypeOrth: rOrth > 0 ? rOrth : null
        };
        return best;
    }

    function buildCompositeModelFromRows(trainRows) {
        return pickCompositeModel(trainRows);
    }

    const COMPOSITE_KEY_LABELS = {
        logPremium: 'ln(price ÷ pull)',
        logCharVol: 'ln(1 + print vol.)',
        trends: 'Popularity',
        hype: 'Hype × scarcity',
        logGradedPop: 'log₁₀(1 + graded pop.)',
        logArtStyle: 'Art style score',
        logNostalgia: 'Nostalgia (species)',
        logGemMintRate: 'Gem Mint Rate (PSA 10)',
        trainerArchetype: 'Trainer Archetype',
        logVintageShowcase: 'Vintage Showcase Premium',
        logFixedProductCorrection: 'Fixed Product Adjustment',
        logSpeciesUtility: 'Species Utility (Pokémon vs Item)',
        logArtistChase: 'Artist Chase Median',
        logIconicPremium: 'Iconic Species Account',
        logPriceMomentum: 'Price Momentum (Current vs History)',
        logSetSaturation: 'Set Saturation (Slot Congestion)'
    };

    function compositeScoreFromRow(feat, model) {
        if (!model || !feat) return null;
        let num = 0;
        let den = 0;
        for (const k of model.keys) {
            const v = feat[k];
            if (v == null || !Number.isFinite(v)) continue;
            const z = (v - model.means[k]) / model.stds[k];
            if (!Number.isFinite(z)) continue;
            const rk = model.r[k];
            num += rk * z;
            den += Math.abs(rk);
        }
        return den > 0 ? num / den : null;
    }

    /** Signed pieces that sum to the composite index (same logic as compositeScoreFromRow). */
    function compositeBreakdownFromRow(feat, model) {
        if (!model || !feat) return null;
        const parts = [];
        let num = 0;
        let den = 0;
        for (const k of model.keys) {
            const v = feat[k];
            if (v == null || !Number.isFinite(v)) continue;
            const z = (v - model.means[k]) / model.stds[k];
            if (!Number.isFinite(z)) continue;
            const rk = model.r[k];
            const term = rk * z;
            num += term;
            den += Math.abs(rk);
            parts.push({
                key: k,
                label: COMPOSITE_KEY_LABELS[k] || k,
                z,
                r: rk,
                term,
                partial: null
            });
        }
        if (den <= 0 || parts.length === 0) return null;
        const total = num / den;
        parts.forEach((p) => {
            p.partial = p.term / den;
        });
        return { total, parts };
    }

    /**
     * Full driver table for tooltips: every composite input; z/Δ only for drivers in the pinned blend (`br.parts`).
     * The **r** column uses the pooled weighted Pearson (linear USD vs driver) from `poolModel` when present so
     * excluded drivers still show why they dropped out of the |r| gate.
     */
    function buildCompositeDriverTableExtended(f, br, escTip, fmtCompositeDriverCell, poolModel) {
        const shortLab = {
            logPremium: 'ln(p/u)',
            logCharVol: 'ln(1+v)',
            trends: 'Popularity',
            hype: 'Hype×',
            logGradedPop: 'ln(1+POP)'
        };
        const partByKey = {};
        if (br && Array.isArray(br.parts)) {
            br.parts.forEach((p) => {
                partByKey[p.key] = p;
            });
        }
        const poolR = poolModel && poolModel.r && typeof poolModel.r === 'object' ? poolModel.r : null;
        const rows = COMPOSITE_KEYS.map((k) => {
            const fv = f && f[k];
            const rs = fmtCompositeDriverCell(k, fv);
            const p = partByKey[k];
            const lab = shortLab[k] || escTip(COMPOSITE_KEY_LABELS[k] || k);
            const rPool = poolR && Number.isFinite(Number(poolR[k])) ? Number(poolR[k]).toFixed(2) : '—';
            if (p) {
                const s = p.partial >= 0 ? '+' : '';
                return `<tr class="composite-driver-row composite-driver-row--blend"><td style="padding:3px 8px 3px 0;color:#cbd5e1;white-space:nowrap;">${lab}</td><td style="padding:3px 6px;text-align:right;font-variant-numeric:tabular-nums;color:#e2e8f0;">${rs}</td><td style="padding:3px 6px;text-align:right;font-variant-numeric:tabular-nums;color:#94a3b8;">${p.z.toFixed(2)}</td><td style="padding:3px 6px;text-align:right;font-variant-numeric:tabular-nums;font-weight:600;color:#e2e8f0;">${s}${p.partial.toFixed(3)}</td><td style="padding:3px 0;text-align:right;font-variant-numeric:tabular-nums;color:#64748b;">${p.r.toFixed(2)}</td></tr>`;
            }
            return `<tr class="composite-driver-row"><td style="padding:3px 8px 3px 0;color:#94a3b8;white-space:nowrap;">${lab}</td><td style="padding:3px 6px;text-align:right;font-variant-numeric:tabular-nums;color:#94a3b8;">${rs}</td><td style="padding:3px 6px;text-align:right;font-variant-numeric:tabular-nums;color:#64748b;">—</td><td style="padding:3px 6px;text-align:right;font-variant-numeric:tabular-nums;color:#64748b;">—</td><td style="padding:3px 0;text-align:right;font-variant-numeric:tabular-nums;color:#64748b;">${rPool}</td></tr>`;
        }).join('');
        const rBasisLabel = poolModel && poolModel.driverScreenYBasis === 'log'
            ? 'log₁₀(price) USD'
            : 'linear USD';
        const orthNote = poolModel && poolModel.trainFeatHypeUsesOrth ? ' Hype in the blend is <strong>hype⊥trends</strong> (WLS residual vs popularity on training rows).' : '';
        const cap = `<div style="font-size:0.58rem;color:#64748b;line-height:1.35;margin:4px 0 2px;">Highlighted: in the blend (z / Δ blend). <strong>r</strong> = pooled weighted Pearson vs <strong>${rBasisLabel}</strong> for driver screening (|r| ≥ 0.4 enters blend; else single strongest with |r| ≥ 0.2). Excluded rows still show <strong>r</strong> when the model is fit.${orthNote}</div>`;
        const tbl = `<table class="composite-driver-table" style="width:100%;border-collapse:collapse;font-size:0.7rem;margin-top:2px;min-width:300px;"><thead><tr><th style="text-align:left;padding:0 8px 4px 0;color:#64748b;font-weight:600;">Driver</th><th style="text-align:right;padding:0 6px 4px;color:#64748b;font-weight:600;">Value</th><th style="text-align:right;padding:0 6px 4px;color:#64748b;font-weight:600;">z</th><th style="text-align:right;padding:0 6px 4px;color:#64748b;font-weight:600;">Δ blend</th><th style="text-align:right;padding:0 0 4px;color:#64748b;font-weight:600;">r</th></tr></thead><tbody>${rows}</tbody></table>`;
        const modelNote = latestCompositeModel && latestCompositeModel.label
            ? `<div style="font-size:0.58rem;color:#94a3b8;margin-top:5px;line-height:1.35;">Current blend: <span style="color:#cbd5e1;">${escTip(String(latestCompositeModel.label))}</span></div>`
            : '';
        return `${cap}${tbl}${modelNote}`;
    }

    /**
     * Hype vs pull: linear trend index per log10(eff. pull + 1) (avoids ln(1+t) squashing high-trend chase cards).
     */
    function hypePullInterestRatio(trendsScore, effectivePull) {
        const t = Number(trendsScore);
        const ep = Number(effectivePull);
        if (!Number.isFinite(t) || t <= 0 || !Number.isFinite(ep) || ep <= 0) return null;
        
        /**
         * Ratio of Trend (Popularity) to Scarcity (Pull Cost).
         * ln(ep + 1) provides a smoother dampening than log10 for mid-market prints.
         */
        const scarcity = Math.log(ep + 1);
        if (scarcity <= 1e-12) return null;
        return t / scarcity;
    }

    /**
     * Ordinal score for aesthetic premiums (SIR > IR > Alt Art > UR > Standard).
     * High-fidelity art carries a market multiplier that pull rates alone don't explain.
     */
    function getArtStyleScore(rarity) {
        if (!rarity || typeof rarity !== 'string') return 1.0;
        const r = rarity.toLowerCase();
        if (r.includes('special illustration') || r.includes('sir')) return 10.0;
        if (r.includes('illustration rare') || r.includes('ir')) return 7.0;
        if (r.includes('rare holo star') || r.includes('gold star')) return 9.0;
        if (r.includes('shining') || r.includes('crystal')) return 8.5;
        if (r.includes('rainbow rare') || r.includes('hyper rare')) return 4.5;
        if (r.includes('ultra rare') || r.includes('full art') || r.includes('secret rare')) return 4.0;
        if (r.includes('shiny ultra rare') || r.includes('shiny rare')) return 3.5;
        if (r.includes('alternate art') || r.includes('alt art')) return 8.0;
        if (r.includes('radiant')) return 1.8;
        if (r.includes('rare holo') || r.includes('vintage_holo')) return 2.5;
        return 1.0;
    }

    /**
     * Ratio of PSA 10s to total PSA population.
     * Captures condition-based scarcity which drives premiums for modern cards.
     */
    function getCardGemMintRate(card) {
        if (!card || !card.gemrate) return 1.0;
        const p10 = card.gemrate.total_gem_mint || card.gemrate.psa_gems || 0;
        const total = card.gemrate.total || 0;
        if (total <= 0) return 1.0;
        return p10 / total;
    }

    /**
     * Grail / Narrative premium for specific iconic print classes from the vintage era.
     * Shining, Gold Star, Crystal, and specific e-card holos.
     */
    function getVintageShowcaseScore(card, setInfo) {
        if (!card) return 0;
        const r = String(card.rarity || '').toLowerCase();
        const name = String(card.name || '').toLowerCase();
        const variant = String(card.variant_primary_label || '').toLowerCase();
        
        let base = 0;
        // Mechanical Grails
        if (r.includes('gold star') || r.includes('rare holo star')) base = 6.2;
        else if (r.includes('shining')) base = 6.4; // Neo icons sit higher than e-card crystals in raw demand
        else if (r.includes('crystal')) base = 5.8;
        else if (r.includes('legend')) base = 4.2;
        
        // E-card holos (Skyridge/Aquapolis/Expedition)
        const sn = String(setInfo.set_name || '').toLowerCase();
        const num = String(card.number || '');
        if ((sn.includes('skyridge') || sn.includes('aquapolis') || sn.includes('expedition')) && num.startsWith('H')) {
            base = Math.max(base, 3.8);
        }

        // 1st Edition Scalar
        let mult = 1.0;
        if (name.includes('1st edition') || variant.includes('1st edition') || variant.includes('1st ed')) {
            mult *= 1.85; 
            if (base === 0) base = 3.0; // Base set / Jungle / Fossil 1st ed holo floor
        }

        const y = parseSetReleaseYear(setInfo.release_date);
        if (y != null && y < 2011 && base > 0) {
            const age = Math.max(0, 2026 - y);
            return base * mult * (1 + age * 0.05);
        }
        return (base * mult) || 0;
    }

    /** Heuristic pull / LSRL weights; same numeric ladder as the collector print-class scatter (`getCollectorPrintClassScore`). */
    function getStaticRarityWeight(rarity) {
        if (!rarity || typeof rarity !== 'string') return 1.0;
        const r = rarity.toLowerCase();
        if (isMegaHyperRare(rarity)) return 2.4;
        if (isGoldSecretBucket(rarity)) return 2.2;
        if (r.includes('special illustration') || r.includes('sir')) return 10.0;
        if (r.includes('illustration rare') || r.includes('ir')) return 3.0;
        if (r.includes('hyper rare') || r.includes('secret rare')) return 4.0;
        if (r.includes('rainbow rare')) return 3.5;
        if (r.includes('ultra rare') || r.includes('full art')) return 1.5;
        if (r.includes('shiny ultra rare') || r.includes('shiny rare')) return 2.0;
        if (r.includes('amazing rare')) return 1.5;
        /* WotC / e-card / EX-era: Dex uses many strings that are not modern SIR/IR/UR; without scores they all collapsed to 1.0 (one vertical column). */
        if (r.includes('holo star') || r.includes('rare holo star')) return 8.2;
        if (/\bshining\b/.test(r)) return 8.5;
        if (r.includes('legend')) return 6.0;
        if (r.includes('rare secret')) return 5.5;
        if (r.includes('crystal')) return 5.0;
        if (r.includes('lv.x') || r.includes('lv x')) return 4.2;
        if (r.includes('rare holo ex') || r.includes('holo ex')) return 3.8;
        if (r.includes('rare prime') || r.includes('rare break')) return 3.0;
        if (r.includes('radiant')) return 1.8;
        if (r.includes('rare holo') || r === 'holo rare') return 2.75;
        return 1.0;
    }

    function getEffectiveRarityWeight(rarity) {
        const t = getRarityTierKey(rarity);
        const c = latestRarityTierCalibration[t];
        const mult = c != null && Number.isFinite(c) && c > 0 ? c : 1;
        return getStaticRarityWeight(rarity) * mult;
    }

    function ensureScatterPointStyleArrays(dataset, dataIndex) {
        while (dataset.pointStyle.length <= dataIndex) dataset.pointStyle.push('circle');
        if (!Array.isArray(dataset.pointBackgroundColor)) dataset.pointBackgroundColor = [];
        if (!Array.isArray(dataset.pointBorderColor)) dataset.pointBorderColor = [];
        while (dataset.pointBackgroundColor.length <= dataIndex) {
            dataset.pointBackgroundColor.push(SCATTER_POINT_BG);
        }
        while (dataset.pointBorderColor.length <= dataIndex) {
            dataset.pointBorderColor.push(SCATTER_POINT_BORDER);
        }
    }

    /**
     * Tinted Chart.js circle (pointBackgroundColor) until `startScatterPointThumbLoad` swaps in a canvas thumb.
     */
    function applyThumbnailStyle(dataset, card, ringColor, _chartId) {
        const dataIndex = dataset.data.length - 1;
        ensureScatterPointStyleArrays(dataset, dataIndex);
        dataset.pointStyle[dataIndex] = 'circle';
        if (!card || !card.image_url) {
            dataset.pointBackgroundColor[dataIndex] = SCATTER_POINT_BG;
            dataset.pointBorderColor[dataIndex] = SCATTER_POINT_BORDER;
            return;
        }
        const url = card.image_url;
        const rawTint = thumbCenterColorByUrl.get(url);
        const fill = rawTint ? scatterPointFillFromHex(rawTint) : urlHashPlaceholderColor(url);
        dataset.pointBackgroundColor[dataIndex] = fill;
        dataset.pointBorderColor[dataIndex] = ringColor;
    }

    function createDatasets(setInfo) {
        const packPriceDetail = getPackPriceMedianDetail(setInfo);
        const packPrice = packPriceDetail.median;

        const scatterDs = (extra = {}) => ({
            label: setInfo.set_name,
            data: [],
            backgroundColor: SCATTER_POINT_BG,
            borderColor: SCATTER_POINT_BORDER,
            borderWidth: 2,
            pointRadius: SCATTER_POINT_RADIUS,
            pointHoverRadius: SCATTER_POINT_HOVER_RADIUS,
            pointStyle: [],
            pointBackgroundColor: [],
            pointBorderColor: [],
            parsing: false,
            clip: true,
            ...extra
        });

        const baseDatasets = {
            pullCost: scatterDs(),
            composite: scatterDs(),
            hypePullRatio: scatterDs(),
            rarityTier: scatterDs(),
            setVintage: scatterDs(),
            artistChase: scatterDs(),
            /** Two layers per set: Pokémon vs trainer/human lines (Is_Human in character JSON). */
            character: [
                scatterDs({
                    label: 'Pokémon',
                    backgroundColor: SCATTER_POINT_BG,
                    borderColor: SCATTER_POINT_BORDER
                }),
                scatterDs({
                    label: 'Trainers',
                    backgroundColor: SCATTER_POINT_BG,
                    borderColor: SCATTER_POINT_BORDER
                })
            ],
            trends: scatterDs(),
            hypeScarcity: scatterDs(),
            tcgMacroInterest: scatterDs(),
            gradedPop: scatterDs()
        };

        if (!setInfo.top_25_cards) return baseDatasets;

        const rows = [];
        setInfo.top_25_cards.forEach((card) => {
            if (isExcludedLegacyBaseCharizardOutlier(setInfo, card)) return;
            const marketPrice = resolveChartMarketUsd(card);
            if (!isChartableMarketPrice(marketPrice)) return;

            const pull = computeEffectivePullCost(card, setInfo, packPrice);
            const charInfo = getCharacterPremiumInfo(card.name);
            const td = getTrendsScoreDetail(card.name);
            const trendsPopScaled = td.avg;
            const trendsFileAvg = td.filePopularityAvg;
            const trendsMatchCount = td.matchCount;
            const trendOnlyAvg = td.trendOnlyAvg;
            const surveyMeanAvg = td.surveyMeanAvg;
            const surveyPollMax = td.surveyPollMax;
            const sparklezVotesAvg = td.sparklezVotesAvg;
            const sparklezSyntheticAvg = td.sparklezSyntheticAvg;
            const gradedSpeciesNorm = td.gradedSpeciesNorm;
            const gradedPopBlendScale = td.gradedPopBlendScale;
            const trendsHasGlobalGraded = td.hasGlobalGraded;
            const effectivePull = pull.value;
            const logPremium = effectivePull > 0 ? Math.log(Math.max(marketPrice / Math.max(effectivePull, 1e-9), 1e-9)) : null;
            const logCharVol = charInfo.volume > 0 ? Math.log1p(charInfo.volume) : null;
            const trends = trendsPopScaled > 0 ? trendsPopScaled : null;
            const hype = effectivePull > 0 && trendsPopScaled > 0 ? trendsPopScaled * Math.log10(effectivePull + 1) : null;
            const logGradedPop = getCardGradedPopLog10X(card);
            const artScore = getArtStyleScore(card.rarity);
            const logArtStyle = Math.log1p(artScore);
            const logGemMintRate = Math.log1p(getCardGemMintRate(card) * 10);
            let nostalgiaScore = 0;
            charInfo.speciesKeys.forEach(sk => {
                if (nostalgiaData[sk]) nostalgiaScore = Math.max(nostalgiaScore, nostalgiaData[sk].nostalgia_score);
            });
            const nostalgiaScalar = Math.max(0.12, artScore / 10.0);
            const logNostalgia = nostalgiaScore > 0 ? Math.log10(1 + nostalgiaScore) * nostalgiaScalar : null;
            const trainerArchetype = charInfo.archetype === 'Waifu' ? 2.5 : (charInfo.archetype === 'Utility' ? 1.4 : 1.0);

            const iconicMap = { 
                'charizard': 2.2, 'pikachu': 1.8, 'mewtwo': 1.7, 'mew': 1.6, 'celebi': 1.5,
                'lugia': 1.45, 'rayquaza': 1.45, 'umbreon': 1.35, 'gengar': 1.3,
                'suicune': 1.25, 'raikou': 1.2, 'entei': 1.2, 'blastoise': 1.2, 'venusaur': 1.2,
                'lucario': 1.1, 'eevee': 1.1, 'darkrai': 1.0, 'arceus': 1.0
            };
            let iconicV = 0;
            charInfo.speciesKeys.forEach(sk => {
                const lowSk = String(sk).toLowerCase();
                if (iconicMap[lowSk]) iconicV = Math.max(iconicV, iconicMap[lowSk]);
            });
            const logIconicPremium = iconicV > 0 ? Math.log1p(iconicV) : null;

            const logVintageShowcase = getVintageShowcaseScore(card, setInfo) || 0;
            const logFixedProductCorrection = usesBoosterStyleCardPullOdds(setInfo, card) ? 0 : (iconicV > 1.2 ? -0.7 : -1.5);
            const logSpeciesUtility = charInfo.speciesKeys.length > 0 ? 0 : -1.2;

            const artChase = lookupArtistChase(card.artist);
            const logArtistChase = artChase != null ? Math.log(1 + artChase.median) : null;

            const histMed = wizardHistoryPositiveUsdMedian(card);
            let logPriceMomentum = (histMed != null && histMed > 0) ? Math.log(marketPrice / histMed) : null; // Use null if no history
            if (logPriceMomentum != null) {
                logPriceMomentum = Math.min(1.8, Math.max(-1.8, logPriceMomentum));
            }

            const setSize = (setInfo.top_25_cards || []).length;
            const rarityCount = getRarityCohortCount(setInfo, card.rarity);
            const logSetSaturation = Math.log1p(setSize / Math.max(1, rarityCount)) * 0.5;

            rows.push({
                marketPrice,
                card,
                effectivePull,
                pullFromCard: pull.fromCard,
                pullFromSlot: pull.fromSlot,
                trendsFileAvg,
                trendsPopScaled,
                trendsMatchCount,
                trendOnlyAvg,
                surveyMeanAvg,
                surveyPollMax,
                sparklezVotesAvg,
                sparklezSyntheticAvg,
                gradedSpeciesNorm,
                gradedPopBlendScale,
                trendsHasGlobalGraded,
                charInfo,
                logPremium,
                logCharVol,
                trends,
                hype,
                logGradedPop,
                logArtStyle,
                logNostalgia,
                logGemMintRate,
                trainerArchetype,
                logVintageShowcase,
                logFixedProductCorrection,
                logSpeciesUtility,
                logArtistChase,
                logIconicPremium,
                logPriceMomentum,
                logSetSaturation
            });
        });

        const posTrends = rows.map((r) => r.trendsPopScaled).filter((t) => t > 0);
        let trendFloor = 0.35;
        if (posTrends.length) {
            const med = medianArray(posTrends);
            trendFloor = Math.max(0.25, med * 0.08);
        }

        const setLogGradedForMedian = [];
        rows.forEach((r) => {
            if (r.logGradedPop != null && Number.isFinite(r.logGradedPop)) {
                setLogGradedForMedian.push(r.logGradedPop);
            }
        });
        const setMedianLogGradedPop = setLogGradedForMedian.length ? medianArray(setLogGradedForMedian) : null;

        rows.forEach((row) => {
            const {
                marketPrice, card, effectivePull, pullFromCard, pullFromSlot,
                trendsFileAvg, trendsPopScaled, trendsMatchCount, trendOnlyAvg, surveyMeanAvg, surveyPollMax,
                sparklezVotesAvg, sparklezSyntheticAvg,
                gradedSpeciesNorm, gradedPopBlendScale, trendsHasGlobalGraded,
                charInfo,
                logPremium, logCharVol, trends, hype, logGradedPop,
                logArtStyle, logNostalgia, logGemMintRate, trainerArchetype,
                logVintageShowcase, logFixedProductCorrection, logSpeciesUtility
            } = row;

            const gbScale = gradedPopBlendScale != null && Number.isFinite(gradedPopBlendScale) ? gradedPopBlendScale : 1;
            const trendsChart = trendsPopScaled > 0
                ? trendsPopScaled
                : (trendsMatchCount === 0 ? trendFloor * tinyTrendSpreadJitter(card.name) * gbScale : null);
            const trendsDisplayImputed = trendsMatchCount === 0 && trendsFileAvg <= 0 && trendsChart != null;
            const trendsScatterX = popularityScatterXWithSlabVsSet(trendsChart, logGradedPop, setMedianLogGradedPop);

            const tcgScatterMeta = {
                analyticsSetCode: String(setInfo.set_code || '').trim(),
                priceAsOf: setInfo.tcgtracking_price_updated != null && String(setInfo.tcgtracking_price_updated).trim() !== ''
                    ? String(setInfo.tcgtracking_price_updated)
                    : null,
                gemrateUniversalCount: setInfo.gemrate_set_total || 0,
                boosterCardPullBlend: usesBoosterStyleCardPullOdds(setInfo, card)
            };

            if (effectivePull > 0) {
                const pullRegW = nonBoosterPullRegressionWeightMult(setInfo, card);
                const weight = getEffectiveRarityWeight(card.rarity) * pullRegW;
                baseDatasets.pullCost.data.push({
                    x: marketPrice,
                    y: logPremium,
                    card,
                    weight,
                    effectivePull,
                    logPremium,
                    pullFromCard,
                    pullFromSlot,
                    analyticsSetName: setInfo.set_name,
                    packPriceDetail,
                    ...tcgScatterMeta
                });
                applyThumbnailStyle(baseDatasets.pullCost, card, SCATTER_THUMB_RING, 'pullCost');
            }

            if (latestCompositeModel) {
                const logPremiumComposite = logPremiumForCompositeScore(logPremium, setInfo, card, latestCompositeModel);
                let hypeBlendVal = hype;
                if (latestCompositeModel.trainFeatHypeUsesOrth
                    && latestCompositeModel.hypeOrthCoeffs
                    && hype != null && Number.isFinite(hype)
                    && trends != null && Number.isFinite(trends)) {
                    const c = latestCompositeModel.hypeOrthCoeffs;
                    hypeBlendVal = hype - (c.b0 + c.b1 * trends);
                }
                const feat = { 
                    logPremium: logPremiumComposite, 
                    logCharVol, 
                    trends, 
                    hype: hypeBlendVal, 
                    logGradedPop,
                    logArtStyle,
                    logNostalgia,
                    logGemMintRate,
                    trainerArchetype,
                    logVintageShowcase,
                    logFixedProductCorrection,
                    logSpeciesUtility
                };
                const cx = compositeScoreFromRow(feat, latestCompositeModel);
                if (cx != null && Number.isFinite(cx)) {
                    const w0 = getEffectiveRarityWeight(card.rarity);
                    const w = w0 * listingLiquidityFactor(card) * nonBoosterPullRegressionWeightMult(setInfo, card);
                    const breakdown = compositeBreakdownFromRow(feat, latestCompositeModel);
                    const mlp = latestCompositeModel.means && latestCompositeModel.means.logPremium;
                    const lpImputed = !usesBoosterStyleCardPullOdds(setInfo, card) && Number.isFinite(mlp);
                    baseDatasets.composite.data.push({
                        x: cx,
                        y: marketPrice,
                        card,
                        weight: w,
                        compositeFeatures: {
                            logPremium: logPremiumComposite,
                            logPremiumPullChart: logPremium,
                            logPremiumImputedMean: lpImputed,
                            logCharVol,
                            trends,
                            hype: hypeBlendVal,
                            logGradedPop,
                            logArtStyle,
                            logNostalgia,
                            logGemMintRate,
                            trainerArchetype,
                            logVintageShowcase,
                            logFixedProductCorrection,
                            logSpeciesUtility
                        },
                        compositeBreakdown: breakdown,
                        analyticsSetName: setInfo.set_name,
                        ...tcgScatterMeta
                    });
                    applyThumbnailStyle(baseDatasets.composite, card, SCATTER_THUMB_RING, 'composite');
                }
            }

            const printClass = getCollectorPrintClassScore(card.rarity);
            if (printClass != null && Number.isFinite(printClass)) {
                const rtW = getEffectiveRarityWeight(card.rarity) * listingLiquidityFactor(card)
                    * nonBoosterPullRegressionWeightMult(setInfo, card);
                baseDatasets.rarityTier.data.push({
                    x: printClass,
                    y: marketPrice,
                    card,
                    weight: rtW,
                    rarityLabel: card.rarity || '',
                    analyticsSetName: setInfo.set_name,
                    ...tcgScatterMeta
                });
                applyThumbnailStyle(baseDatasets.rarityTier, card, SCATTER_THUMB_RING, 'rarityTier');
            }

            const yearsSinceRelease = parseSetReleaseYearsSince(setInfo.release_date, ANALYTICS_AS_OF_MS);
            const vintageX = setVintageDisplayX(yearsSinceRelease);
            if (yearsSinceRelease != null && Number.isFinite(yearsSinceRelease) && vintageX != null && Number.isFinite(vintageX)) {
                baseDatasets.setVintage.data.push({
                    x: vintageX,
                    y: marketPrice,
                    card,
                    weight: 1.0,
                    setVintageRawYears: yearsSinceRelease,
                    setReleaseDate: setInfo.release_date,
                    setName: setInfo.set_name,
                    analyticsSetName: setInfo.set_name,
                    ...tcgScatterMeta
                });
                applyThumbnailStyle(baseDatasets.setVintage, card, SCATTER_THUMB_RING, 'setVintage');
            }

            const releaseCalYear = parseSetReleaseYear(setInfo.release_date);
            const macroX = lookupTcgMacroInterestForReleaseYear(releaseCalYear);
            if (macroX != null && Number.isFinite(macroX)) {
                baseDatasets.tcgMacroInterest.data.push({
                    x: macroX,
                    y: marketPrice,
                    card,
                    weight: 1.0,
                    tcgMacroReleaseYear: releaseCalYear,
                    tcgMacroSeriesLabel: latestTcgMacroSeriesLabel,
                    analyticsSetName: setInfo.set_name,
                    ...tcgScatterMeta
                });
                applyThumbnailStyle(baseDatasets.tcgMacroInterest, card, SCATTER_THUMB_RING, 'tcgMacroInterest');
            }

            const logGradedPopX = getCardGradedPopLog10X(card);
            if (logGradedPopX != null && Number.isFinite(logGradedPopX)) {
                const gtot = getCardGradedPopTotal(card);
                const gsrc = card.gemrate != null ? 'Gemrate' : 'Sidecar';
                baseDatasets.gradedPop.data.push({
                    x: logGradedPopX,
                    y: marketPrice,
                    card,
                    weight: 1.0,
                    gradedPopTotal: gtot,
                    gradedPopSource: gsrc,
                    analyticsSetName: setInfo.set_name,
                    ...tcgScatterMeta
                });
                applyThumbnailStyle(baseDatasets.gradedPop, card, SCATTER_THUMB_RING, 'gradedPop');
            }

            if (charInfo.volume > 0) {
                const charDsIdx = charInfo.isHuman ? 1 : 0;
                const charDs = baseDatasets.character[charDsIdx];
                const s = latestCharacterVolumeTransform.vScale || 1;
                const volX = latestCharacterVolumeTransform.mode === 'quadratic'
                    ? charInfo.volume + (charInfo.volume * charInfo.volume) / s
                    : charInfo.volume;
                charDs.data.push({
                    x: volX,
                    y: marketPrice,
                    card,
                    weight: 1.0,
                    charVolumeRaw: charInfo.volume,
                    charSpeciesKeys: Array.isArray(charInfo.speciesKeys) ? charInfo.speciesKeys.slice() : [],
                    charLineKind: charInfo.isHuman ? 'trainers' : 'species',
                    analyticsSetName: setInfo.set_name,
                    ...tcgScatterMeta
                });
                applyThumbnailStyle(charDs, card, SCATTER_THUMB_RING, 'character');
            }

            const artistName = card.artist != null ? String(card.artist).trim() : '';
            const artistProf = lookupArtistChase(artistName);
            if (artistProf && Number.isFinite(artistProf.median) && artistProf.median > 0) {
                const xArtist = Math.log1p(artistProf.median);
                baseDatasets.artistChase.data.push({
                    x: xArtist,
                    y: marketPrice,
                    card,
                    weight: 1.0,
                    artistChaseMedian: artistProf.median,
                    artistChaseCount: artistProf.count,
                    artistDisplayName: artistProf.displayArtist,
                    analyticsSetName: setInfo.set_name,
                    ...tcgScatterMeta
                });
                applyThumbnailStyle(baseDatasets.artistChase, card, SCATTER_THUMB_RING, 'artistChase');
            }

            if (trendsScatterX != null) {
                baseDatasets.trends.data.push({
                    x: trendsScatterX,
                    y: marketPrice,
                    card,
                    weight: 1.0,
                    trendsScore: trendsScatterX,
                    trendsPopularityAxisBase: trendsChart,
                    trendsSetMedianLogGradedPop: setMedianLogGradedPop,
                    trendsCardLogGradedPop: logGradedPop,
                    trendsSourceRaw: trendsFileAvg,
                    trendGoogleAvg: trendOnlyAvg,
                    surveyMeanAvg,
                    surveyPollMax,
                    sparklezVotesAvg,
                    sparklezSyntheticAvg,
                    trendsDisplayImputed,
                    trendsGradedSpeciesNorm: gradedSpeciesNorm,
                    trendsGradedBlendScale: gradedPopBlendScale,
                    trendsHasGlobalGraded,
                    analyticsSetName: setInfo.set_name,
                    ...tcgScatterMeta
                });
                applyThumbnailStyle(baseDatasets.trends, card, SCATTER_THUMB_RING, 'trends');
            }

            if (effectivePull > 0 && trendsScatterX != null) {
                const scarcityMetric = hypeScarcityMetricCombined(
                    effectivePull,
                    logGradedPop,
                    setMedianLogGradedPop,
                    scatterModernGradedMedianLogPop
                );
                const hypeScarcityScore = trendsScatterX * scarcityMetric;
                const hypePullRatio = hypePullInterestRatio(trendsScatterX, effectivePull);
                baseDatasets.hypeScarcity.data.push({
                    x: hypeScarcityScore,
                    y: marketPrice,
                    card,
                    weight: nonBoosterPullRegressionWeightMult(setInfo, card),
                    trendsScore: trendsScatterX,
                    trendsPopularityAxisBase: trendsChart,
                    trendsSetMedianLogGradedPop: setMedianLogGradedPop,
                    trendsCardLogGradedPop: logGradedPop,
                    trendsSourceRaw: trendsFileAvg,
                    trendGoogleAvg: trendOnlyAvg,
                    surveyMeanAvg,
                    surveyPollMax,
                    sparklezVotesAvg,
                    sparklezSyntheticAvg,
                    trendsDisplayImputed,
                    trendsGradedSpeciesNorm: gradedSpeciesNorm,
                    trendsGradedBlendScale: gradedPopBlendScale,
                    trendsHasGlobalGraded,
                    effectivePull,
                    hypeScarcityPullLog10: Math.log10(effectivePull + 1),
                    hypeScarcityMetricCombined: scarcityMetric,
                    hypeScarcityEraMedianLogPop: scatterModernGradedMedianLogPop,
                    hypePullRatio,
                    analyticsSetName: setInfo.set_name,
                    ...tcgScatterMeta
                });
                applyThumbnailStyle(baseDatasets.hypeScarcity, card, SCATTER_THUMB_RING, 'hypeScarcity');
                if (hypePullRatio != null && Number.isFinite(hypePullRatio)) {
                    baseDatasets.hypePullRatio.data.push({
                        x: hypePullRatio,
                        y: marketPrice,
                        card,
                        weight: nonBoosterPullRegressionWeightMult(setInfo, card),
                        trendsScore: trendsScatterX,
                        trendsPopularityAxisBase: trendsChart,
                        trendsSetMedianLogGradedPop: setMedianLogGradedPop,
                        trendsCardLogGradedPop: logGradedPop,
                        trendsSourceRaw: trendsFileAvg,
                        trendGoogleAvg: trendOnlyAvg,
                        surveyMeanAvg,
                        surveyPollMax,
                        sparklezVotesAvg,
                        sparklezSyntheticAvg,
                        trendsDisplayImputed,
                        trendsGradedSpeciesNorm: gradedSpeciesNorm,
                        trendsGradedBlendScale: gradedPopBlendScale,
                        trendsHasGlobalGraded,
                        effectivePull,
                        analyticsSetName: setInfo.set_name,
                        ...tcgScatterMeta
                    });
                    applyThumbnailStyle(baseDatasets.hypePullRatio, card, SCATTER_THUMB_RING, 'hypePullRatio');
                }
            }
        });

        return baseDatasets;
    }

    function stripNonLsrlScatter(chart) {
        if (!chart || !chart.data || !Array.isArray(chart.data.datasets)) return;
        for (let i = chart.data.datasets.length - 1; i >= 0; i--) {
            if (!isLsrlDataset(chart.data.datasets[i])) {
                chart.data.datasets.splice(i, 1);
            }
        }
    }

    function sortSecondaryChartsByAbsCorrelation() {
        if (!scatterRegressionRMap) return;
        secondaryChartOrder.sort((a, b) => {
            const ra = scatterRegressionRMap.get(a) || 0;
            const rb = scatterRegressionRMap.get(b) || 0;
            return rb - ra;
        });
    }

    function sortChartDashboardPanels() {
        const sub = document.getElementById('chart-dashboard-sub');
        if (!sub) return;
        secondaryChartOrder.forEach((key) => {
            const panel = sub.querySelector(`[data-chart-key="${key}"]`);
            if (panel) sub.appendChild(panel);
        });
    }

    /** Build LSRL audit HTML off the critical path so first paint stays responsive. */
    function scheduleIdleCompositeLsrlAudit(gen) {
        const run = () => {
            if (gen !== scatterRebuildGen) return;
            renderCompositeLsrlResidualAudit();
        };
        if (typeof window.requestIdleCallback === 'function') {
            window.requestIdleCallback(run, { timeout: 450 });
        } else {
            setTimeout(run, 16);
        }
    }

    /**
     * Precompute weighted LSRL for all charts once, then batch Chart.update across rAF frames
     * (avoids deep recursion from updating every chart in one task).
     */
    function runScatterChartRefreshPass(onDone, gen, onSuperseded) {
        warmScatterRegressionCache();
        const keys = allChartKeys();
        let ki = 0;

        function finishPass() {
            sortSecondaryChartsByAbsCorrelation();
            sortChartDashboardPanels();
            syncCharacterChartXAxisTitle();
            syncSetVintageChartXAxisTitle();
            syncTcgMacroInterestChartXAxisTitle();
            syncArtistChaseChartXAxisTitle();
            rebuildScatterCardSearchIndex();
            refreshAnalyticsCardSearchPanelIfNeeded();
            updateAllFooters();
            renderSetChips();
            updateAnalyticsFilterSummary();
            scheduleIdleCompositeLsrlAudit(gen);
            ensureScatterThumbScrollListener();
            requestAnimationFrame(() => {
                refreshScatterChartThumbVisibility();
            });
            if (typeof onDone === 'function') onDone();
        }

        function step() {
            if (gen !== scatterRebuildGen) {
                if (typeof onSuperseded === 'function') onSuperseded();
                return;
            }
            if (ki >= keys.length) {
                finishPass();
                return;
            }

            let batch = 0;
            while (ki < keys.length && batch < CHART_REFRESH_PER_FRAME) {
                const key = keys[ki++];
                const ch = charts[key];
                if (ch) {
                    refreshLSRL(key);
                    applyScatterAxisFit(key);
                    ch.update('none');
                }
                batch++;
            }
            requestAnimationFrame(step);
        }

        requestAnimationFrame(step);
    }

    function invalidateScatterRegressionCache() {
        scatterRegressionCache = null;
        scatterRegressionCacheGen = -1;
    }

    function warmScatterRegressionCache() {
        scatterRegressionCache = new Map();
        scatterRegressionRMap = new Map(); // New map for precomputed r values
        allChartKeys().forEach((k) => {
            const reg = computeWeightedRegressionForChart(k);
            scatterRegressionCache.set(k, reg);
            scatterRegressionRMap.set(k, reg ? Math.abs(reg.r) : 0);
        });
        scatterRegressionCacheGen = scatterRebuildGen;
    }

    function rebuildAllScatterDatasetsInner(onDone, gen, onSuperseded) {
        clearPinnedScatterCard();
        cancelPendingThumbnailUpdates();
        invalidateScatterRegressionCache();
        const ordered = getEffectiveSetIndicesOrdered();
        allChartKeys().forEach((k) => stripNonLsrlScatter(charts[k]));

        if (ordered.length === 0) {
            scatterModernGradedMedianLogPop = null;
            runScatterChartRefreshPass(onDone, gen, onSuperseded);
            return;
        }

        scatterModernGradedMedianLogPop = buildScatterModernGradedMedianLog(ordered);

        let setIdx = 0;
        const step = () => {
            if (gen !== scatterRebuildGen) {
                if (typeof onSuperseded === 'function') onSuperseded();
                return;
            }
            if (setIdx >= ordered.length) {
                runScatterChartRefreshPass(onDone, gen, onSuperseded);
                return;
            }

            // Process one set per frame to keep UI responsive
            const sIdx = ordered[setIdx++];
            const setInfo = allSetsData[sIdx];
            if (setInfo) {
                const ds = createDatasets(setInfo);
                allChartKeys().forEach((key) => {
                    const payload = ds[key];
                    if (Array.isArray(payload)) {
                        payload.forEach((sub) => addScatterBeforeLsrl(charts[key], sub));
                    } else {
                        addScatterBeforeLsrl(charts[key], payload);
                    }
                });
            }
            requestAnimationFrame(step);
        };
        requestAnimationFrame(step);
    }

    function rebuildAllScatterDatasets(onDone, onSuperseded) {
        const gen = ++scatterRebuildGen;
        assertCompositeDriverScatterCoverage();
        const train = gatherCompositeTrainingRows();
        latestRarityTierCalibration = computeRarityTierCalibrationFactorsJackknife(train);
        train.forEach((r) => {
            const mult = latestRarityTierCalibration[r.tierKey];
            r.weight = r.staticWeight * (mult != null && Number.isFinite(mult) && mult > 0 ? mult : 1);
        });
        latestCompositeModel = buildCompositeModelFromRows(train);
        latestCharacterVolumeTransform = computeCharacterVolumeTransform(getEffectiveSetIndicesOrdered());
        rebuildAllScatterDatasetsInner(onDone, gen, onSuperseded);
    }

    function medianAbsoluteDeviationScale(values) {
        if (!values.length) return 0;
        const med = medianArray(values.slice());
        const dev = values.map((v) => Math.abs(v - med));
        return medianArray(dev);
    }

    function fitWeightedLsrlCore(pts, useLogY) {
        const n = pts.length;
        if (n < 2) return null;

        let sumW = 0;
        let sumWX = 0;
        let sumWY = 0;
        let sumWXY = 0;
        let sumWX2 = 0;
        let sumWY2 = 0;
        pts.forEach((p) => {
            const w = p.weight;
            sumW += w;
            sumWX += w * p.x;
            sumWY += w * p.yFit;
            sumWXY += w * p.x * p.yFit;
            sumWX2 += w * p.x * p.x;
            sumWY2 += w * p.yFit * p.yFit;
        });

        const denominator = (sumW * sumWX2 - sumWX * sumWX);
        if (denominator === 0) return null;

        const m = (sumW * sumWXY - sumWX * sumWY) / denominator;
        const b = (sumWY - m * sumWX) / sumW;

        const rNumerator = (sumW * sumWXY - sumWX * sumWY);
        const rDenominator = Math.sqrt((sumW * sumWX2 - sumWX * sumWX) * (sumW * sumWY2 - sumWY * sumWY));
        const r = rDenominator !== 0 ? rNumerator / rDenominator : 0;
        const r2 = r * r;

        let sumWAbs = 0;
        let sumWE2 = 0;
        pts.forEach((p) => {
            const pred = m * p.x + b;
            const e = p.yFit - pred;
            sumWAbs += p.weight * Math.abs(e);
            sumWE2 += p.weight * e * e;
        });
        const meanAbsResid = sumW > 0 ? sumWAbs / sumW : null;
        const rmseY = sumW > 0 ? Math.sqrt(sumWE2 / sumW) : null;

        return { m, b, r, r2, n, meanAbsResid, rmseY, useLogY };
    }

    function computeWeightedRegressionForChart(chartId) {
        const chart = charts[chartId];
        if (!chart) return null;

        /** Composite + market-$ scatters: fit log₁₀(price) vs X (heavy-tailed $ otherwise deflates |r|). */
        const useLogY = chartId === 'composite' || CHART_LSRL_LOG10_MARKET_PRICE.has(chartId);
        const useLogX = CHART_LSRL_LOG10_X_FIT.has(chartId);

        const pts = [];
        chart.data.datasets.forEach((ds) => {
            if (isLsrlDataset(ds)) return;
            ds.data.forEach((p) => {
                const xDisp = Number(p.x);
                const yRaw = Number(p.y);
                if (!Number.isFinite(xDisp) || !Number.isFinite(yRaw)) return;
                const xFit = lsrlDisplayXToFitX(chartId, xDisp);
                if (!Number.isFinite(xFit)) return;
                const yFit = useLogY
                    ? Math.log10(Math.max(yRaw, MIN_CHART_MARKET_PRICE))
                    : yRaw;
                let w = p.weight != null && Number.isFinite(p.weight) ? p.weight : 1.0;
                if (chartId === 'composite') {
                    const ax = Math.abs(xDisp);
                    const span = 1 - COMPOSITE_LSRL_LOW_X_WEIGHT_FLOOR;
                    w *= COMPOSITE_LSRL_LOW_X_WEIGHT_FLOOR
                        + span * Math.min(1, ax / Math.max(COMPOSITE_LSRL_LOW_X_BLEND_AT_ABS, 1e-9));
                }
                pts.push({
                    x: xFit,
                    yRaw,
                    yFit,
                    weight: w
                });
            });
        });

        const n = pts.length;
        if (n < 2) return null;

        let stats = fitWeightedLsrlCore(pts, useLogY);
        if (!stats) return null;
        stats.useLogX = useLogX;

        if (stats.n >= LSRL_ROBUST_SECOND_PASS_MIN_N) {
            const resids = pts.map((p) => p.yFit - (stats.m * p.x + stats.b));
            const mad = medianAbsoluteDeviationScale(resids);
            const sigma = Math.max(
                useLogY ? 0.035 : Math.max(stats.rmseY || 0, 0) * 0.22,
                1.4826 * mad,
                1e-9
            );
            const mults = resids.map((e) => {
                const t = Math.abs(e) / sigma;
                if (t <= LSRL_HUBER_C) return 1;
                return Math.max(LSRL_HUBER_MIN_WEIGHT_MULT, LSRL_HUBER_C / t);
            });
            const pts2 = pts.map((p, i) => ({
                ...p,
                weight: p.weight * mults[i]
            }));
            const stats2 = fitWeightedLsrlCore(pts2, useLogY);
            if (stats2) {
                stats2.huberReweighted = true;
                stats2.useLogX = useLogX;
                stats = stats2;
            }
        }

        stats.useLogX = useLogX;
        return stats;
    }

    function calculateWeightedRegression(chartId) {
        if (scatterRegressionCache != null
            && scatterRegressionCacheGen === scatterRebuildGen
            && scatterRegressionCache.has(chartId)) {
            return scatterRegressionCache.get(chartId);
        }
        return computeWeightedRegressionForChart(chartId);
    }

    /** Structured LSRL residual for a plotted (x, y) pair on `chartId` (same convention as hover tooltips). */
    function computeScatterLsrlGap(chartId, x, y) {
        const reg = calculateWeightedRegression(chartId);
        if (!reg || reg.n < 2 || !Number.isFinite(x) || !Number.isFinite(y)) return null;
        const yFloor = chartScatterMinUsd();
        const xF = reg.useLogX ? lsrlDisplayXToFitX(chartId, x) : x;
        if (!Number.isFinite(xF)) return null;
        const predLog = reg.m * xF + reg.b;
        if (reg.useLogY) {
            const yLog = Math.log10(Math.max(y, yFloor));
            const dLog = yLog - predLog;
            const predY = Math.pow(10, Math.max(predLog, Math.log10(yFloor)));
            const dLin = y - predY;
            return { useLogY: true, dLog, dLin, dir: dLog >= 0 ? 'above' : 'below' };
        }
        const pred = predLog;
        const dy = y - pred;
        return { useLogY: false, dy, dir: dy >= 0 ? 'above' : 'below' };
    }

    function formatScatterLsrlGapShort(gap) {
        if (!gap) return '—';
        if (gap.useLogY) {
            const f = gap.dLog >= 0 ? `+${gap.dLog.toFixed(3)}` : gap.dLog.toFixed(3);
            return `Δlog₁₀y ${f} (${gap.dir}) · $Δ ${gap.dLin >= 0 ? '+' : ''}${gap.dLin.toFixed(0)}`;
        }
        const fd = gap.dy >= 0 ? `+${gap.dy.toFixed(3)}` : gap.dy.toFixed(3);
        return `Δy ${fd} (${gap.dir})`;
    }

    const CHART_KEY_SEARCH_TITLES = {
        pullCost: 'Pull cost (ln $/pull)',
        composite: 'Composite',
        character: 'Print volume',
        trends: 'Popularity (slab-adjusted index)',
        hypeScarcity: 'Hype × scarcity (pull + slabs)',
        hypePullRatio: 'Hype ÷ pull',
        rarityTier: 'Print class',
        setVintage: 'Set age',
        tcgMacroInterest: 'Macro hobby index',
        artistChase: 'Artist chase',
        gradedPop: 'Graded population'
    };

    function rebuildScatterCardSearchIndex() {
        scatterCardSearchIndex.length = 0;
        allChartKeys().forEach((chartKey) => {
            const chart = charts[chartKey];
            if (!chart || !chart.data || !Array.isArray(chart.data.datasets)) return;
            chart.data.datasets.forEach((ds) => {
                if (isLsrlDataset(ds) || !ds.data) return;
                ds.data.forEach((p) => {
                    if (!p || !p.card) return;
                    scatterCardSearchIndex.push({ chartKey, raw: p });
                });
            });
        });
    }

    function cardMatchesAnalyticsSearchQuery(card, qLo) {
        if (!qLo || !card) return false;
        const nm = String(card.name || '').toLowerCase();
        if (nm.includes(qLo)) return true;
        return cleanCharacterName(card.name).some((id) => String(id).toLowerCase().includes(qLo));
    }

    function searchRankForCard(card, qLo) {
        const nm = String(card.name || '').toLowerCase();
        if (nm === qLo) return 0;
        if (nm.startsWith(qLo)) return 1;
        const ids = cleanCharacterName(card.name);
        if (ids.some((id) => String(id).toLowerCase() === qLo)) return 2;
        if (nm.includes(qLo)) return 3;
        if (ids.some((id) => String(id).toLowerCase().includes(qLo))) return 4;
        return 99;
    }

    function renderAnalyticsCardSearch(query) {
        const out = document.getElementById('analyticsCardSpeciesSearchOut');
        if (!out) return;
        const q = String(query || '').trim().toLowerCase();
        if (!q) {
            out.innerHTML = '';
            out.hidden = true;
            return;
        }
        if (!scatterCardSearchIndex.length) {
            out.innerHTML = '<p class="analytics-card-search-empty">No scatter points yet — select sets and wait for charts to finish.</p>';
            out.hidden = false;
            return;
        }
        const hits = scatterCardSearchIndex.filter((e) => cardMatchesAnalyticsSearchQuery(e.raw.card, q));
        if (!hits.length) {
            out.innerHTML = '<p class="analytics-card-search-empty">No plotted cards match that query (check spelling, year filter, and selected sets).</p>';
            out.hidden = false;
            return;
        }
        const groups = new Map();
        hits.forEach((e) => {
            const setNm = String(e.raw.analyticsSetName || '');
            const cname = String((e.raw.card && e.raw.card.name) || '');
            const k = `${setNm}|||${cname}`;
            if (!groups.has(k)) {
                groups.set(k, {
                    setNm,
                    card: e.raw.card,
                    rank: searchRankForCard(e.raw.card, q),
                    rows: []
                });
            }
            groups.get(k).rows.push(e);
        });
        const list = [...groups.values()].sort((a, b) => {
            if (a.rank !== b.rank) return a.rank - b.rank;
            return String(a.card.name || '').localeCompare(String(b.card.name || ''));
        }).slice(0, 12);

        const esc = (s) => escapeLoadingHtml(String(s ?? ''));
        let html = `<p class="analytics-card-search-hint">${hits.length} point(s) on charts — <strong>${list.length}</strong> card(s) in selected sample (cap 12).</p>`;
        list.forEach((g) => {
            const rows = [...g.rows].sort((a, b) => String(a.chartKey).localeCompare(String(b.chartKey)));
            const sub = rows.map((e) => {
                const title = CHART_KEY_SEARCH_TITLES[e.chartKey] || e.chartKey;
                const xt = Number.isFinite(Number(e.raw.x)) ? Number(e.raw.x).toFixed(3) : '—';
                const yt = Number.isFinite(Number(e.raw.y)) ? Number(e.raw.y).toFixed(2) : '—';
                const gx = computeScatterLsrlGap(e.chartKey, Number(e.raw.x), Number(e.raw.y));
                const gapS = formatScatterLsrlGapShort(gx);
                
                // Clicking the row should open the card preview
                return `<tr class="analytics-card-search-row" onclick="window.showCardFromAnalyticsSearch('${esc(g.card.name)}', '${esc(g.setNm)}')">
                    <td>${esc(title)}</td>
                    <td class="analytics-card-search-num">${xt}</td>
                    <td class="analytics-card-search-num">${yt}</td>
                    <td class="analytics-card-search-gap">${esc(gapS)}</td>
                </tr>`;
            }).join('');
            html += `<div class="analytics-card-search-group">
                <div class="analytics-card-search-group-h" onclick="window.showCardFromAnalyticsSearch('${esc(g.card.name)}', '${esc(g.setNm)}')">
                    <strong>${esc(g.card.name)}</strong> · ${esc(g.setNm)}
                </div>
                <table class="analytics-card-search-table">
                    <thead><tr><th>Chart</th><th>X</th><th>Y</th><th>vs LSRL</th></tr></thead>
                    <tbody>${sub}</tbody>
                </table>
            </div>`;
        });
        out.innerHTML = html;
        out.hidden = false;
    }

    function refreshAnalyticsCardSearchPanelIfNeeded() {
        const inp = document.getElementById('analyticsCardSpeciesSearch');
        if (!inp || !String(inp.value || '').trim()) return;
        renderAnalyticsCardSearch(inp.value);
    }

    function wireAnalyticsCardSearch() {
        const inp = document.getElementById('analyticsCardSpeciesSearch');
        if (!inp || inp.dataset.wired === '1') return;
        inp.dataset.wired = '1';
        let t = null;
        inp.addEventListener('input', () => {
            clearTimeout(t);
            t = setTimeout(() => renderAnalyticsCardSearch(inp.value), 200);
        });
    }

    function escapeAuditHtml(str) {
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    function isVintageTierEraSetName(setName) {
        const s = String(setName || '').toLowerCase();
        if (/\b(skyridge|aquapolis|neo destiny|neo revelation|neo genesis)\b/.test(s)) return true;
        if (/\blegendary collection\b/.test(s)) return true;
        if (/\bpop series\b/.test(s)) return true;
        const t = String(setName || '').trim();
        if (/^base$/i.test(t)) return true;
        if (/\bbase set\b/i.test(s)) return true;
        return false;
    }

    function isCollectorShowcaseCard(card) {
        if (!card) return false;
        const n = String(card.name || '').toLowerCase();
        const r = String(card.rarity || '').toLowerCase();
        if (/\bshining\b/.test(n) || r.includes('shining')) return true;
        if (n.includes('☆')) return true;
        if (r.includes('gold star') || r.includes('holo star')) return true;
        if (r.includes('legend') && !r.includes('vmax') && !r.includes('v-star')) return true;
        return false;
    }

    /**
     * Footnotes for the composite LSRL audit row. Pass `audit` with dLog, price, blendX for “distance” narratives.
     * Goal: either shrink residuals (other code paths) or explain why the line sits far from the point.
     */
    function explainCompositeResidualFlags(raw, card, audit) {
        const bits = [];
        const a = audit || {};
        const dLog = Number.isFinite(Number(a.dLog)) ? Number(a.dLog) : null;
        const price = Number.isFinite(Number(a.price)) ? Number(a.price) : null;
        const blendX = Number.isFinite(Number(a.blendX))
            ? Number(a.blendX)
            : (raw && Number.isFinite(Number(raw.x)) ? Number(raw.x) : null);
        const setNm = raw && raw.analyticsSetName ? String(raw.analyticsSetName) : '';

        const x = raw && Number.isFinite(Number(raw.x)) ? Number(raw.x) : null;
        if (x != null && Math.abs(x) < 0.08) {
            bits.push('composite X≈0 (many POP/promo rows land here after ln(p/u) neutralization—chase and bulk share one log-price LSRL slice)');
        }
        if (raw && raw.boosterCardPullBlend === false) bits.push('fixed-product pull');
        if (raw && raw.compositeFeatures && raw.compositeFeatures.logPremiumImputedMean) bits.push('ln(p/u) neutralized in blend');
        if (card) {
            const td = getTrendsScoreDetail(card.name);
            if (td.matchCount === 0) bits.push('Popularity file: no species token match');
            const rawLiq = cardEbayLiquidityCountForWeights(card);
            if (rawLiq != null && Number.isFinite(rawLiq) && rawLiq > 0 && rawLiq <= 5) {
                const src = usesCollectricsEbayLiquidity(card) ? 'Collectrics eBay' : 'NM/EN TCG';
                bits.push(`thin ${src} liquidity (${rawLiq})`);
            }
        }

        const narrative = [];
        if (dLog != null && dLog > 1.05 && price != null && price >= 600) {
            narrative.push('Very large positive log residual: grail / narrative $ tail—auction, slab, and icon demand often sit above what pull×Trends×volume×graded alone span');
        }
        if (blendX != null && blendX < -0.42 && price != null && price >= 350) {
            narrative.push('Strong negative blend X with high $: index is pull/volume/trends-weighted; this named holo still prices as a chase SKU in a thin list');
        }
        if (setNm && isVintageTierEraSetName(setNm)) {
            narrative.push('Vintage or e-card / POP-era set: holo dispersion is wide—one pooled LSRL will miss some icon listings');
        }
        if (card && isCollectorShowcaseCard(card)) {
            narrative.push('Collector showcase print (Shining / ☆ / Gold-style / LEGEND): tier and story often outrun the mid-market geometry of the blend');
        }

        const merged = bits.concat(narrative);
        return merged.length ? merged.join(' · ') : 'no strong audit flags';
    }

    function renderCompositeLsrlResidualAudit() {
        const list = document.getElementById('analyticsLsrlOutlierList');
        if (!list) return;
        const chart = charts.composite;
        const reg = calculateWeightedRegression('composite');
        if (!chart || !reg || reg.n < 2) {
            list.innerHTML = '<li class="analytics-lsrl-outlier-empty">Need at least two composite points (check year filter and selected sets).</li>';
            return;
        }
        const rows = [];
        chart.data.datasets.forEach((ds) => {
            if (isLsrlDataset(ds) || !ds.data) return;
            ds.data.forEach((p) => {
                if (!p || !p.card) return;
                const x = Number(p.x);
                const y = Number(p.y);
                if (!Number.isFinite(x) || !Number.isFinite(y)) return;
                const predLog = reg.m * x + reg.b;
                const yLog = Math.log10(Math.max(y, MIN_CHART_MARKET_PRICE));
                const dLog = yLog - predLog;
                const predY = Math.pow(10, Math.max(predLog, Math.log10(MIN_CHART_MARKET_PRICE)));
                const dLin = y - predY;
                rows.push({
                    absDLog: Math.abs(dLog),
                    setName: p.analyticsSetName || '',
                    card: p.card,
                    raw: p,
                    dLog,
                    dLin
                });
            });
        });
        rows.sort((a, b) => {
            if (b.absDLog !== a.absDLog) return b.absDLog - a.absDLog;
            const na = (a.card && a.card.name) || '';
            const nb = (b.card && b.card.name) || '';
            const c = na.localeCompare(nb);
            if (c !== 0) return c;
            return String(a.setName || '').localeCompare(String(b.setName || ''));
        });
        const top = rows.slice(0, 28);
        list.innerHTML = top.map((r) => {
            const nm = escapeAuditHtml(r.card.name || 'Card');
            const sn = escapeAuditHtml(r.setName);
            const above = r.dLog >= 0 ? 'above' : 'below';
            const mp = resolveChartMarketUsd(r.card);
            const priceStr = Number.isFinite(mp) ? mp.toFixed(2) : escapeAuditHtml(String(r.card.market_price ?? '—'));
            const xStr = r.raw && Number.isFinite(Number(r.raw.x)) ? Number(r.raw.x).toFixed(3) : '—';
            const mpAudit = resolveChartMarketUsd(r.card);
            const bxAudit = r.raw && Number.isFinite(Number(r.raw.x)) ? Number(r.raw.x) : null;
            const why = escapeAuditHtml(explainCompositeResidualFlags(r.raw, r.card, {
                dLog: r.dLog,
                price: mpAudit,
                blendX: bxAudit
            }));
            const sgn = (v) => (v >= 0 ? '+' : '');
            return `<li class="analytics-lsrl-outlier-li"><span class="analytics-lsrl-outlier-line1"><strong>${nm}</strong> · ${sn} · $${priceStr} · blend X ${xStr}</span><span class="analytics-lsrl-outlier-line2">Δlog₁₀y ${sgn(r.dLog)}${r.dLog.toFixed(3)} (${above} line) · $Δ ${sgn(r.dLin)}${r.dLin.toFixed(0)}</span><span class="analytics-lsrl-outlier-line3">${why}</span></li>`;
        }).join('');
    }

    function setFooterField(root, field, text) {
        const el = root.querySelector(`[data-field="${field}"]`);
        if (el) el.textContent = text;
    }

    function setFooterHtml(root, field, html) {
        const el = root.querySelector(`[data-field="${field}"]`);
        if (el) el.innerHTML = html;
    }

    function scatterPointCount(chart) {
        if (!chart) return 0;
        return chart.data.datasets.reduce((a, ds) => a + (isLsrlDataset(ds) ? 0 : ds.data.length), 0);
    }

    function updateFooter(chartId) {
        const root = document.querySelector(`[data-stats-for="${chartId}"]`);
        if (!root) return;

        const chart = charts[chartId];
        const stats = chart ? calculateWeightedRegression(chartId) : null;
        const nPts = scatterPointCount(chart);

        if (!stats) {
            setFooterField(root, 'eq', '—');
            setFooterField(root, 'r', '—');
            setFooterField(root, 'r2', '—');
            setFooterField(root, 'm', '—');
            setFooterField(root, 'n', String(nPts));
            setFooterHtml(root, 'insight', nPts < 2
                ? 'Need at least two points (and spread on x) to fit a regression line.'
                : 'Not enough variation on x for a stable weighted fit.');
            return;
        }

        if (stats.useLogY) {
            if (stats.useLogX) {
                setFooterField(root, 'eq', `log₁₀(y) = ${stats.m.toFixed(4)}·log₁₀(x′) ${stats.b >= 0 ? '+' : '-'} ${Math.abs(stats.b).toFixed(3)} (x′=max(x,ε); y = chart $)`);
            } else {
                setFooterField(root, 'eq', `log₁₀(y) = ${stats.m.toFixed(4)}x ${stats.b >= 0 ? '+' : '-'} ${Math.abs(stats.b).toFixed(3)} (y = market $)`);
            }
        } else {
            setFooterField(root, 'eq', `y = ${stats.m.toFixed(4)}x ${stats.b >= 0 ? '+' : '-'} ${Math.abs(stats.b).toFixed(2)}`);
        }
        setFooterField(root, 'r', stats.r.toFixed(4));
        setFooterField(root, 'r2', stats.r2.toFixed(4));
        setFooterField(root, 'm', stats.m.toFixed(4));
        setFooterField(root, 'n', String(stats.n));

        const absR = Math.abs(stats.r);
        let strength;
        if (absR >= 0.7) strength = 'strong';
        else if (absR >= 0.4) strength = 'moderate';
        else if (absR >= 0.2) strength = 'weak';
        else strength = 'negligible';

        const lines = {
            strong: 'This indicates a <strong>strong</strong> linear relationship in the weighted model.',
            moderate: 'This indicates a <strong>moderate</strong> linear relationship between the axes in this sample.',
            weak: 'This indicates a <strong>weak</strong> linear relationship; other factors likely dominate.',
            negligible: 'This indicates a <strong>negligible</strong> linear relationship for this sample.'
        };
        let extra = '';
        if (chartId === 'pullCost') {
            extra = '<div class="stats-footnote">Y is <strong>ln(market / effective pull)</strong>. Pull blends card odds with slot odds divided by <code>rarity_counts</code> cohort K; mega and gold-style rarities get extra shrink.</div>';
            extra += '<div class="stats-footnote"><strong>POP / McDonald\'s / Trainer Kit</strong> and scraped <strong>Promo</strong> rows: Dex per-card "1 in N packs" is treated as <strong>non-booster</strong> (slot odds only), with <strong>LSRL weight &times;0.4</strong> so a few fixed-pack SKUs do not lever the line; composite and hype-vs-pull charts use the same rule.</div>';
            extra += '<div class="stats-footnote"><strong>$/pack</strong> uses the <strong>median</strong> of every valid signal (TCG sealed box ÷ packs, Dex box EV ÷ packs, TCGPlayer pack)—hover shows the breakdown. <strong>Card chart $</strong> uses a <strong>median</strong> across List, Dex, TCGTracking, optional tcgapi, Pokémon Wizard current, and the median of Wizard positive history when several anchors disagree (dampens thin-list spikes).</div>';
        } else if (chartId === 'composite') {
            const desc = latestCompositeModel && latestCompositeModel.label
                ? latestCompositeModel.label
                : 'Blend unavailable until enough points with valid drivers.';
            extra = `<div class="stats-footnote">X is a z-scored mix of drivers weighted by marginal Pearson <em>r</em> vs <strong>${latestCompositeModel && latestCompositeModel.driverScreenYBasis === 'log' ? 'log₁₀(price)' : 'linear price'}</strong> (|r| ≥ ${COMPOSITE_MODERATE_ABS_R} when possible; else strongest with |r| ≥ ${COMPOSITE_FALLBACK_ABS_R}). Current drivers: <strong>${desc}</strong></div>`;
            extra += '<div class="stats-footnote">Pin a point (click) for the full per-driver table and an Explorer deep link. LSRL shows <strong>association</strong> in this sample, not proof of causation (set mix, rarity, and top-card selection all confound).</div>';
            extra += '<div class="stats-footnote">Driver <em>r</em> and z-scores use <strong>weighted</strong> rows: static rarity priors × tier factors from ln(price) vs ln(pull) residual medians (tiers with enough cards only). With <strong>2+ sets</strong>, tier factors are the <strong>median across leave-one-set-out</strong> calibrations, then renormalized.</div>';
            extra += '<div class="stats-footnote">The <strong>Popularity</strong> driver uses the same <strong>species graded-slab multiplier</strong> as the file row in <code>google_trends_momentum.json</code> (export-wide slab totals per base species). The <strong>Trends scatter</strong> X further nudges each point by this card’s <strong>log₁₀(1+graded)</strong> vs the <strong>set median</strong> among charted rows (composite driver itself is unchanged).</div>';
            extra += '<div class="stats-footnote">Secondary scatter charts (expand the section below the composite) are <strong>re-ordered by |r| vs price</strong> (strongest linear link first) after each data refresh. On <strong>popularity, hype×scarcity, hype÷pull, and character volume</strong>, LSRL uses <strong>log₁₀(x)</strong> internally so headline |r| matches heavy-tailed drivers better; axes stay in natural units.</div>';
            extra += '<div class="stats-footnote"><strong>Y-axis:</strong> market price is <strong>logarithmic</strong>; the dashed LSRL fits <strong>log₁₀(price)</strong> vs composite X, then draws the line in $ so it matches the scale. Hover shows Δlog₁₀y and an approximate $ gap.</div>';
            extra += '<div class="stats-footnote">Scatter <strong>weights</strong> for Double Rare+ use <strong>eBay liquidity</strong> when <code>collectrics_ebay_*</code> fields exist ( √(min(count,1200)/1200) ); otherwise √(min(TCG NM EN listings,25)/25). Data: <code>python scrape/sync_collectrics_data.py</code> or <code>python scrape/merge_collectrics_ebay.py</code>.</div>';
            extra += '<div class="stats-footnote">For <strong>POP / McDonald\'s / Trainer Kit</strong> and scraped <strong>Promo</strong> rows, <strong>ln(price &divide; pull)</strong> is <strong>left out of the composite fit</strong> for that driver (μ, σ, <em>r</em> use booster-style rows only); on the chart those cards use the <strong>pooled booster mean</strong> for that driver so <strong>z &asymp; 0</strong> on ln(p/u). The pull-cost scatter still uses full ln(p/u).</div>';
            extra += '<div class="stats-footnote">Many of those rows then land near <strong>X ≈ 0</strong> on the composite—<strong>Gold Star chase</strong> and <strong>cheap trainers</strong> share the same horizontal slice, so one log-price LSRL cannot match both; big <strong>Δlog₁₀y</strong> there usually means <strong>vertical stacking</strong> at neutral X, not a broken blend.</div>';
            extra += '<div class="stats-footnote">The dashed LSRL applies <strong>extra downweight near X=0</strong> (same stack) and a single <strong>Huber</strong> reweight on log₁₀(price) residuals (MAD-based scale, <em>c</em> = 1.345) so a few vertical outliers move the line less.</div>';
            {
                const rAdj = latestCompositeModel && latestCompositeModel.driverScreenYBasis === 'log'
                    ? 'log₁₀(price) USD'
                    : 'linear USD';
                extra += `<div class="stats-footnote"><strong>Headline |r|</strong> here is weighted Pearson on <strong>log₁₀(price)</strong> vs composite X. The per-driver <strong>r</strong> in the hover table matches the same <strong>${rAdj}</strong> marginal screen used to build this blend.</div>`;
            }
            if (latestCompositeModelPickMeta && latestCompositeModelPickMeta.tag) {
                const m = latestCompositeModelPickMeta;
                const fmt = (x) => (x != null && Number.isFinite(Number(x)) ? Number(x).toFixed(4) : '—');
                extra += `<div class="stats-footnote"><strong>Blend auto-pick</strong> (max training |<em>r</em>| of composite X vs log₁₀ price): <strong>${escapeLoadingHtml(m.tag)}</strong> · |r|=${fmt(m.absRLogPriceVsX)}. Alternatives — linear-price screen: ${fmt(m.absR_eval_linear)} · log₁₀-price screen: ${fmt(m.absR_eval_log)}${m.absR_eval_hypeOrth != null ? ` · log₁₀ + hype⊥trends: ${fmt(m.absR_eval_hypeOrth)}` : ''}.</div>`;
            }
        } else if (chartId === 'character') {
            extra = '<div class="stats-footnote">X is high-tier print volume';
            if (latestCharacterVolumeTransform.mode === 'quadratic') {
                extra += ` as <strong>v + v²/s</strong> (s ≈ ${latestCharacterVolumeTransform.vScale.toFixed(0)}) when that form improves |r| vs log(price) on the pooled sample (≥${CHARACTER_QUADRATIC_MIN_N} points).`;
            } else {
                extra += ' on a <strong>linear</strong> axis';
            }
            extra += '. Card names are mapped to <strong>base species keys</strong> (e.g. Mega Gengar → Gengar) before joining the character file so form variants share the same print-count signal.</div>';
            extra += '<div class="stats-footnote"><strong>Pokémon</strong> vs <strong>Trainers</strong> are split by <code>Is_Human</code> in the character file (token-majority rule when names map to multiple rows).</div>';
        } else if (chartId === 'trends') {
            extra = '<div class="stats-footnote"><strong>Y</strong> is <strong>chart market price (USD)</strong>: median blend across list / Dex / Track / optional anchors (same resolver as the Explorer). <strong>X</strong> is the <strong>popularity index</strong> — per species, <code>Popularity_Index</code> blends Google <strong>Trends (1y-style)</strong> with the mean of (a) <strong>multi-poll adjusted ranks</strong> and (b) a <strong>Sparklez 2025</strong> vote-rank synthetic score when both exist (see <code>species_popularity_build_meta.json</code> for URLs and weights). Averaged across <strong>base species</strong> tokens (same Mega / regional stripping as the character chart, plus <strong>Shining / Crystal / Dark / Light / Baby</strong> prefixes). When a species is missing from <code>google_trends_momentum.json</code>, the app uses <code>species_popularity_list.json</code> (same build script) if that row has a positive index; otherwise a small <strong>imputed</strong> X from the set’s positive median — hover marks imputed points.</div>';
            extra += `<div class="stats-footnote">The index then applies a <strong>mild export-wide graded nudge per base species</strong> (slab totals summed per species in the export), then a <strong>per-card slab vs set-median</strong> stretch: <strong>× exp(${POPULARITY_SCATTER_SLAB_VS_SET_BLEND}×clamp(Δ log₁₀(1+graded), ±1.25))</strong> where Δ is this card minus the median log₁₀(1+graded) among charted rows in <strong>this set</strong> (when enough cards carry pop). That reads <strong>high search plus heavy grading</strong> as stronger on-card popularity than search alone.</div>`;
            extra += `<div class="stats-footnote">Earlier step: file avg × <strong>(1 + ${POPULARITY_GRADED_TO_AXIS_BLEND}×(stretch−1))</strong> from species share in <strong>[${POPULARITY_SPECIES_GRADED_FLOOR}, 1]</strong> (same as composite popularity input).</div>`;
            extra += '<div class="stats-footnote">The <strong>composite</strong> blend uses matched popularity from <code>google_trends_momentum.json</code> with the same <code>species_popularity_list.json</code> fallback as the Trends chart (no set-median imputation). Rebuild: <code>python scrape/build_species_popularity_index.py</code>; optional Trends refresh: <code>python scrape/fetch_google_trends_batch.py</code>.</div>';
        } else if (chartId === 'hypeScarcity') {
            extra = `<div class="stats-footnote"><strong>Y</strong> is <strong>chart market price (USD)</strong>. <strong>X</strong> is <strong>(Trends-chart popularity X)</strong> × <strong>S</strong>, where <strong>S = log₁₀(eff. pull+1)</strong> plus <strong>${HYPE_SCARCITY_SLAB_VS_SET_WEIGHT}×</strong>clamp(set median log₁₀(1+graded) − this card, …) when both exist, and <strong>${HYPE_SCARCITY_SLAB_VS_MODERN_ERA_WEIGHT}×</strong>clamp(<em>modern-era</em> median log₁₀(1+graded) − this card, …). The modern-era pool is chartable-$ top-list cards from sets with <strong>release year ≥ 2015</strong> or <strong>≤12 years</strong> since release in the <strong>current selection</strong> — a coarse proxy for high-volume English waves vs. older lines. Fewer slabs than that reference reads as <strong>extra scarcity</strong> alongside hard pulls.</div>`;
            extra += '<div class="stats-footnote">Popularity on this panel matches the <strong>Trends scatter</strong> X (file + species graded + per-card slab vs set median). Composite <code>hype</code> still uses file-matched popularity × log₁₀(pull+1) on training rows.</div>';
            extra += '<div class="stats-footnote"><strong>Hype ÷ pull gap</strong> chart uses the same blended popularity in the numerator; denominator is ln(eff. pull+1) per <code>hypePullInterestRatio</code>.</div>';
        } else if (chartId === 'hypePullRatio') {
            extra = '<div class="stats-footnote"><strong>Y</strong> is <strong>chart market price (USD)</strong>. <strong>X</strong> is <strong>blended popularity</strong> (same numerator as the Trends scatter) ÷ ln(effective pull + 1) via <code>hypePullInterestRatio</code>. Composite hype still requires matched file rows (no imputation).</div>';
            extra += '<div class="stats-footnote">Blended popularity includes the species graded-slab multiplier and the per-card slab vs set-median nudge.</div>';
        } else if (chartId === 'artistChase') {
            extra = '<div class="stats-footnote"><strong>X</strong> is ln(1 + <strong>median $</strong>) of that illustrator’s <strong>chase</strong> cards across the whole scrape (<code>artist_scores.json</code> from the scraper). <strong>Y</strong> is this card’s market price—explore “illustrators whose chase work clears high” vs “this card’s price,” not a causal claim.</div>';
        } else if (chartId === 'rarityTier') {
            extra = '<div class="stats-footnote"><strong>X</strong> is a <strong>collector print-class ladder</strong> (ordinal score from the card’s <code>rarity</code> string—SIR, IR, UR, etc.—using the same heuristic weights as LSRL point weights, <em>not</em> pull odds). <strong>Y</strong> is market price ($). This is a “what SKU class is this listing?” view for browsing, not a claim that rarity alone causes price.</div>';
            extra += '<div class="stats-footnote">Vintage / EX-era strings (e.g. <strong>Rare Shining</strong>, <strong>Rare Holo Star</strong>, <strong>Rare Secret</strong>, <strong>Rare Holo EX</strong>, <strong>Rare Holo</strong>) get distinct ladder scores so WotC pools are not a single vertical column.</div>';
            extra += '<div class="stats-footnote"><strong>LSRL weights</strong> match the composite ladder (calibrated static rarity × NM/EN listing depth × fixed-product pull factor). This panel plots <strong>every</strong> chartable-$ row in the export for selected sets; <strong>composite</strong> can be smaller when no blend score exists. <strong>Pull / Trends / character / hype / artist-chase / graded-pop</strong> omit rows missing pull, Trends match, volume, chase-artist hit, or pop. Re-scrape to refresh the export: <strong>top ~100 by $</strong> plus up to <strong>28 chase-rarity</strong> cards outside that slice (e.g. a second <strong>LEGEND</strong> half).</div>';
            extra += '<div class="stats-footnote">LSRL uses one <strong>Huber</strong> residual reweight (MAD scale, <em>c</em> = 1.345, min mult 0.12) when <em>n</em> ≥ 5; composite adds a separate <strong>low-|X|</strong> weight floor for the dashed line only.</div>';
        } else if (chartId === 'setVintage') {
            extra = `<div class="stats-footnote"><strong>X</strong> is <strong>sqrt-scaled capped age</strong>: sqrt(min(raw years, ${SET_VINTAGE_SQRT_CAP_YEARS})) ÷ sqrt(${SET_VINTAGE_SQRT_CAP_YEARS}) since <code>release_date</code>. Recent eras get more horizontal spread; very old sets share the right tail so a 1999–2026 pool is less dominated by WotC leverage alone. Raw years stay in tooltips. <strong>Y</strong> is market price ($). Expect <em>weak</em> linear r when mixing decades—era, rarity mix, and reprints confound a simple age→price story.</div>`;
        } else if (chartId === 'tcgMacroInterest') {
            extra = '<div class="stats-footnote"><strong>X</strong> is a <strong>hobby-wide interest index</strong> for the set’s <strong>calendar release year</strong> (same value for every card in that set), from <code>tcg_macro_interest_by_year.json</code> — typically a <strong>Google Trends</strong> (or similar) annual mean for a macro query such as “pokemon tcg” / “pokemon cards,” <em>not</em> per-species Trends. Use this to explore eras (e.g. <strong>Skyridge</strong> ≈ 2003) where macro interest was cooler vs. today’s secondary prices — association only; supply, English share, and survivorship in this scrape all confound a simple “less search → fewer pulls → higher $ now” story.</div>';
            extra += '<div class="stats-footnote">Populate numeric <code>by_year</code> values (run <code>python scrape/seed_tcg_macro_interest_years.py</code> to list release years, then paste indices). Until years have numbers, this panel stays empty.</div>';
        } else if (chartId === 'gradedPop') {
            extra = '<div class="stats-footnote"><strong>X</strong> is <strong>log₁₀(1 + total graded)</strong> from Gemrate Universal Pop Report when present, else optional PSA sidecar totals. <strong>Same filters as other scatters:</strong> selected sets, release-year toolbar, and the chart’s <strong>minimum USD floor</strong> (see Data drawer; sub-$5 can be included). <strong>Y</strong> is <strong>chart market price (USD)</strong> (median blend). High pop often tracks liquidity; <em>low</em> pop vs modern-era medians (see hype×scarcity footnote) can read as relative scarcity. For alternate aggregations see <a href="https://gemrate.com/" target="_blank" rel="noopener noreferrer">GemRate</a>. Subset / Trainer Gallery rows may map to the main expansion slug—association only.</div>';
            if (nPts === 0) {
                extra += '<div class="stats-footnote stats-footnote--warn">No graded totals on any plotted row — run <code>python gemrate_scraper.py</code> to populate dataset with Gemrate totals.</div>';
            }
        }
        if (CHART_LSRL_LOG10_MARKET_PRICE.has(chartId)) {
            extra += '<div class="stats-footnote"><strong>Correlation (r)</strong> uses the same core convention as composite: weighted Pearson on <strong>log₁₀(chart price)</strong> vs this chart’s X (Chart.js still draws both axes in <strong>natural units</strong>). Expect <strong>lower |r|</strong> than composite when X is one coarse proxy (ordinal print class stacks many SKUs on the same X, etc.). <strong>Fewer points</strong> than print-class vs composite happens when this panel needs pull, Trends, character volume, graded pop, or artist-chase data the row does not have.</div>';
            if (stats.useLogX) {
                extra += '<div class="stats-footnote">On this panel the LSRL internally fits <strong>log₁₀(y)</strong> vs <strong>log₁₀(x)</strong> (x is still plotted on a linear scale). That usually <strong>raises |r|</strong> vs a linear-x fit when the driver is heavy-tailed (popularity, hype×scarcity, print volume) while prices are roughly log-normal.</div>';
            }
        }
        let residNote = '';
        if (stats.meanAbsResid != null && Number.isFinite(stats.meanAbsResid)
            && stats.rmseY != null && Number.isFinite(stats.rmseY)) {
            if (stats.useLogY) {
                if (chartId === 'composite') {
                    residNote = `<div class="stats-footnote"><strong>LSRL residuals (composite):</strong> in <strong>log₁₀(price)</strong> units — weighted mean |Δlog₁₀y| = ${stats.meanAbsResid.toFixed(3)} · RMSE = ${stats.rmseY.toFixed(3)}. Open <strong>LSRL residual audit</strong> below for cards sorted by |Δlog₁₀y| and heuristic flags.</div>`;
                } else {
                    residNote = `<div class="stats-footnote"><strong>LSRL residuals:</strong> in <strong>log₁₀(price)</strong> units — weighted mean |Δlog₁₀y| = ${stats.meanAbsResid.toFixed(3)} · RMSE = ${stats.rmseY.toFixed(3)}. The sorted audit list below is <strong>composite-only</strong>.</div>`;
                }
            } else {
                residNote = `<div class="stats-footnote"><strong>LSRL spread (y on x):</strong> weighted mean |residual| = ${stats.meanAbsResid.toFixed(3)} · RMSE = ${stats.rmseY.toFixed(3)} on this chart’s Y axis. Card tooltips show signed gap vs the same line.</div>`;
            }
        }
        setFooterHtml(root, 'insight', lines[strength] + extra + residNote);
    }

    function syncGradedPopScatterFrameMeta() {
        const el = document.getElementById('gradedPopScatterFrameMeta');
        const chart = charts.gradedPop;
        if (!el || !chart) return;
        const nPts = scatterPointCount(chart);
        if (nPts <= 0) {
            el.textContent = 'No graded-pop points on the current selection — cards need Gemrate or graded_pop merge totals, and must pass the chart price filter.';
            return;
        }
        let minT = Infinity;
        let maxT = -Infinity;
        chart.data.datasets.forEach((ds) => {
            if (isLsrlDataset(ds) || !ds.data) return;
            ds.data.forEach((p) => {
                const t = p && p.gradedPopTotal;
                if (t != null && Number.isFinite(t) && t > 0) {
                    minT = Math.min(minT, t);
                    maxT = Math.max(maxT, t);
                }
            });
        });
        const span = Number.isFinite(minT) && Number.isFinite(maxT) && maxT > 0
            ? ` Slab totals on this view: ${Math.round(minT).toLocaleString()}–${Math.round(maxT).toLocaleString()}.`
            : '';
        el.textContent = `${nPts} card(s) plotted (X = log₁₀(1 + PSA-style graded count)).${span}`;
    }

    function updateAllFooters() {
        allChartKeys().forEach(updateFooter);
        syncGradedPopScatterFrameMeta();
    }

    function refreshLSRL(chartId) {
        const chart = charts[chartId];
        if (!chart) return;
        const lineDs = chart.data.datasets.find(isLsrlDataset);
        if (!lineDs) return;

        const stats = calculateWeightedRegression(chartId);
        if (!stats) {
            lineDs.data = [];
            return;
        }

        const xs = [];
        chart.data.datasets.forEach((ds, dsi) => {
            if (isLsrlDataset(ds) || !ds.data) return;
            const meta = chart.getDatasetMeta(dsi);
            if (meta && meta.hidden) return;
            ds.data.forEach((p) => {
                const x = Number(p.x);
                if (Number.isFinite(x)) xs.push(x);
            });
        });
        if (xs.length === 0) {
            lineDs.data = [];
            return;
        }
        let xMin = Math.min(...xs);
        let xMax = Math.max(...xs);
        if (xMin === xMax) {
            xMin -= 1;
            xMax += 1;
        }
        if (chartId === 'composite') {
            xMin = Math.max(COMPOSITE_AXIS_X_MIN, Math.min(COMPOSITE_AXIS_X_MAX, xMin));
            xMax = Math.min(COMPOSITE_AXIS_X_MAX, Math.max(COMPOSITE_AXIS_X_MIN, xMax));
            if (!(xMax > xMin)) {
                xMin = COMPOSITE_AXIS_X_MIN;
                xMax = COMPOSITE_AXIS_X_MAX;
            }
        }

        const yAt = (xv) => {
            const xF = stats.useLogX ? lsrlDisplayXToFitX(chartId, xv) : xv;
            const lp = stats.m * (Number.isFinite(xF) ? xF : xv) + stats.b;
            if (stats.useLogY) {
                return Math.pow(10, Math.max(lp, Math.log10(MIN_CHART_MARKET_PRICE)));
            }
            return lp;
        };
        let yLo = yAt(xMin);
        let yHi = yAt(xMax);
        if (chartId === 'composite' || CHART_LSRL_LOG10_MARKET_PRICE.has(chartId)) {
            yLo = Math.max(MIN_CHART_MARKET_PRICE, yLo);
            yHi = Math.max(MIN_CHART_MARKET_PRICE, yHi);
        }
        if (chartId === 'composite') {
            yLo = Math.min(yLo, COMPOSITE_AXIS_Y_MAX_USD);
            yHi = Math.min(yHi, COMPOSITE_AXIS_Y_MAX_USD);
        }
        lineDs.data = [
            { x: xMin, y: yLo },
            { x: xMax, y: yHi }
        ];
    }

    /** Bounds from scatter datasets only (LSRL is ignored so it does not expand the frame). */
    function collectScatterBounds(chart) {
        let minX = Infinity;
        let maxX = -Infinity;
        let minY = Infinity;
        let maxY = -Infinity;
        let n = 0;
        chart.data.datasets.forEach((ds, dsi) => {
            if (isLsrlDataset(ds) || !ds.data) return;
            const meta = chart.getDatasetMeta(dsi);
            if (meta && meta.hidden) return;
            ds.data.forEach(p => {
                const x = Number(p.x);
                const y = Number(p.y);
                if (!Number.isFinite(x) || !Number.isFinite(y)) return;
                n++;
                minX = Math.min(minX, x);
                maxX = Math.max(maxX, x);
                minY = Math.min(minY, y);
                maxY = Math.max(maxY, y);
            });
        });
        if (n === 0) return null;
        if (minX === maxX) {
            minX -= 1;
            maxX += 1;
        }
        if (minY === maxY) {
            minY -= 1;
            maxY += 1;
        }
        return { minX, maxX, minY, maxY };
    }

    function estimateScatterPlotSizePx(chart) {
        const ca = chart && chart.chartArea;
        if (ca && ca.width > 20 && ca.height > 20) {
            return { w: ca.width, h: ca.height };
        }
        const c = chart && chart.canvas;
        const cw = (c && c.clientWidth) ? c.clientWidth : 520;
        const ch = (c && c.clientHeight) ? c.clientHeight : 360;
        return {
            w: Math.max(140, cw * 0.74),
            h: Math.max(100, ch * 0.68)
        };
    }

    function isLogScale125TimesPow10(v) {
        if (!Number.isFinite(v) || v <= 0) return false;
        const e = Math.floor(Math.log10(v));
        const m = v / Math.pow(10, e);
        return Math.abs(m - 1) < 0.06 || Math.abs(m - 2) < 0.06 || Math.abs(m - 5) < 0.06;
    }

    /** Data-axis padding so a fixed pixel margin exists at each edge (avoids clipping scatter + overlay). */
    function scatterDataPadFromPlotPx(span, plotPx) {
        if (!Number.isFinite(span) || span <= 0) return 1e-9;
        const px = Math.max(plotPx, 64);
        return (span * SCATTER_AXIS_EDGE_PAD_PX) / px;
    }

    function scatterMoneyTickLabel(value) {
        const v = Number(value);
        if (!Number.isFinite(v)) return '';
        if (v >= 1000) return `$${Math.round(v).toLocaleString()}`;
        if (v >= 100) return `$${v.toFixed(0)}`;
        if (v >= 10) return `$${v.toFixed(1)}`;
        return `$${v.toFixed(2)}`;
    }

    function scatterPlainTickLabel(value) {
        const v = Number(value);
        if (!Number.isFinite(v)) return '';
        const a = Math.abs(v);
        let d = 2;
        if (a >= 200) d = 0;
        else if (a >= 40) d = 1;
        else if (a >= 10) d = 2;
        else if (a >= 1) d = 2;
        else d = 3;
        const s = v.toFixed(Math.min(4, d));
        return s.replace(/\.?0+$/, '') || '0';
    }

    /**
     * Set tick density / rotation once. Do not mutate ticks during `applyScatterAxisFit` — Chart.js 4
     * options are proxied and repeated tick writes during `update()` can recurse (`set` stack overflow).
     */
    function wireAnalyticsScatterTicksOnce() {
        const base = {
            color: '#64748b',
            font: { size: 10 },
            maxTicksLimit: 10,
            autoSkip: true,
            padding: 6,
            minRotation: 0,
            maxRotation: 0
        };
        allChartKeys().forEach((key) => {
            const ch = charts[key];
            if (!ch || !ch.options.scales) return;
            const sx = ch.options.scales.x;
            const sy = ch.options.scales.y;
            Object.assign(sx.ticks, base);
            if (key === 'composite') {
                Object.assign(sy.ticks, { ...base, maxTicksLimit: 16 });
            } else {
                Object.assign(sy.ticks, base);
            }
            if (key === 'pullCost') {
                sx.ticks.callback = scatterMoneyTickLabel;
                sy.ticks.callback = scatterPlainTickLabel;
            } else if (key === 'composite') {
                sx.ticks.callback = scatterPlainTickLabel;
            } else {
                sx.ticks.callback = scatterPlainTickLabel;
                sy.ticks.callback = scatterMoneyTickLabel;
            }
        });
    }

    /**
     * Fit axes to card points (tight padding).
     * Composite: X fixed [COMPOSITE_AXIS_X_MIN, COMPOSITE_AXIS_X_MAX]; Y log with dynamic min, max COMPOSITE_AXIS_Y_MAX_USD, bounds `data` (avoids log scale growing past max for tick padding).
     * (chartjs-plugin-zoom was removed from this page: its beforeUpdate merge with Chart.js 4
     * proxied options caused infinite `Object.set` / stack overflow when scales were updated.)
     */
    function applyScatterAxisFit(chartId) {
        const chart = charts[chartId];
        if (!chart || !chart.options.scales) return;
        const sx = chart.options.scales.x;
        const sy = chart.options.scales.y;
        const b = collectScatterBounds(chart);
        if (!b) {
            if (chartId === 'composite') {
                sy.type = 'logarithmic';
                sx.min = COMPOSITE_AXIS_X_MIN;
                sx.max = COMPOSITE_AXIS_X_MAX;
                sx.bounds = 'data';
                sy.bounds = 'data';
                sy.min = chartScatterMinUsd() * 0.82;
                sy.max = COMPOSITE_AXIS_Y_MAX_USD;
                sx.beginAtZero = false;
                sy.beginAtZero = false;
                sx.grace = 0;
                sy.grace = 0;
                if (sx.ticks) delete sx.ticks.stepSize;
                if (sy.ticks) delete sy.ticks.stepSize;
                return;
            }
            sx.min = undefined;
            sx.max = undefined;
            sy.min = undefined;
            sy.max = undefined;
            sx.grace = undefined;
            sy.grace = undefined;
            if (sx.ticks) delete sx.ticks.stepSize;
            if (sy.ticks) delete sy.ticks.stepSize;
            sx.beginAtZero = true;
            sy.type = 'linear';
            sy.beginAtZero = chartId === 'pullCost' ? false : true;
            return;
        }
        let { minX, maxX, minY, maxY } = b;
        const spanX = maxX - minX;
        const spanY = maxY - minY;
        const { w: plotW, h: plotH } = estimateScatterPlotSizePx(chart);
        const padFracX = chartId === 'composite' ? 0.026 : 0.034;
        const padFracY = 0.034;
        const padX = Math.max(spanX * padFracX, scatterDataPadFromPlotPx(spanX, plotW), 1e-12);
        const padY = Math.max(spanY * padFracY, scatterDataPadFromPlotPx(spanY, plotH), 1e-12);

        let nXMin = minX - padX;
        let nXMax = maxX + padX;
        let nYMin = minY - padY;
        let nYMax = maxY + padY;

        if (chartId === 'pullCost') {
            nXMin = Math.max(0, nXMin);
        } else if (chartId === 'composite') {
            sy.type = 'logarithmic';
            const yFloor = chartScatterMinUsd();
            const yLo = Math.max(minY, yFloor);
            const yHi = Math.max(maxY, yLo * 1.0001);
            const logSpan = Math.log10(yHi / Math.max(yLo, yFloor * 1.0000001));
            const logPad = Math.min(0.16, Math.max(0.028, (logSpan * SCATTER_AXIS_EDGE_PAD_PX) / Math.max(plotH, 64)));
            const extraDec = 0.045;
            let yMinLog = Math.log10(yLo) - logPad - extraDec;
            nYMin = Math.pow(10, yMinLog);
            nYMin = Math.max(yFloor * 0.82, Math.max(1e-12, nYMin));
            if (nYMin >= yLo * 0.998) nYMin = Math.max(yFloor * 0.78, yLo * Math.pow(10, -0.06));
            nYMax = COMPOSITE_AXIS_Y_MAX_USD;
            if (!(nYMax > nYMin)) nYMax = Math.max(COMPOSITE_AXIS_Y_MAX_USD, nYMin * 1.15);
            nXMin = COMPOSITE_AXIS_X_MIN;
            nXMax = COMPOSITE_AXIS_X_MAX;
        } else {
            if (sy.type !== 'linear') sy.type = 'linear';
            if (SCATTER_X_NON_NEGATIVE_CHARTS.has(chartId)) nXMin = Math.max(0, nXMin);
            const floorUsd = chartScatterMinUsd();
            if (minY >= floorUsd * 0.985) {
                nYMin = Math.min(nYMin, minY - Math.max(padY, minY * 0.042, 0.35));
            }
            nYMin = Math.max(0, nYMin);
        }

        sx.beginAtZero = false;
        sy.beginAtZero = false;
        sx.min = nXMin;
        sx.max = nXMax;
        sy.min = nYMin;
        sy.max = nYMax;
        if (chartId === 'composite') {
            sx.bounds = 'data';
            sy.bounds = 'data';
            sx.grace = 0;
            sy.grace = 0;
        } else {
            sx.grace = '0%';
            sy.grace = '0%';
        }
    }

    function resetScatterView(chartId) {
        const chart = charts[chartId];
        if (!chart) return;
        applyScatterAxisFit(chartId);
        chart.update();
    }

    function attachScatterChartInteractions() {
        allChartKeys().forEach((key) => {
            const chart = charts[key];
            if (!chart || !chart.canvas || chart.canvas.dataset.scatterNavBound === '1') return;
            chart.canvas.dataset.scatterNavBound = '1';
            chart.canvas.addEventListener('dblclick', (e) => {
                e.preventDefault();
                resetScatterView(key);
            });
        });
    }

    function makeLsrlDataset() {
        return {
            type: 'line',
            lsrlLine: true,
            label: 'LSRL',
            data: [],
            borderColor: 'rgba(148, 163, 184, 0.92)',
            backgroundColor: 'transparent',
            borderWidth: 2,
            borderDash: [7, 5],
            pointRadius: 0,
            pointHoverRadius: 0,
            fill: false,
            tension: 0,
            order: 999,
            hidden: false,
            clip: true
        };
    }

    function addScatterBeforeLsrl(chart, scatterDataset) {
        const i = chart.data.datasets.findIndex(isLsrlDataset);
        if (i === -1) chart.data.datasets.push(scatterDataset);
        else chart.data.datasets.splice(i, 0, scatterDataset);
    }

    /** Pinned scatter point: one global selection; cleared on empty click, outside click, scroll-off, Escape, or rebuild. */
    let pinnedScatterSelection = null;
    let chartPinVisibilityObserver = null;

    function clearPinnedScatterCard() {
        if (!pinnedScatterSelection) return;
        const ch = pinnedScatterSelection.chart;
        pinnedScatterSelection = null;
        const wrap = ch && ch.canvas && ch.canvas.parentNode;
        const el = wrap && wrap.querySelector('div.custom-tooltip');
        if (el) {
            el.classList.remove('custom-tooltip--pinned');
            el.style.opacity = '0';
            el.style.pointerEvents = 'none';
        }
    }

    function chartCanvasIdToChartKey(canvasId) {
        const map = {
            pullCostChart: 'pullCost',
            compositeChart: 'composite',
            characterChart: 'character',
            trendsChart: 'trends',
            hypeScarcityChart: 'hypeScarcity',
            hypePullRatioChart: 'hypePullRatio',
            rarityTierChart: 'rarityTier',
            setVintageChart: 'setVintage',
            tcgMacroInterestChart: 'tcgMacroInterest',
            artistChaseChart: 'artistChase',
            gradedPopChart: 'gradedPop'
        };
        return map[canvasId] || null;
    }

    /** Signed vertical gap vs current chart LSRL; market-$ panels use log₁₀(price) fit when `reg.useLogY`. */
    function formatScatterCardLsrlGapLine(canvasId, raw, compact) {
        const key = chartCanvasIdToChartKey(canvasId);
        if (!key) return '';
        const x = Number(raw.x);
        const y = Number(raw.y);
        if (!Number.isFinite(x) || !Number.isFinite(y)) return '';
        const reg = calculateWeightedRegression(key);
        if (!reg || reg.n < 2) return '';
        const xF = reg.useLogX ? lsrlDisplayXToFitX(key, x) : x;
        if (!Number.isFinite(xF)) return '';
        const predLog = reg.m * xF + reg.b;
        if (reg.useLogY) {
            const yLog = Math.log10(Math.max(y, MIN_CHART_MARKET_PRICE));
            const dLog = yLog - predLog;
            const predY = Math.pow(10, Math.max(predLog, Math.log10(MIN_CHART_MARKET_PRICE)));
            const dLin = y - predY;
            const dir = dLog >= 0 ? 'above' : 'below';
            const fL = dLog >= 0 ? `+${dLog.toFixed(3)}` : dLog.toFixed(3);
            const ratio = predY > 0 ? y / predY : null;
            const mult = ratio != null && Number.isFinite(ratio) ? ` · ~${ratio.toFixed(2)}× vs line` : '';
            if (compact) {
                return `<div style="margin-top:8px;padding-top:6px;border-top:1px solid rgba(255,255,255,0.08);font-size:0.68rem;color:#94a3b8;">LSRL: <strong style="color:#e2e8f0;">Δlog₁₀y ${fL}</strong>${mult} <span style="color:#64748b;">(${dir})</span> · $Δ ${dLin >= 0 ? '+' : ''}${dLin.toFixed(0)}</div>`;
            }
            return `<span style="display:block;font-size:0.68rem;color:#94a3b8;margin-top:6px;border-top:1px solid rgba(255,255,255,0.06);padding-top:6px;">vs LSRL (log₁₀ price vs X): <strong style="color:#e2e8f0;">Δlog₁₀y ${fL}</strong>${mult} (${dir}) · $Δ ${dLin >= 0 ? '+' : ''}${dLin.toFixed(0)}</span>`;
        }
        const pred = predLog;
        const dy = y - pred;
        const dir = dy >= 0 ? 'above' : 'below';
        const fd = dy >= 0 ? `+${dy.toFixed(3)}` : dy.toFixed(3);
        if (compact) {
            return `<div style="margin-top:8px;padding-top:6px;border-top:1px solid rgba(255,255,255,0.08);font-size:0.68rem;color:#94a3b8;">LSRL Δy: <strong style="color:#e2e8f0;">${fd}</strong> <span style="color:#64748b;">(${dir})</span></div>`;
        }
        return `<span style="display:block;font-size:0.68rem;color:#94a3b8;margin-top:6px;border-top:1px solid rgba(255,255,255,0.06);padding-top:6px;">vs LSRL (y on x): <strong style="color:#e2e8f0;">${fd}</strong> on Y (${dir} line)</span>`;
    }

    /** Pull-chart footnote: which $/pack inputs fed the median (from `getPackPriceMedianDetail`). */
    function formatPackPriceBasisHtml(detail) {
        if (!detail || !detail.parts || !detail.parts.length) return '';
        const bits = detail.parts.map((p) => `${p.label} $${p.value.toFixed(2)}`);
        return `<div style="font-size:0.62rem;color:#64748b;margin-top:4px;line-height:1.38;">$/pack used: median <strong style="color:#cbd5e1;">$${detail.median.toFixed(2)}</strong> · ${bits.join(' · ')}</div>`;
    }

    /** Promo / subset reprint heuristics + Dex variant that set chart $ (from scraper). */
    function variantSlugToLabel(slug) {
        if (!slug || typeof slug !== 'string') return '';
        const s = String(slug).replace(/_/g, ' ').replace(/([a-z])([A-Z])/g, '$1 $2');
        return s.replace(/\b\w/g, (c) => c.toUpperCase());
    }

    function formatScatterCardProductFlagsHtml(card, escTip, raw) {
        if (!card) return '';
        const parts = [];
        if (card.is_promo === true) parts.push('Promo');
        if (card.is_reprint_subset === true) parts.push('Subset / reprint-style');
        if (raw && raw.boosterCardPullBlend === false) {
            parts.push('Fixed-product / promo pull: per-card pack odds omitted (slot table only); LSRL weight x0.4');
        }
        const primLab = card.variant_primary_label && String(card.variant_primary_label).trim();
        const prim = card.variant_primary && String(card.variant_primary).trim();
        if (primLab) {
            parts.push(`Priced variant: ${primLab}`);
        } else if (prim) {
            parts.push(`Priced variant: ${variantSlugToLabel(prim)}`);
        }
        const pk = Array.isArray(card.variant_keys) ? card.variant_keys.filter(Boolean) : [];
        if (pk.length > 1) {
            parts.push(`${pk.length} Dex finishes`);
        }
        const rm = card.regulation_mark != null && String(card.regulation_mark).trim() !== ''
            ? String(card.regulation_mark).trim()
            : '';
        if (rm) parts.push(`Regulation ${rm}`);
        if (!parts.length) return '';
        return `<div style="font-size:0.64rem;color:#64748b;margin-top:4px;line-height:1.35;">${parts.map((p) => escTip(p)).join(' · ')}</div>`;
    }

    /** Graded pop: Gemrate Universal Pop Report (total + pop stats) + legacy sidecar */
    function formatScatterCardGradedPopHtml(card, escTip, raw) {
        if (!card) return '';
        const lines = [];
        const hasGemData = card.gemrate != null;
        if (hasGemData) {
            const bits = [];
            if (card.gemrate.total != null) bits.push(`Universal Total ${Number(card.gemrate.total).toLocaleString()}`);
            if (card.gemrate.total_gem_mint != null) bits.push(`Gem-Mint Total ${Number(card.gemrate.total_gem_mint).toLocaleString()}`);
            if (card.gemrate.psa_gems != null) bits.push(`PSA 10 ${Number(card.gemrate.psa_gems).toLocaleString()}`);
            if (card.gemrate.beckett_gems != null) bits.push(`BGS 9.5+ ${Number(card.gemrate.beckett_gems).toLocaleString()}`);
            if (card.gemrate.cgc_gems != null) bits.push(`CGC Gem ${Number(card.gemrate.cgc_gems).toLocaleString()}`);
            lines.push(`<div style="font-size:0.64rem;color:#7dd3fc;margin-top:4px;line-height:1.35;"><span style="color:#94a3b8;">Gemrate Pop Report</span> · ${bits.join(' · ')}</div>`);
        }
        const tot = card.psa_graded_pop_total;
        const g10 = card.psa_graded_pop_gem10;
        const asOf = card.psa_graded_pop_as_of;
        const src = card.psa_graded_pop_source;
        if (tot != null || g10 != null) {
            const bits = [];
            if (tot != null && Number.isFinite(Number(tot))) bits.push(`PSA pop total ${Number(tot).toLocaleString()}`);
            if (g10 != null && Number.isFinite(Number(g10))) bits.push(`Gem 10 ${Number(g10).toLocaleString()}`);
            if (asOf && String(asOf).trim()) bits.push(`as of ${escTip(String(asOf).trim())}`);
            if (src && String(src).trim()) bits.push(`src ${escTip(String(src).trim())}`);
            lines.push(`<div style="font-size:0.64rem;color:#5eead4;margin-top:3px;line-height:1.35;"><span style="color:#94a3b8;">Sidecar Data</span> · ${bits.join(' · ')}</div>`);
        }
        return lines.join('');
    }

    /** Warnings: thin eBay / NM book, TCG API cap, wide TCG low–market on the matched SKU. */
    function formatScatterLiquidityNoteHtml(card, escTip) {
        const bits = [];
        const cl = Number(card.collectrics_ebay_listings);
        const csv = Number(card.collectrics_ebay_sold_volume);
        if (Number.isFinite(cl) && cl > 0 && cl <= 5) bits.push(`thin Collectrics eBay listings (${cl})`);
        if (Number.isFinite(csv) && csv > 0 && csv <= 5) bits.push(`thin Collectrics eBay volume (${csv})`);
        const n = card.tcgtracking_listings_nm_en;
        const nn = Number(n);
        if (!usesCollectricsEbayLiquidity(card) && Number.isFinite(nn)) {
            if (nn <= 5) bits.push(`thin NM/EN book (${nn})`);
            if (nn === 25) bits.push('listings may cap at 25');
        }
        const low = Number(card.tcgtracking_low_usd);
        const mkt = Number(card.tcgtracking_market_usd);
        if (Number.isFinite(low) && Number.isFinite(mkt) && low > 0 && mkt > 0) {
            const rel = (mkt - low) / mkt;
            if (rel >= 0.25) bits.push(`wide TCG NM low–market (${(rel * 100).toFixed(0)}%)`);
        }
        if (!bits.length) return '';
        return `<div style="font-size:0.62rem;color:#b45309;margin-top:3px;line-height:1.35;">Liquidity: ${bits.map((b) => escTip(b)).join(' · ')}</div>`;
    }

    /** NM EN listing count, price subtype, and set-level TCGTracking refresh time when present on merged data. */
    function formatScatterTcgTrackingMetaHtml(raw, card, escTip) {
        const parts = [];
        const ceL = Number(card.collectrics_ebay_listings);
        const ceV = Number(card.collectrics_ebay_sold_volume);
        if (Number.isFinite(ceL) && ceL > 0) {
            parts.push(`Collectrics eBay listings <strong style="color:#a5f3fc;">${ceL.toLocaleString()}</strong>`);
        }
        if (Number.isFinite(ceV) && ceV > 0) {
            parts.push(`Collectrics eBay volume <strong style="color:#a5f3fc;">${ceV.toLocaleString()}</strong>`);
        }
        const nList = card.tcgtracking_listings_nm_en;
        if (nList != null && Number.isFinite(Number(nList))) {
            const n = Number(nList);
            const atCap = n === 25;
            const shown = atCap ? String(n) : String(n);
            const capNote = atCap
                ? ' <span style="font-weight:400;color:#475569;">(TCG API cap)</span>'
                : '';
            const label = usesCollectricsEbayLiquidity(card) ? 'TCG NM EN (ref.)' : 'NM EN listings';
            parts.push(`${label} <strong style="color:#cbd5e1;">${shown}</strong>${capNote}`);
        }
        if (card.tcgtracking_price_subtype) {
            parts.push(`subtype <strong style="color:#94a3b8;">${escTip(String(card.tcgtracking_price_subtype))}</strong>`);
        }
        const lowU = Number(card.tcgtracking_low_usd);
        if (Number.isFinite(lowU) && lowU > 0) {
            parts.push(`NM low <strong style="color:#94a3b8;">$${lowU.toFixed(2)}</strong>`);
        }
        const asof = raw && raw.priceAsOf;
        if (asof) {
            const d = escTip(String(asof).replace('T', ' ').slice(0, 19));
            parts.push(`set prices as of <strong style="color:#64748b;">${d}</strong>`);
        }
        if (!parts.length) return '';
        return `<div style="font-size:0.64rem;color:#64748b;margin-top:4px;line-height:1.4;">TCGTracking · ${parts.join(' · ')}</div>`;
    }

    /** When ≥2 distinct price anchors exist, show blended median vs components (Wizard + history included). */
    function formatScatterPriceProvenanceLine(card, escTip) {
        if (!card) return '';
        const anchors = collectDedupedPositiveUsdPrices(card);
        if (anchors.length < 2) return '';
        const parts = [];
        const mp = Number(card.market_price);
        if (Number.isFinite(mp) && mp > 0) parts.push(`List $${mp.toFixed(2)}`);
        const dsrc = card.pricedex_market_usd != null ? Number(card.pricedex_market_usd) : null;
        const d = Number.isFinite(dsrc) && dsrc > 0 ? dsrc : null;
        if (d != null && (!Number.isFinite(mp) || Math.abs(d - mp) > 0.01 * Math.max(mp, d, 1))) {
            parts.push(`Dex $${d.toFixed(2)}`);
        }
        const tcg = Number(card.tcgtracking_market_usd);
        const t = Number.isFinite(tcg) && tcg > 0 ? tcg : null;
        if (t != null) parts.push(`TCGTracking $${t.toFixed(2)}`);
        const api = Number(card.tcgapi_market_usd);
        const a = Number.isFinite(api) && api > 0 ? api : null;
        if (a != null) parts.push(`tcgapi $${a.toFixed(2)}`);
        const wc = Number(card.pokemon_wizard_current_price_usd);
        if (Number.isFinite(wc) && wc > 0) parts.push(`Wizard now $${wc.toFixed(2)}`);
        const wh = wizardHistoryPositiveUsdMedian(card);
        if (wh != null) parts.push(`Wizard hist Ø $${wh.toFixed(2)}`);
        const m = resolveChartMarketUsd(card);
        if (!Number.isFinite(m)) return '';
        let spreadHint = '';
        if (d != null && t != null) {
            const rel = Math.abs(d - t) / Math.min(d, t);
            if (rel >= 0.08) {
                spreadHint = ` <span style="color:#ca8a04;">(Dex vs TCG ${(rel * 100).toFixed(1)}%)</span>`;
            }
        }
        const lab = parts.length >= 2 ? parts.map((p) => escTip(p)).join(' · ') : anchors.map((v) => `$${v.toFixed(2)}`).join(' · ');
        return `<div style="font-size:0.62rem;color:#475569;margin-top:3px;">Chart median: <strong style="color:#94a3b8;">$${m.toFixed(2)}</strong> · ${lab}${spreadHint}</div>`;
    }

    /** Deep-link into Explorer (`index.html`) — same card detail modal as the main tab. */
    function buildExplorerCardDetailHref(raw) {
        if (!raw || !raw.card) return '';
        const sc = String(raw.analyticsSetCode != null ? raw.analyticsSetCode : '').trim();
        if (!sc) return '';
        const num = encodeURIComponent(String(raw.card.number != null ? raw.card.number : '').trim());
        const name = encodeURIComponent(String(raw.card.name != null ? raw.card.name : '').trim());
        return `index.html?detail=1&set=${encodeURIComponent(sc)}&num=${num}&name=${name}`;
    }

    function buildScatterCardTooltipHtml(chart, datasetIndex, dataIndex, opts) {
        const pinnedInteractions = opts && opts.pinnedInteractions === true;
        const dataset = chart.data.datasets[datasetIndex];
        if (!dataset || isLsrlDataset(dataset)) return null;
        const raw = dataset.data[dataIndex];
        if (!raw || !raw.card) return null;

        const card = raw.card;
        const chartMkt = resolveChartMarketUsd(card);
        const market = Number.isFinite(chartMkt)
            ? chartMkt
            : Number(card.market_price != null ? card.market_price : raw.y);
        const id = chart.canvas.id;
        const tipW = id === 'compositeChart' ? 'min(96vw, 500px)' : 'min(92vw, 280px)';

        const escTip = (s) => String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
        const setTitle = raw.analyticsSetName || raw.setName || '';
        const nameEsc = escTip(card.name);
        const mktStr = Number.isFinite(market) ? market.toFixed(2) : '—';

        const metaOneLine = setTitle
            ? `<div style="font-size:0.74rem;color:#94a3b8;line-height:1.35;margin-top:2px;"><span style="color:#cbd5e1;">${escTip(setTitle)}</span> · <span style="color:#4ade80;font-weight:700;">$${mktStr}</span></div>`
            : `<div style="font-size:0.8rem;color:#4ade80;font-weight:700;margin-top:2px;">$${mktStr}</div>`;

        const tcgMetaLine = formatScatterTcgTrackingMetaHtml(raw, card, escTip);
        const priceProvLine = formatScatterPriceProvenanceLine(card, escTip);
        const liquidityLine = formatScatterLiquidityNoteHtml(card, escTip);
        const productFlagsLine = formatScatterCardProductFlagsHtml(card, escTip, raw);
        const gradedPopLine = formatScatterCardGradedPopHtml(card, escTip, raw);

        const imgThumb = card.image_url
            ? `<img src="${card.image_url}" alt="" style="width:76px;height:auto;border-radius:6px;display:block;">`
            : `<div style="width:76px;height:104px;background:#27272f;border-radius:6px;"></div>`;

        const lsrlC = formatScatterCardLsrlGapLine(id, raw, true);

        if (id === 'compositeChart') {
            const f = raw.compositeFeatures;
            const br = raw.compositeBreakdown;
            const idxStr = Number.isFinite(raw.x) ? raw.x.toFixed(3) : '—';
            const explorerHref = buildExplorerCardDetailHref(raw);

            const fmtCompositeDriverCell = (key, v) => {
                if (!Number.isFinite(v)) return '—';
                if (key === 'trends' || key === 'logNostalgia') return v.toFixed(1);
                if (key === 'logGradedPop' || key === 'logGemMintRate') return v.toFixed(3);
                if (key === 'logArtStyle' || key === 'trainerArchetype') return v.toFixed(2);
                if (key === 'logVintageShowcase' || key === 'logFixedProductCorrection' || key === 'logSpeciesUtility') return v.toFixed(2);
                if (key === 'logPremium' && f && f.logPremiumImputedMean && f.logPremiumPullChart != null
                    && Number.isFinite(f.logPremiumPullChart)) {
                    return `<span style="color:#94a3b8">μ</span>${v.toFixed(2)}<span style="font-size:0.6rem;color:#64748b"> /pull ${f.logPremiumPullChart.toFixed(2)}</span>`;
                }
                if (key === 'logPremium' && f && f.logPremiumPullChart != null && Number.isFinite(f.logPremiumPullChart)
                    && Math.abs(f.logPremiumPullChart - v) > 0.03) {
                    return `${v.toFixed(2)}<span style="font-size:0.6rem;color:#64748b"> /pull ${f.logPremiumPullChart.toFixed(2)}</span>`;
                }
                return v.toFixed(2);
            };

            const driverFeat = (f && typeof f === 'object') ? f : {};
            const driverPanelInner = (br && Array.isArray(br.parts) && br.parts.length) || (f && typeof f === 'object')
                ? `<div style="margin-top:4px;font-size:0.62rem;color:#64748b;text-transform:uppercase;letter-spacing:0.04em;">Driver inputs</div>${buildCompositeDriverTableExtended(driverFeat, br, escTip, fmtCompositeDriverCell, latestCompositeModel)}`
                : '';

            const rawBitsLine = (() => {
                if (!f || typeof f !== 'object') return '';
                const bits = [];
                if (f.logPremium != null && Number.isFinite(f.logPremium)) {
                    let lp = f.logPremiumImputedMean
                        ? `ln(p/u) μ ${f.logPremium.toFixed(2)}`
                        : `ln(p/u) ${f.logPremium.toFixed(2)}`;
                    if (f.logPremiumPullChart != null && Number.isFinite(f.logPremiumPullChart)
                        && (f.logPremiumImputedMean || Math.abs(f.logPremiumPullChart - f.logPremium) > 0.03)) {
                        lp += ` <span style="color:#64748b">(pull ${f.logPremiumPullChart.toFixed(2)})</span>`;
                    }
                    bits.push(lp);
                }
                if (f.logCharVol != null && Number.isFinite(f.logCharVol)) bits.push(`ln(1+v) ${f.logCharVol.toFixed(2)}`);
                if (f.trends != null && Number.isFinite(f.trends)) bits.push(`Pop ${f.trends.toFixed(1)}`);
                if (f.hype != null && Number.isFinite(f.hype)) bits.push(`Hype×scarcity ${f.hype.toFixed(2)}`);
                if (f.logGradedPop != null && Number.isFinite(f.logGradedPop)) bits.push(`log₁₀pop ${f.logGradedPop.toFixed(3)}`);
                if (f.logArtStyle != null && Number.isFinite(f.logArtStyle)) bits.push(`Art ${f.logArtStyle.toFixed(2)}`);
                if (f.logNostalgia != null && Number.isFinite(f.logNostalgia)) bits.push(`Nostalgia ${f.logNostalgia.toFixed(1)}`);
                if (f.logGemMintRate != null && Number.isFinite(f.logGemMintRate)) {
                    const rate = (getCardGemMintRate(card) * 100).toFixed(1);
                    bits.push(`PSA10 ${rate}%`);
                }
                if (f.trainerArchetype != null && Number.isFinite(f.trainerArchetype) && Math.abs(f.trainerArchetype) > 0.1) {
                    bits.push(`Type Premium ${f.trainerArchetype > 0 ? '+' : ''}${f.trainerArchetype.toFixed(2)}`);
                }
                if (f.logVintageShowcase != null && f.logVintageShowcase > 0) {
                    bits.push(`<span style="color:#fcd34d;">Showcase ${f.logVintageShowcase.toFixed(2)}</span>`);
                }
                if (f.logFixedProductCorrection != null && f.logFixedProductCorrection < 0) {
                    bits.push(`<span style="color:#f87171;">Fixed Adj.</span>`);
                }
                if (f.logSpeciesUtility != null && f.logSpeciesUtility < 0) {
                    bits.push(`<span style="color:#94a3b8;">Item/Util</span>`);
                }
                return bits.length
                    ? `<div style="margin-top:8px;font-size:0.7rem;color:#cbd5e1;line-height:1.4;">${bits.join(' · ')}</div>`
                    : '';
            })();

            let detail = '';
            if (pinnedInteractions) {
                detail = `${rawBitsLine}<div style="margin-top:6px;font-size:0.76rem;color:#94a3b8;">Blend <strong style="color:#e2e8f0;font-size:0.88rem;">${idxStr}</strong>${driverPanelInner ? ' · <span style="color:#64748b;">Use <strong>Driver breakdown</strong> above for z / Δ / r.</span>' : ''}</div>`;
            } else {
                const pinHint = `<div style="margin-top:8px;font-size:0.7rem;color:#94a3b8;line-height:1.45;">Click the point to <strong>pin</strong> it, then open <strong>Driver breakdown</strong> or <strong>Explorer details</strong> (same card panel as the Explorer tab).</div>`;
                detail = `${rawBitsLine}${pinHint}<div style="margin-top:6px;font-size:0.76rem;color:#94a3b8;">Blend <strong style="color:#e2e8f0;font-size:0.88rem;">${idxStr}</strong></div>`;
            }

            let pinnedToolbarHtml = '';
            if (pinnedInteractions) {
                const exBtn = explorerHref
                    ? `<a class="analytics-tooltip-btn" href="${explorerHref}" target="_blank" rel="noopener noreferrer">Explorer details</a>`
                    : '<span class="analytics-tooltip-btn analytics-tooltip-btn--disabled" title="Missing set code on this row">Explorer details</span>';
                const drvBtn = driverPanelInner
                    ? '<button type="button" class="analytics-tooltip-btn js-scatter-driver-toggle" aria-expanded="false">Driver breakdown</button>'
                    : '';
                pinnedToolbarHtml = `<div class="scatter-pin-toolbar">${exBtn}${drvBtn}</div>`;
                if (driverPanelInner) {
                    pinnedToolbarHtml += `<div class="js-scatter-driver-panel" hidden style="margin-top:2px;">${driverPanelInner}</div>`;
                }
            }

            const html = `<div style="display:grid;grid-template-columns:76px minmax(0,1fr);gap:10px;padding:10px;max-width:min(96vw,500px);align-items:start;text-align:left;">
                <div>${imgThumb}</div>
                <div style="min-width:0;">
                    <div style="font-weight:700;color:#60a5fa;font-size:0.9rem;line-height:1.2;">${nameEsc}</div>
                    ${metaOneLine}
                    ${priceProvLine}
                    ${tcgMetaLine}
                    ${liquidityLine}
                    ${productFlagsLine}
                    ${gradedPopLine}
                    ${detail}
                    ${lsrlC}
                </div></div>`;
            return { html, width: tipW, raw, pinnedToolbarHtml };
        }

        let charSeriesLine = '';
        if (id === 'characterChart' && raw.charLineKind) {
            const kindLabel = raw.charLineKind === 'trainers' ? 'Trainers / humans' : 'Pokémon';
            charSeriesLine = `<div style="font-size:0.68rem;color:#64748b;margin-top:3px;">Series <span style="color:#cbd5e1;font-weight:600;">${kindLabel}</span></div>`;
        }

        let secondaryLabel = '';
        let secondaryVal = '';
        let tertiary = '';
        if (id === 'pullCostChart') {
            secondaryLabel = 'ln(price / pull)';
            secondaryVal = Number.isFinite(raw.y) ? raw.y.toFixed(3) : '—';
            const ep = raw.effectivePull;
            const packB = formatPackPriceBasisHtml(raw.packPriceDetail);
            tertiary = `<div style="font-size:0.7rem;color:#64748b;margin-top:4px;line-height:1.4;">Eff. pull $${ep != null ? ep.toFixed(2) : '—'}${raw.pullFromCard != null && raw.pullFromSlot != null ? ' · blended odds' : ''}</div>${packB}`;
        } else if (id === 'characterChart') {
            const isQuad = latestCharacterVolumeTransform.mode === 'quadratic';
            secondaryLabel = isQuad ? 'Axis value (v + v²/s)' : 'Print volume (high-tier)';
            secondaryVal = `${raw.x.toFixed(1)}`;
            let tertChar = '';
            if (isQuad && raw.charVolumeRaw != null && Number.isFinite(raw.charVolumeRaw)) {
                const s = latestCharacterVolumeTransform.vScale || 1;
                tertChar += `<div style="font-size:0.68rem;color:#64748b;margin-top:4px;line-height:1.35;">Vol <strong style="color:#e2e8f0;">${raw.charVolumeRaw.toFixed(1)}</strong> · x = v + v²/s (s≈${Number.isFinite(s) ? s.toFixed(0) : '1'})</div>`;
            }
            if (Array.isArray(raw.charSpeciesKeys) && raw.charSpeciesKeys.length) {
                const sk = raw.charSpeciesKeys.map((k) => escTip(k)).join(', ');
                tertChar += `<div style="font-size:0.65rem;color:#64748b;margin-top:5px;line-height:1.35;">Keys (base species for file lookup): <span style="color:#cbd5e1;">${sk}</span></div>`;
            }
            tertiary = tertChar;
        } else if (id === 'hypeScarcityChart') {
            secondaryLabel = 'Popularity × scarcity (axis)';
            secondaryVal = raw.x.toFixed(2);
            const r = raw.hypePullRatio;
            const tr = raw.trendsScore;
            const trBase = raw.trendsPopularityAxisBase;
            const trSrc = raw.trendsSourceRaw;
            const ep = raw.effectivePull;
            const scm = raw.hypeScarcityMetricCombined;
            const spl = raw.hypeScarcityPullLog10;
            const imputedLine = raw.trendsDisplayImputed
                ? `<div style="font-size:0.65rem;color:#c4b5fd;margin-top:3px;line-height:1.35;">Chart popularity X is <strong>imputed</strong> (no match in <code>google_trends_momentum.json</code> or <code>species_popularity_list.json</code>).${Number.isFinite(trSrc) && trSrc > 0 ? ` Matched-file avg: <strong style="color:#e2e8f0;">${trSrc.toFixed(1)}</strong>.` : ''}</div>`
                : '';
            let slabLine = '';
            if (spl != null && Number.isFinite(spl) && scm != null && Number.isFinite(scm)) {
                slabLine = `<div style="font-size:0.66rem;color:#64748b;margin-top:4px;line-height:1.4;">Scarcity <strong style="color:#e2e8f0;">${scm.toFixed(2)}</strong> (incl. log₁₀ pull+1 = ${spl.toFixed(2)} + slab terms)</div>`;
            }
            const baseLine = (trBase != null && Number.isFinite(trBase) && tr != null && Number.isFinite(tr) && Math.abs(trBase - tr) > 0.02)
                ? `<div style="font-size:0.65rem;color:#7dd3fc;margin-top:3px;">Pop. file+species <strong>${trBase.toFixed(1)}</strong> → axis <strong>${tr.toFixed(1)}</strong> (slab vs set)</div>`
                : '';
            if (r != null && Number.isFinite(r)) {
                tertiary = `<div style="font-size:0.7rem;color:#64748b;margin-top:4px;line-height:1.4;">Gap <strong style="color:#e2e8f0;">${r.toFixed(2)}</strong> · Pop(axis) ${Number.isFinite(tr) ? tr.toFixed(1) : '—'} · pull $${ep != null ? ep.toFixed(2) : '—'}</div>${baseLine}${slabLine}${imputedLine}`;
            } else if (imputedLine) {
                tertiary = `${baseLine}${slabLine}${imputedLine}`;
            } else {
                tertiary = `${baseLine}${slabLine}`;
            }
        } else if (id === 'hypePullRatioChart') {
            secondaryLabel = 'Blended popularity ÷ ln(pull+1)';
            secondaryVal = Number.isFinite(raw.x) ? raw.x.toFixed(3) : '—';
            const tr = raw.trendsScore;
            const trSrc = raw.trendsSourceRaw;
            const ep = raw.effectivePull;
            const imputedLine = raw.trendsDisplayImputed
                ? `<div style="font-size:0.65rem;color:#c4b5fd;margin-top:3px;line-height:1.35;">Chart popularity X is <strong>imputed</strong> (no token match).${Number.isFinite(trSrc) && trSrc > 0 ? ` Matched-file avg: <strong style="color:#e2e8f0;">${trSrc.toFixed(1)}</strong>.` : ''}</div>`
                : '';
            tertiary = `<div style="font-size:0.7rem;color:#64748b;margin-top:4px;">Pop(axis) ${Number.isFinite(tr) ? tr.toFixed(1) : '—'} · pull $${ep != null ? ep.toFixed(2) : '—'}</div>${imputedLine}`;
        } else if (id === 'rarityTierChart') {
            secondaryLabel = 'Print-class score (rarity ladder)';
            secondaryVal = Number.isFinite(raw.x) ? raw.x.toFixed(2) : '—';
            const rl = raw.rarityLabel ? String(raw.rarityLabel).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '';
            tertiary = `<div style="font-size:0.7rem;color:#64748b;margin-top:4px;line-height:1.35;">Listed <strong style="color:#e2e8f0;">${rl || '—'}</strong> · collector ladder (ordinal)</div>`;
        } else if (id === 'setVintageChart') {
            secondaryLabel = 'Sqrt-capped set age (axis)';
            secondaryVal = Number.isFinite(raw.x) ? raw.x.toFixed(3) : '—';
            const escMini = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
            const rd = raw.setReleaseDate ? escMini(raw.setReleaseDate) : '';
            const rawY = raw.setVintageRawYears;
            const rawStr = rawY != null && Number.isFinite(rawY) ? rawY.toFixed(2) : '—';
            tertiary = `<div style="font-size:0.7rem;color:#64748b;margin-top:4px;line-height:1.35;">Release <code style="font-size:0.66rem;">${rd || '—'}</code> · <strong style="color:#e2e8f0;">${rawStr}</strong> y since release (raw)</div>`;
        } else if (id === 'artistChaseChart') {
            secondaryLabel = 'ln(1 + artist chase median $)';
            secondaryVal = Number.isFinite(raw.x) ? raw.x.toFixed(3) : '—';
            const med = raw.artistChaseMedian;
            const cnt = raw.artistChaseCount;
            const disp = raw.artistDisplayName ? escTip(String(raw.artistDisplayName)) : '';
            tertiary = `<div style="font-size:0.7rem;color:#64748b;margin-top:4px;line-height:1.35;">Illustrator <strong style="color:#cbd5e1;">${disp || '—'}</strong> · global chase median <strong style="color:#e2e8f0;">$${Number.isFinite(med) ? med.toFixed(2) : '—'}</strong>${cnt != null && Number.isFinite(cnt) ? ` · ${cnt} chase card(s) in scrape` : ''}</div>`;
        } else if (id === 'gradedPopChart') {
            secondaryLabel = 'log₁₀(1 + PSA-style pop.)';
            secondaryVal = Number.isFinite(raw.x) ? raw.x.toFixed(3) : '—';
            tertiary = '';
        } else if (id === 'tcgMacroInterestChart') {
            secondaryLabel = 'Macro hobby index (release year)';
            secondaryVal = Number.isFinite(raw.x) ? raw.x.toFixed(2) : '—';
            const y = raw.tcgMacroReleaseYear;
            const ser = raw.tcgMacroSeriesLabel ? escTip(String(raw.tcgMacroSeriesLabel)) : '';
            tertiary = `<div style="font-size:0.7rem;color:#64748b;margin-top:4px;line-height:1.35;">Set release year <strong style="color:#e2e8f0;">${y != null && Number.isFinite(y) ? String(Math.round(y)) : '—'}</strong> · same X for all cards from that year</div>${ser ? `<div style="font-size:0.64rem;color:#64748b;margin-top:4px;line-height:1.35;">Series: <span style="color:#cbd5e1;">${ser}</span></div>` : ''}`;
        } else if (id === 'trendsChart') {
            secondaryLabel = 'Popularity (axis, slab-adjusted)';
            secondaryVal = Number.isFinite(raw.x) ? raw.x.toFixed(2) : '—';
            const trBase = raw.trendsPopularityAxisBase;
            const tg = raw.trendGoogleAvg;
            const sm = raw.surveyMeanAvg;
            const sp = raw.surveyPollMax;
            const szv = raw.sparklezVotesAvg;
            const szAdj = raw.sparklezSyntheticAvg;
            let matchedDetail = '';
            if (!raw.trendsDisplayImputed && Number.isFinite(tg) && tg > 0) {
                matchedDetail = `<div style="font-size:0.68rem;color:#64748b;margin-top:4px;line-height:1.35;">Google Trends (file): <strong style="color:#e2e8f0;">${tg.toFixed(1)}</strong>`;
                if (sm != null && Number.isFinite(sm)) {
                    matchedDetail += ` · Multi-poll mean adj. rank: <strong style="color:#e2e8f0;">${sm.toFixed(2)}</strong> (≤${Number.isFinite(sp) ? sp : 0} poll cols / species)`;
                }
                if (szv != null && Number.isFinite(szv) && szv > 0) {
                    matchedDetail += ` · Sparklez 2025 vote total (variants summed to base species): <strong style="color:#e2e8f0;">${szv.toFixed(0)}</strong>`;
                    if (szAdj != null && Number.isFinite(szAdj)) {
                        matchedDetail += ` · Sparklez synth. adj.: <strong style="color:#e2e8f0;">${szAdj.toFixed(2)}</strong>`;
                    }
                }
                matchedDetail += '</div>';
            }
            if (raw.trendsDisplayImputed) {
                tertiary = `<div style="font-size:0.65rem;color:#c4b5fd;margin-top:4px;line-height:1.35;"><strong>Imputed</strong> X: no match in <code>google_trends_momentum.json</code> or <code>species_popularity_list.json</code>. Uses this set’s baseline × a small name-based jitter so the card still plots (composite excludes unmatched rows).</div>`;
            } else {
                tertiary = matchedDetail || '<div style="font-size:0.7rem;color:#64748b;margin-top:4px;">From popularity file (matched tokens).</div>';
            }
            if (raw.trendsHasGlobalGraded === true
                && Number.isFinite(raw.trendsGradedSpeciesNorm)
                && Number.isFinite(raw.trendsGradedBlendScale)) {
                const pct = (raw.trendsGradedSpeciesNorm * 100).toFixed(1);
                const fileStr = Number.isFinite(raw.trendsSourceRaw) && raw.trendsSourceRaw > 0
                    ? `File popularity avg <strong style="color:#e2e8f0;">${raw.trendsSourceRaw.toFixed(1)}</strong> · `
                    : '';
                tertiary += `<div style="font-size:0.64rem;color:#7dd3fc;margin-top:5px;line-height:1.35;">${fileStr}Graded species share vs export max: <strong>${pct}%</strong> · axis <strong>×${raw.trendsGradedBlendScale.toFixed(3)}</strong> (graded nudge blend ${POPULARITY_GRADED_TO_AXIS_BLEND}, floor ${POPULARITY_SPECIES_GRADED_FLOOR}).</div>`;
            }
            if (trBase != null && Number.isFinite(trBase) && Number.isFinite(raw.x) && Math.abs(trBase - raw.x) > 0.02) {
                const sm = raw.trendsSetMedianLogGradedPop;
                const lp = raw.trendsCardLogGradedPop;
                const medStr = sm != null && Number.isFinite(sm) ? sm.toFixed(2) : '—';
                const lpStr = lp != null && Number.isFinite(lp) ? lp.toFixed(2) : '—';
                tertiary += `<div style="font-size:0.64rem;color:#a5f3fc;margin-top:5px;line-height:1.35;">Per-card slab nudge: base <strong>${trBase.toFixed(1)}</strong> → axis <strong>${raw.x.toFixed(1)}</strong> · log₁₀(1+graded) <strong>${lpStr}</strong> vs set median <strong>${medStr}</strong>.</div>`;
            }
        } else {
            secondaryLabel = 'Trend index';
            secondaryVal = raw.x.toFixed(2);
        }

        const statBlock = `<div style="margin-top:6px;font-size:0.78rem;line-height:1.35;"><span style="color:#94a3b8;">${escTip(secondaryLabel)}</span> <strong style="color:#e2e8f0;">${secondaryVal}</strong></div>`;
        let pinnedToolbarHtml = '';
        if (pinnedInteractions) {
            const explorerHref = buildExplorerCardDetailHref(raw);
            const exBtn = explorerHref
                ? `<a class="analytics-tooltip-btn" href="${explorerHref}" target="_blank" rel="noopener noreferrer">Explorer details</a>`
                : '<span class="analytics-tooltip-btn analytics-tooltip-btn--disabled" title="Missing set code on this row">Explorer details</span>';
            pinnedToolbarHtml = `<div class="scatter-pin-toolbar">${exBtn}</div>`;
        }
        const html = `<div style="display:grid;grid-template-columns:76px minmax(0,1fr);gap:10px;padding:10px;max-width:${tipW};align-items:start;text-align:left;">
            <div>${imgThumb}</div>
            <div style="min-width:0;">
                <div style="font-weight:700;color:#60a5fa;font-size:0.88rem;line-height:1.2;">${nameEsc}</div>
                ${metaOneLine}
                ${priceProvLine}
                ${tcgMetaLine}
                ${liquidityLine}
                ${productFlagsLine}
                ${gradedPopLine}
                ${charSeriesLine}
                ${statBlock}
                ${tertiary}
                ${lsrlC}
            </div></div>`;
        return { html, width: tipW, raw, pinnedToolbarHtml };
    }

    function setScatterTooltipDomPosition(chart, tooltipEl, raw) {
        const canvas = chart.canvas;
        const parent = canvas.parentNode;
        if (!parent || !tooltipEl || !raw) return;
        try {
            const px = chart.scales.x.getPixelForValue(raw.x);
            const py = chart.scales.y.getPixelForValue(raw.y);
            const pad = 10;
            const yOff = 15;
            tooltipEl.style.marginLeft = '0px';
            tooltipEl.style.transform = 'translate(-50%, 0)';
            tooltipEl.style.top = `${canvas.offsetTop + py + yOff}px`;
            const centerPx = canvas.offsetLeft + px;
            const w = tooltipEl.offsetWidth || 280;
            const half = w / 2;
            const leftMin = pad + half;
            const leftMax = Math.max(leftMin, parent.clientWidth - pad - half);
            const cx = Math.min(Math.max(centerPx, leftMin), leftMax);
            tooltipEl.style.left = `${cx}px`;
        } catch (e) {
            tooltipEl.style.left = `${canvas.offsetLeft + canvas.width / 2}px`;
            tooltipEl.style.top = `${canvas.offsetTop + 24}px`;
            tooltipEl.style.transform = 'translate(-50%, 0)';
        }
    }

    function refreshPinnedScatterTooltip(chart) {
        if (!pinnedScatterSelection || pinnedScatterSelection.chart !== chart) return;
        const { datasetIndex, dataIndex } = pinnedScatterSelection;
        const wrap = chart.canvas.parentNode;
        const tooltipEl = wrap.querySelector('div.custom-tooltip');
        if (!tooltipEl) return;
        const built = buildScatterCardTooltipHtml(chart, datasetIndex, dataIndex, { pinnedInteractions: true });
        if (!built) {
            clearPinnedScatterCard();
            return;
        }
        const pinBar = '<div class="custom-tooltip-pinbar">Pinned — click empty chart area, outside charts, or press Esc to dismiss</div>';
        const pinTools = built.pinnedToolbarHtml || '';
        tooltipEl.innerHTML = pinBar + pinTools + built.html;
        tooltipEl.style.width = built.width;
        tooltipEl.style.maxWidth = chart.canvas && chart.canvas.id === 'compositeChart' ? 'min(96vw, 520px)' : '';
        tooltipEl.style.boxSizing = 'border-box';
        tooltipEl.classList.add('custom-tooltip--pinned');
        requestAnimationFrame(() => {
            setScatterTooltipDomPosition(chart, tooltipEl, built.raw);
        });
        tooltipEl.style.opacity = '1';
        tooltipEl.style.pointerEvents = 'auto';
    }

    function bindScatterToolbarDelegation() {
        if (bindScatterToolbarDelegation.done) return;
        bindScatterToolbarDelegation.done = true;
        document.addEventListener('click', (e) => {
            const btn = e.target.closest('.js-scatter-driver-toggle');
            if (!btn || !btn.closest('.custom-tooltip')) return;
            e.preventDefault();
            const wrap = btn.closest('.custom-tooltip');
            const panel = wrap && wrap.querySelector('.js-scatter-driver-panel');
            if (!panel) return;
            const opening = panel.hasAttribute('hidden');
            if (opening) panel.removeAttribute('hidden');
            else panel.setAttribute('hidden', '');
            btn.setAttribute('aria-expanded', opening ? 'true' : 'false');
            if (pinnedScatterSelection && pinnedScatterSelection.chart) {
                const ch = pinnedScatterSelection.chart;
                const { datasetIndex, dataIndex } = pinnedScatterSelection;
                const ds = ch.data.datasets[datasetIndex];
                const rawPt = ds && ds.data ? ds.data[dataIndex] : null;
                const wrap = ch.canvas.parentNode;
                const te = wrap && wrap.querySelector('div.custom-tooltip');
                if (rawPt && te) {
                    requestAnimationFrame(() => setScatterTooltipDomPosition(ch, te, rawPt));
                }
            }
        }, false);
    }

    function bindScatterCardPinGlobalListeners() {
        if (bindScatterCardPinGlobalListeners.done) return;
        bindScatterCardPinGlobalListeners.done = true;
        document.addEventListener('click', (e) => {
            if (!pinnedScatterSelection) return;
            if (e.target.closest('.custom-tooltip')) return;
            if (e.target.closest('.chart-canvas-container')) return;
            clearPinnedScatterCard();
        }, false);
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') clearPinnedScatterCard();
        });
    }

    function setupChartPinVisibilityObserver() {
        if (typeof IntersectionObserver === 'undefined') return;
        if (chartPinVisibilityObserver) chartPinVisibilityObserver.disconnect();
        chartPinVisibilityObserver = new IntersectionObserver((entries) => {
            entries.forEach((en) => {
                if (en.isIntersecting) return;
                if (!pinnedScatterSelection) return;
                if (pinnedScatterSelection.chart.canvas.parentNode === en.target) {
                    clearPinnedScatterCard();
                }
            });
        }, { threshold: 0, rootMargin: '0px' });
        allChartKeys().forEach((k) => {
            const ch = charts[k];
            if (ch && ch.canvas && ch.canvas.parentNode) {
                chartPinVisibilityObserver.observe(ch.canvas.parentNode);
            }
        });
    }

    /**
     * Composite: X ∈ [COMPOSITE_AXIS_X_MIN, COMPOSITE_AXIS_X_MAX], log Y max COMPOSITE_AXIS_Y_MAX_USD; `bounds:'data'` so Chart.js does not widen log axes to outer tick values.
     * `applyScatterAxisFit('composite')` runs after rebuilds, but `chart.update('none')` from thumb tint / overlay repaint
     * does not — Chart can otherwise widen the scale from datasets (including LSRL endpoints).
     */
    const scatterCompositeAxisLockPlugin = {
        id: 'scatterCompositeAxisLock',
        beforeUpdate(chart) {
            if (!chart || chart.canvas?.id !== 'compositeChart') return;
            const sx = chart.options.scales && chart.options.scales.x;
            const sy = chart.options.scales && chart.options.scales.y;
            if (!sx || !sy) return;
            sx.min = COMPOSITE_AXIS_X_MIN;
            sx.max = COMPOSITE_AXIS_X_MAX;
            sx.bounds = 'data';
            sx.beginAtZero = false;
            sx.grace = 0;
            sy.type = 'logarithmic';
            sy.bounds = 'data';
            sy.beginAtZero = false;
            sy.max = COMPOSITE_AXIS_Y_MAX_USD;
            sy.grace = 0;
            const ymin = Number(sy.min);
            if (!Number.isFinite(ymin) || ymin <= 0) {
                sy.min = Math.max(1e-12, chartScatterMinUsd() * 0.82);
            }
        }
    };

    const scatterImageOverlayPlugin = {
        id: 'scatterImageOverlay',
        afterDatasetsDraw(chart) {
            if (!chart || chart.config.type !== 'scatter') return;
            const ca = chart.chartArea;
            if (!ca || ca.width <= 0 || ca.height <= 0) return;
            const ctx = chart.ctx;
            ctx.save();
            ctx.beginPath();
            ctx.rect(ca.left, ca.top, ca.width, ca.height);
            ctx.clip();
            const ring = SCATTER_THUMB_RING;
            const datasets = chart.data.datasets;
            const drawnPixels = [];
            const R = SCATTER_POINT_RADIUS;
            try {
            for (let dsi = datasets.length - 1; dsi >= 0; dsi--) {
                const ds = datasets[dsi];
                if (isLsrlDataset(ds) || !ds.data) continue;
                const meta = chart.getDatasetMeta(dsi);
                if (!meta || meta.hidden || !meta.data) continue;
                for (let index = 0; index < meta.data.length; index++) {
                    try {
                        const element = meta.data[index];
                        if (!element || element.skip) continue;
                        const raw = ds.data[index];
                        if (!raw || !raw.card || !raw.card.image_url) continue;
                        const cacheKey = `${raw.card.image_url}_${ring}`;
                        const img = imageCache.get(cacheKey);
                        if (!img || !(img instanceof HTMLImageElement) || !img.complete || !img.naturalWidth) {
                            continue;
                        }
                        const pos = typeof element.tooltipPosition === 'function'
                            ? element.tooltipPosition()
                            : { x: element.x, y: element.y };
                        if (!pos || !Number.isFinite(pos.x) || !Number.isFinite(pos.y)) continue;
                        let occluded = false;
                        for (let j = 0; j < drawnPixels.length; j++) {
                            const dx = pos.x - drawnPixels[j].x;
                            const dy = pos.y - drawnPixels[j].y;
                            if (dx * dx + dy * dy < R * R) { occluded = true; break; }
                        }
                        if (occluded) continue;
                        drawnPixels.push({ x: pos.x, y: pos.y });
                        let r = SCATTER_POINT_RADIUS;
                        if (Number.isFinite(Number(element.options?.radius))) {
                            r = Number(element.options.radius);
                        } else if (Number.isFinite(Number(element.width)) && element.width > 0) {
                            r = element.width / 2;
                        }
                        const ir = Math.max(1, r - 2);
                        const nw = img.naturalWidth || img.width;
                        const nh = img.naturalHeight || img.height;
                        const srcH = Math.max(1, Math.floor(nh / 2));
                        ctx.save();
                        ctx.beginPath();
                        ctx.arc(pos.x, pos.y, ir, 0, Math.PI * 2);
                        ctx.clip();
                        ctx.drawImage(img, 0, 0, nw, srcH, pos.x - r, pos.y - r, 2 * r, 2 * r);
                        ctx.restore();
                        ctx.beginPath();
                        ctx.arc(pos.x, pos.y, ir, 0, Math.PI * 2);
                        ctx.lineWidth = 3;
                        ctx.strokeStyle = ring;
                        ctx.stroke();
                    } catch (_e) {
                        /* CORS-tainted draw can throw; skip this point */
                    }
                }
            }
            } finally {
                ctx.restore();
            }
        }
    };

    const scatterPinnedTooltipSync = {
        id: 'scatterPinnedTooltipSync',
        afterUpdate(chart) {
            if (pinnedScatterSelection && pinnedScatterSelection.chart === chart) {
                requestAnimationFrame(() => refreshPinnedScatterTooltip(chart));
            }
            const cid = chart && chart.canvas && chart.canvas.id;
            const k = cid ? chartCanvasIdToChartKey(cid) : null;
            if (k) scheduleScatterThumbLayout(k);
        }
    };

    const externalTooltipHandler = (context) => {
        const { chart, tooltip } = context;
        let tooltipEl = chart.canvas.parentNode.querySelector('div.custom-tooltip');
        if (!tooltipEl) {
            tooltipEl = document.createElement('div');
            tooltipEl.classList.add('custom-tooltip');
            tooltipEl.style.background = 'rgba(15, 23, 42, 0.9)';
            tooltipEl.style.border = '1px solid rgba(255,255,255,0.1)';
            tooltipEl.style.borderRadius = '12px';
            tooltipEl.style.color = '#fff';
            tooltipEl.style.opacity = 0;
            tooltipEl.style.pointerEvents = 'none';
            tooltipEl.style.position = 'absolute';
            tooltipEl.style.transform = 'translate(-50%, 0)';
            tooltipEl.style.transition = 'opacity .2s ease';
            tooltipEl.style.backdropFilter = 'blur(10px)';
            tooltipEl.style.boxShadow = '0 10px 30px rgba(0,0,0,0.5)';
            tooltipEl.style.zIndex = '100';
            tooltipEl.style.width = '220px';
            tooltipEl.style.maxWidth = 'min(92vw, 320px)';
            chart.canvas.parentNode.appendChild(tooltipEl);
        }

        if (tooltip.opacity === 0) {
            if (pinnedScatterSelection && pinnedScatterSelection.chart === chart) {
                refreshPinnedScatterTooltip(chart);
                return;
            }
            tooltipEl.style.opacity = 0;
            tooltipEl.style.pointerEvents = 'none';
            tooltipEl.classList.remove('custom-tooltip--pinned');
            return;
        }

        if (!tooltip.dataPoints || !tooltip.dataPoints.length) return;

        const dp0 = tooltip.dataPoints[0];
        if (pinnedScatterSelection && pinnedScatterSelection.chart === chart) {
            if (dp0.datasetIndex !== pinnedScatterSelection.datasetIndex || dp0.dataIndex !== pinnedScatterSelection.dataIndex) {
                refreshPinnedScatterTooltip(chart);
                return;
            }
        }

        const raw = dp0.raw;
        if (!raw || !raw.card) {
            tooltipEl.style.opacity = 0;
            return;
        }

        const built = buildScatterCardTooltipHtml(chart, dp0.datasetIndex, dp0.dataIndex);
        if (!built) {
            tooltipEl.style.opacity = 0;
            return;
        }

        tooltipEl.innerHTML = built.html;
        tooltipEl.style.width = built.width;
        if (!(pinnedScatterSelection && pinnedScatterSelection.chart === chart)) {
            tooltipEl.classList.remove('custom-tooltip--pinned');
            tooltipEl.style.pointerEvents = 'none';
        }
        requestAnimationFrame(() => {
            if (tooltip.opacity === 0) return; // mouse moved out before frame fired
            setScatterTooltipDomPosition(chart, tooltipEl, built.raw);
            tooltipEl.style.opacity = '1';
        });
    };

    function createChart(id, xTitle, yTitle) {
        const canvas = document.getElementById(id);
        if (!canvas) {
            throw new Error(`Missing canvas #${id}`);
        }
        const ctx = canvas.getContext('2d');
        const isComposite = id === 'compositeChart';
        const xScaleBase = { title: { display: true, text: xTitle, color: '#94a3b8', font: { size: 12, weight: '600' } }, beginAtZero: true, grid: { color: 'rgba(255, 255, 255, 0.05)' }, ticks: { color: '#64748b', font: { size: 10 } } };
        const yScaleBase = { title: { display: true, text: yTitle, color: '#94a3b8', font: { size: 12, weight: '600' } }, beginAtZero: true, grid: { color: 'rgba(255, 255, 255, 0.05)' }, ticks: { color: '#64748b', font: { size: 10 } } };
        const xScale = isComposite
            ? {
                ...xScaleBase,
                min: COMPOSITE_AXIS_X_MIN,
                max: COMPOSITE_AXIS_X_MAX,
                bounds: 'data',
                beginAtZero: false,
                grace: 0
            }
            : xScaleBase;
        const yScale = isComposite
            ? {
                ...yScaleBase,
                type: 'logarithmic',
                min: Math.max(1e-12, chartScatterMinUsd() * 0.82),
                max: COMPOSITE_AXIS_Y_MAX_USD,
                bounds: 'data',
                beginAtZero: false,
                grace: 0
            }
            : yScaleBase;
        return new Chart(ctx, {
            type: 'scatter',
            data: { datasets: [] },
            plugins: [scatterPinnedTooltipSync, scatterCompositeAxisLockPlugin, scatterImageOverlayPlugin],
            options: {
                responsive: true,
                maintainAspectRatio: false,
                resizeDelay: 64,
                events: ['mousemove', 'mouseout', 'click', 'touchstart', 'touchmove'],
                interaction: {
                    mode: 'nearest',
                    intersect: true,
                    axis: 'xy'
                },
                layout: {
                    padding: { left: 26, right: 14, top: 12, bottom: 26 }
                },
                plugins: {
                    legend: { display: false },
                    tooltip: { enabled: false, external: externalTooltipHandler }
                },
                scales: {
                    x: xScale,
                    y: yScale
                },
                onClick(_ev, activeEls, ch) {
                    if (!activeEls || !activeEls.length) {
                        if (pinnedScatterSelection && pinnedScatterSelection.chart === ch) clearPinnedScatterCard();
                        return;
                    }
                    const el = activeEls[0];
                    const ds = ch.data.datasets[el.datasetIndex];
                    if (!ds || isLsrlDataset(ds)) return;
                    const raw = ds.data[el.index];
                    if (!raw || !raw.card) return;
                    const same = pinnedScatterSelection
                        && pinnedScatterSelection.chart === ch
                        && pinnedScatterSelection.datasetIndex === el.datasetIndex
                        && pinnedScatterSelection.dataIndex === el.index;
                    if (same) {
                        clearPinnedScatterCard();
                        return;
                    }
                    clearPinnedScatterCard();
                    pinnedScatterSelection = { chart: ch, datasetIndex: el.datasetIndex, dataIndex: el.index };
                    refreshPinnedScatterTooltip(ch);
                }
            }
        });
    }

    function configurePullCostChartAxes() {
        const ch = charts.pullCost;
        if (!ch) return;
        ch.options.scales.y.beginAtZero = false;
    }

    function configureCompositeLogY() {
        const ch = charts.composite;
        if (!ch || !ch.options.scales || !ch.options.scales.y) return;
        const sx = ch.options.scales.x;
        const sy = ch.options.scales.y;
        sx.min = COMPOSITE_AXIS_X_MIN;
        sx.max = COMPOSITE_AXIS_X_MAX;
        sx.bounds = 'data';
        sx.beginAtZero = false;
        sx.grace = 0;
        sy.type = 'logarithmic';
        sy.bounds = 'data';
        sy.beginAtZero = false;
        sy.max = COMPOSITE_AXIS_Y_MAX_USD;
        sy.min = Math.max(1e-12, chartScatterMinUsd() * 0.82);
        sy.grace = 0;
        if (sy.title) sy.title.text = 'Market price ($, log scale)';
        if (sy.ticks) {
            sy.ticks.callback = (value) => {
                const v = Number(value);
                if (!Number.isFinite(v)) return String(value);
                if (!isLogScale125TimesPow10(v)) return '';
                if (v >= 1000) return `$${Math.round(v).toLocaleString()}`;
                if (v >= 100) return `$${v.toFixed(0)}`;
                if (v >= 10) return `$${v.toFixed(1)}`;
                return `$${v.toFixed(2)}`;
            };
        }
    }

    document.querySelectorAll('.lsrl-toggle').forEach(cb => {
        cb.addEventListener('change', () => {
            const chartId = cb.getAttribute('data-chart');
            const chart = charts[chartId];
            if (!chart) return;
            const lineDs = chart.data.datasets.find(isLsrlDataset);
            if (!lineDs) return;
            lineDs.hidden = !cb.checked;
            chart.update();
        });
    });

    const SPECIES_POPULARITY_RENDER_CAP = 450;
    let speciesPopularityFilterWired = false;

    function fmtPopularityCell(x, decimals = 2) {
        if (x == null || !Number.isFinite(Number(x))) return '—';
        return Number(x).toFixed(decimals);
    }

    async function fetchJsonOptionalDocument(filename, fetchOpts) {
        const url = SHARED_UTILS.resolveDataAssetUrl(filename);
        try {
            const r = await fetch(url, fetchOpts);
            if (!r.ok) {
                console.warn('fetch not ok', url, r.status);
                return null;
            }
            return await r.json();
        } catch (e) {
            console.warn('fetch failed', url, e);
            return null;
        }
    }

    function renderSpeciesPopularityPanel(doc) {
        const meta = document.getElementById('speciesPopularityMeta');
        const tbody = document.getElementById('speciesPopularityTbody');
        const filt = document.getElementById('speciesPopularityFilter');
        if (!tbody) return;
        if (!doc || !Array.isArray(doc.species)) {
            tbody.innerHTML = '<tr><td colspan="7">No <code>data/assets/species_popularity_list.json</code> — run <code>python scrape/build_species_popularity_index.py</code> in the project root.</td></tr>';
            if (meta) meta.textContent = '';
            return;
        }
        const n = doc.species_count != null ? Number(doc.species_count) : doc.species.length;
        if (meta) {
            const built = doc.built_at_utc != null ? escapeLoadingHtml(String(doc.built_at_utc)) : '—';
            const med = doc.median_trend_index_for_list_imputation != null
                ? escapeLoadingHtml(String(doc.median_trend_index_for_list_imputation))
                : '—';
            meta.innerHTML = `<strong>${n}</strong> species · built <code>${built}</code> · median Trend (list imputer) <strong>${med}</strong> · showing up to <strong>${SPECIES_POPULARITY_RENDER_CAP}</strong> filtered rows.`;
        }
        const applyFilter = () => {
            const q = (filt && filt.value ? filt.value : '').trim().toLowerCase();
            const rows = doc.species.filter((r) => {
                if (!q) return true;
                const nm = String(r.display_name || '').toLowerCase();
                const sk = String(r.species_key || '').toLowerCase();
                return nm.includes(q) || sk.includes(q);
            });
            const slice = rows.slice(0, SPECIES_POPULARITY_RENDER_CAP);
            const rowsHtml = slice.map((r) => {
                const nm = escapeLoadingHtml(String(r.display_name || ''));
                const inTcg = r.in_tcg_trends_file ? 'Yes' : '';
                const sz = r.sparklez_vote_total != null && Number.isFinite(Number(r.sparklez_vote_total))
                    ? String(Number(r.sparklez_vote_total))
                    : '—';
                return `<tr>
                    <td>${r.rank != null ? escapeLoadingHtml(String(r.rank)) : '—'}</td>
                    <td>${nm}</td>
                    <td>${fmtPopularityCell(r.popularity_index_sort)}</td>
                    <td>${fmtPopularityCell(r.trend_index_average)}</td>
                    <td>${fmtPopularityCell(r.panel_survey_adj_mean)}</td>
                    <td>${escapeLoadingHtml(sz)}</td>
                    <td>${inTcg ? 'Yes' : '—'}</td>
                </tr>`;
            }).join('');
            let foot = '';
            if (rows.length > SPECIES_POPULARITY_RENDER_CAP) {
                foot = `<tr class="analytics-popularity-cap-note"><td colspan="7"><em>Showing first ${SPECIES_POPULARITY_RENDER_CAP} of ${rows.length} matches — narrow the filter.</em></td></tr>`;
            }
            tbody.innerHTML = rowsHtml + foot;
        };
        if (filt && !speciesPopularityFilterWired) {
            speciesPopularityFilterWired = true;
            filt.addEventListener('input', () => applyFilter());
        }
        applyFilter();
    }

    function escapeLoadingHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    async function loadAllData() {
        const pg = typeof window.PTCG_PAGE_PROGRESS !== 'undefined' ? window.PTCG_PAGE_PROGRESS : null;
        if (pg) pg.begin();
        try {
            const fetchOpts = { cache: 'no-store' };

            /** Optional JSON: missing / bad file yields [] (304 and other non-OK must not wipe silently for required files) */
            const fetchJsonOptional = (path) => {
                const url = SHARED_UTILS.resolveDataAssetUrl(path);
                return fetch(url, fetchOpts)
                    .then(r => {
                        if (!r.ok) {
                            console.warn('fetch not ok', url, r.status);
                            return [];
                        }
                        return r.json().catch((e) => {
                            console.warn('JSON parse failed', url, e);
                            return [];
                        });
                    })
                    .catch((e) => {
                        console.warn('fetch failed', url, e);
                        return [];
                    });
            };

            async function fetchRequiredJsonArray(path, label) {
                const url = SHARED_UTILS.resolveDataAssetUrl(path);
                const r = await fetch(url, fetchOpts);
                if (!r.ok) {
                    throw new Error(`${label}: HTTP ${r.status} ${r.statusText || ''} for ${url}`);
                }
                let data;
                try {
                    data = await r.json();
                } catch (e) {
                    throw new Error(`${label}: invalid JSON from ${url} (${e.message})`);
                }
                if (!Array.isArray(data)) {
                    throw new Error(`${label}: expected a JSON array at ${url}`);
                }
                return data;
            }

            const sets = await fetchPokemonSetsFromSupabase();
            if (pg) pg.setDeterminate(0.28);
            const [characters, trends, artists, nostalgia, speciesPopularityDoc, tcgMacroDoc] = await Promise.all([
                fetchJsonOptional('character_premium_scores.json'),
                fetchJsonOptional('google_trends_momentum.json'),
                fetchJsonOptional('artist_scores.json'),
                fetchJsonOptional('nostalgia_index.json'),
                fetchJsonOptionalDocument('species_popularity_list.json', fetchOpts),
                fetchJsonOptionalDocument('tcg_macro_interest_by_year.json', fetchOpts)
            ]);
            allSetsData = sets.reverse();
            updateAnalyticsGemrateStrip();
            rebuildSpeciesGradedPopFromExport();
            characterData = Array.isArray(characters) ? characters : [];
            nostalgiaData = (nostalgia && typeof nostalgia === 'object' && !Array.isArray(nostalgia)) ? nostalgia : {};
            trendsData = Array.isArray(trends) ? trends : [];
            artistChaseData = Array.isArray(artists) ? artists : [];
            buildArtistChaseLookup(artistChaseData);
            ingestSpeciesPopularityList(speciesPopularityDoc);
            ingestTcgMacroInterestDoc(tcgMacroDoc);
            if (pg) pg.setDeterminate(0.52);

            if (allSetsData.length === 0) throw new Error('No sets available');

            const setOptsFrag = document.createDocumentFragment();
            allSetsData.forEach((set, index) => {
                const opt = document.createElement('option');
                opt.value = index;
                opt.innerText = set.set_name;
                setOptsFrag.appendChild(opt);
            });
            addSetSelect.appendChild(setOptsFrag);

            initAnalyticsYearSlidersFromData();
            wireAnalyticsDataControls();
            window.showCardFromAnalyticsSearch = (name, setNm) => {
                // Find the point in the composite chart specifically
                const ch = charts.composite;
                if (!ch || !ch.data || !Array.isArray(ch.data.datasets)) return;
                
                let found = null;
                ch.data.datasets.forEach((ds, dsIdx) => {
                    if (found || isLsrlDataset(ds) || !ds.data) return;
                    ds.data.forEach((p, pIdx) => {
                        if (found) return;
                        if (p && p.card && String(p.card.name) === name && String(p.analyticsSetName) === setNm) {
                            found = { datasetIndex: dsIdx, dataIndex: pIdx };
                        }
                    });
                });
                
                if (found) {
                    clearPinnedScatterCard();
                    pinnedScatterSelection = { chart: ch, datasetIndex: found.datasetIndex, dataIndex: found.dataIndex };
                    refreshPinnedScatterTooltip(ch);
                    // Scroll to the composite chart
                    const canvas = document.getElementById('compositeChart');
                    if (canvas) canvas.scrollIntoView({ behavior: 'smooth', block: 'center' });
                }
            };

            wireAnalyticsCardSearch();
            if (pg) pg.setDeterminate(0.62);

            charts.pullCost = createChart('pullCostChart', 'Market price ($)', 'ln(market / effective pull)');
            configurePullCostChartAxes();
            charts.composite = createChart('compositeChart', 'Composite signal (z-blend)', 'Chart market price ($)');
            configureCompositeLogY();
            charts.character = createChart('characterChart', 'High-tier print volume', 'Chart market price ($)');
            charts.trends = createChart('trendsChart', 'Popularity (Trends + slabs vs set)', 'Chart market price ($)');
            charts.rarityTier = createChart('rarityTierChart', 'Print class (rarity ladder)', 'Chart market price ($)');
            charts.setVintage = createChart('setVintageChart', 'Sqrt-capped set age', 'Chart market price ($)');
            charts.tcgMacroInterest = createChart('tcgMacroInterestChart', 'Hobby-wide interest (release year)', 'Chart market price ($)');
            charts.gradedPop = createChart('gradedPopChart', 'log₁₀(1 + graded pop.)', 'Chart market price ($)');
            charts.hypeScarcity = createChart('hypeScarcityChart', 'Popularity × scarcity (pull + slabs)', 'Chart market price ($)');
            charts.hypePullRatio = createChart('hypePullRatioChart', 'Blended popularity ÷ ln(pull+1)', 'Chart market price ($)');
            charts.artistChase = createChart('artistChaseChart', 'ln(1 + artist chase median $)', 'Chart market price ($)');
            wireAnalyticsScatterTicksOnce();

            const bandLo = Math.min(yearFilterLo, yearFilterHi);
            const bandHi = Math.max(yearFilterLo, yearFilterHi);
            const bandPick = pickAllSetIndicesInYearRange(bandLo, bandHi);
            if (bandPick.length > 0) {
                bandPick.forEach((i) => {
                    addedSetIndices.add(i);
                    const option = Array.from(addSetSelect.options).find((o) => parseInt(o.value, 10) === i);
                    if (option) option.disabled = true;
                });
            } else {
                const initialPick = pickInitialAnalyticsSetIndices(allSetsData.length, Math.min(10, allSetsData.length));
                initialPick.forEach((i) => {
                    addedSetIndices.add(i);
                    const option = Array.from(addSetSelect.options).find((o) => parseInt(o.value, 10) === i);
                    if (option) option.disabled = true;
                });
            }

            allChartKeys().forEach(key => {
                charts[key].data.datasets.push(makeLsrlDataset());
            });
            if (pg) pg.setDeterminate(0.78);

            await new Promise((resolve) => rebuildAllScatterDatasets(resolve));
            if (pg) pg.setDeterminate(0.94);
            attachScatterChartInteractions();
            bindScatterCardPinGlobalListeners();
            bindScatterToolbarDelegation();

            const secondaryDetails = document.getElementById('analyticsSecondaryCharts');
            if (secondaryDetails && !secondaryDetails.dataset.resizeWired) {
                secondaryDetails.dataset.resizeWired = '1';
                secondaryDetails.addEventListener('toggle', () => {
                    if (!secondaryDetails.open) return;
                    requestAnimationFrame(() => {
                        secondaryChartOrder.forEach((k) => {
                            const ch = charts[k];
                            if (ch && typeof ch.resize === 'function') ch.resize();
                        });
                    });
                });
            }
            setupChartPinVisibilityObserver();

            try {
                await runInitialScatterThumbPreload();
            } catch (e) {
                console.warn('initial scatter thumb preload', e);
            } finally {
                reapplyScatterPointTintsFromPreloadCache();
            }

            titleEl.innerText = 'Comparative Analytics Dashboard';
            updateAnalyticsDashboardMeta();
            if (pg) pg.setDeterminate(1);
            loadingEl.style.display = 'none';
            if (loadingEl) loadingEl.setAttribute('aria-busy', 'false');
            containerEl.style.display = 'block';

            renderSpeciesPopularityPanel(speciesPopularityDoc);

            requestAnimationFrame(() => {
                requestAnimationFrame(() => {
                    allChartKeys().forEach(k => {
                        if (charts[k]) charts[k].resize();
                    });
                });
            });

            addSetSelect.addEventListener('change', (e) => {
                const index = parseInt(e.target.value, 10);
                if (isNaN(index) || addedSetIndices.has(index)) return;
                addedSetIndices.add(index);
                e.target.options[e.target.selectedIndex].disabled = true;
                rebuildChartsWithBusy('Adding set and rebuilding charts…', resizeAllChartsAfterRebuild);
            });
        } catch (error) {
            console.error(error);
            const tried = SHARED_UTILS.resolveDataAssetUrl('pokemon_sets_data.json');
            const detail = error && error.message ? escapeLoadingHtml(error.message) : String(error);
            loadingEl.innerHTML = `<p style="color:#ef4444;">Failed to load datasets.</p><p style="color:#fca5a5;font-size:0.9rem;margin-top:0.5rem;font-weight:600;">${detail}</p><p style="color:#94a3b8;font-size:0.85rem;margin-top:0.5rem;">Sets URL: <code style="word-break:break-all;">${escapeLoadingHtml(tried)}</code></p>`;
        } finally {
            if (pg) pg.end();
        }
    }

    loadAllData().then(() => {
        setTimeout(() => {
            const qU = new URL(window.location.href);
            const sqName = qU.searchParams.get('select_card_name');
            const sqSet = qU.searchParams.get('select_card_set');
            if (sqName && sqSet) {
                const targetSetIdx = allSetsData.findIndex(s => s.set_name === sqSet);
                if (targetSetIdx !== -1 && !addedSetIndices.has(targetSetIdx)) {
                    addedSetIndices.add(targetSetIdx);
                    rebuildChartsWithBusy('Adding set from predictor redirect…', () => {
                        // Allow chart render to settle before pinning
                        setTimeout(() => window.showCardFromAnalyticsSearch(sqName, sqSet), 300);
                    });
                } else {
                    window.showCardFromAnalyticsSearch(sqName, sqSet);
                }
            }
        }, 800);
    });
});
