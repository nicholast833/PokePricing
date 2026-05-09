/**
 * regression.js - statistical engine for Pokémon TCG price models
 */

const REGRESSION_ENGINE = {
    COMPOSITE_KEYS: [
        'pullCost',
        'charVol',
        'trends',
        'rarityTier',
        'setAge',
        'tcgMacro',
        'gradedPop',
        'hypeScarcity',
        'hypePullRatio',
        'artistChase',
        'pcGradedUsedRatio',
        'pcChaseSlabPremium',
    ],

    weightedMeanStd(xs, ws) {
        if (!xs.length) return null;
        let sumW = 0;
        let sumWX = 0;
        for (let i = 0; i < xs.length; i++) {
            sumW += ws[i];
            sumWX += ws[i] * xs[i];
        }
        if (sumW <= 0) return null;
        const mean = sumWX / sumW;
        let sumVar = 0;
        for (let i = 0; i < xs.length; i++) {
            sumVar += ws[i] * Math.pow(xs[i] - mean, 2);
        }
        const std = Math.sqrt(sumVar / sumW);
        return { mean, std };
    },

    weightedPearsonR(xs, ys, ws) {
        if (xs.length < 2) return 0;
        const msX = REGRESSION_ENGINE.weightedMeanStd(xs, ws);
        const msY = REGRESSION_ENGINE.weightedMeanStd(ys, ws);
        if (!msX || !msY || msX.std === 0 || msY.std === 0) return 0;
        let sumCov = 0;
        let sumW = 0;
        for (let i = 0; i < xs.length; i++) {
            sumCov += ws[i] * (xs[i] - msX.mean) * (ys[i] - msY.mean);
            sumW += ws[i];
        }
        return sumCov / (sumW * msX.std * msY.std);
    },

    fitWeightedLinearYOnX(xs, ys, ws) {
        if (xs.length < 2) return null;
        const msX = REGRESSION_ENGINE.weightedMeanStd(xs, ws);
        const msY = REGRESSION_ENGINE.weightedMeanStd(ys, ws);
        const r = REGRESSION_ENGINE.weightedPearsonR(xs, ys, ws);
        if (msX.std === 0) return { b0: msY.mean, b1: 0, r };
        const b1 = r * (msY.std / msX.std);
        const b0 = msY.mean - b1 * msX.mean;
        const r2 = r * r;
        return { b0, b1, r, r2 };
    },

    /** Calculate the composite signal Z-score blend for a set of driver features. */
    compositeScoreFromRow(features, model) {
        if (!model || !model.keys || !model.keys.length) return null;
        let sumZ = 0;
        let sumW = 0;
        model.keys.forEach((k) => {
            const v = features[k];
            if (v == null || !Number.isFinite(v)) return;
            const mean = model.means[k];
            const std = model.stds[k];
            const r = model.r[k];
            if (std === 0 || r == null) return;
            const z = (v - mean) / std;
            sumZ += z * r;
            sumW += Math.abs(r);
        });
        if (sumW === 0) return null;
        return sumZ / sumW;
    },

    /** Extract driver features from a card object for prediction. */
    extractFeatures(card, set, analyticsState) {
        const feat = {};
        
        // 1. pullCost - Robust parsing for "1 in N" strings
        let rate = card.card_pull_rate;
        if (typeof rate === 'string') {
            const match = rate.match(/1 in ([\d,.]+)/);
            if (match) {
                const n = parseFloat(match[1].replace(/,/g, ''));
                if (n > 0) rate = 1 / n;
            } else {
                rate = parseFloat(rate);
            }
        }
        if (Number.isFinite(rate) && rate > 0) {
            feat.pullCost = -Math.log(rate);
        }

        // 2. charVol
        const charData = analyticsState.characterData || [];
        const charHit = charData.find(c => c.species === card.species);
        feat.charVol = charHit ? charHit.volume_score : 0;

        // 3. trends (Popularity)
        const trends = analyticsState.trendsData || [];
        const tHit = trends.find(t => t.species === card.species);
        feat.trends = tHit && tHit.trends_score != null ? tHit.trends_score : 0;

        // 4. rarityTier
        feat.rarityTier = card.rarity_ordinal || 0;

        // 5. setAge
        if (set && set.release_date) {
            const rel = new Date(set.release_date);
            const now = new Date();
            const years = (now - rel) / (1000 * 60 * 60 * 24 * 365.25);
            feat.setAge = Math.sqrt(Math.max(0, years));
        }

        // 6. tcgMacro (flat year map, or nested { by_year } from tcg_macro_interest_by_year.json)
        if (set && set.release_date) {
            const yr = new Date(set.release_date).getFullYear();
            const macroRaw = analyticsState.tcgMacroInterest || {};
            const macro =
                macroRaw.by_year && typeof macroRaw.by_year === 'object' ? macroRaw.by_year : macroRaw;
            const yk = String(yr);
            const v = macro[yr] ?? macro[yk];
            feat.tcgMacro = v != null && Number.isFinite(Number(v)) ? Number(v) : 0;
        }

        // 7. gradedPop - Use shared helper for accuracy
        const popTotal = SHARED_UTILS.getCardGradedPopTotal(card);
        if (popTotal != null) feat.gradedPop = Math.log10(1 + Number(popTotal));

        // 8. hypeScarcity
        if (feat.trends != null && feat.pullCost != null) {
            feat.hypeScarcity = feat.trends * feat.pullCost;
        }

        // 9. hypePullRatio
        if (feat.trends != null && feat.pullCost != null) {
            feat.hypePullRatio = feat.trends / (feat.pullCost + 1);
        }

        // 10. artistChase
        const artists = analyticsState.artistChaseLookup || {};
        feat.artistChase = artists[card.artist] || 0;

        // 11–12. PriceCharting collector signals (when sync_pricecharting merged rows)
        const pcUsed = Number(card.pricecharting_used_price_usd);
        const pcGradedAgg = Number(card.pricecharting_graded_price_usd);
        if (Number.isFinite(pcUsed) && pcUsed > 0 && Number.isFinite(pcGradedAgg) && pcGradedAgg > 0) {
            feat.pcGradedUsedRatio = Math.log10(Math.max(1.001, pcGradedAgg / pcUsed));
        }
        const chaseGem = SHARED_UTILS.pricechartingChaseGradeUsd(card);
        if (chaseGem != null && Number.isFinite(pcUsed) && pcUsed > 0) {
            feat.pcChaseSlabPremium = Math.log10(Math.max(1.001, chaseGem / pcUsed));
        }

        return feat;
    }
};

window.REGRESSION_ENGINE = REGRESSION_ENGINE;
