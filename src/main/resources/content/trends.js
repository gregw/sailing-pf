'use strict';

// Trend lines shared by the boat and design comparison charts.
const Trends = (function () {

    // A result's weight for averaging: its optimiser weight, never negative; 1 if it has none.
    function weightOf(e) {
        return e.weight != null && isFinite(e.weight) ? Math.max(e.weight, 0) : 1;
    }

    // Weighted mean of backCalcFactor; the plain mean if every weight is 0.
    function weightedMean(entries) {
        let sw = 0, swv = 0;
        entries.forEach(e => {
            const w = weightOf(e);
            sw += w;
            swv += w * e.backCalcFactor;
        });
        return sw > 0 ? swv / sw
            : entries.reduce((a, e) => a + e.backCalcFactor, 0) / entries.length;
    }

    // Sliding average over each entry's last n finishes (by date), as {x: dates, y: values}, or
    // null with fewer than 2 entries. Of the n, the `drops` lowest factors — the worst races
    // (a higher factor is a faster race) — are dropped, and the rest averaged weighted by
    // each result's weight, so a result the optimiser has down-weighted as an outlier moves
    // the line only as much as it moves the PF.
    //
    // With a seed (the PF, or RF for designs), n virtual entries at the seed fill the window
    // before the first race, so the line starts at the seed rather than at the first result.
    // They weigh as much as a typical result of this boat (the mean weight of its entries).
    function slidingAverage(entries, n, drops, seed) {
        if (entries.length < 2) return null;
        const pts = [...entries].sort((a, b) => a.date.localeCompare(b.date));
        const keep = Math.max(1, n - (drops || 0));
        const seedWeight = pts.reduce((a, p) => a + weightOf(p), 0) / pts.length;
        const virtual = seed != null
            ? Array.from({length: n}, () => ({backCalcFactor: seed, weight: seedWeight}))
            : [];
        const xs = [], ys = [];
        for (let i = 0; i < pts.length; i++) {
            const realWindow = pts.slice(Math.max(0, i - n + 1), i + 1);
            const pad = virtual.slice(0, n - realWindow.length);   // virtual entries that still fit
            const used = [...pad, ...realWindow]
                .sort((a, b) => b.backCalcFactor - a.backCalcFactor)   // best first
                .slice(0, keep);
            xs.push(pts[i].date);
            ys.push(weightedMean(used));
        }
        return {x: xs, y: ys};
    }

    return {slidingAverage};
})();

if (typeof window !== 'undefined') window.Trends = Trends;
if (typeof module !== 'undefined' && module.exports) module.exports = Trends;
