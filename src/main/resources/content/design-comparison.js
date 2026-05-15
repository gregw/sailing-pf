'use strict';

/*
 * Multi-design comparison — mirrors comparison.js (boats) with design-level semantics:
 *   - single selector adds any number of designs to a chip list
 *   - BCF chart draws each design's back-calc dots/trends across all member-boat residuals,
 *     each design filtered to its own per-design variant; when an RF or allocated-handicap
 *     set is selected in the calculator the dots are divided by it (no PF for designs)
 *   - shared HandicapCalc module (design mode): allocated handicap sets + RF + Best Fit,
 *     per-design variant dropdowns, compare-checkbox pair selection. No fetch/save.
 *   - the two designs ticked in the compare column feed the elapsed-time chart
 */

const PALETTE = [
    '#3a7ec4', '#e67e22', '#27ae60', '#8e44ad',
    '#c0392b', '#16a085', '#d35400', '#2c3e50',
    '#f39c12', '#1abc9c'
];

const STORAGE_KEY = 'pf-designComparison-items';
const HANDICAP_STORAGE_KEY = 'pf.design.allocated.handicaps';

let selectedItems = [];   // {type:'design', id, label, color, initialVariant?}
let allAvailable    = false;
let showRfLine       = true;
let showTrendLinear  = true;
let showTrendSliding = true;
let hideLegend       = false;
let recentMonths = 0;     // 0 = all time, otherwise filter to last N months
let showCommonRacesOnly = false;
let slidingAverageCount = 8;
let slidingAverageDrops = 0;
let candidateDesigns = [];
let focusedDesignId  = null;
let designDebounce   = null;
let lastChartData    = null;
// Cached /api/design-comparison/chart response for the current pair so per-design
// variant changes re-filter without hitting the server. Keyed by `${idA}|${idB}`.
let elapsedChartCache = {key: null, data: null};
// Last best-fit slope for the elapsed chart. Updated every renderElapsedChart so
// "Use Best Fit" can re-apply the displayed value.
let lastElapsedFit = null;

function nextColor() {
    return PALETTE[selectedItems.length % PALETTE.length];
}

// ---- Session storage ----

function saveSelection() {
    try { sessionStorage.setItem(STORAGE_KEY, JSON.stringify(selectedItems)); } catch (e) {}
}

function restoreSelection() {
    try {
        const saved = sessionStorage.getItem(STORAGE_KEY);
        if (saved) selectedItems = JSON.parse(saved).filter(i => i.type === 'design');
    } catch (e) {}
}

// ---- Candidate loading ----

async function loadCandidates() {
    const q = document.getElementById('design-search').value.trim();
    const designIds = selectedItems.map(i => i.id);

    const params = new URLSearchParams();
    if (q)                params.set('q',            q);
    if (designIds.length) params.set('designIds',    designIds.join(','));
    if (allAvailable)     params.set('allAvailable', 'true');

    const data = await fetchJson('/api/design-comparison/candidates?' + params);
    if (!data) return;
    candidateDesigns = data.designs || [];
    renderDesignList();
}

function renderDesignList() {
    const list = document.getElementById('design-list');
    list.innerHTML = '';
    if (candidateDesigns.length === 0) {
        const el = document.createElement('div');
        el.className = 'selector-empty';
        el.textContent = 'No designs found';
        list.appendChild(el);
        document.getElementById('add-design-btn').disabled = true;
        return;
    }
    candidateDesigns.forEach(d => {
        const div = document.createElement('div');
        div.className = 'selector-item' + (d.id === focusedDesignId ? ' focused' : '');
        div.textContent = d.canonicalName || d.id;
        div.title = d.id;
        div.addEventListener('click', () => {
            focusedDesignId = d.id;
            renderDesignList();
            document.getElementById('add-design-btn').disabled = false;
        });
        div.addEventListener('dblclick', () => { focusedDesignId = d.id; addDesign(); });
        list.appendChild(div);
    });
    if (!candidateDesigns.find(d => d.id === focusedDesignId)) {
        focusedDesignId = null;
        document.getElementById('add-design-btn').disabled = true;
    }
}

// ---- Selection ----

function addDesign() {
    if (!focusedDesignId) return;
    const d = candidateDesigns.find(x => x.id === focusedDesignId);
    if (!d) return;
    selectedItems.push({
        type:  'design',
        id:    d.id,
        label: d.canonicalName || d.id,
        color: nextColor(),
        // Seed the calc's per-design variant for this new design from the table majority.
        initialVariant: majorityVariant()
    });
    focusedDesignId = null;
    document.getElementById('add-design-btn').disabled = true;
    document.getElementById('design-search').value = '';
    candidateDesigns = [];
    renderDesignList();
    renderChips();
    saveSelection();
    loadCandidates();
    loadChart();
}

function removeItem(idx) {
    selectedItems.splice(idx, 1);
    renderChips();
    saveSelection();
    loadCandidates();
    loadChart();
}

function clearAll() {
    selectedItems = [];
    renderChips();
    saveSelection();
    loadCandidates();
    loadChart();
}

function renderChips() {
    const container = document.getElementById('chip-list');
    container.innerHTML = '';
    selectedItems.forEach((item, idx) => {
        const chip = document.createElement('span');
        chip.className = 'chip';
        chip.style.borderColor = item.color;
        chip.style.color = item.color;
        chip.innerHTML = `${esc(item.label)} <button class="chip-close" onclick="removeItem(${idx})" title="Remove">×</button>`;
        container.appendChild(chip);
    });
    if (selectedItems.length > 0) {
        const btn = document.createElement('button');
        btn.className = 'chip-clear-all';
        btn.title = 'Remove all designs';
        btn.textContent = '✕ Clear all';
        btn.onclick = clearAll;
        container.appendChild(btn);
    }
}

// ---- Trend helpers ----

function weightedOlsTrend(entries) {
    if (entries.length < 3) return null;
    const toDay = s => Date.parse(s) / 86400000;
    const xs = entries.map(e => toDay(e.date));
    const ys = entries.map(e => e.backCalcFactor);
    const ws = entries.map(e => e.weight);
    const sw = ws.reduce((a, w) => a + w, 0);
    if (sw === 0) return null;
    const xb = xs.reduce((a, x, i) => a + ws[i] * x, 0) / sw;
    const yb = ys.reduce((a, y, i) => a + ws[i] * y, 0) / sw;
    const cov  = xs.reduce((a, x, i) => a + ws[i] * (x - xb) * (ys[i] - yb), 0);
    const varx = xs.reduce((a, x, i) => a + ws[i] * (x - xb) ** 2, 0);
    if (varx === 0) return null;
    const slope = cov / varx, intercept = yb - slope * xb;
    const x0 = Math.min(...xs), x1 = Math.max(...xs);
    const fromDay = d => new Date(d * 86400000).toISOString().slice(0, 10);
    return { x: [fromDay(x0), fromDay(x1)],
             y: [slope * x0 + intercept, slope * x1 + intercept] };
}

function slidingAverage(entries, n, drops, seed) {
    if (entries.length < 2) return null;
    const pts = [...entries].sort((a, b) => a.date.localeCompare(b.date));
    const xs = [], ys = [];
    const keep = Math.max(1, n - (drops || 0));
    const virtual = seed != null
        ? Array.from({ length: n }, () => ({ backCalcFactor: seed }))
        : [];
    for (let i = 0; i < pts.length; i++) {
        const realWindow = pts.slice(Math.max(0, i - n + 1), i + 1);
        const pad = virtual.slice(Math.max(0, n - i - 1));
        const window = [...pad, ...realWindow];
        const sorted = [...window].sort((a, b) => a.backCalcFactor - b.backCalcFactor);
        const used = sorted.slice(0, Math.min(keep, sorted.length));
        xs.push(pts[i].date);
        ys.push(used.reduce((a, p) => a + p.backCalcFactor, 0) / used.length);
    }
    return xs.length >= 2 ? { x: xs, y: ys } : null;
}

// ---- Variant helpers ----

// The page has no global variant selector — each design's variant lives in the
// handicap calculator's per-design dropdown. This reads it (defaulting to 'spin'
// before the calc exists).
function boatVariantFor(designId) {
    return dCalcController ? dCalcController.getBoatVariant(designId) : 'spin';
}

// Variant to seed a newly added design with: the majority variant among the designs
// already in the comparison, or 'spin' when there's no clear single majority.
function majorityVariant() {
    if (!dCalcController || selectedItems.length === 0) return 'spin';
    const counts = {spin: 0, nonSpin: 0};
    selectedItems.forEach(i => {
        const v = dCalcController.getBoatVariant(i.id);
        if (counts[v] != null) counts[v]++;
    });
    const ranked = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    if (ranked[0][1] === 0) return 'spin';                  // no designs counted
    if (ranked[0][1] === ranked[1][1]) return 'spin';       // tie → no clear majority
    return ranked[0][0];
}

function rfVariantFor(design, variant) {
    return variant === 'nonSpin' ? design.rfNonSpin : design.rfSpin;
}

function filterByVariant(entries, variant) {
    return entries.filter(e =>
        variant === 'nonSpin' ? e.nonSpinnaker : !e.nonSpinnaker && !e.twoHanded
    );
}

function filterEntries(entries, variant) {
    let result = filterByVariant(entries, variant);
    if (recentMonths > 0) {
        const cutoff = new Date();
        cutoff.setMonth(cutoff.getMonth() - recentMonths);
        const cutoffStr = cutoff.toISOString().slice(0, 10);
        result = result.filter(e => e.date >= cutoffStr);
    }
    return result;
}

// Active divisor for the BCF chart, when the calc's RF or a set "show" tickbox is on
// (singleSelectShow ensures at most one). Designs have no PF. Returns null when no
// divisor is active. perBoat values mirror Factor.applyInverse — the chart plots
// y' = e.backCalcFactor / value and intensity weight w' = e.weight × weight.
function computeBcfDivisor(data) {
    const calc = dCalcController;
    if (!calc) return null;
    if (calc.getShowRf()) {
        const perBoat = new Map();
        (data.designs || []).forEach(d => {
            const f = rfVariantFor(d, calc.getBoatVariant(d.id));
            if (f) perBoat.set(d.id, {value: f.value, weight: f.weight});
        });
        return {label: 'RF', perBoat};
    }
    const activeSet = calc.getAllSets().find(s => s.show);
    if (activeSet && activeSet.values.size > 0) {
        const perBoat = new Map();
        // Allocated entries are user-typed — treat as full confidence.
        activeSet.values.forEach((v, id) => perBoat.set(id, {value: v, weight: 1}));
        return {label: activeSet.name, perBoat};
    }
    return null;
}

// ---- Main back-calc chart ----

async function loadChart() {
    const designIds = selectedItems.map(i => i.id);
    if (designIds.length === 0) {
        Plotly.purge('comparison-chart');
        lastChartData = null;
        document.getElementById('pf-calc').style.display = 'none';
        document.getElementById('elapsed-charts-section').style.display = 'none';
        return;
    }
    const params = new URLSearchParams({ designIds: designIds.join(',') });
    const data = await fetchJson('/api/comparison/chart?' + params);
    if (!data) return;
    lastChartData = data;
    renderChart(data);
    loadElapsedCharts();
}

function renderChart(data) {
    // Calc first: it seeds each design's per-design variant (boatVariants), which
    // renderBcfChart then reads for entry filtering and RF lookup.
    renderHandicapCalc(data);
    renderBcfChart(data);
}

// BCF chart only — no calc reset. Safe to call from inside the calc's onChange,
// because it never invokes setBoats / recalc and so cannot re-fire onChange.
function renderBcfChart(data) {
    const traces = [];
    const designs = data.designs || [];

    // Pre-compute filtered entries per design (per-design variant + recent-months).
    const filteredPerDesign = new Map(
        designs.map(d => [d.id, filterEntries(d.entries || [], boatVariantFor(d.id))]));

    if (showCommonRacesOnly && designs.length >= 2) {
        const sets = designs.map(d => new Set(filteredPerDesign.get(d.id).map(e => e.raceId)));
        const common = sets.reduce((acc, s) => new Set([...acc].filter(id => s.has(id))));
        designs.forEach(d =>
            filteredPerDesign.set(d.id, filteredPerDesign.get(d.id).filter(e => common.has(e.raceId))));
    }

    // When a divisor is active, dots are plotted as BCF / divisor (mirrors
    // Factor.applyInverse). Designs with no divisor entry are skipped entirely.
    const divisor = computeBcfDivisor(data);

    let minDate = null, maxDate = null;
    designs.forEach(d => {
        if (divisor && !divisor.perBoat.has(d.id)) return;
        filteredPerDesign.get(d.id).forEach(e => {
            if (!minDate || e.date < minDate) minDate = e.date;
            if (!maxDate || e.date > maxDate) maxDate = e.date;
        });
    });
    const lineX = minDate
        ? [minDate, maxDate]
        : ['2018-01-01', new Date().toISOString().slice(0, 10)];

    const plottedYs = [];

    designs.forEach(design => {
        const div = divisor ? divisor.perBoat.get(design.id) : null;
        if (divisor && !div) return;   // design has no divisor entry → skip

        const item  = selectedItems.find(i => i.id === design.id);
        const color = item ? item.color : '#888';
        const name  = item ? item.label : (design.canonicalName || design.id);

        const variant = boatVariantFor(design.id);
        const rfFactor = rfVariantFor(design, variant);

        // RF line is about the unscaled domain — hide it while a divisor is active.
        if (showRfLine && rfFactor && !divisor) {
            traces.push({
                x: lineX, y: [rfFactor.value, rfFactor.value],
                type: 'scatter', mode: 'lines',
                name: `${name} RF`,
                line: { color, dash: 'dashdot', width: 1.5 },
                legendgroup: design.id,
                hovertemplate: `${esc(name)} RF: %{y:.4f}<extra></extra>`
            });
        }

        // Apply divisor to entries (Factor.applyInverse semantics: divide value,
        // multiply weights). The trend line implicitly uses the scaled values too.
        const rawEntries = filteredPerDesign.get(design.id);
        const entries = div
            ? rawEntries.map(e => ({
                ...e,
                backCalcFactor: e.backCalcFactor / div.value,
                weight: e.weight * div.weight
            }))
            : rawEntries;
        if (entries.length > 0) {
            const xs = [], ys = [], sizes = [], opacities = [], symbols = [], texts = [], custom = [];
            entries.forEach(e => {
                const w = Math.min(Math.max(e.weight, 0), 1);
                xs.push(e.date);
                ys.push(e.backCalcFactor);
                plottedYs.push(e.backCalcFactor);
                sizes.push(4 + 6 * w);
                opacities.push(parseFloat((0.35 + 0.65 * w).toFixed(2)));
                symbols.push(e.weight < 0.01 ? 'x' : 'circle');
                const boatLabel = e.boatSailNumber ? `${e.boatSailNumber} ${e.boatName}` : (e.boatName || e.boatId);
                texts.push(
                    `${esc(name)}<br>` +
                    (boatLabel ? `${esc(boatLabel)}<br>` : '') +
                    (e.seriesName ? `${esc(e.seriesName)}<br>` : '') +
                    (e.raceName   ? `${esc(e.raceName)}<br>`   : '') +
                    `${e.date} — ${esc(e.division)}<br>` +
                    `Factor: ${e.backCalcFactor.toFixed(4)}<br>` +
                    `Weight: ${e.weight.toFixed(3)}`
                );
                custom.push({
                    raceId: e.raceId, divisionName: e.division,
                    seriesId: e.seriesId, seriesName: e.seriesName
                });
            });
            traces.push({
                x: xs, y: ys,
                type: 'scatter', mode: 'markers',
                name,
                marker: { color, size: sizes, opacity: opacities, symbol: symbols,
                    line: { color: 'rgba(0,0,0,0.3)', width: 0.5 } },
                text: texts,
                customdata: custom,
                hoverinfo: 'text',
                showlegend: false,
                legendgroup: design.id
            });

            if (showTrendLinear) {
                const t = weightedOlsTrend(entries);
                if (t) traces.push({
                    x: t.x, y: t.y, type: 'scatter', mode: 'lines',
                    name: `${name} linear trend`,
                    line: { color, dash: 'dash', width: 1.5 },
                    legendgroup: design.id,
                    hovertemplate: `${esc(name)} linear trend: %{y:.4f}<extra></extra>`
                });
            }
            // Sliding average is hidden in divisor mode — its seed value (RF) lives in
            // the unscaled domain and would skew the early window.
            if (showTrendSliding && !divisor) {
                const rfSeed = rfFactor ? rfFactor.value : null;
                const s = slidingAverage(entries, slidingAverageCount, slidingAverageDrops, rfSeed);
                const best = slidingAverageCount - slidingAverageDrops;
                const avgLabel = slidingAverageDrops > 0
                    ? `best ${best} of ${slidingAverageCount} avg`
                    : `${slidingAverageCount}-finish avg`;
                if (s) traces.push({
                    x: s.x, y: s.y, type: 'scatter', mode: 'lines',
                    name: `${name} ${avgLabel}`,
                    line: { color, dash: 'dot', width: 1.5 },
                    legendgroup: design.id,
                    hovertemplate: `${esc(name)} ${avgLabel}: %{y:.4f}<extra></extra>`
                });
            }
        }
    });

    // Y-range: padded to the plotted (post-divisor) values; keep the historic
    // min-[0.5,1.5] clamp only when no divisor is active (ratios cluster near 1.0).
    let yRange;
    if (plottedYs.length > 0) {
        let lo = Math.min(...plottedYs), hi = Math.max(...plottedYs);
        const pad = (hi - lo) * 0.05 + 0.02;
        lo = Math.floor((lo - pad) * 20) / 20;
        hi = Math.ceil((hi + pad) * 20) / 20;
        yRange = divisor ? [lo, hi] : [Math.min(0.5, lo), Math.max(1.5, hi)];
    } else if (!divisor) {
        yRange = [0.5, 1.5];
    }

    const yTitle = divisor ? `Factor ratio: BCF / ${divisor.label}` : 'Factor';
    const layout = {
        xaxis: { title: 'Date', type: 'date' },
        yaxis: yRange ? {title: yTitle, range: yRange} : {title: yTitle},
        showlegend: !hideLegend,
        legend: {orientation: 'v', xanchor: 'left', x: 0},
        margin: { t: 20, b: 60, l: 60, r: 20 },
        hovermode: 'closest'
    };

    const chartDiv = document.getElementById('comparison-chart');
    Plotly.react('comparison-chart', traces, layout, { responsive: true });

    chartDiv.removeAllListeners && chartDiv.removeAllListeners('plotly_click');
    chartDiv.on('plotly_click', (eventData) => {
        if (!eventData.points || !eventData.points.length) return;
        const pt = eventData.points[0];
        if (!pt.customdata) return;
        const { raceId, seriesId } = pt.customdata;
        // seriesId wins: filter races by series; otherwise open the single race detail.
        if (seriesId) {
            window.location.href = 'races.html?' + new URLSearchParams({seriesId});
        } else if (raceId) {
            window.location.href = 'races.html?' + new URLSearchParams({id: raceId});
        }
    });
}

// ---- Handicap calculator (thin adapter over shared HandicapCalc module, design mode) ----

let dCalcController = null;

function dCalc() {
    if (dCalcController) return dCalcController;
    dCalcController = HandicapCalc.create({
        entityKind: 'design',
        section: document.getElementById('pf-calc'),
        table: document.querySelector('#pf-calc table'),
        sessionKey: HANDICAP_STORAGE_KEY,
        compareSelect: true,
        compareMax: 2,
        // Design page repurposes the RF / per-set "show" tickboxes as a single divisor
        // selector for the BCF chart, so at most one may be ticked.
        singleSelectShow: true,
        onCompareSelectionChange: () => loadElapsedCharts(),
        onChange: () => {
            // Re-render the BCF chart so a divisor toggle (or its clearing) is reflected.
            // Calls the BCF-only renderer — calling renderChart here would re-enter
            // renderHandicapCalc → setBoats → recalc → onChange (infinite recursion).
            if (lastChartData) renderBcfChart(lastChartData);
            // A per-design variant change in the calc table fires onChange too; the
            // cached elapsed payload lets this re-render without hitting the API.
            const elapsedSection = document.getElementById('elapsed-charts-section');
            if (elapsedSection && elapsedSection.style.display !== 'none')
                loadElapsedCharts();
        }
    });
    return dCalcController;
}

function renderHandicapCalc(data) {
    const designs = data.designs || [];
    const showBestFit = designs.length <= 8;

    const calcBoats = designs.map(d => {
        const item  = selectedItems.find(i => i.id === d.id);
        const color = item ? item.color : '#888';
        const name  = item ? item.label : (d.canonicalName || d.id);

        // Per-design variant: a known design keeps the variant the calc already tracks;
        // a new design takes its seeded variant if any, else the table majority.
        const variant = item?.initialVariant
            || (dCalcController ? dCalcController.getBoatVariant(d.id) : null)
            || majorityVariant();

        const rfFactor = rfVariantFor(d, variant);

        let bestFit = null;
        if (showBestFit) {
            const entries = filterEntries(d.entries || [], variant);
            const trend = weightedOlsTrend(entries);
            if (trend) bestFit = trend.y[1];
        }

        return {
            id: d.id, name, color,
            boatName: name,
            variant,
            rfAll: {
                spin: d.rfSpin ? d.rfSpin.value : null,
                nonSpin: d.rfNonSpin ? d.rfNonSpin.value : null,
            },
            rfWeightAll: {
                spin: d.rfSpin ? d.rfSpin.weight : null,
                nonSpin: d.rfNonSpin ? d.rfNonSpin.weight : null,
            },
            rf: rfFactor ? rfFactor.value : null,
            rfWeight: rfFactor ? rfFactor.weight : null,
            bestFit
        };
    });

    dCalc().setBoats(calcBoats, {showBestFit});
}

// ---- Elapsed time comparison chart ----

// Render the elapsed-time chart for the pair currently ticked in the handicap
// calculator's compare column. Shows the section when exactly two designs are ticked;
// hides it otherwise. Each point's finishers are filtered to the variant currently
// selected for each design, then medianed.
async function loadElapsedCharts() {
    const section = document.getElementById('elapsed-charts-section');
    const container = document.getElementById('elapsed-charts-container');

    if (!dCalcController) {
        section.style.display = 'none';
        container.innerHTML = '';
        return;
    }
    const selected = [...dCalcController.getCompareSelection()];
    if (selected.length !== 2) {
        section.style.display = 'none';
        container.innerHTML = '';
        return;
    }

    const [idA, idB] = selected;
    const itemA = selectedItems.find(i => i.id === idA);
    const itemB = selectedItems.find(i => i.id === idB);
    if (!itemA || !itemB) {
        section.style.display = 'none';
        container.innerHTML = '';
        return;
    }

    section.style.display = '';

    const key = `${idA}|${idB}`;
    let data;
    if (elapsedChartCache.key === key && elapsedChartCache.data) {
        data = elapsedChartCache.data;
    } else {
        const params = new URLSearchParams({designAId: idA, designBId: idB});
        data = await fetchJson('/api/design-comparison/chart?' + params);
        if (!data) return;
        elapsedChartCache = {key, data};
    }

    const variantA = dCalcController.getBoatVariant(idA);
    const variantB = dCalcController.getBoatVariant(idB);

    // Re-use the existing chart container when it's already for this pair — avoids a
    // Plotly tear-down/recreate on every onChange tick (handicap edit / variant flip).
    let titleEl = container.querySelector('.elapsed-chart-title');
    let chartDiv = container.querySelector('#elapsed-chart-0');
    let bestFitBtn = container.querySelector('#elapsed-use-best-fit');
    if (!chartDiv || container.dataset.pairKey !== key) {
        container.innerHTML = '';
        container.dataset.pairKey = key;
        const wrapper = document.createElement('div');
        wrapper.style.marginBottom = '1.5rem';
        titleEl = document.createElement('div');
        titleEl.className = 'elapsed-chart-title';
        titleEl.style.cssText = 'font-weight:bold;margin-bottom:0.25rem;';
        wrapper.appendChild(titleEl);
        chartDiv = document.createElement('div');
        chartDiv.id = 'elapsed-chart-0';
        chartDiv.style.cssText = 'width:100%;height:500px;';
        wrapper.appendChild(chartDiv);
        // "Use Best Fit" — pushes the best-fit slope into the focused-set handicaps
        // for designs A and B (geometric-mean-balanced against the other anchors).
        bestFitBtn = document.createElement('button');
        bestFitBtn.id = 'elapsed-use-best-fit';
        bestFitBtn.type = 'button';
        bestFitBtn.style.cssText = 'margin-top:0.5rem;padding:6px 14px;cursor:pointer;';
        bestFitBtn.addEventListener('click', applyBestFitToHandicaps);
        wrapper.appendChild(bestFitBtn);
        container.appendChild(wrapper);
    }
    titleEl.textContent = `${itemA.label} vs ${itemB.label}`;
    renderElapsedChart('elapsed-chart-0', data, itemA.color, itemB.color, variantA, variantB);

    // Refresh the button's label / enabled state to reflect the current fit.
    const slope = lastElapsedFit?.slope;
    if (bestFitBtn) {
        if (slope && isFinite(slope) && slope > 0) {
            const ratio = 1 / slope;
            bestFitBtn.disabled = false;
            bestFitBtn.textContent = `Use Best Fit (handicap ratio ${ratio.toFixed(4)})`;
            bestFitBtn.title = `Set ${itemA.label} and ${itemB.label} handicaps so their ratio equals `
                + `${ratio.toFixed(4)} (best-fit elapsed-time slope ${slope.toFixed(4)}), `
                + `balanced against other handicaps in the focused column.`;
            bestFitBtn.dataset.idA = idA;
            bestFitBtn.dataset.idB = idB;
            bestFitBtn.dataset.ratio = String(ratio);
        } else {
            bestFitBtn.disabled = true;
            bestFitBtn.textContent = 'Use Best Fit — not enough data';
            bestFitBtn.title = 'Need at least two co-raced points to compute a best-fit slope';
        }
    }
}

function applyBestFitToHandicaps() {
    const btn = document.getElementById('elapsed-use-best-fit');
    if (!btn || btn.disabled || !dCalcController) return;
    const idA = btn.dataset.idA;
    const idB = btn.dataset.idB;
    const ratio = parseFloat(btn.dataset.ratio);
    if (!idA || !idB || !isFinite(ratio) || ratio <= 0) return;
    dCalcController.applyPairwiseFit(idA, idB, ratio);
}

/** Re-renders the elapsed-time chart without refetching; used by the From-0 toggle. */
function onElapsedFromZeroChange() {
    loadElapsedCharts();
}

function median(nums) {
    if (!nums.length) return null;
    const s = [...nums].sort((a, b) => a - b);
    const n = s.length;
    return n % 2 ? s[(n - 1) / 2] : (s[n / 2 - 1] + s[n / 2]) / 2;
}

function renderElapsedChart(divId, data, colorA, colorB, variantA, variantB) {
    let rawPoints = data.points || [];

    // Apply recent-months filter if active
    if (recentMonths > 0) {
        const cutoff = new Date();
        cutoff.setMonth(cutoff.getMonth() - recentMonths);
        const cutoffStr = cutoff.toISOString().slice(0, 10);
        rawPoints = rawPoints.filter(p => p.date >= cutoffStr);
    }

    // Each point carries per-design finisher lists; median only the finishes of each
    // design's currently-selected variant. Drop points where either side has none.
    const points = rawPoints.map(p => {
        const aSel = (p.aFinishers || []).filter(f => f.variant === variantA);
        const bSel = (p.bFinishers || []).filter(f => f.variant === variantB);
        if (!aSel.length || !bSel.length) return null;
        return {
            ...p,
            y: median(aSel.map(f => f.elapsed)),
            x: median(bSel.map(f => f.elapsed)),
            aBoats: aSel.map(f => f.name),
            bBoats: bSel.map(f => f.name),
        };
    }).filter(Boolean);

    if (points.length === 0) {
        Plotly.purge(divId);
        lastElapsedFit = null;
        return;
    }

    const xs = points.map(p => p.x / 3600);
    const ys = points.map(p => p.y / 3600);

    const nameA = data.designA.canonicalName;
    const nameB = data.designB.canonicalName;

    const texts = points.map(p =>
        `${esc(p.date || '')}<br>` +
        (p.seriesName ? `${esc(p.seriesName)}<br>` : '') +
        (p.raceName   ? `${esc(p.raceName)}<br>`   : '') +
        `${esc(p.division || '')}<br>` +
        `${esc(nameA)}: ${fmtTime(p.y)}  (${(p.aBoats || []).map(esc).join(', ')})<br>` +
        `${esc(nameB)}: ${fmtTime(p.x)}  (${(p.bBoats || []).map(esc).join(', ')})`
    );
    const customdata = points.map(p => ({ raceId: p.raceId }));

    const traces = [];
    traces.push({
        x: xs, y: ys,
        type: 'scatter', mode: 'markers',
        name: 'Co-raced divisions',
        marker: { color: colorA, size: 7, opacity: 0.75,
                  line: { color: 'rgba(0,0,0,0.3)', width: 0.5 } },
        text: texts, hoverinfo: 'text',
        customdata
    });

    const xMin = Math.min(...xs), xMax = Math.max(...xs);
    const xPad = (xMax - xMin) * 0.05 || xMin * 0.05;
    const x0 = 0, x1 = xMax + xPad;

    const fit = linearFitElapsed(xs, ys);
    lastElapsedFit = fit;
    if (fit) {
        traces.push({
            x: [x0, x1],
            y: [0, fit.slope * x1],
            type: 'scatter', mode: 'lines',
            name: `Best fit (slope ${fit.slope.toFixed(4)})`,
            line: { color: colorA, width: 2 }
        });
    }

    // Expected RF ratio line (through origin) — each design under its own variant.
    const rfA = rfVariantFor(data.designA, variantA);
    const rfB = rfVariantFor(data.designB, variantB);
    if (rfA && rfB && rfA.value && rfB.value) {
        const slope = rfB.value / rfA.value;
        traces.push({
            x: [x0, x1],
            y: [0, slope * x1],
            type: 'scatter', mode: 'lines',
            name: `RF ratio (${rfB.value.toFixed(4)} / ${rfA.value.toFixed(4)} = ${slope.toFixed(4)})`,
            line: { color: colorB, width: 2, dash: 'dot' }
        });
    }

    const fromZero = document.getElementById('elapsed-from-zero')?.checked ?? true;
    const layout = {
        xaxis: { title: `${esc(nameB)} elapsed (h)`,
                 rangemode: fromZero ? 'tozero' : 'normal' },
        yaxis: { title: `${esc(nameA)} elapsed (h)`,
                 rangemode: fromZero ? 'tozero' : 'normal' },
        showlegend: !hideLegend,
        legend: { orientation: 'h', y: -0.2 },
        margin: { t: 20, b: hideLegend ? 70 : 100, l: 80, r: 20 },
        hovermode: 'closest'
    };

    Plotly.react(divId, traces, layout, { responsive: true });

    const chartDiv = document.getElementById(divId);
    chartDiv.removeAllListeners && chartDiv.removeAllListeners('plotly_click');
    chartDiv.on('plotly_click', (eventData) => {
        if (!eventData.points || !eventData.points.length) return;
        const pt = eventData.points[0];
        if (!pt.customdata || !pt.customdata.raceId) return;
        window.location.href = 'races.html?' + new URLSearchParams({id: pt.customdata.raceId});
    });
}

function linearFitElapsed(xs, ys) {
    const n = xs.length;
    if (n < 2) return null;
    let num = 0, den = 0;
    for (let i = 0; i < n; i++) {
        num += xs[i] * ys[i];
        den += xs[i] * xs[i];
    }
    if (den === 0) return null;
    return { slope: num / den, intercept: 0 };
}

function fmtTime(secs) {
    if (secs == null) return '—';
    const h = Math.floor(secs / 3600);
    const m = Math.floor((secs % 3600) / 60);
    const s = Math.round(secs % 60);
    if (h > 0) return `${h}h ${m}m ${s}s`;
    return `${m}m ${s}s`;
}

// ---- Init ----

async function loadConfig() {
    const data = await fetchJson('/api/importers');
    if (data && data.slidingAverageCount) slidingAverageCount = data.slidingAverageCount;
    if (data && data.slidingAverageDrops != null) slidingAverageDrops = data.slidingAverageDrops;
}

document.addEventListener('DOMContentLoaded', async () => {
    await loadConfig();
    restoreSelection();
    renderChips();

    document.getElementById('all-available').addEventListener('change', e => {
        allAvailable = e.target.checked;
        loadCandidates();
    });
    document.getElementById('show-rf-line')       .addEventListener('change', e => { showRfLine        = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-trend-linear')  .addEventListener('change', e => { showTrendLinear   = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-trend-sliding') .addEventListener('change', e => { showTrendSliding  = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('hide-legend')        .addEventListener('change', e => { hideLegend        = e.target.checked; if (lastChartData) renderChart(lastChartData); loadElapsedCharts(); });
    document.getElementById('recent-months').addEventListener('change', e => {
        recentMonths = parseInt(e.target.value, 10) || 0;
        if (lastChartData) renderChart(lastChartData);
        loadElapsedCharts();
    });
    document.getElementById('common-races-only')  .addEventListener('change', e => { showCommonRacesOnly = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('design-search').addEventListener('input', () => {
        clearTimeout(designDebounce);
        designDebounce = setTimeout(loadCandidates, 250);
    });
    document.getElementById('add-design-btn').addEventListener('click', addDesign);
    loadCandidates();
    if (selectedItems.length > 0) loadChart();
});
