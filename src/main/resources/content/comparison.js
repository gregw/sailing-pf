'use strict';

const PALETTE = [
    '#3a7ec4', '#e67e22', '#27ae60', '#8e44ad',
    '#c0392b', '#16a085', '#d35400', '#2c3e50',
    '#f39c12', '#1abc9c'
];

const STORAGE_KEY = 'pf-comparison-items';
const HANDICAP_STORAGE_KEY = 'pf.allocated.handicaps';

let selectedItems   = [];   // {type:'boat', id, label, color}
let allAvailable    = false;
let showErrorBars    = false;
let showRfLine       = true;
let showPfLine      = true;
let showTrendLinear  = true;
let showTrendSliding = true;
let hideLegend       = false;
let recentMonths = 0;     // 0 = all time, otherwise filter to last N months
let showCommonRacesOnly = false;
let slidingAverageCount = 8;
let slidingAverageDrops = 0;
let candidateBoats  = [];
let focusedBoatId   = null;
let boatDebounce    = null;
let lastChartData   = null;
let inlineDivisionData = null; // most recently loaded /api/comparison/division payload
const INLINE_DIV_XFACTOR_KEY = 'pf.inlineDiv.xFactor';
const INLINE_DIV_SHOW_ELAPSED_KEY = 'pf.inlineDiv.showElapsed';
let inlineDivXFactor = sessionStorage.getItem(INLINE_DIV_XFACTOR_KEY) || '---';
let inlineShowElapsed = sessionStorage.getItem(INLINE_DIV_SHOW_ELAPSED_KEY) !== 'false';
let inlineDivisionRaceId = null;
let inlineDivisionName = null;
let inlineDivisionSeriesId = null;
let inlineSeriesId = null;  // seriesId for which inlineSeriesRaces was fetched
let inlineSeriesRaces = null;  // [{raceId, raceName, date}] sorted by date, or null
// Cached /api/comparison/elapsed-chart response for the current pair so per-boat
// variant changes re-filter without hitting the server. Keyed by `${idA}|${idB}`.
let elapsedChartCache = {key: null, data: null};
// Last best-fit slope for the elapsed chart (Y/X = elapsedA/elapsedB). Updated every
// time renderElapsedChart runs so "Use Best Fit" can re-apply the displayed value.
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
        if (saved) selectedItems = JSON.parse(saved).filter(i => i.type === 'boat');
    } catch (e) {}
}

// ---- Candidate loading ----

async function loadCandidates() {
    const boatQ   = document.getElementById('boat-search').value.trim();
    const boatIds = selectedItems.map(i => i.id);

    const params = new URLSearchParams();
    if (boatQ)          params.set('boatQ',       boatQ);
    if (boatIds.length) params.set('boatIds',      boatIds.join(','));
    if (allAvailable)   params.set('allAvailable', 'true');

    const data = await fetchJson('/api/comparison/candidates?' + params);
    if (!data) return;
    candidateBoats = data.boats || [];
    renderBoatList();
}

function renderBoatList() {
    const list = document.getElementById('boat-list');
    list.innerHTML = '';
    if (candidateBoats.length === 0) {
        const el = document.createElement('div');
        el.className = 'selector-empty';
        el.textContent = 'No boats found';
        list.appendChild(el);
        document.getElementById('add-boat-btn').disabled = true;
        return;
    }
    candidateBoats.forEach(b => {
        const label = b.sailNumber ? `${b.sailNumber} ${b.name}` : b.name;
        const div = document.createElement('div');
        div.className = 'selector-item' + (b.id === focusedBoatId ? ' focused' : '');
        div.textContent = label;
        div.title = b.id;
        div.addEventListener('click', () => {
            focusedBoatId = b.id;
            renderBoatList();
            document.getElementById('add-boat-btn').disabled = false;
        });
        div.addEventListener('dblclick', () => { focusedBoatId = b.id; addBoat(); });
        list.appendChild(div);
    });
    if (!candidateBoats.find(b => b.id === focusedBoatId)) {
        focusedBoatId = null;
        document.getElementById('add-boat-btn').disabled = true;
    }
}

// ---- Selection management ----

function addBoat() {
    if (!focusedBoatId) return;
    const boat = candidateBoats.find(b => b.id === focusedBoatId);
    if (!boat) return;
    selectedItems.push({
        type:  'boat',
        id:    boat.id,
        label: boat.sailNumber ? `${boat.sailNumber} ${boat.name}` : boat.name,
        color: nextColor(),
        // Seed the calc's per-boat variant for this new boat from the table majority.
        initialVariant: majorityVariant()
    });
    focusedBoatId = null;
    document.getElementById('add-boat-btn').disabled = true;
    document.getElementById('boat-search').value = '';
    candidateBoats = [];   // clear immediately so stale highlighted list doesn't linger
    renderBoatList();
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
    loadElapsedCharts();
}

function clearAll() {
    selectedItems = [];
    renderChips();
    saveSelection();
    loadCandidates();
    loadChart();
    loadElapsedCharts();
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
        btn.title = 'Remove all boats';
        btn.textContent = '✕ Clear all';
        btn.onclick = clearAll;
        container.appendChild(btn);
    }
}

// ---- Error band helpers ----

function hexToRgba(hex, alpha) {
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `rgba(${r},${g},${b},${alpha})`;
}

function addBandTrace(factor, weight, color, lineX) {
    if (!showErrorBars) return null;
    const b = errorBounds(factor, weight);
    if (!b) return null;
    return {
        x: [lineX[0], lineX[1], lineX[1], lineX[0], lineX[0]],
        y: [b.upper,  b.upper,  b.lower,  b.lower,  b.upper],
        type: 'scatter', mode: 'lines', fill: 'toself',
        fillcolor: hexToRgba(color, 0.10),
        line: { color: 'transparent' },
        showlegend: false, hoverinfo: 'skip'
    };
}

// ---- Trend helpers ----

function weightedOlsTrend(entries) {
    if (entries.length < 3) return null;
    const toDay = s => Date.parse(s) / 86400000;
    const xs = entries.map(e => toDay(e.date));
    const ys = entries.map(e => e.backCalcFactor);
    const ws = entries.map(e => e.weight);
    const sw = ws.reduce((a, w) => a + w, 0);
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
    // Build N virtual seed entries at the PF value so the average is fully initialised
    // from race 1; these are not plotted but fill the window before real data.
    const virtual = seed != null
        ? Array.from({ length: n }, () => ({ backCalcFactor: seed }))
        : [];
    for (let i = 0; i < pts.length; i++) {
        // Window = last n real entries, padded on the left with virtual seed entries
        const realWindow = pts.slice(Math.max(0, i - n + 1), i + 1);
        const pad = virtual.slice(Math.max(0, n - i - 1));  // virtual entries that still fit
        const window = [...pad, ...realWindow];
        // Sort window by backCalcFactor ascending, drop the worst (highest) values
        const sorted = [...window].sort((a, b) => a.backCalcFactor - b.backCalcFactor);
        const used = sorted.slice(0, Math.min(keep, sorted.length));
        xs.push(pts[i].date);
        ys.push(used.reduce((a, p) => a + p.backCalcFactor, 0) / used.length);
    }
    return xs.length >= 2 ? { x: xs, y: ys } : null;
}

// ---- Main chart ----

async function loadChart() {
    const boatIds = selectedItems.map(i => i.id);

    if (boatIds.length === 0) {
        Plotly.purge('comparison-chart');
        lastChartData = null;
        document.getElementById('comparison-chart-section').style.display = 'none';
        document.getElementById('pf-calc').style.display = '';
        document.getElementById('pf-calc-table-section').style.display = 'none';
        document.getElementById('bcfc-race-division-section').style.display = 'none';
        inlineDivisionData = null;
        pfCalc();
        return;
    }
    document.getElementById('comparison-chart-section').style.display = '';

    const params = new URLSearchParams();
    params.set('boatIds', boatIds.join(','));

    const data = await fetchJson('/api/comparison/chart?' + params);
    if (!data) return;
    lastChartData = data;
    renderChart(data);
    loadElapsedCharts();
}

// The page has no global variant selector — each boat's variant lives in the
// handicap calculator's per-boat dropdown. This reads it (defaulting to 'spin'
// before the calc exists).
function boatVariantFor(boatId) {
    return pfCalcController ? pfCalcController.getBoatVariant(boatId) : 'spin';
}

// Variant to seed a newly added boat with: the majority variant among the boats
// already in the comparison, or 'spin' when there's no clear single majority.
function majorityVariant() {
    if (!pfCalcController || selectedItems.length === 0) return 'spin';
    const counts = {spin: 0, nonSpin: 0, twoHanded: 0};
    selectedItems.forEach(i => {
        const v = pfCalcController.getBoatVariant(i.id);
        if (counts[v] != null) counts[v]++;
    });
    const ranked = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    if (ranked[0][1] === 0) return 'spin';                       // no boats counted
    if (ranked[0][1] === ranked[1][1]) return 'spin';            // tie → no clear majority
    return ranked[0][0];
}

function filterByVariant(entries, variant) {
    return entries.filter(e =>
        variant === 'twoHanded' ? e.twoHanded
            : variant === 'nonSpin' ? e.nonSpinnaker
        : !e.nonSpinnaker && !e.twoHanded
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

function pfVariantFor(boat, variant) {
    return variant === 'nonSpin' ? boat.pfNonSpin
        : variant === 'twoHanded' ? boat.pfTwoHanded
            : boat.pfSpin;
}

function rfVariantFor(boat, variant) {
    return variant === 'nonSpin' ? boat.rfNonSpin
        : variant === 'twoHanded' ? null : boat.rfSpin;
}

// Active divisor for the BCF chart, when one of the calc's PF / RF / set "show"
// tickboxes is on (singleSelectShow ensures at most one). Returns null when no
// divisor is active. perBoat values mirror Factor.applyInverse — the chart plots
// y' = e.backCalcFactor / value and intensity weight w' = e.weight × weight.
//
// PF / RF are looked up using the calc's per-boat variant (not the global selector)
// so the PF divisor matches an allocated set built via "Use PF" — which copies each
// boat's per-boat-variant PF.
function computeBcfDivisor(data) {
    const calc = pfCalcController;
    if (!calc) return null;
    if (calc.getShowPf()) {
        const perBoat = new Map();
        data.boats.forEach(b => {
            const f = pfVariantFor(b, calc.getBoatVariant(b.id));
            if (f) perBoat.set(b.id, {value: f.value, weight: f.weight});
        });
        return {label: 'PF', perBoat};
    }
    if (calc.getShowRf()) {
        const perBoat = new Map();
        data.boats.forEach(b => {
            const f = rfVariantFor(b, calc.getBoatVariant(b.id));
            if (f) perBoat.set(b.id, {value: f.value, weight: f.weight});
        });
        return {label: 'RF', perBoat};
    }
    const activeSet = calc.getAllSets().find(s => s.show);
    if (activeSet && activeSet.values.size > 0) {
        const perBoat = new Map();
        // Allocated entries are user-typed — treat as full confidence.
        activeSet.values.forEach((v, boatId) => perBoat.set(boatId, {value: v, weight: 1}));
        return {label: activeSet.name, perBoat};
    }
    return null;
}

function renderChart(data) {
    // Calc first: it seeds each boat's per-boat variant (boatVariants), which
    // renderBcfChart then reads for entry filtering and PF/RF lookup.
    renderHandicapCalc(data);
    renderBcfChart(data);
}

// BCF chart only — no calc reset. Safe to call from inside pfCalc onChange,
// because it never invokes setBoats / recalc and so cannot re-fire onChange.
function renderBcfChart(data) {
    const traces = [];

    // Pre-compute filtered entries per boat (per-boat variant + recent-months)
    const filteredPerBoat = new Map(
        data.boats.map(b => [b.id, filterEntries(b.entries, boatVariantFor(b.id))]));

    // If "common races only", further restrict each boat to the intersection of raceIds
    if (showCommonRacesOnly && data.boats.length >= 2) {
        const sets = data.boats.map(b => new Set(filteredPerBoat.get(b.id).map(e => e.raceId)));
        const common = sets.reduce((acc, s) => new Set([...acc].filter(id => s.has(id))));
        data.boats.forEach(b =>
            filteredPerBoat.set(b.id, filteredPerBoat.get(b.id).filter(e => common.has(e.raceId))));
    }

    // When a divisor is active, dots are plotted as BCF / divisor (mirrors
    // Factor.applyInverse). Boats with no divisor entry are skipped entirely.
    const divisor = computeBcfDivisor(data);

    let minDate = null, maxDate = null;
    data.boats.forEach(b => {
        if (divisor && !divisor.perBoat.has(b.id)) return;
        filteredPerBoat.get(b.id).forEach(e => {
            if (!minDate || e.date < minDate) minDate = e.date;
            if (!maxDate || e.date > maxDate) maxDate = e.date;
        });
    });
    const lineX = minDate
        ? [minDate, maxDate]
        : ['2018-01-01', new Date().toISOString().slice(0, 10)];

    data.boats.forEach(boat => {
        const div = divisor ? divisor.perBoat.get(boat.id) : null;
        if (divisor && !div) return;   // boat has no divisor entry → skip

        const item  = selectedItems.find(i => i.type === 'boat' && i.id === boat.id);
        const color = item ? item.color : '#888';
        const name  = item ? item.label : (boat.sailNumber ? `${boat.sailNumber} ${boat.name}` : boat.name);

        const variant = boatVariantFor(boat.id);
        const rfFactor = rfVariantFor(boat, variant);
        const pfFactor = pfVariantFor(boat, variant);

        // PF / RF horizontal lines are about the unscaled domain — hide them while a
        // divisor is active (they would just collapse near 1.0 or be misleading).
        if (showRfLine && rfFactor && !divisor) {
            traces.push({
                x: lineX, y: [rfFactor.value, rfFactor.value],
                type: 'scatter', mode: 'lines',
                name: `${name} RF`,
                line: { color, dash: 'dashdot', width: 1.5 },
                legendgroup: boat.id,
                hovertemplate: `${esc(name)} RF: %{y:.4f}<extra></extra>`
            });
        }
        if (showPfLine && pfFactor && !divisor) {
            traces.push({
                x: lineX, y: [pfFactor.value, pfFactor.value],
                type: 'scatter', mode: 'lines',
                name: `${name} PF`,
                line: { color, dash: 'solid', width: 2 },
                legendgroup: boat.id,
                hovertemplate: `${esc(name)} PF: %{y:.4f}<extra></extra>`
            });
        }

        // Apply divisor to entries (Factor.applyInverse semantics: divide value,
        // multiply weights). The dot layer reads backCalcFactor and weight from the
        // returned entries, so the trend line implicitly uses the scaled values too.
        const rawEntries = filteredPerBoat.get(boat.id);
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
                sizes.push(4 + 6 * w);
                opacities.push(parseFloat((0.35 + 0.65 * w).toFixed(2)));
                symbols.push(e.weight < 0.01 ? 'x' : 'circle');
                texts.push(
                    `${esc(name)}<br>` +
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
                legendgroup: boat.id
            });

            if (showTrendLinear) {
                const t = weightedOlsTrend(entries);
                if (t) traces.push({
                    x: t.x, y: t.y, type: 'scatter', mode: 'lines',
                    name: `${name} linear trend`,
                    line: { color, dash: 'dash', width: 1.5 },
                    legendgroup: boat.id,
                    hovertemplate: `${esc(name)} linear trend: %{y:.4f}<extra></extra>`
                });
            }
            // Sliding average is hidden in divisor mode — its seed value (PF) lives in
            // the unscaled domain and would skew the early window.
            if (showTrendSliding && !divisor) {
                const pfSeed = pfFactor ? pfFactor.value : null;
                const s = slidingAverage(entries, slidingAverageCount, slidingAverageDrops, pfSeed);
                const best = slidingAverageCount - slidingAverageDrops;
                const avgLabel = slidingAverageDrops > 0
                    ? `best ${best} of ${slidingAverageCount} avg`
                    : `${slidingAverageCount}-finish avg`;
                if (s) traces.push({
                    x: s.x, y: s.y, type: 'scatter', mode: 'lines',
                    name: `${name} ${avgLabel}`,
                    line: { color, dash: 'dot', width: 1.5 },
                    legendgroup: boat.id,
                    hovertemplate: `${esc(name)} ${avgLabel}: %{y:.4f}<extra></extra>`
                });
            }
        }
    });

    const yFromZero = document.getElementById('bcfc-y-from-zero')?.checked ?? false;
    const yTitle = divisor ? `Factor ratio: BCF / ${divisor.label}` : 'Factor';
    const layout = {
        xaxis: { title: 'Date', type: 'date' },
        yaxis: {title: yTitle, rangemode: yFromZero ? 'tozero' : 'normal'},
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
        const {raceId, divisionName, seriesId} = pt.customdata;
        if (raceId) showRaceDivisionInline(raceId, divisionName || '', seriesId || null);
    });
}

// ---- Handicap calculator (thin adapter over shared HandicapCalc module) ----

let pfCalcController = null;

function pfCalc() {
    if (pfCalcController) return pfCalcController;
    pfCalcController = HandicapCalc.create({
        section: document.getElementById('pf-calc'),
        table: document.querySelector('#pf-calc table'),
        showBestFit: false,
        sessionKey: HANDICAP_STORAGE_KEY,
        urlInput: document.getElementById('handicap-url'),
        fetchBtn: document.getElementById('fetch-handicaps-btn'),
        fetchStatus: document.getElementById('fetch-status'),
        fileInput: document.getElementById('handicap-file'),
        fileStatus: document.getElementById('file-status'),
        variantModeSelect: document.getElementById('handicap-variant-mode'),
        sourceVariantSelect: document.getElementById('handicap-source-variant'),
        downloadBtn: document.getElementById('download-handicaps-btn'),
        downloadStatus: document.getElementById('download-status'),
        compareSelect: true,
        compareMax: 2,
        // Compare boats page repurposes the PF / RF / per-set "show" tickboxes as a
        // single divisor selector for the BCF chart, so at most one may be ticked.
        singleSelectShow: true,
        onCompareSelectionChange: () => loadElapsedCharts(),
        onChange: () => {
            // Re-render the BCF chart so a divisor toggle (or its clearing) is reflected.
            // Calls the BCF-only renderer — calling renderChart here would re-enter
            // renderHandicapCalc → setBoats → recalc → onChange (infinite recursion).
            if (lastChartData) renderBcfChart(lastChartData);
            if (inlineDivisionData) renderInlineDivisionChart();
            // A per-boat variant change in the calc table fires onChange too; the cached
            // elapsed payload lets this re-render without hitting the API.
            const elapsedSection = document.getElementById('elapsed-charts-section');
            if (elapsedSection && elapsedSection.style.display !== 'none')
                loadElapsedCharts();
        },
        onFetchedRows: (rows) => addBoatsFromRows(rows)
    });
    const clearBtn = document.getElementById('clear-handicaps-btn');
    if (clearBtn) clearBtn.addEventListener('click', () => pfCalcController.clearAll());
    const usePfBtn = document.getElementById('use-pf-btn');
    if (usePfBtn) usePfBtn.addEventListener('click', () => pfCalcController.useDisplayedFactor('pf'));
    const useRfBtn = document.getElementById('use-rf-btn');
    if (useRfBtn) useRfBtn.addEventListener('click', () => pfCalcController.useDisplayedFactor('rf'));
    return pfCalcController;
}

async function addBoatsFromRows(rows) {
    if (selectedItems.length > 0) return null;

    // Matching is done server-side (alias-aware via DataStore.findBoat) and surfaced as
    // row.boatId. The frontend trusts the boatId; rows with no server match are skipped.
    const added = new Set();
    const matched = [];

    for (const row of rows) {
        if (!row.boatId) {
            console.warn('fetch-handicaps: server returned no boatId for row, skipping', row);
            continue;
        }
        if (added.has(row.boatId)) continue;
        added.add(row.boatId);
        matched.push(row);
    }

    matched.forEach(row => {
        const label = row.sailno ? `${row.sailno} ${row.name}` : row.name;
        selectedItems.push({
            type: 'boat',
            id: row.boatId,
            label,
            color: nextColor(),
            // Seed the calculator's variant for THIS new boat from the source row, so the
            // fetched handicap loads against the matching variant; fall back to the table
            // majority when the source row carries no variant info.
            initialVariant: row.variant || majorityVariant()
        });
    });

    if (matched.length > 0) {
        renderChips();
        saveSelection();
        loadCandidates();
        loadChart();
        loadElapsedCharts();
    }

    return {handled: true, matched: matched.length};
}

function renderHandicapCalc(data) {
    document.getElementById('pf-calc-table-section').style.display = '';
    const showBestFit = data.boats.length <= 3;

    const calcBoats = data.boats.map(b => {
        const item  = selectedItems.find(i => i.type === 'boat' && i.id === b.id);
        const color = item ? item.color : '#888';
        const name  = item ? item.label : (b.sailNumber ? `${b.sailNumber} ${b.name}` : b.name);

        // Per-boat variant: a known boat keeps the variant the calc already tracks;
        // a new boat takes its source-row variant if any, else the table majority.
        // setBoats only seeds boatVariants for boats it hasn't seen, so this `variant`
        // field only takes effect for genuinely new boats.
        const variant = item?.initialVariant
            || (pfCalcController ? pfCalcController.getBoatVariant(b.id) : null)
            || majorityVariant();

        const pfFactor = pfVariantFor(b, variant);
        const rfFactor = rfVariantFor(b, variant);

        let bestFit = null;
        if (showBestFit) {
            const entries = filterEntries(b.entries || [], variant);
            const trend = weightedOlsTrend(entries);
            if (trend) bestFit = trend.y[1];
        }

        return {
            id: b.id, name, color,
            sailNumber: b.sailNumber || null,
            boatName: b.name || null,
            designName: b.designName || null,
            variant,
            pfAll: {
                spin: b.pfSpin ? b.pfSpin.value : null,
                nonSpin: b.pfNonSpin ? b.pfNonSpin.value : null,
                twoHanded: b.pfTwoHanded ? b.pfTwoHanded.value : null,
            },
            pfWeightAll: {
                spin: b.pfSpin ? b.pfSpin.weight : null,
                nonSpin: b.pfNonSpin ? b.pfNonSpin.weight : null,
                twoHanded: b.pfTwoHanded ? b.pfTwoHanded.weight : null,
            },
            rfAll: {
                spin: b.rfSpin ? b.rfSpin.value : null,
                nonSpin: b.rfNonSpin ? b.rfNonSpin.value : null,
            },
            rfWeightAll: {
                spin: b.rfSpin ? b.rfSpin.weight : null,
                nonSpin: b.rfNonSpin ? b.rfNonSpin.weight : null,
            },
            pf: pfFactor ? pfFactor.value : null,
            pfWeight: pfFactor ? pfFactor.weight : null,
            rf: rfFactor ? rfFactor.value : null,
            rfWeight: rfFactor ? rfFactor.weight : null,
            bestFit
        };
    });

    pfCalc().setBoats(calcBoats, {showBestFit});
}

// ---- Inline race-division chart (shown below BCFC chart on dot click) ----

async function showRaceDivisionInline(raceId, divisionName, seriesId = null) {
    const params = new URLSearchParams({raceId, divisionName});
    const data = await fetchJson('/api/comparison/division?' + params);
    if (!data) return;
    inlineDivisionData = data;
    inlineDivisionRaceId = raceId;
    inlineDivisionName = divisionName;
    inlineDivisionSeriesId = seriesId;

    // Fetch ordered series race list if we've moved to a different series
    if (seriesId && seriesId !== inlineSeriesId) {
        inlineSeriesRaces = null;
        inlineSeriesId = seriesId;
        const sd = await fetchJson('/api/series/chart?' + new URLSearchParams({seriesId}));
        if (sd && sd.races) {
            inlineSeriesRaces = sd.races.map(r => ({raceId: r.raceId, raceName: r.raceName, date: r.date}));
        }
    } else if (!seriesId) {
        inlineSeriesRaces = null;
        inlineSeriesId = null;
    }

    document.getElementById('bcfc-race-division-section').style.display = '';
    const titleParts = [data.date, data.seriesName, data.raceName,
        divisionName ? divisionName : 'Results'].filter(Boolean);
    document.getElementById('bcfc-race-division-title').textContent = titleParts.join(' — ');
    updateInlineDivNavButtons();
    renderInlineDivisionChart();
}

function updateInlineDivNavButtons() {
    const prevBtn = document.getElementById('bcfc-div-prev-btn');
    const nextBtn = document.getElementById('bcfc-div-next-btn');
    if (!prevBtn || !nextBtn) return;
    const idx = inlineSeriesRaces ? inlineSeriesRaces.findIndex(r => r.raceId === inlineDivisionRaceId) : -1;
    prevBtn.disabled = idx <= 0;
    nextBtn.disabled = idx < 0 || idx >= (inlineSeriesRaces?.length ?? 0) - 1;
}

function inlineDivPrev() {
    if (!inlineSeriesRaces) return;
    const idx = inlineSeriesRaces.findIndex(r => r.raceId === inlineDivisionRaceId);
    if (idx > 0) showRaceDivisionInline(inlineSeriesRaces[idx - 1].raceId, inlineDivisionName, inlineDivisionSeriesId);
}

function inlineDivNext() {
    if (!inlineSeriesRaces) return;
    const idx = inlineSeriesRaces.findIndex(r => r.raceId === inlineDivisionRaceId);
    if (idx >= 0 && idx < inlineSeriesRaces.length - 1)
        showRaceDivisionInline(inlineSeriesRaces[idx + 1].raceId, inlineDivisionName, inlineDivisionSeriesId);
}

function onInlineDivXFactorChange() {
    inlineDivXFactor = document.getElementById('bcfc-div-xfactor').value;
    sessionStorage.setItem(INLINE_DIV_XFACTOR_KEY, inlineDivXFactor);
    renderInlineDivisionChart();
}

function onInlineShowElapsedChange(cb) {
    inlineShowElapsed = cb.checked;
    sessionStorage.setItem(INLINE_DIV_SHOW_ELAPSED_KEY, inlineShowElapsed ? 'true' : 'false');
    renderInlineDivisionChart();
}

function renderInlineDivisionChart() {
    const data = inlineDivisionData;
    if (!data) return;
    const finishers = (data.finishers || []).filter(f => f.pf != null && f.elapsed > 0);
    if (finishers.length === 0) {
        Plotly.purge('bcfc-race-division-chart');
        return;
    }

    const rfFinishersList = finishers.filter(f => f.rf != null && f.rfCorrected != null);

    // Calc-driven visibility flags: PF / RF corrected and per-set Allocated lines.
    const showPf = pfCalc().getShowPf();
    const showRf = pfCalc().getShowRf();

    // Allocated-handicap data: one bundle per set from the calculator.
    const allocSets = pfCalc().getAllSets().map(s => {
        const pts = finishers
            .filter(f => s.values.has(f.boatId))
            .map(f => ({
                f,
                name: f.sailNumber ? `${f.sailNumber} ${f.name}` : f.name,
                handicap: s.values.get(f.boatId),
                correctedMin: f.elapsed * s.values.get(f.boatId) / 60
            }))
            .sort((a, b) => a.handicap - b.handicap);
        return {name: s.name, color: s.color, focused: s.focused, show: s.show, values: s.values, pts};
    });
    const allocSetsWithPts = allocSets.filter(s => s.pts.length > 0);
    const visibleAllocSets = allocSetsWithPts.filter(s => s.show);
    const focusedAlloc = allocSets.find(s => s.focused) || allocSets[0];

    // Sync the Elapsed tickbox UI so its state survives reloads / nav.
    const elapsedCb = document.getElementById('bcfc-show-elapsed');
    if (elapsedCb) elapsedCb.checked = inlineShowElapsed;

    // Rebuild the x-factor selector with options valid for this data.
    const xSelect = document.getElementById('bcfc-div-xfactor');
    if (xSelect) {
        const opts = ['---', 'PF',
            ...(rfFinishersList.length > 0 ? ['RF'] : []),
            ...(allocSetsWithPts.length > 0 ? ['Allocated'] : [])
        ];
        if (!opts.includes(inlineDivXFactor)) inlineDivXFactor = '---';
        if (xSelect.options.length !== opts.length ||
            [...xSelect.options].map(o => o.value).join() !== opts.join()) {
            xSelect.innerHTML = opts.map(o => `<option value="${o}">${o}</option>`).join('');
        }
        xSelect.value = inlineDivXFactor;
    }

    function hoverText(n, label, t) {
        return t != null ? `${esc(n)}<br>${label}: ${fmtTime(t * 60)}` : '';
    }

    let traces, annotations, xAxisTitle;

    if (inlineDivXFactor === '---') {
        // Natural mode: each trace at 1/factor on the x-axis so the elapsed line is
        // (approximately) straight rather than hyperbolic. Faster boats sit on the left.
        const xs = finishers.map(f => f.pf > 0 ? 1 / f.pf : null);
        const names = finishers.map(f => f.sailNumber ? `${f.sailNumber} ${f.name}` : f.name);
        const elapsed = finishers.map(f => f.elapsed / 60);
        const pfCorr = finishers.map(f => f.pfCorrected != null ? f.pfCorrected / 60 : null);

        traces = [];
        if (inlineShowElapsed) traces.push({
            x: xs, y: elapsed, mode: 'lines+markers', type: 'scatter', name: 'Elapsed',
            line: {dash: 'dash', color: '#555', width: 1.5}, marker: {size: 7},
            text: names.map((n, i) => hoverText(n, 'Elapsed', elapsed[i])),
            hoverinfo: 'text'
        });
        if (showPf) traces.push({
            x: xs, y: pfCorr, mode: 'lines+markers', type: 'scatter', name: 'PF corrected',
            line: {dash: 'solid', color: '#2255aa', width: 2}, marker: {size: 7},
            text: names.map((n, i) => hoverText(n, 'PF corrected', pfCorr[i])),
            hoverinfo: 'text'
        });

        if (showPf) addPodiumTraces(traces, finishers, xs, pfCorr);

        visibleAllocSets.forEach(s => {
            const allocXs = s.pts.map(p => p.handicap > 0 ? 1 / p.handicap : null);
            const allocYs = s.pts.map(p => p.correctedMin);
            const traceName = allocSetsWithPts.length > 1
                ? `${s.name} corrected` : 'Allocated handicap corrected';
            traces.push({
                x: allocXs, y: allocYs,
                mode: 'lines+markers', type: 'scatter',
                name: traceName,
                line: {dash: 'longdash', color: s.color, width: 2},
                marker: {size: 8, symbol: 'square'},
                text: s.pts.map(p =>
                    `${esc(p.name)}<br>${esc(s.name)}: ${p.handicap.toFixed(4)}`
                    + `<br>Corrected: ${fmtTime(p.correctedMin * 60)}`),
                hoverinfo: 'text'
            });
            if (s.focused) addAllocPodiumTraces(traces, s.pts, allocXs, allocYs, s.color);
        });

        annotations = finishers.map((f, i) => {
            const ys = [];
            if (inlineShowElapsed && elapsed[i] != null) ys.push(elapsed[i]);
            if (showPf && pfCorr[i] != null) ys.push(pfCorr[i]);
            return {
                x: xs[i], y: ys.length > 0 ? Math.max(...ys) : 0,
                text: f.name, textangle: -90,
                xanchor: 'center', yanchor: 'bottom', yshift: 6,
                showarrow: false, cliponaxis: false, font: {size: 11}
            };
        });

        xAxisTitle = speedFactorAxisTitle('1/PF');

    } else {
        // Common-factor mode: all traces share the same x-axis factor (inverted to
        // 1/factor so the elapsed line is straight rather than hyperbolic).
        // For "Allocated" x-axis, use the focused set's values to define x.
        const xByFocusedAlloc = focusedAlloc ? focusedAlloc.values : new Map();
        const getX = f => {
            const raw =
                inlineDivXFactor === 'RF' ? f.rf :
                    inlineDivXFactor === 'Allocated' ? xByFocusedAlloc.get(f.boatId) :
                        f.pf;
            return (raw != null && raw > 0) ? 1 / raw : null;
        };
        const plotFinishers = finishers
            .filter(f => getX(f) != null)
            .sort((a, b) => getX(a) - getX(b));
        if (plotFinishers.length === 0) return;

        const xs = plotFinishers.map(getX);
        const names = plotFinishers.map(f => f.sailNumber ? `${f.sailNumber} ${f.name}` : f.name);
        const elapsed = plotFinishers.map(f => f.elapsed / 60);
        const pfCorr = plotFinishers.map(f => f.pfCorrected != null ? f.pfCorrected / 60 : null);
        const rfCorr = plotFinishers.map(f => f.rfCorrected != null ? f.rfCorrected / 60 : null);

        traces = [];
        if (inlineShowElapsed) traces.push({
            x: xs, y: elapsed, mode: 'lines+markers', type: 'scatter', name: 'Elapsed',
            line: {dash: 'dash', color: '#555', width: 1.5}, marker: {size: 7},
            text: names.map((n, i) => hoverText(n, 'Elapsed', elapsed[i])), hoverinfo: 'text'
        });
        if (showPf) traces.push({
            x: xs, y: pfCorr, mode: 'lines+markers', type: 'scatter', name: 'PF corrected',
            line: {dash: 'solid', color: '#2255aa', width: 2}, marker: {size: 7},
            text: names.map((n, i) => hoverText(n, 'PF corrected', pfCorr[i])), hoverinfo: 'text'
        });
        if (showRf && rfCorr.some(v => v != null)) traces.push({
            x: xs, y: rfCorr, mode: 'lines+markers', type: 'scatter', name: 'RF corrected',
            line: {dash: 'dot', color: '#c47900', width: 1.5}, marker: {size: 7},
            text: names.map((n, i) => hoverText(n, 'RF corrected', rfCorr[i])), hoverinfo: 'text'
        });

        if (showPf) addPodiumTraces(traces, plotFinishers, xs, pfCorr);

        // One allocated trace per non-empty visible set; podium markers only for the focused set.
        visibleAllocSets.forEach(s => {
            const allocCorr = plotFinishers.map(f => {
                const h = s.values.get(f.boatId);
                return h != null ? f.elapsed * h / 60 : null;
            });
            const allocFiltered = plotFinishers
                .map((f, i) => ({
                    f, x: xs[i], y: allocCorr[i],
                    name: names[i],
                    handicap: s.values.get(f.boatId),
                    correctedMin: allocCorr[i]
                }))
                .filter(p => p.y != null);
            if (allocFiltered.length === 0) return;
            const traceName = allocSetsWithPts.length > 1
                ? `${s.name} corrected` : 'Allocated handicap corrected';
            traces.push({
                x: allocFiltered.map(p => p.x),
                y: allocFiltered.map(p => p.y),
                mode: 'lines+markers', type: 'scatter',
                name: traceName,
                line: {dash: 'longdash', color: s.color, width: 2}, marker: {size: 8, symbol: 'square'},
                text: allocFiltered.map(p =>
                    `${esc(p.name)}<br>${esc(s.name)}: ${p.handicap.toFixed(4)}<br>Corrected: ${fmtTime(p.y * 60)}`),
                hoverinfo: 'text'
            });
            if (s.focused) {
                addAllocPodiumTraces(traces, allocFiltered,
                    allocFiltered.map(p => p.x), allocFiltered.map(p => p.y), s.color);
            }
        });

        annotations = plotFinishers.map((f, i) => {
            const ys = [];
            if (inlineShowElapsed && elapsed[i] != null) ys.push(elapsed[i]);
            if (showPf && pfCorr[i] != null) ys.push(pfCorr[i]);
            if (showRf && rfCorr[i] != null) ys.push(rfCorr[i]);
            return {
                x: xs[i], y: ys.length > 0 ? Math.max(...ys) : 0, text: f.name, textangle: -90,
                xanchor: 'center', yanchor: 'bottom', yshift: 6,
                showarrow: false, cliponaxis: false, font: {size: 11}
            };
        });

        xAxisTitle = speedFactorAxisTitle(inlineDivXFactor === 'Allocated' ? '1/Allocated Handicap' : '1/' + inlineDivXFactor);
    }

    const yFromZero = document.getElementById('bcfc-y-from-zero')?.checked ?? false;
    const layout = {
        xaxis: {title: xAxisTitle},
        yaxis: {title: 'Time (min)', tickformat: '.1f', rangemode: yFromZero ? 'tozero' : 'normal'},
        legend: {orientation: 'h', y: -0.18},
        margin: {t: 80, b: 80, l: 60, r: 20},
        hovermode: 'closest',
        annotations
    };

    Plotly.react('bcfc-race-division-chart', traces, layout, {responsive: true});
}

// ---- Elapsed time comparison charts ----

// Render the elapsed-time chart for the pair currently ticked in the handicap
// calculator's compare column. Shows the section when exactly two boats are ticked;
// hides it otherwise. Points are filtered to races where each boat sailed under the
// variant currently selected for it in the calculator — so a pair displayed under
// (Spin, Non-Spin) only sees races where boat A sailed spin AND boat B sailed non-spin.
async function loadElapsedCharts() {
    const section = document.getElementById('elapsed-charts-section');
    const container = document.getElementById('elapsed-charts-container');

    if (!pfCalcController) {
        section.style.display = 'none';
        container.innerHTML = '';
        return;
    }
    const selected = [...pfCalcController.getCompareSelection()];
    if (selected.length !== 2) {
        section.style.display = 'none';
        container.innerHTML = '';
        return;
    }

    const [idA, idB] = selected;
    const itemA = selectedItems.find(i => i.type === 'boat' && i.id === idA);
    const itemB = selectedItems.find(i => i.type === 'boat' && i.id === idB);
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
        const params = new URLSearchParams({boatAId: idA, boatBId: idB});
        data = await fetchJson('/api/comparison/elapsed-chart?' + params);
        if (!data) return;
        elapsedChartCache = {key, data};
    }

    // Filter points to races where each boat sailed under the variant currently
    // selected for it in the calculator. The renderer doesn't mutate `data.points`
    // (we hand it a shallow copy), so cached `data` stays usable for the next call.
    const variantA = pfCalcController.getBoatVariant(idA);
    const variantB = pfCalcController.getBoatVariant(idB);
    const matches = (variant, nonSpin, twoH) =>
        variant === 'twoHanded' ? twoH
            : variant === 'nonSpin' ? nonSpin
                : !nonSpin && !twoH;
    const filteredPoints = (data.points || []).filter(p =>
        matches(variantA, p.aNonSpinnaker, p.aTwoHanded) &&
        matches(variantB, p.bNonSpinnaker, p.bTwoHanded));
    const renderData = {...data, points: filteredPoints};

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
        // for boats A and B (geometric-mean-balanced against the other anchors).
        bestFitBtn = document.createElement('button');
        bestFitBtn.id = 'elapsed-use-best-fit';
        bestFitBtn.type = 'button';
        bestFitBtn.style.cssText = 'margin-top:0.5rem;padding:6px 14px;cursor:pointer;';
        bestFitBtn.addEventListener('click', applyBestFitToHandicaps);
        wrapper.appendChild(bestFitBtn);
        container.appendChild(wrapper);
    }
    titleEl.textContent = `${itemA.label} vs ${itemB.label}`;
    renderElapsedChart('elapsed-chart-0', renderData, itemA.color, itemB.color, variantA, variantB);

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
    if (!btn || btn.disabled || !pfCalcController) return;
    const idA = btn.dataset.idA;
    const idB = btn.dataset.idB;
    const ratio = parseFloat(btn.dataset.ratio);
    if (!idA || !idB || !isFinite(ratio) || ratio <= 0) return;
    pfCalcController.applyPairwiseFit(idA, idB, ratio);
}

/** Re-renders the elapsed-time charts without refetching; used by the From-0 toggle. */
function onElapsedFromZeroChange() {
    loadElapsedCharts();
}

function renderElapsedChart(divId, data, colorA, colorB, variantA, variantB) {
    let points = data.points || [];

    // Apply recent-months filter if active
    if (recentMonths > 0) {
        const cutoff = new Date();
        cutoff.setMonth(cutoff.getMonth() - recentMonths);
        const cutoffStr = cutoff.toISOString().slice(0, 10);
        points = points.filter(p => p.date >= cutoffStr);
    }

    if (points.length === 0) {
        Plotly.purge(divId);
        lastElapsedFit = null;
        return;
    }

    const xs = points.map(p => p.x / 3600);
    const ys = points.map(p => p.y / 3600);

    const nameA = data.boatA.sailNumber ? `${data.boatA.sailNumber} ${data.boatA.name}` : data.boatA.name;
    const nameB = data.boatB.sailNumber ? `${data.boatB.sailNumber} ${data.boatB.name}` : data.boatB.name;

    const texts = points.map(p =>
        `${esc(p.date || '')}<br>` +
        (p.seriesName ? `${esc(p.seriesName)}<br>` : '') +
        (p.raceName   ? `${esc(p.raceName)}<br>`   : '') +
        `${esc(p.division || '')}<br>` +
        `${esc(nameA)}: ${fmtTime(p.y)}<br>` +
        `${esc(nameB)}: ${fmtTime(p.x)}`
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

    // Best-fit line through origin
    const fit = linearFitElapsed(xs, ys);
    lastElapsedFit = fit;
    if (fit) {
        const x0 = 0, x1 = xMax + xPad;
        traces.push({
            x: [x0, x1],
            y: [0, fit.slope * x1],
            type: 'scatter', mode: 'lines',
            name: `Best fit (slope ${fit.slope.toFixed(4)})`,
            line: { color: colorA, width: 2 }
        });
    }

    const x0 = 0, x1 = xMax + xPad;

    // Expected RF ratio line (through origin) — each boat under its own variant.
    const rfA = rfVariantFor(data.boatA, variantA);
    const rfB = rfVariantFor(data.boatB, variantB);
    if (rfA && rfB && rfA.value && rfB.value) {
        const slope = rfB.value / rfA.value;
        traces.push({
            x: [x0, x1],
            y: [0, slope * x1],
            type: 'scatter', mode: 'lines',
            name: `RF ratio (${rfB.value.toFixed(4)} / ${rfA.value.toFixed(4)} = ${slope.toFixed(4)})`,
            line: { color: '#c47900', width: 2, dash: 'dot' }
        });
    }

    // Expected PF ratio line (through origin) — each boat under its own variant.
    const pfA = pfVariantFor(data.boatA, variantA);
    const pfB = pfVariantFor(data.boatB, variantB);
    if (pfA && pfB && pfA.value && pfB.value) {
        const slope = pfB.value / pfA.value;
        traces.push({
            x: [x0, x1],
            y: [0, slope * x1],
            type: 'scatter', mode: 'lines',
            name: `PF ratio (${pfB.value.toFixed(4)} / ${pfA.value.toFixed(4)} = ${slope.toFixed(4)})`,
            line: { color: colorB, width: 2, dash: 'dash' }
        });
    }

    const yMin = Math.min(...ys), yMax = Math.max(...ys);
    const yPad = (yMax - yMin) * 0.05 || yMin * 0.05;

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
    // Constrained through origin: elapsed times are proportional (y = k·x), so intercept = 0.
    // Slope = Σ(xi·yi) / Σ(xi²)
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

// ---- Initialisation ----

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
    document.getElementById('show-rf-line')       .addEventListener('change', e => { showRfLine          = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-pf-line')      .addEventListener('change', e => { showPfLine         = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-trend-linear') .addEventListener('change', e => { showTrendLinear    = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-trend-sliding').addEventListener('change', e => { showTrendSliding   = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('hide-legend')       .addEventListener('change', e => { hideLegend         = e.target.checked; if (lastChartData) renderChart(lastChartData); loadElapsedCharts(); });
    document.getElementById('recent-months').addEventListener('change', e => {
        recentMonths = parseInt(e.target.value, 10) || 0;
        if (lastChartData) renderChart(lastChartData);
        loadElapsedCharts();
    });
    document.getElementById('common-races-only') .addEventListener('change', e => { showCommonRacesOnly = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('bcfc-y-from-zero').addEventListener('change', () => {
        if (lastChartData) renderChart(lastChartData);
        if (inlineDivisionData) renderInlineDivisionChart();
    });
    document.getElementById('boat-search').addEventListener('input', () => {
        clearTimeout(boatDebounce);
        boatDebounce = setTimeout(loadCandidates, 250);
    });
    document.getElementById('add-boat-btn').addEventListener('click', addBoat);
    loadCandidates();
    loadChart();
});
