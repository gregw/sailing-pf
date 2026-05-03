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
let selectedVariant = 'spin';
let showErrorBars    = false;
let showRfLine       = true;
let showPfLine      = true;
let showTrendLinear  = true;
let showTrendSliding = true;
let hideLegend       = false;
let showLast12Months  = false;
let showCommonRacesOnly = false;
let slidingAverageCount = 8;
let slidingAverageDrops = 0;
let candidateBoats  = [];
let focusedBoatId   = null;
let boatDebounce    = null;
let lastChartData   = null;
let inlineDivisionData = null; // most recently loaded /api/comparison/division payload
const INLINE_DIV_XFACTOR_KEY = 'pf.inlineDiv.xFactor';
let inlineDivXFactor = sessionStorage.getItem(INLINE_DIV_XFACTOR_KEY) || '---';
let inlineDivisionRaceId = null;
let inlineDivisionName = null;
let inlineDivisionSeriesId = null;
let inlineSeriesId = null;  // seriesId for which inlineSeriesRaces was fetched
let inlineSeriesRaces = null;  // [{raceId, raceName, date}] sorted by date, or null

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
        color: nextColor()
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
        document.getElementById('pf-calc').style.display = 'none';
        document.getElementById('bcfc-race-division-section').style.display = 'none';
        inlineDivisionData = null;
        return;
    }

    const params = new URLSearchParams();
    params.set('boatIds', boatIds.join(','));

    const data = await fetchJson('/api/comparison/chart?' + params);
    if (!data) return;
    lastChartData = data;
    renderChart(data);
    loadElapsedCharts();
}

function onVariantChange() {
    selectedVariant = document.getElementById('variant-selector').value;
    if (lastChartData) renderChart(lastChartData);
    loadElapsedCharts();
}

function filterByVariant(entries) {
    return entries.filter(e =>
        selectedVariant === 'twoHanded' ? e.twoHanded
        : selectedVariant === 'nonSpin' ? e.nonSpinnaker
        : !e.nonSpinnaker && !e.twoHanded
    );
}

function filterEntries(entries) {
    let result = filterByVariant(entries);
    if (showLast12Months) {
        const cutoff = new Date();
        cutoff.setFullYear(cutoff.getFullYear() - 1);
        const cutoffStr = cutoff.toISOString().slice(0, 10);
        result = result.filter(e => e.date >= cutoffStr);
    }
    return result;
}

function renderChart(data) {
    const traces = [];

    // Pre-compute filtered entries per boat (variant + last-12-months)
    const filteredPerBoat = new Map(data.boats.map(b => [b.id, filterEntries(b.entries)]));

    // If "common races only", further restrict each boat to the intersection of raceIds
    if (showCommonRacesOnly && data.boats.length >= 2) {
        const sets = data.boats.map(b => new Set(filteredPerBoat.get(b.id).map(e => e.raceId)));
        const common = sets.reduce((acc, s) => new Set([...acc].filter(id => s.has(id))));
        data.boats.forEach(b =>
            filteredPerBoat.set(b.id, filteredPerBoat.get(b.id).filter(e => common.has(e.raceId))));
    }

    let minDate = null, maxDate = null;
    data.boats.forEach(b => {
        filteredPerBoat.get(b.id).forEach(e => {
            if (!minDate || e.date < minDate) minDate = e.date;
            if (!maxDate || e.date > maxDate) maxDate = e.date;
        });
    });
    const lineX = minDate
        ? [minDate, maxDate]
        : ['2018-01-01', new Date().toISOString().slice(0, 10)];

    data.boats.forEach(boat => {
        const item  = selectedItems.find(i => i.type === 'boat' && i.id === boat.id);
        const color = item ? item.color : '#888';
        const name  = item ? item.label : (boat.sailNumber ? `${boat.sailNumber} ${boat.name}` : boat.name);

        const rfFactor  = selectedVariant === 'nonSpin' ? boat.rfNonSpin
            : selectedVariant === 'twoHanded' ? null : boat.rfSpin;
        const pfFactor = selectedVariant === 'nonSpin' ? boat.pfNonSpin
            : selectedVariant === 'twoHanded' ? boat.pfTwoHanded : boat.pfSpin;

        if (showRfLine && rfFactor) {
            traces.push({
                x: lineX, y: [rfFactor.value, rfFactor.value],
                type: 'scatter', mode: 'lines',
                name: `${name} RF`,
                line: { color, dash: 'dashdot', width: 1.5 },
                legendgroup: boat.id,
                hovertemplate: `${esc(name)} RF: %{y:.4f}<extra></extra>`
            });
        }
        if (showPfLine && pfFactor) {
            traces.push({
                x: lineX, y: [pfFactor.value, pfFactor.value],
                type: 'scatter', mode: 'lines',
                name: `${name} PF`,
                line: { color, dash: 'solid', width: 2 },
                legendgroup: boat.id,
                hovertemplate: `${esc(name)} PF: %{y:.4f}<extra></extra>`
            });
        }

        const entries = filteredPerBoat.get(boat.id);
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
            if (showTrendSliding) {
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
    const layout = {
        xaxis: { title: 'Date', type: 'date' },
        yaxis: {title: 'Factor', rangemode: yFromZero ? 'tozero' : 'normal'},
        showlegend: !hideLegend,
        legend: { orientation: 'v', xanchor: 'right', x: 1 },
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

    renderHandicapCalc(data);
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
        downloadBtn: document.getElementById('download-handicaps-btn'),
        downloadStatus: document.getElementById('download-status'),
        onChange: () => {
            if (inlineDivisionData) renderInlineDivisionChart();
        }
    });
    const clearBtn = document.getElementById('clear-handicaps-btn');
    if (clearBtn) clearBtn.addEventListener('click', () => pfCalcController.clearAll());
    return pfCalcController;
}

function renderHandicapCalc(data) {
    const showBestFit = data.boats.length <= 3;

    const calcBoats = data.boats.map(b => {
        const item  = selectedItems.find(i => i.type === 'boat' && i.id === b.id);
        const color = item ? item.color : '#888';
        const name  = item ? item.label : (b.sailNumber ? `${b.sailNumber} ${b.name}` : b.name);

        const pfFactor = selectedVariant === 'nonSpin' ? b.pfNonSpin
            : selectedVariant === 'twoHanded' ? b.pfTwoHanded
                : b.pfSpin;
        const rfFactor = selectedVariant === 'nonSpin' ? b.rfNonSpin
            : selectedVariant === 'twoHanded' ? null
                : b.rfSpin;

        let bestFit = null;
        if (showBestFit) {
            const entries = filterEntries(b.entries || []);
            const trend = weightedOlsTrend(entries);
            if (trend) bestFit = trend.y[1];
        }

        return {
            id: b.id, name, color,
            sailNumber: b.sailNumber || null,
            boatName: b.name || null,
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

function renderInlineDivisionChart() {
    const data = inlineDivisionData;
    if (!data) return;
    const finishers = (data.finishers || []).filter(f => f.pf != null && f.elapsed > 0);
    if (finishers.length === 0) {
        Plotly.purge('bcfc-race-division-chart');
        return;
    }

    const rfFinishersList = finishers.filter(f => f.rf != null && f.rfCorrected != null);

    // Allocated-handicap data: pulled live from the handicap calculator inputs.
    const allocByBoat = new Map();
    document.querySelectorAll('.pf-calc-input').forEach(inp => {
        const v = parseFloat(inp.value);
        if (!isNaN(v)) allocByBoat.set(inp.dataset.boatId, v);
    });
    const allocPts = finishers
        .filter(f => allocByBoat.has(f.boatId))
        .map(f => ({
            f,
            name: f.sailNumber ? `${f.sailNumber} ${f.name}` : f.name,
            handicap: allocByBoat.get(f.boatId),
            correctedMin: f.elapsed * allocByBoat.get(f.boatId) / 60
        }))
        .sort((a, b) => a.handicap - b.handicap);

    // Rebuild the x-factor selector with options valid for this data.
    const xSelect = document.getElementById('bcfc-div-xfactor');
    if (xSelect) {
        const opts = ['---', 'PF',
            ...(rfFinishersList.length > 0 ? ['RF'] : []),
            ...(allocPts.length > 0 ? ['Allocated'] : [])
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
        // Natural mode: each trace at its own factor's x-axis.
        const xs = finishers.map(f => f.pf);
        const names = finishers.map(f => f.sailNumber ? `${f.sailNumber} ${f.name}` : f.name);
        const elapsed = finishers.map(f => f.elapsed / 60);
        const pfCorr = finishers.map(f => f.pfCorrected != null ? f.pfCorrected / 60 : null);

        traces = [
            {
                x: xs, y: elapsed, mode: 'lines+markers', type: 'scatter', name: 'Elapsed',
                line: {dash: 'dash', color: '#555', width: 1.5}, marker: {size: 7},
                text: names.map((n, i) => hoverText(n, 'Elapsed', elapsed[i])),
                hoverinfo: 'text'
            },
            {
                x: xs, y: pfCorr, mode: 'lines+markers', type: 'scatter', name: 'PF corrected',
                line: {dash: 'solid', color: '#2255aa', width: 2}, marker: {size: 7},
                text: names.map((n, i) => hoverText(n, 'PF corrected', pfCorr[i])),
                hoverinfo: 'text'
            }
        ];

        addPodiumTraces(traces, finishers, xs, pfCorr);

        if (allocPts.length > 0) {
            const allocXs = allocPts.map(p => p.handicap);
            const allocYs = allocPts.map(p => p.correctedMin);
            traces.push({
                x: allocXs, y: allocYs,
                mode: 'lines+markers', type: 'scatter',
                name: 'Allocated handicap corrected',
                line: {dash: 'longdash', color: '#a04020', width: 2},
                marker: {size: 8, symbol: 'square'},
                text: allocPts.map(p =>
                    `${esc(p.name)}<br>Allocated: ${p.handicap.toFixed(4)}`
                    + `<br>Corrected: ${fmtTime(p.correctedMin * 60)}`),
                hoverinfo: 'text'
            });
            addAllocPodiumTraces(traces, allocPts, allocXs, allocYs);
        }

        annotations = finishers.map((f, i) => ({
            x: xs[i], y: Math.max(...[elapsed[i], pfCorr[i]].filter(v => v != null)),
            text: f.name, textangle: -90,
            xanchor: 'center', yanchor: 'bottom', yshift: 6,
            showarrow: false, cliponaxis: false, font: {size: 11}
        }));

        xAxisTitle = 'Handicap (PF)';

    } else {
        // Common-factor mode: all traces share the same x-axis factor.
        const getX = f => {
            if (inlineDivXFactor === 'RF') return f.rf;
            if (inlineDivXFactor === 'Allocated') return allocByBoat.get(f.boatId);
            return f.pf;
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
        const allocCorr = plotFinishers.map(f => {
            const h = allocByBoat.get(f.boatId);
            return h != null ? f.elapsed * h / 60 : null;
        });
        // Drop gaps so the allocated line connects across boats without an entered handicap.
        const allocFiltered = plotFinishers
            .map((f, i) => ({
                f, x: xs[i], y: allocCorr[i],
                name: names[i],
                handicap: allocByBoat.get(f.boatId),
                correctedMin: allocCorr[i]
            }))
            .filter(p => p.y != null);

        traces = [
            {
                x: xs, y: elapsed, mode: 'lines+markers', type: 'scatter', name: 'Elapsed',
                line: {dash: 'dash', color: '#555', width: 1.5}, marker: {size: 7},
                text: names.map((n, i) => hoverText(n, 'Elapsed', elapsed[i])), hoverinfo: 'text'
            },
            {
                x: xs, y: pfCorr, mode: 'lines+markers', type: 'scatter', name: 'PF corrected',
                line: {dash: 'solid', color: '#2255aa', width: 2}, marker: {size: 7},
                text: names.map((n, i) => hoverText(n, 'PF corrected', pfCorr[i])), hoverinfo: 'text'
            },
            ...(rfCorr.some(v => v != null) ? [{
                x: xs, y: rfCorr, mode: 'lines+markers', type: 'scatter', name: 'RF corrected',
                line: {dash: 'dot', color: '#c47900', width: 1.5}, marker: {size: 7},
                text: names.map((n, i) => hoverText(n, 'RF corrected', rfCorr[i])), hoverinfo: 'text'
            }] : []),
            ...(allocFiltered.length > 0 ? [{
                x: allocFiltered.map(p => p.x),
                y: allocFiltered.map(p => p.y),
                mode: 'lines+markers', type: 'scatter',
                name: 'Allocated handicap corrected',
                line: {dash: 'longdash', color: '#a04020', width: 2}, marker: {size: 8, symbol: 'square'},
                text: allocFiltered.map(p =>
                    `${esc(p.name)}<br>Allocated: ${p.handicap.toFixed(4)}<br>Corrected: ${fmtTime(p.y * 60)}`),
                hoverinfo: 'text'
            }] : [])
        ];

        addPodiumTraces(traces, plotFinishers, xs, pfCorr);
        if (allocFiltered.length > 0) {
            addAllocPodiumTraces(traces, allocFiltered,
                allocFiltered.map(p => p.x), allocFiltered.map(p => p.y));
        }

        annotations = plotFinishers.map((f, i) => {
            const ys = [elapsed[i], pfCorr[i]].filter(v => v != null);
            return {
                x: xs[i], y: Math.max(...ys), text: f.name, textangle: -90,
                xanchor: 'center', yanchor: 'bottom', yshift: 6,
                showarrow: false, cliponaxis: false, font: {size: 11}
            };
        });

        xAxisTitle = inlineDivXFactor === 'Allocated' ? 'Allocated Handicap' : inlineDivXFactor;
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

async function loadElapsedCharts() {
    const section = document.getElementById('elapsed-charts-section');
    const container = document.getElementById('elapsed-charts-container');

    const boats = selectedItems.filter(i => i.type === 'boat');
    if (boats.length < 2 || boats.length > 3) {
        if (boats.length >= 4) {
            section.style.display = '';
            container.innerHTML = '<p style="color:#666;">Too many boats selected — select 2 or 3 boats to see elapsed time comparisons.</p>';
        } else {
            section.style.display = 'none';
        }
        return;
    }

    // Build pairs: [A,B] for 2 boats; [A,B],[A,C],[B,C] for 3 boats
    const pairs = [];
    for (let i = 0; i < boats.length; i++)
        for (let j = i + 1; j < boats.length; j++)
            pairs.push([boats[i], boats[j]]);

    section.style.display = '';
    container.innerHTML = '';

    // Fetch all pairs in parallel
    const results = await Promise.all(pairs.map(([a, b]) => {
        const params = new URLSearchParams({ boatAId: a.id, boatBId: b.id });
        return fetchJson('/api/comparison/elapsed-chart?' + params);
    }));

    // Render one chart per pair
    pairs.forEach(([boatItemA, boatItemB], idx) => {
        const data = results[idx];
        if (!data) return;
        const divId = `elapsed-chart-${idx}`;
        const wrapper = document.createElement('div');
        wrapper.style.marginBottom = '1.5rem';
        const title = document.createElement('div');
        title.style.cssText = 'font-weight:bold;margin-bottom:0.25rem;';
        title.textContent = `${boatItemA.label} vs ${boatItemB.label}`;
        const chartDiv = document.createElement('div');
        chartDiv.id = divId;
        chartDiv.style.cssText = 'width:100%;height:500px;';
        wrapper.appendChild(title);
        wrapper.appendChild(chartDiv);
        container.appendChild(wrapper);
        renderElapsedChart(divId, data, boatItemA.color, boatItemB.color);
    });
}

/** Re-renders the elapsed-time charts without refetching; used by the From-0 toggle. */
function onElapsedFromZeroChange() {
    loadElapsedCharts();
}

function renderElapsedChart(divId, data, colorA, colorB) {
    let points = data.points || [];

    // Apply last-12-months filter if active
    if (showLast12Months) {
        const cutoff = new Date();
        cutoff.setFullYear(cutoff.getFullYear() - 1);
        const cutoffStr = cutoff.toISOString().slice(0, 10);
        points = points.filter(p => p.date >= cutoffStr);
    }

    if (points.length === 0) {
        Plotly.purge(divId);
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

    // Expected RF ratio line (through origin)
    const rfA = selectedVariant === 'nonSpin' ? data.boatA.rfNonSpin : data.boatA.rfSpin;
    const rfB = selectedVariant === 'nonSpin' ? data.boatB.rfNonSpin : data.boatB.rfSpin;
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

    // Expected PF ratio line (through origin)
    const pfA = selectedVariant === 'nonSpin' ? data.boatA.pfNonSpin
               : selectedVariant === 'twoHanded' ? data.boatA.pfTwoHanded : data.boatA.pfSpin;
    const pfB = selectedVariant === 'nonSpin' ? data.boatB.pfNonSpin
               : selectedVariant === 'twoHanded' ? data.boatB.pfTwoHanded : data.boatB.pfSpin;
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
        window.location.href = 'data.html?' + new URLSearchParams({ tab: 'races', raceId: pt.customdata.raceId });
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
    document.getElementById('variant-selector').addEventListener('change', onVariantChange);
    document.getElementById('show-rf-line')       .addEventListener('change', e => { showRfLine          = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-pf-line')      .addEventListener('change', e => { showPfLine         = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-trend-linear') .addEventListener('change', e => { showTrendLinear    = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-trend-sliding').addEventListener('change', e => { showTrendSliding   = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('hide-legend')       .addEventListener('change', e => { hideLegend         = e.target.checked; if (lastChartData) renderChart(lastChartData); loadElapsedCharts(); });
    document.getElementById('last-12-months')    .addEventListener('change', e => { showLast12Months   = e.target.checked; if (lastChartData) renderChart(lastChartData); loadElapsedCharts(); });
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
    if (selectedItems.length > 0) loadChart();
});
