'use strict';

/*
 * Multi-design comparison — mirrors comparison.js (boats) with design-level semantics:
 *   - single selector adds any number of designs to a chip list
 *   - main chart draws each design's RF line plus back-calc dots/trends across all
 *     member-boat residuals (anchored against the design's RF for the variant)
 *   - 2 or 3 designs → pairwise elapsed-time charts
 *   - handicap calculator accepts allocated handicaps for any subset and colours
 *     entered cells by fit quality, unentered cells by confidence
 */

const PALETTE = [
    '#3a7ec4', '#e67e22', '#27ae60', '#8e44ad',
    '#c0392b', '#16a085', '#d35400', '#2c3e50',
    '#f39c12', '#1abc9c'
];

const STORAGE_KEY = 'pf-designComparison-items';

let selectedItems   = [];   // {type:'design', id, label, color}
let allAvailable    = false;
let selectedVariant = 'spin';
let showRfLine       = true;
let showTrendLinear  = true;
let showTrendSliding = true;
let hideLegend       = false;
let showLast12Months  = false;
let showCommonRacesOnly = false;
let slidingAverageCount = 8;
let slidingAverageDrops = 0;
let candidateDesigns = [];
let focusedDesignId  = null;
let designDebounce   = null;
let lastChartData    = null;
let calcSort         = { col: 'rf', dir: 'desc' };

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
        color: nextColor()
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

// ---- Trend helpers (copied from comparison.js) ----

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

function onVariantChange() {
    selectedVariant = document.getElementById('variant-selector').value;
    if (lastChartData) renderChart(lastChartData);
    loadElapsedCharts();
}

function filterByVariant(entries) {
    return entries.filter(e =>
        selectedVariant === 'nonSpin' ? e.nonSpinnaker : !e.nonSpinnaker && !e.twoHanded
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
    const designs = data.designs || [];

    const filteredPerDesign = new Map(designs.map(d => [d.id, filterEntries(d.entries || [])]));

    if (showCommonRacesOnly && designs.length >= 2) {
        const sets = designs.map(d => new Set(filteredPerDesign.get(d.id).map(e => e.raceId)));
        const common = sets.reduce((acc, s) => new Set([...acc].filter(id => s.has(id))));
        designs.forEach(d =>
            filteredPerDesign.set(d.id, filteredPerDesign.get(d.id).filter(e => common.has(e.raceId))));
    }

    let yMin = 0.5, yMax = 1.5;
    designs.forEach(d => {
        filteredPerDesign.get(d.id).forEach(e => {
            if (e.backCalcFactor < yMin) yMin = e.backCalcFactor;
            if (e.backCalcFactor > yMax) yMax = e.backCalcFactor;
        });
    });
    const pad = (yMax - yMin) * 0.05 + 0.02;
    yMin = Math.floor((yMin - pad) * 20) / 20;
    yMax = Math.ceil ((yMax + pad) * 20) / 20;
    const yRange = [Math.min(0.5, yMin), Math.max(1.5, yMax)];

    let minDate = null, maxDate = null;
    designs.forEach(d => {
        filteredPerDesign.get(d.id).forEach(e => {
            if (!minDate || e.date < minDate) minDate = e.date;
            if (!maxDate || e.date > maxDate) maxDate = e.date;
        });
    });
    const lineX = minDate
        ? [minDate, maxDate]
        : ['2018-01-01', new Date().toISOString().slice(0, 10)];

    designs.forEach(design => {
        const item  = selectedItems.find(i => i.id === design.id);
        const color = item ? item.color : '#888';
        const name  = item ? item.label : (design.canonicalName || design.id);

        const rfFactor = selectedVariant === 'nonSpin' ? design.rfNonSpin : design.rfSpin;

        if (showRfLine && rfFactor) {
            traces.push({
                x: lineX, y: [rfFactor.value, rfFactor.value],
                type: 'scatter', mode: 'lines',
                name: `${name} RF`,
                line: { color, dash: 'dashdot', width: 1.5 },
                legendgroup: design.id,
                hovertemplate: `${esc(name)} RF: %{y:.4f}<extra></extra>`
            });
        }

        const entries = filteredPerDesign.get(design.id);
        if (entries.length > 0) {
            const xs = [], ys = [], sizes = [], opacities = [], symbols = [], texts = [], custom = [];
            entries.forEach(e => {
                const w = Math.min(Math.max(e.weight, 0), 1);
                xs.push(e.date);
                ys.push(e.backCalcFactor);
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
            if (showTrendSliding) {
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

    const layout = {
        xaxis: { title: 'Date', type: 'date' },
        yaxis: { title: 'Factor', range: yRange },
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
        const { raceId, seriesId } = pt.customdata;
        // seriesId wins: filter races by series; otherwise open the single race detail.
        if (seriesId) {
            window.location.href = 'races.html?' + new URLSearchParams({seriesId});
        } else if (raceId) {
            window.location.href = 'races.html?' + new URLSearchParams({id: raceId});
        }
    });

    renderHandicapCalc(data);
}

// ---- Handicap calculator ----

function renderHandicapCalc(data) {
    const section = document.getElementById('pf-calc');
    const table   = section.querySelector('table');

    const enteredValues = new Map();
    document.querySelectorAll('.pf-calc-input').forEach(inp => {
        if (inp.value !== '') enteredValues.set(inp.dataset.boatId, inp.value);
    });

    const designs = data.designs || [];
    const showBestFit = designs.length <= 8;

    const calcBoats = designs.map(d => {
        const item  = selectedItems.find(i => i.id === d.id);
        const color = item ? item.color : '#888';
        const name  = item ? item.label : (d.canonicalName || d.id);

        const rfFactor = selectedVariant === 'nonSpin' ? d.rfNonSpin : d.rfSpin;

        let bestFit = null;
        if (showBestFit) {
            const entries = filterEntries(d.entries || []);
            const trend = weightedOlsTrend(entries);
            if (trend) bestFit = trend.y[1];
        }

        return {
            id: d.id, name, color,
            rf:       rfFactor ? rfFactor.value : null,
            bestFit
        };
    }).filter(d => d.rf != null || d.bestFit != null);

    if (calcBoats.length === 0) {
        section.style.display = 'none';
        return;
    }

    section.style.display = '';
    table.innerHTML = '';

    const cols = [
        { key: 'name',    label: 'Design',         align: 'left'   },
        { key: 'input',   label: 'Enter handicap', align: 'center' },
        { key: 'rf',      label: 'RF',             align: 'right'  },
    ];
    if (showBestFit) cols.push({ key: 'bestFit', label: 'Best Fit', align: 'right' });

    if (!cols.some(c => c.key === calcSort.col)) calcSort = { col: 'rf', dir: 'desc' };

    sortCalcBoats(calcBoats);

    const thead = document.createElement('thead');
    const hdrTr = document.createElement('tr');
    cols.forEach(c => {
        const th = document.createElement('th');
        const isActive = c.key === calcSort.col;
        const arrow = isActive ? (calcSort.dir === 'asc' ? ' ↑' : ' ↓') : '';
        th.textContent = c.label + arrow;
        th.style.cssText = `padding:2px 8px;font-size:0.8rem;color:#555;text-align:${c.align};cursor:pointer;user-select:none;`
            + (isActive ? 'font-weight:bold;' : '');
        th.addEventListener('click', () => {
            if (calcSort.col === c.key) calcSort.dir = (calcSort.dir === 'asc' ? 'desc' : 'asc');
            else calcSort = { col: c.key, dir: c.key === 'name' ? 'asc' : 'desc' };
            renderHandicapCalc(data);
        });
        hdrTr.appendChild(th);
    });
    thead.appendChild(hdrTr);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    calcBoats.forEach(b => {
        const tr = document.createElement('tr');
        cols.forEach(c => {
            if (c.key === 'name') {
                const td = document.createElement('td');
                td.style.cssText = `color:${b.color};font-weight:bold;`;
                td.textContent = b.name;
                tr.appendChild(td);
            } else if (c.key === 'input') {
                const td = document.createElement('td');
                td.style.cssText = 'padding:2px 4px;text-align:center;';
                const input = document.createElement('input');
                input.type = 'number';
                input.step = '0.0001';
                input.min  = '0.1';
                input.max  = '2.0';
                input.className = 'pf-calc-input';
                input.dataset.boatId = b.id;
                input.placeholder = 'enter…';
                input.style.cssText = 'width:90px;font-family:monospace;text-align:right;';
                if (enteredValues.has(b.id)) input.value = enteredValues.get(b.id);
                input.addEventListener('input', () => recalcAll(calcBoats));
                td.appendChild(input);
                tr.appendChild(td);
            } else {
                const td = document.createElement('td');
                td.className = 'pf-calc-value';
                td.style.cssText = 'font-family:monospace;padding:2px 8px;text-align:right;';
                const v = b[c.key];
                td.textContent = v != null ? v.toFixed(4) : '—';
                td.dataset.boatId = b.id;
                td.dataset.factorType = c.key;
                td.dataset.origValue = v != null ? String(v) : '';
                tr.appendChild(td);
            }
        });
        tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    if (enteredValues.size > 0) recalcAll(calcBoats);
}

function sortCalcBoats(calcBoats) {
    const { col, dir } = calcSort;
    const mul = dir === 'asc' ? 1 : -1;
    if (col === 'name') {
        calcBoats.sort((a, b) => mul * a.name.localeCompare(b.name));
    } else if (col === 'input') {
        calcBoats.sort((a, b) => mul * ((a.rf ?? 0) - (b.rf ?? 0)));
    } else {
        calcBoats.sort((a, b) => {
            const av = a[col], bv = b[col];
            if (av == null && bv == null) return 0;
            if (av == null) return 1;
            if (bv == null) return -1;
            return mul * (av - bv);
        });
    }
}

function fitColor(deviation) {
    const t = Math.min(deviation / 0.05, 1);
    const h = 120 * (1 - t);
    return `hsl(${h}, 60%, 38%)`;
}
function fitLabel(deviation) {
    const pct = (deviation * 100).toFixed(1);
    if (deviation < 0.01) return `Entered value — deviation ${pct}% from consensus (excellent fit)`;
    if (deviation < 0.025) return `Entered value — deviation ${pct}% from consensus (good fit)`;
    if (deviation < 0.05) return `Entered value — deviation ${pct}% from consensus (moderate fit)`;
    return `Entered value — deviation ${pct}% from consensus (poor fit)`;
}
function confidenceColor(cv) {
    const t = Math.min(cv / 0.05, 1);
    const h = 210 - 180 * t;
    const l = 45 - 7 * t;
    return `hsl(${h}, 55%, ${l}%)`;
}
function confidenceLabel(cv) {
    const pct = (cv * 100).toFixed(1);
    if (cv < 0.01) return `Scaled from consensus — spread ${pct}% (high confidence)`;
    if (cv < 0.025) return `Scaled from consensus — spread ${pct}% (moderate confidence)`;
    if (cv < 0.05) return `Scaled from consensus — spread ${pct}% (low confidence)`;
    return `Scaled from consensus — spread ${pct}% (very low confidence)`;
}

// Small inline delta indicator: shows how the displayed cell value differs from the
// column's original (RF / Best Fit) allocation.
function deltaSpan(displayed, orig) {
    if (orig == null || isNaN(orig)) return '';
    const delta = displayed - orig;
    if (Math.abs(delta) < 0.00005) return '';
    const arrow = delta > 0 ? '↑' : '↓';
    const sign = delta > 0 ? '+' : '−';
    const color = delta > 0 ? '#a04020' : '#206020';
    return ` <span style="font-size:0.72rem;color:${color};margin-left:4px;font-weight:normal;">${arrow}${sign}${Math.abs(delta).toFixed(4)}</span>`;
}

function restoreAll() {
    document.querySelectorAll('.pf-calc-value').forEach(td => {
        const origStr = td.dataset.origValue;
        td.textContent = origStr ? parseFloat(origStr).toFixed(4) : '—';
        td.style.color = '';
        td.title = '';
    });
}

function scaleSingle(anchor) {
    document.querySelectorAll('.pf-calc-value').forEach(td => {
        const ft      = td.dataset.factorType;
        const origStr = td.dataset.origValue;
        if (!origStr) return;
        const origVal = parseFloat(origStr);
        if (isNaN(origVal)) return;

        const srcFactor = anchor.boat[ft];
        if (srcFactor == null) {
            td.textContent = origVal.toFixed(4);
            td.style.color = '';
            td.title = '';
            return;
        }
        // Single-anchor: every cell IS the consensus prediction, so delta is always 0.
        const newVal = origVal * (anchor.value / srcFactor);
        td.textContent = newVal.toFixed(4);
        td.style.color = '#c05000';
        td.title = 'Scaled from single entered value — no consensus spread available';
    });
}

function scaleMulti(anchors) {
    const anchorIds = new Set(anchors.map(a => a.boat.id));
    const anchorByBoat = new Map(anchors.map(a => [a.boat.id, a]));

    const ftSet = new Set();
    document.querySelectorAll('.pf-calc-value').forEach(td => ftSet.add(td.dataset.factorType));

    const ftStats = {};
    for (const ft of ftSet) {
        const ratios = [];
        for (const a of anchors) {
            const orig = a.boat[ft];
            if (orig != null && orig !== 0) ratios.push({ boatId: a.boat.id, r: a.value / orig });
        }
        if (ratios.length === 0) { ftStats[ft] = null; continue; }
        const R = ratios.reduce((s, x) => s + x.r, 0) / ratios.length;
        const S = ratios.length > 1
            ? Math.sqrt(ratios.reduce((s, x) => s + (x.r - R) ** 2, 0) / ratios.length)
            : 0;
        const cv = R > 0 ? S / R : 0;
        ftStats[ft] = { ratios, R, S, cv, ratioMap: new Map(ratios.map(x => [x.boatId, x.r])) };
    }

    document.querySelectorAll('.pf-calc-value').forEach(td => {
        const ft      = td.dataset.factorType;
        const boatId  = td.dataset.boatId;
        const origStr = td.dataset.origValue;
        if (!origStr) return;
        const origVal = parseFloat(origStr);
        if (isNaN(origVal)) return;

        const stats = ftStats[ft];
        if (!stats) {
            td.textContent = origVal.toFixed(4);
            td.style.color = '';
            td.title = '';
            return;
        }
        if (stats.ratios.length === 1) {
            td.textContent = (origVal * stats.R).toFixed(4);
            td.style.color = '#c05000';
            td.title = 'Scaled from single entered value — no consensus spread available';
            return;
        }
        const isAnchor = anchorIds.has(boatId);
        if (isAnchor) {
            // Show the entered value; delta is entered − consensus prediction (origVal * R).
            const a = anchorByBoat.get(boatId);
            const predicted = origVal * stats.R;
            td.innerHTML = a.value.toFixed(4) + deltaSpan(a.value, predicted);
            const r = stats.ratioMap.get(boatId);
            if (r != null) {
                const deviation = Math.abs(r - stats.R) / stats.R;
                td.style.color = fitColor(deviation);
                td.title = fitLabel(deviation);
            } else {
                td.style.color = '';
                td.title = '';
            }
        } else {
            // Unentered boat: cell IS the consensus prediction, so delta = 0.
            td.textContent = (origVal * stats.R).toFixed(4);
            td.style.color = confidenceColor(stats.cv);
            td.title = confidenceLabel(stats.cv);
        }
    });
}

function recalcAll(calcBoats) {
    const anchors = [];
    document.querySelectorAll('.pf-calc-input').forEach(inp => {
        const v = parseFloat(inp.value);
        if (!isNaN(v)) {
            const boat = calcBoats.find(b => b.id === inp.dataset.boatId);
            if (boat) anchors.push({ boat, value: v });
        }
    });
    if (anchors.length === 0) { restoreAll(); return; }
    if (anchors.length === 1) { scaleSingle(anchors[0]); return; }
    scaleMulti(anchors);
}

// ---- Elapsed time comparison charts ----

async function loadElapsedCharts() {
    const section = document.getElementById('elapsed-charts-section');
    const container = document.getElementById('elapsed-charts-container');

    const designs = selectedItems;
    if (designs.length < 2 || designs.length > 3) {
        if (designs.length >= 4) {
            section.style.display = '';
            container.innerHTML = '<p style="color:#666;">Too many designs selected — select 2 or 3 designs to see elapsed time comparisons.</p>';
        } else {
            section.style.display = 'none';
        }
        return;
    }

    const pairs = [];
    for (let i = 0; i < designs.length; i++)
        for (let j = i + 1; j < designs.length; j++)
            pairs.push([designs[i], designs[j]]);

    section.style.display = '';
    container.innerHTML = '';

    const results = await Promise.all(pairs.map(([a, b]) => {
        const params = new URLSearchParams({ designAId: a.id, designBId: b.id });
        return fetchJson('/api/design-comparison/chart?' + params);
    }));

    pairs.forEach(([dA, dB], idx) => {
        const data = results[idx];
        if (!data) return;
        const divId = `elapsed-chart-${idx}`;
        const wrapper = document.createElement('div');
        wrapper.style.marginBottom = '1.5rem';
        const title = document.createElement('div');
        title.style.cssText = 'font-weight:bold;margin-bottom:0.25rem;';
        title.textContent = `${dA.label} vs ${dB.label}`;
        const chartDiv = document.createElement('div');
        chartDiv.id = divId;
        chartDiv.style.cssText = 'width:100%;height:500px;';
        wrapper.appendChild(title);
        wrapper.appendChild(chartDiv);
        container.appendChild(wrapper);
        renderElapsedChart(divId, data, dA.color, dB.color);
    });
}

function onElapsedFromZeroChange() { loadElapsedCharts(); }

function renderElapsedChart(divId, data, colorA, colorB) {
    let points = data.points || [];

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
    if (fit) {
        traces.push({
            x: [x0, x1],
            y: [0, fit.slope * x1],
            type: 'scatter', mode: 'lines',
            name: `Best fit (slope ${fit.slope.toFixed(4)})`,
            line: { color: colorA, width: 2 }
        });
    }

    const rfA = selectedVariant === 'nonSpin' ? data.designA.rfNonSpin : data.designA.rfSpin;
    const rfB = selectedVariant === 'nonSpin' ? data.designB.rfNonSpin : data.designB.rfSpin;
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
    document.getElementById('variant-selector').addEventListener('change', onVariantChange);
    document.getElementById('show-rf-line')       .addEventListener('change', e => { showRfLine        = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-trend-linear')  .addEventListener('change', e => { showTrendLinear   = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-trend-sliding') .addEventListener('change', e => { showTrendSliding  = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('hide-legend')        .addEventListener('change', e => { hideLegend        = e.target.checked; if (lastChartData) renderChart(lastChartData); loadElapsedCharts(); });
    document.getElementById('last-12-months')     .addEventListener('change', e => { showLast12Months  = e.target.checked; if (lastChartData) renderChart(lastChartData); loadElapsedCharts(); });
    document.getElementById('common-races-only')  .addEventListener('change', e => { showCommonRacesOnly = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('design-search').addEventListener('input', () => {
        clearTimeout(designDebounce);
        designDebounce = setTimeout(loadCandidates, 250);
    });
    document.getElementById('add-design-btn').addEventListener('click', addDesign);
    loadCandidates();
    if (selectedItems.length > 0) loadChart();
});
