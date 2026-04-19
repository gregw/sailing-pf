'use strict';

const PALETTE = [
    '#3a7ec4', '#e67e22', '#27ae60', '#8e44ad',
    '#c0392b', '#16a085', '#d35400', '#2c3e50',
    '#f39c12', '#1abc9c'
];

const STORAGE_KEY = 'hpf-comparison-items';

let selectedItems   = [];   // {type:'boat', id, label, color}
let allAvailable    = false;
let selectedVariant = 'spin';
let showErrorBars    = false;
let showRfLine       = true;
let showHpfLine      = true;
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
    // Build N virtual seed entries at the HPF value so the average is fully initialised
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
        document.getElementById('hpf-calc').style.display = 'none';
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

    // Compute Y range from all filtered entries
    let yMin = 0.5, yMax = 1.5;
    data.boats.forEach(b => {
        filteredPerBoat.get(b.id).forEach(e => {
            if (e.backCalcFactor < yMin) yMin = e.backCalcFactor;
            if (e.backCalcFactor > yMax) yMax = e.backCalcFactor;
        });
    });
    const pad = (yMax - yMin) * 0.05 + 0.02;
    yMin = Math.floor((yMin - pad) * 20) / 20;
    yMax = Math.ceil ((yMax + pad) * 20) / 20;
    const yRange = [Math.min(0.5, yMin), Math.max(1.5, yMax)];

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
        const hpfFactor = selectedVariant === 'nonSpin' ? boat.hpfNonSpin
            : selectedVariant === 'twoHanded' ? boat.hpfTwoHanded : boat.hpfSpin;

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
        if (showHpfLine && hpfFactor) {
            traces.push({
                x: lineX, y: [hpfFactor.value, hpfFactor.value],
                type: 'scatter', mode: 'lines',
                name: `${name} HPF`,
                line: { color, dash: 'solid', width: 2 },
                legendgroup: boat.id,
                hovertemplate: `${esc(name)} HPF: %{y:.4f}<extra></extra>`
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
                const hpfSeed = hpfFactor ? hpfFactor.value : null;
                const s = slidingAverage(entries, slidingAverageCount, slidingAverageDrops, hpfSeed);
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
        const { raceId, seriesId, seriesName } = pt.customdata;
        const p = new URLSearchParams({ tab: 'races' });
        if (seriesId)   p.set('seriesId',   seriesId);
        else if (raceId) p.set('raceId', raceId);
        window.location.href = 'data.html?' + p;
    });

    renderHandicapCalc(data);
}

// ---- Handicap calculator ----

function renderHandicapCalc(data) {
    const section = document.getElementById('hpf-calc');
    const table   = section.querySelector('table');

    const showBestFit = data.boats.length <= 3;

    const calcBoats = data.boats.map(b => {
        const item  = selectedItems.find(i => i.type === 'boat' && i.id === b.id);
        const color = item ? item.color : '#888';
        const name  = item ? item.label : (b.sailNumber ? `${b.sailNumber} ${b.name}` : b.name);

        const hpfFactor = selectedVariant === 'nonSpin'   ? b.hpfNonSpin
                        : selectedVariant === 'twoHanded' ? b.hpfTwoHanded
                        : b.hpfSpin;
        const rfFactor  = selectedVariant === 'nonSpin'   ? b.rfNonSpin
                        : selectedVariant === 'twoHanded' ? null
                        : b.rfSpin;

        // Best fit: latest endpoint of the weighted OLS trend through filtered back-calc factors
        let bestFit = null;
        if (showBestFit) {
            const entries = filterEntries(b.entries || []);
            const trend = weightedOlsTrend(entries);
            if (trend) bestFit = trend.y[1];
        }

        return {
            id: b.id, name, color,
            hpf:     hpfFactor ? hpfFactor.value : null,
            rf:      rfFactor  ? rfFactor.value  : null,
            bestFit
        };
    }).filter(b => b.hpf != null).sort((a, b) => b.hpf - a.hpf);

    if (calcBoats.length === 0) {
        section.style.display = 'none';
        return;
    }

    section.style.display = '';
    table.innerHTML = '';

    const factorTypes = showBestFit ? ['hpf', 'rf', 'bestFit'] : ['hpf', 'rf'];

    // Header row
    const thead = document.createElement('thead');
    const hdrTr = document.createElement('tr');
    const hdrLabels = ['', 'HPF', 'RF', ...(showBestFit ? ['Best Fit'] : []), 'Enter handicap'];
    hdrLabels.forEach((text, i) => {
        const th = document.createElement('th');
        th.textContent = text;
        const align = i === 0 ? 'left' : i === hdrLabels.length - 1 ? 'center' : 'right';
        th.style.cssText = `padding:2px 8px;font-size:0.8rem;color:#555;text-align:${align};`;
        hdrTr.appendChild(th);
    });
    thead.appendChild(hdrTr);
    table.appendChild(thead);

    // Data rows — one input drives all three value columns via per-column ratio
    const tbody = document.createElement('tbody');
    calcBoats.forEach(b => {
        const tr = document.createElement('tr');

        // Boat name
        const tdName = document.createElement('td');
        tdName.style.cssText = `color:${b.color};font-weight:bold;`;
        tdName.textContent = b.name;
        tr.appendChild(tdName);

        // Value cells: HPF, RF, [Best Fit]
        factorTypes.forEach(ft => {
            const td = document.createElement('td');
            td.className = 'hpf-calc-value';
            td.style.cssText = 'font-family:monospace;padding:2px 8px;text-align:right;';
            const v = b[ft];
            td.textContent = v != null ? v.toFixed(4) : '—';
            td.dataset.boatId = b.id;
            td.dataset.factorType = ft;
            td.dataset.origValue = v != null ? String(v) : '';
            tr.appendChild(td);
        });

        // Single input cell
        const tdInput = document.createElement('td');
        tdInput.style.cssText = 'padding:2px 4px;text-align:center;';
        const input = document.createElement('input');
        input.type = 'number';
        input.step = '0.0001';
        input.min  = '0.1';
        input.max  = '2.0';
        input.className = 'hpf-calc-input';
        input.dataset.boatId = b.id;
        input.placeholder = 'enter…';
        input.style.cssText = 'width:90px;font-family:monospace;text-align:right;';
        input.addEventListener('input', () => recalcAll(calcBoats));
        tdInput.appendChild(input);
        tr.appendChild(tdInput);

        tbody.appendChild(tr);
    });
    table.appendChild(tbody);
}

// Fit-quality color: green (good fit) → red (bad fit)
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

// Confidence color: blue (low variance) → brown (high variance)
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

function restoreAll() {
    document.querySelectorAll('.hpf-calc-value').forEach(td => {
        const origStr = td.dataset.origValue;
        td.textContent = origStr ? parseFloat(origStr).toFixed(4) : '—';
        td.style.color = '';
        td.title = '';
    });
}

function scaleSingle(anchor, calcBoats) {
    document.querySelectorAll('.hpf-calc-value').forEach(td => {
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

        td.textContent = (origVal * (anchor.value / srcFactor)).toFixed(4);
        td.style.color = '#c05000';
        td.title = 'Scaled from single entered value — no consensus spread available';
    });
}

function scaleMulti(anchors, calcBoats) {
    // Build per-factor-type ratio stats
    const anchorIds = new Set(anchors.map(a => a.boat.id));
    const anchorByBoat = new Map(anchors.map(a => [a.boat.id, a]));

    // Collect factor types from the cells
    const ftSet = new Set();
    document.querySelectorAll('.hpf-calc-value').forEach(td => ftSet.add(td.dataset.factorType));

    // Per factor type: compute ratios, mean, stddev
    const ftStats = {};
    for (const ft of ftSet) {
        const ratios = [];
        for (const a of anchors) {
            const orig = a.boat[ft];
            if (orig != null && orig !== 0) ratios.push({ boatId: a.boat.id, r: a.value / orig });
        }
        if (ratios.length === 0) {
            ftStats[ft] = null;
            continue;
        }
        const R = ratios.reduce((s, x) => s + x.r, 0) / ratios.length;
        const S = ratios.length > 1
            ? Math.sqrt(ratios.reduce((s, x) => s + (x.r - R) ** 2, 0) / ratios.length)
            : 0;
        const cv = R > 0 ? S / R : 0;
        ftStats[ft] = { ratios, R, S, cv, ratioMap: new Map(ratios.map(x => [x.boatId, x.r])) };
    }

    // Update all value cells
    document.querySelectorAll('.hpf-calc-value').forEach(td => {
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

        // Single valid ratio for this column — treat as single-anchor
        if (stats.ratios.length === 1) {
            td.textContent = (origVal * stats.R).toFixed(4);
            td.style.color = '#c05000';
            td.title = 'Scaled from single entered value — no consensus spread available';
            return;
        }

        const isAnchor = anchorIds.has(boatId);
        if (isAnchor) {
            // Show the entered value (entered / orig * orig = entered)
            const a = anchorByBoat.get(boatId);
            td.textContent = a.value.toFixed(4);
            // Color: fit quality — how far this boat's ratio is from consensus
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
            // Unentered boat: consensus-scaled value
            td.textContent = (origVal * stats.R).toFixed(4);
            // Color: confidence based on coefficient of variation
            td.style.color = confidenceColor(stats.cv);
            td.title = confidenceLabel(stats.cv);
        }
    });
}

function recalcAll(calcBoats) {
    const anchors = [];
    document.querySelectorAll('.hpf-calc-input').forEach(inp => {
        const v = parseFloat(inp.value);
        if (!isNaN(v)) {
            const boat = calcBoats.find(b => b.id === inp.dataset.boatId);
            if (boat) anchors.push({ boat, value: v });
        }
    });

    if (anchors.length === 0) { restoreAll(); return; }
    if (anchors.length === 1) { scaleSingle(anchors[0], calcBoats); return; }
    scaleMulti(anchors, calcBoats);
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

    // Expected HPF ratio line (through origin)
    const hpfA = selectedVariant === 'nonSpin' ? data.boatA.hpfNonSpin
               : selectedVariant === 'twoHanded' ? data.boatA.hpfTwoHanded : data.boatA.hpfSpin;
    const hpfB = selectedVariant === 'nonSpin' ? data.boatB.hpfNonSpin
               : selectedVariant === 'twoHanded' ? data.boatB.hpfTwoHanded : data.boatB.hpfSpin;
    if (hpfA && hpfB && hpfA.value && hpfB.value) {
        const slope = hpfB.value / hpfA.value;
        traces.push({
            x: [x0, x1],
            y: [0, slope * x1],
            type: 'scatter', mode: 'lines',
            name: `HPF ratio (${hpfB.value.toFixed(4)} / ${hpfA.value.toFixed(4)} = ${slope.toFixed(4)})`,
            line: { color: colorB, width: 2, dash: 'dash' }
        });
    }

    const yMin = Math.min(...ys), yMax = Math.max(...ys);
    const yPad = (yMax - yMin) * 0.05 || yMin * 0.05;

    const layout = {
        xaxis: { title: `${esc(nameB)} elapsed (h)`, range: [xMax + xPad, 0] },
        yaxis: { title: `${esc(nameA)} elapsed (h)`, range: [yMax + yPad, 0] },
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
    document.getElementById('show-hpf-line')      .addEventListener('change', e => { showHpfLine         = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-trend-linear') .addEventListener('change', e => { showTrendLinear    = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-trend-sliding').addEventListener('change', e => { showTrendSliding   = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('hide-legend')       .addEventListener('change', e => { hideLegend         = e.target.checked; if (lastChartData) renderChart(lastChartData); loadElapsedCharts(); });
    document.getElementById('last-12-months')    .addEventListener('change', e => { showLast12Months   = e.target.checked; if (lastChartData) renderChart(lastChartData); loadElapsedCharts(); });
    document.getElementById('common-races-only') .addEventListener('change', e => { showCommonRacesOnly = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('boat-search').addEventListener('input', () => {
        clearTimeout(boatDebounce);
        boatDebounce = setTimeout(loadCandidates, 250);
    });
    document.getElementById('add-boat-btn').addEventListener('click', addBoat);
    loadCandidates();
    if (selectedItems.length > 0) loadChart();
});
