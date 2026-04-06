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
let showErrorBars   = false;
let showTrendLinear  = true;
let showTrendSliding = true;
let hideLegend       = false;
let slidingAverageCount = 8;
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

function slidingAverage(entries, n) {
    if (entries.length < 2) return null;
    const pts = [...entries].sort((a, b) => a.date.localeCompare(b.date));
    const xs = [], ys = [];
    for (let i = 0; i < pts.length; i++) {
        const window = pts.slice(Math.max(0, i - n + 1), i + 1);
        xs.push(pts[i].date);
        ys.push(window.reduce((a, p) => a + p.backCalcFactor, 0) / window.length);
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
}

function onVariantChange() {
    selectedVariant = document.getElementById('variant-selector').value;
    if (lastChartData) renderChart(lastChartData);
}

function filterByVariant(entries) {
    return entries.filter(e =>
        selectedVariant === 'twoHanded' ? e.twoHanded
        : selectedVariant === 'nonSpin' ? e.nonSpinnaker
        : !e.nonSpinnaker && !e.twoHanded
    );
}

function renderChart(data) {
    const traces = [];

    // Compute Y range from all filtered entries
    let yMin = 0.5, yMax = 1.5;
    data.boats.forEach(b => {
        filterByVariant(b.entries).forEach(e => {
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
        filterByVariant(b.entries).forEach(e => {
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

        if (rfFactor) {
            traces.push({
                x: lineX, y: [rfFactor.value, rfFactor.value],
                type: 'scatter', mode: 'lines',
                name: `${name} RF`,
                line: { color, dash: 'dashdot', width: 1.5 },
                legendgroup: boat.id,
                hovertemplate: `${esc(name)} RF: %{y:.4f}<extra></extra>`
            });
        }
        if (hpfFactor) {
            traces.push({
                x: lineX, y: [hpfFactor.value, hpfFactor.value],
                type: 'scatter', mode: 'lines',
                name: `${name} HPF`,
                line: { color, dash: 'solid', width: 2 },
                legendgroup: boat.id,
                hovertemplate: `${esc(name)} HPF: %{y:.4f}<extra></extra>`
            });
        }

        const entries = filterByVariant(boat.entries);
        if (entries.length > 0) {
            const xs = [], ys = [], sizes = [], texts = [], custom = [];
            entries.forEach(e => {
                xs.push(e.date);
                ys.push(e.backCalcFactor);
                sizes.push(5 + 5 * Math.min(e.weight, 1));
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
                marker: { color, size: sizes, symbol: 'circle',
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
                const s = slidingAverage(entries, slidingAverageCount);
                const avgLabel = `${slidingAverageCount}-finish avg`;
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

    const calcBoats = data.boats.map(b => {
        const item  = selectedItems.find(i => i.type === 'boat' && i.id === b.id);
        const color = item ? item.color : '#888';
        const name  = item ? item.label : (b.sailNumber ? `${b.sailNumber} ${b.name}` : b.name);
        const hpf   = selectedVariant === 'nonSpin'    ? b.hpfNonSpin
                    : selectedVariant === 'twoHanded'  ? b.hpfTwoHanded
                    : b.hpfSpin;
        return { id: b.id, name, color, hpf: hpf ? hpf.value : null };
    }).filter(b => b.hpf != null).sort((a, b) => b.hpf - a.hpf);

    if (calcBoats.length === 0) {
        section.style.display = 'none';
        return;
    }

    section.style.display = '';
    table.innerHTML = '';

    calcBoats.forEach(b => {
        const tr = document.createElement('tr');
        const tdName = document.createElement('td');
        tdName.style.color = b.color;
        tdName.style.fontWeight = 'bold';
        tdName.textContent = b.name;

        const tdHpf = document.createElement('td');
        tdHpf.style.fontFamily = 'monospace';
        tdHpf.style.paddingRight = '0.5rem';
        tdHpf.textContent = b.hpf.toFixed(4);

        const tdInput = document.createElement('td');
        const input = document.createElement('input');
        input.type = 'number';
        input.step = '0.0001';
        input.min  = '0.1';
        input.max  = '2.0';
        input.className = 'hpf-calc-input';
        input.dataset.boatId = b.id;
        input.placeholder = 'enter…';
        input.style.cssText = 'width:90px;font-family:monospace;text-align:right;';
        input.addEventListener('input', () => onCalcInput(input, calcBoats));
        tdInput.appendChild(input);

        tr.appendChild(tdName);
        tr.appendChild(tdHpf);
        tr.appendChild(tdInput);
        table.appendChild(tr);
    });
}

function onCalcInput(changedInput, calcBoats) {
    const raw = changedInput.value.trim();
    const val = parseFloat(raw);
    const srcBoat = calcBoats.find(b => b.id === changedInput.dataset.boatId);
    if (!srcBoat) return;

    document.querySelectorAll('.hpf-calc-input').forEach(input => {
        if (input === changedInput) return;
        if (!raw || isNaN(val)) {
            input.value = '';
        } else {
            const otherBoat = calcBoats.find(b => b.id === input.dataset.boatId);
            if (!otherBoat) return;
            input.value = (otherBoat.hpf * (val / srcBoat.hpf)).toFixed(4);
        }
    });
}

// ---- Initialisation ----

async function loadConfig() {
    const data = await fetchJson('/api/importers');
    if (data && data.slidingAverageCount) slidingAverageCount = data.slidingAverageCount;
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
    document.getElementById('show-trend-linear') .addEventListener('change', e => { showTrendLinear  = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('show-trend-sliding').addEventListener('change', e => { showTrendSliding = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('hide-legend')       .addEventListener('change', e => { hideLegend       = e.target.checked; if (lastChartData) renderChart(lastChartData); });
    document.getElementById('boat-search').addEventListener('input', () => {
        clearTimeout(boatDebounce);
        boatDebounce = setTimeout(loadCandidates, 250);
    });
    document.getElementById('add-boat-btn').addEventListener('click', addBoat);
    loadCandidates();
    if (selectedItems.length > 0) loadChart();
});
