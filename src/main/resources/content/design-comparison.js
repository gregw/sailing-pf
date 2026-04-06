'use strict';

let designA = null;   // {id, canonicalName, rfSpin, rfNonSpin}
let designB = null;   // {id, canonicalName, rfSpin, rfNonSpin}
let candidatesA = [];
let candidatesB = [];
let focusedA    = null;
let focusedB    = null;
let debounceA   = null;
let debounceB   = null;
let selectedVariant = 'spin';
let chartData   = null;
let lastFit     = null;   // {slope, intercept} from most recent linearFit
let lastRfRatio = null;   // rfB.value / rfA.value for current variant

// ---- Candidate loading ----

async function loadCandidatesA() {
    const q = document.getElementById('search-a').value.trim();
    const params = new URLSearchParams();
    if (q) params.set('q', q);
    const data = await fetchJson('/api/design-comparison/candidates?' + params);
    if (!data) return;
    candidatesA = data.designs || [];
    renderListA();
}

async function loadCandidatesB() {
    if (!designA) {
        candidatesB = [];
        renderListB();
        return;
    }
    const q = document.getElementById('search-b').value.trim();
    const params = new URLSearchParams({ designAId: designA.id });
    if (q) params.set('q', q);
    const data = await fetchJson('/api/design-comparison/candidates?' + params);
    if (!data) return;
    candidatesB = data.designs || [];
    renderListB();
}

// ---- List rendering ----

function renderListA() {
    const list = document.getElementById('list-a');
    list.innerHTML = '';
    if (candidatesA.length === 0) {
        const el = document.createElement('div');
        el.className = 'selector-empty';
        el.textContent = 'No designs found';
        list.appendChild(el);
        document.getElementById('select-a-btn').disabled = true;
        return;
    }
    candidatesA.forEach(d => {
        const div = document.createElement('div');
        div.className = 'selector-item' + (d.id === focusedA ? ' focused' : '')
            + (designA && d.id === designA.id ? ' focused' : '');
        div.textContent = d.canonicalName;
        div.title = d.id;
        div.addEventListener('click', () => { focusedA = d.id; renderListA(); });
        div.addEventListener('dblclick', () => { focusedA = d.id; selectA(); });
        list.appendChild(div);
    });
    document.getElementById('select-a-btn').disabled = focusedA == null;
}

function renderListB() {
    const list = document.getElementById('list-b');
    list.innerHTML = '';
    if (!designA) {
        const el = document.createElement('div');
        el.className = 'selector-empty';
        el.textContent = 'Select Design A first';
        list.appendChild(el);
        document.getElementById('select-b-btn').disabled = true;
        return;
    }
    if (candidatesB.length === 0) {
        const el = document.createElement('div');
        el.className = 'selector-empty';
        el.textContent = 'No co-racing designs found';
        list.appendChild(el);
        document.getElementById('select-b-btn').disabled = true;
        return;
    }
    candidatesB.forEach(d => {
        const div = document.createElement('div');
        div.className = 'selector-item' + (d.id === focusedB ? ' focused' : '')
            + (designB && d.id === designB.id ? ' focused' : '');
        div.textContent = d.canonicalName;
        div.title = d.id;
        div.addEventListener('click', () => { focusedB = d.id; renderListB(); });
        div.addEventListener('dblclick', () => { focusedB = d.id; selectB(); });
        list.appendChild(div);
    });
    document.getElementById('select-b-btn').disabled = focusedB == null;
}

// ---- Selection ----

function selectA() {
    const d = candidatesA.find(x => x.id === focusedA);
    if (!d) return;
    designA  = d;
    focusedA = null;
    designB  = null;
    focusedB = null;
    document.getElementById('label-a').textContent = d.canonicalName;
    document.getElementById('label-b').textContent = '';
    document.getElementById('select-a-btn').disabled = true;
    renderListA();
    loadCandidatesB();
    clearChart();
}

function selectB() {
    const d = candidatesB.find(x => x.id === focusedB);
    if (!d) return;
    designB  = d;
    focusedB = null;
    document.getElementById('label-b').textContent = d.canonicalName;
    document.getElementById('select-b-btn').disabled = true;
    renderListB();
    loadChart();
}

// ---- Chart ----

function clearChart() {
    Plotly.purge('design-comparison-chart');
    chartData = null;
    lastFit = null;
    lastRfRatio = null;
    document.getElementById('design-calc').style.display = 'none';
}

async function loadChart() {
    if (!designA || !designB) return;
    const params = new URLSearchParams({ designAId: designA.id, designBId: designB.id });
    const data = await fetchJson('/api/design-comparison/chart?' + params);
    if (!data) return;
    chartData = data;
    renderChart(data);
}

function onVariantChange() {
    selectedVariant = document.getElementById('variant-selector').value;
    if (chartData) renderChart(chartData);
}

function renderChart(data) {
    const points = data.points || [];
    if (points.length === 0) {
        Plotly.purge('design-comparison-chart');
        return;
    }

    const xs = points.map(p => p.x);
    const ys = points.map(p => p.y);

    const texts = points.map(p =>
        `${esc(p.date || '')}<br>` +
        (p.seriesName ? `${esc(p.seriesName)}<br>` : '') +
        (p.raceName   ? `${esc(p.raceName)}<br>`   : '') +
        `${esc(p.division || '')}<br>` +
        `${esc(data.designA.canonicalName)}: ${p.aBoats.map(esc).join(', ')}<br>` +
        `${esc(data.designB.canonicalName)}: ${p.bBoats.map(esc).join(', ')}<br>` +
        `A: ${fmtTime(p.y)} &nbsp; B: ${fmtTime(p.x)}`
    );

    const customdata = points.map(p => ({ raceId: p.raceId, seriesName: p.seriesName }));

    const traces = [];

    // Scatter data points
    traces.push({
        x: xs, y: ys,
        type: 'scatter', mode: 'markers',
        name: 'Observed divisions',
        marker: { color: '#2255aa', size: 7, opacity: 0.7,
                  line: { color: 'rgba(0,0,0,0.3)', width: 0.5 } },
        text: texts, hoverinfo: 'text',
        customdata
    });

    const xMin = Math.min(...xs), xMax = Math.max(...xs);
    const xPad = (xMax - xMin) * 0.05 || xMin * 0.05;

    // Best-fit line (OLS: Y on X)
    const fit = linearFit(xs, ys);
    if (fit) {
        const x0 = xMin - xPad, x1 = xMax + xPad;
        traces.push({
            x: [x0, x1],
            y: [fit.slope * x0 + fit.intercept, fit.slope * x1 + fit.intercept],
            type: 'scatter', mode: 'lines',
            name: `Best fit (slope ${fit.slope.toFixed(4)})`,
            line: { color: '#2255aa', width: 2 }
        });
    }

    // Expected line from reference factors: slope = rfB / rfA (through data centroid)
    const rfA = selectedVariant === 'nonSpin' ? data.designA.rfNonSpin : data.designA.rfSpin;
    const rfB = selectedVariant === 'nonSpin' ? data.designB.rfNonSpin : data.designB.rfSpin;
    if (rfA && rfB && rfA.value && rfB.value) {
        const slope = rfB.value / rfA.value;
        const meanX = xs.reduce((s, v) => s + v, 0) / xs.length;
        const meanY = ys.reduce((s, v) => s + v, 0) / ys.length;
        const x0 = xMin - xPad, x1 = xMax + xPad;
        traces.push({
            x: [x0, x1],
            y: [meanY + slope * (x0 - meanX), meanY + slope * (x1 - meanX)],
            type: 'scatter', mode: 'lines',
            name: `Expected RF (${rfA.value.toFixed(4)} / ${rfB.value.toFixed(4)} = ${slope.toFixed(4)})`,
            line: { color: '#c47900', width: 2, dash: 'dot' }
        });
    }

    const yMin = Math.min(...ys), yMax = Math.max(...ys);
    const yPad = (yMax - yMin) * 0.05 || yMin * 0.05;

    const layout = {
        xaxis: { title: `${esc(data.designB.canonicalName)} elapsed (s)`,
                 range: [xMax + xPad, xMin - xPad] },
        yaxis: { title: `${esc(data.designA.canonicalName)} elapsed (s)`,
                 range: [yMax + yPad, yMin - yPad] },
        legend: { orientation: 'v', xanchor: 'right', x: 1 },
        margin: { t: 20, b: 70, l: 80, r: 20 },
        hovermode: 'closest'
    };

    const chartDiv = document.getElementById('design-comparison-chart');
    Plotly.react('design-comparison-chart', traces, layout, { responsive: true });

    chartDiv.removeAllListeners && chartDiv.removeAllListeners('plotly_click');
    chartDiv.on('plotly_click', (eventData) => {
        if (!eventData.points || !eventData.points.length) return;
        const pt = eventData.points[0];
        if (!pt.customdata || !pt.customdata.raceId) return;
        const p = new URLSearchParams({ tab: 'races', raceId: pt.customdata.raceId });
        window.location.href = 'data.html?' + p;
    });

    renderCalc(data, fit);
}

// ---- Handicap calculator ----

function renderCalc(data, fit) {
    const section = document.getElementById('design-calc');
    const rfA = selectedVariant === 'nonSpin' ? data.designA.rfNonSpin : data.designA.rfSpin;
    const rfB = selectedVariant === 'nonSpin' ? data.designB.rfNonSpin : data.designB.rfSpin;

    lastFit = fit;
    lastRfRatio = (rfA && rfB) ? rfB.value / rfA.value : null;

    document.getElementById('calc-name-a').textContent = data.designA.canonicalName;
    document.getElementById('calc-name-b').textContent = data.designB.canonicalName;
    document.getElementById('calc-rf-a').textContent = rfA ? rfA.value.toFixed(4) : '—';
    document.getElementById('calc-rf-b').textContent = rfB ? rfB.value.toFixed(4) : '—';
    document.getElementById('calc-input-a').value = '';
    document.getElementById('calc-input-b').value = '';
    document.getElementById('calc-rf-result-a').textContent = '—';
    document.getElementById('calc-rf-result-b').textContent = '—';
    document.getElementById('calc-fit-result-a').textContent = '—';
    document.getElementById('calc-fit-result-b').textContent = '—';

    const parts = [];
    if (lastRfRatio != null) parts.push(`RF ratio B/A: ${lastRfRatio.toFixed(4)}`);
    if (fit) parts.push(`Best fit slope: ${fit.slope.toFixed(4)}`);
    document.getElementById('calc-ratios').textContent = parts.join('   |   ');

    section.style.display = '';
}

function onCalcInput(source) {
    const inputA = document.getElementById('calc-input-a');
    const inputB = document.getElementById('calc-input-b');
    const rfResA  = document.getElementById('calc-rf-result-a');
    const rfResB  = document.getElementById('calc-rf-result-b');
    const fitResA = document.getElementById('calc-fit-result-a');
    const fitResB = document.getElementById('calc-fit-result-b');

    if (source === 'a') {
        inputB.value = '';
        const val = parseFloat(inputA.value);
        if (!isNaN(val) && val > 0) {
            rfResA.textContent  = '—';
            fitResA.textContent = '—';
            rfResB.textContent  = lastRfRatio != null ? (val * lastRfRatio).toFixed(4) : '—';
            fitResB.textContent = lastFit       ? (val * lastFit.slope).toFixed(4)  : '—';
        } else {
            rfResA.textContent = rfResB.textContent = fitResA.textContent = fitResB.textContent = '—';
        }
    } else {
        inputA.value = '';
        const val = parseFloat(inputB.value);
        if (!isNaN(val) && val > 0) {
            rfResB.textContent  = '—';
            fitResB.textContent = '—';
            rfResA.textContent  = lastRfRatio != null ? (val / lastRfRatio).toFixed(4) : '—';
            fitResA.textContent = lastFit       ? (val / lastFit.slope).toFixed(4)  : '—';
        } else {
            rfResA.textContent = rfResB.textContent = fitResA.textContent = fitResB.textContent = '—';
        }
    }
}

// ---- Helpers ----

function linearFit(xs, ys) {
    const n = xs.length;
    if (n < 2) return null;
    const meanX = xs.reduce((s, v) => s + v, 0) / n;
    const meanY = ys.reduce((s, v) => s + v, 0) / n;
    let num = 0, den = 0;
    for (let i = 0; i < n; i++) {
        num += (xs[i] - meanX) * (ys[i] - meanY);
        den += (xs[i] - meanX) ** 2;
    }
    if (den === 0) return null;
    const slope = num / den;
    return { slope, intercept: meanY - slope * meanX };
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

document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('search-a').addEventListener('input', () => {
        clearTimeout(debounceA);
        debounceA = setTimeout(loadCandidatesA, 250);
    });
    document.getElementById('search-b').addEventListener('input', () => {
        clearTimeout(debounceB);
        debounceB = setTimeout(loadCandidatesB, 250);
    });
    document.getElementById('select-a-btn').addEventListener('click', selectA);
    document.getElementById('select-b-btn').addEventListener('click', selectB);
    document.getElementById('variant-selector').addEventListener('change', onVariantChange);
    loadCandidatesA();
});
