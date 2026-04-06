// State
const state = {
    pages: { boats: 0, designs: 0, races: 0 },
    searchTimers: {},
    statusPoller: null,
    activeTab: 'boats'
};

const DAYS = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY'];
const IMPORTER_NAMES = ['sailsys-boats', 'sailsys-races', 'orc', 'ams'];

// --- Stats ---

async function loadStats() {
    const data = await fetchJson('/api/stats');
    if (!data) return;
    document.getElementById('stat-boats').textContent = data.boats.toLocaleString();
    document.getElementById('stat-designs').textContent = data.designs.toLocaleString();
    document.getElementById('stat-races').textContent = data.races.toLocaleString();
}

// --- Importers ---

async function loadImporters() {
    const data = await fetchJson('/api/importers');
    if (!data) return;

    const container = document.getElementById('importers');
    container.innerHTML = '';

    let anyRunning = false;
    for (const imp of data) {
        if (imp.status === 'running') anyRunning = true;
        container.appendChild(buildImporterCard(imp));
    }

    if (anyRunning) startStatusPoller();
}

function buildImporterCard(imp) {
    const sc = (imp.schedule && imp.schedule.name) ? imp.schedule : {
        enabled: false, day: 'FRIDAY', time: '03:00', mode: 'api'
    };
    const isRunning = imp.status === 'running';
    const timeVal = (sc.time || '03:00').substring(0, 5);

    const card = document.createElement('div');
    card.className = 'importer-card';
    card.id = 'card-' + imp.name;

    card.innerHTML = `
      <h3>
        ${imp.name}
        <span class="badge ${isRunning ? 'badge-running' : 'badge-idle'}" id="badge-${imp.name}">${imp.status}</span>
      </h3>
      <div class="run-row">
        <select id="run-mode-${imp.name}">
          <option value="api">API</option>
          <option value="directory">Directory</option>
        </select>
        <button onclick="runImporter('${imp.name}')">Run now</button>
      </div>
      <div class="schedule-form">
        <label>
          <input type="checkbox" id="sched-enabled-${imp.name}" ${sc.enabled ? 'checked' : ''}>
          Scheduled
        </label>
        <label>Day:
          <select id="sched-day-${imp.name}">
            ${DAYS.map(d => `<option value="${d}" ${(sc.day || 'FRIDAY') === d ? 'selected' : ''}>${d}</option>`).join('')}
          </select>
        </label>
        <label>Time: <input type="time" id="sched-time-${imp.name}" value="${timeVal}"></label>
        <label>Mode:
          <select id="sched-mode-${imp.name}">
            <option value="api" ${sc.mode === 'api' ? 'selected' : ''}>API</option>
            <option value="directory" ${sc.mode === 'directory' ? 'selected' : ''}>Directory</option>
          </select>
        </label>
        <button onclick="saveSchedule('${imp.name}')">Save schedule</button>
      </div>`;

    return card;
}

async function runImporter(name) {
    const mode = document.getElementById('run-mode-' + name).value;
    const resp = await fetch('/api/importers/' + name + '/run?mode=' + encodeURIComponent(mode), {
        method: 'POST'
    });
    const data = await resp.json().catch(() => ({}));
    if (resp.status === 409) {
        alert('An import is already running');
    } else if (resp.status === 202) {
        setBadge(name, 'running');
        startStatusPoller();
    } else {
        alert('Unexpected response: ' + resp.status);
    }
}

async function saveSchedule(name) {
    const enabled = document.getElementById('sched-enabled-' + name).checked;
    const day = document.getElementById('sched-day-' + name).value;
    const time = document.getElementById('sched-time-' + name).value;
    const mode = document.getElementById('sched-mode-' + name).value;

    const resp = await fetch('/api/importers/' + name + '/schedule', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, enabled, day, time, mode })
    });
    if (resp.ok) {
        alert('Schedule saved for ' + name);
    } else {
        const err = await resp.json().catch(() => ({ error: 'unknown error' }));
        alert('Failed: ' + (err.error || resp.status));
    }
}

function startStatusPoller() {
    if (state.statusPoller) return;
    state.statusPoller = setInterval(async () => {
        const data = await fetchJson('/api/importers/status');
        if (!data) return;
        if (!data.running) {
            clearInterval(state.statusPoller);
            state.statusPoller = null;
            // Refresh everything once import finishes
            loadImporters();
            loadStats();
        } else {
            setBadge(data.name, 'running');
        }
    }, 2000);
}

function setBadge(name, status) {
    const badge = document.getElementById('badge-' + name);
    if (!badge) return;
    badge.textContent = status;
    badge.className = 'badge ' + (status === 'running' ? 'badge-running' : 'badge-idle');
}

// --- Data Browser Tabs ---

function switchTab(entity) {
    ['boats', 'designs', 'races'].forEach(e => {
        document.getElementById('tab-btn-' + e).classList.toggle('active', e === entity);
        document.getElementById('panel-' + e).classList.toggle('active', e === entity);
    });
    state.activeTab = entity;
    if (document.querySelector('#tbody-' + entity + ' tr') === null) {
        loadList(entity, 0);
    }
}

function debounceSearch(entity) {
    clearTimeout(state.searchTimers[entity]);
    state.searchTimers[entity] = setTimeout(() => doSearch(entity), 300);
}

function doSearch(entity) {
    state.pages[entity] = 0;
    loadList(entity, 0);
}

async function loadList(entity, page) {
    state.pages[entity] = page;
    const q = document.getElementById('q-' + entity).value;
    const data = await fetchJson(`/api/${entity}?page=${page}&size=50&q=${encodeURIComponent(q)}`);
    if (!data) return;

    renderTable(entity, data.items);
    renderPager(entity, data);
}

function renderTable(entity, items) {
    const tbody = document.getElementById('tbody-' + entity);
    tbody.innerHTML = '';

    for (const item of items) {
        const tr = document.createElement('tr');
        tr.onclick = () => loadDetail(entity, item.id);
        if (entity === 'boats') {
            tr.innerHTML = `<td>${esc(item.id)}</td><td>${esc(item.sailNumber)}</td><td>${esc(item.name)}</td><td>${esc(item.designId)}</td>`;
        } else if (entity === 'designs') {
            tr.innerHTML = `<td>${esc(item.id)}</td><td>${esc(item.canonicalName)}</td><td>${esc((item.makerIds || []).join(', '))}</td>`;
        } else if (entity === 'races') {
            tr.innerHTML = `<td>${esc(item.id)}</td><td>${esc(item.clubId)}</td><td>${esc(item.date)}</td><td>${esc(item.handicapSystem)}</td>`;
        }
        tbody.appendChild(tr);
    }
}

function renderPager(entity, data) {
    const div = document.getElementById('pager-' + entity);
    div.innerHTML = '';
    const page = data.page;
    const totalPages = Math.ceil(data.total / data.size) || 1;

    if (page > 0) {
        const btn = document.createElement('button');
        btn.textContent = '← Prev';
        btn.onclick = () => loadList(entity, page - 1);
        div.appendChild(btn);
    }

    const info = document.createElement('span');
    info.textContent = `Page ${page + 1} of ${totalPages} (${data.total.toLocaleString()} total)`;
    div.appendChild(info);

    if ((page + 1) * data.size < data.total) {
        const btn = document.createElement('button');
        btn.textContent = 'Next →';
        btn.onclick = () => loadList(entity, page + 1);
        div.appendChild(btn);
    }
}

async function loadDetail(entity, id) {
    const data = await fetchJson('/api/' + entity + '/' + encodeURIComponent(id));
    if (!data) return;

    const panel = document.getElementById('detail-' + entity);
    const pre = document.getElementById('pre-' + entity);
    pre.textContent = JSON.stringify(data, null, 2);

    if (entity === 'boats') {
        const refDiv = document.getElementById('ref-factors-boats');
        refDiv.innerHTML = '<em>Loading reference factors…</em>';
        const ref = await fetchJson('/api/boats/' + encodeURIComponent(id) + '/reference');
        refDiv.innerHTML = ref ? renderReferenceFactors(ref) : '<em>No reference factors available</em>';
    }

    panel.classList.add('visible');
    panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function renderReferenceFactors(ref) {
    function row(label, f) {
        if (!f) return `<tr><td>${label}</td><td colspan="2" style="color:#999">—</td></tr>`;
        const barWidth = Math.round(f.weight * 80);
        return `<tr>
          <td>${label}</td>
          <td>${f.value.toFixed(4)}</td>
          <td>${f.weight.toFixed(3)} <span class="weight-bar" style="width:${barWidth}px"></span></td>
        </tr>`;
    }
    return `<strong>Reference factors (IRC equivalent, ${ref.currentYear})</strong>
      <table style="width:auto;margin-top:0.4rem;">
        <thead><tr><th>Variant</th><th>Value (TCF)</th><th>Weight</th></tr></thead>
        <tbody>
          ${row('Spin',       ref.spin)}
          ${row('Non-spin',   ref.nonSpin)}
          ${row('Two-handed', ref.twoHanded)}
        </tbody>
      </table>`
}

// --- Utilities ---

function esc(val) {
    if (val == null) return '';
    return String(val)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

async function fetchJson(url) {
    try {
        const resp = await fetch(url);
        if (!resp.ok) return null;
        return await resp.json();
    } catch (e) {
        console.error('fetchJson failed:', url, e);
        return null;
    }
}

// --- Handicap Analysis ---

// Track currently loaded analysis id
state.currentAnalysisId = null;

async function loadAnalysisList() {
    const data = await fetchJson('/api/analyse');
    if (!data) return;

    const sel = document.getElementById('analysis-select');
    // Keep the placeholder option
    while (sel.options.length > 1) sel.remove(1);

    for (const item of data) {
        const opt = document.createElement('option');
        opt.value = item.id;
        const r2str = item.r2 != null ? `  R²=${item.r2.toFixed(3)}` : '  (insufficient data)';
        opt.textContent = `${item.id}  (n=${item.n}${r2str})`;
        sel.appendChild(opt);
    }
}

function onAnalysisSelect() {
    const id = document.getElementById('analysis-select').value;
    document.getElementById('analysis-load-btn').disabled = !id;
}

async function loadAnalysis() {
    const id = document.getElementById('analysis-select').value;
    if (!id) return;

    state.currentAnalysisId = id;
    const data = await fetchJson('/api/analyse/' + encodeURIComponent(id));
    if (!data) return;

    renderScatterPlot(id, data);

    const labels = axisLabels(id);
    document.getElementById('th-conv-x').textContent = labels.x;
    document.getElementById('th-conv-y').textContent = 'Predicted ' + labels.y;

    const MIN_R2 = 0.75;
    if (data.fit) {
        const r2 = data.fit.r2;
        document.getElementById('analysis-summary').textContent =
            `n=${data.pairs ? data.pairs.length : 0}  R²=${r2.toFixed(4)}  y = ${data.fit.slope.toFixed(5)}·x ${data.fit.intercept >= 0 ? '+' : ''}${data.fit.intercept.toFixed(5)}`;
        if (r2 >= MIN_R2) {
            document.getElementById('analysis-table-section').style.display = 'block';
            document.getElementById('analysis-table-warning').style.display = 'none';
            loadConversionTable();
        } else {
            document.getElementById('analysis-table-section').style.display = 'none';
            document.getElementById('analysis-table-warning').style.display = 'block';
            document.getElementById('analysis-table-warning').textContent =
                `Conversion table suppressed: R²=${r2.toFixed(3)} < ${MIN_R2} (fit too poor to be useful)`;
        }
    } else {
        document.getElementById('analysis-table-section').style.display = 'none';
        document.getElementById('analysis-table-warning').style.display = 'none';
        document.getElementById('analysis-summary').textContent =
            `n=${data.pairs ? data.pairs.length : 0}  — insufficient data for a fit`;
    }
}

function axisLabels(id) {
    function variantLabel(v) {
        if (v === 'nonspin') return ' NS';
        if (v === 'twohanded') return ' 2H';
        return '';
    }
    // Year transition: {sys}-{yearA}-to-{yearB}  e.g. irc-2023-to-2024
    let m = id.match(/^([a-z]+)-(\d{4})-to-(\d{4})$/);
    if (m) {
        const sys = m[1].toUpperCase();
        return { x: `${m[2]} ${sys}`, y: `${m[3]} ${sys}` };
    }
    // Cross-system with variant: {sysA}-vs-{sysB}-{variant}-{year}  e.g. orc-vs-irc-nonspin-2024
    m = id.match(/^([a-z]+)-vs-([a-z]+)-([a-z]+)-(\d{4})$/);
    if (m) {
        const sA = m[1].toUpperCase(), sB = m[2].toUpperCase(), v = variantLabel(m[3]), year = m[4];
        return { x: `${year} ${sA}${v}`, y: `${year} ${sB}${v}` };
    }
    // Cross-system spin: {sysA}-vs-{sysB}-{year}  e.g. orc-vs-irc-2024
    m = id.match(/^([a-z]+)-vs-([a-z]+)-(\d{4})$/);
    if (m) {
        const sA = m[1].toUpperCase(), sB = m[2].toUpperCase(), year = m[3];
        return { x: `${year} ${sA}`, y: `${year} ${sB}` };
    }
    // Variant within same system: {sys}-{vA}-vs-{vB}-{year}  e.g. irc-spin-vs-nonspin-2024
    m = id.match(/^([a-z]+)-([a-z]+)-vs-([a-z]+)-(\d{4})$/);
    if (m) {
        const sys = m[1].toUpperCase(), vA = variantLabel(m[2]) || ' Spin';
        const vB = variantLabel(m[3]) || ' Spin', year = m[4];
        return { x: `${year} ${sys}${vA}`, y: `${year} ${sys}${vB}` };
    }
    return { x: 'x (TCF)', y: 'y (TCF)' };
}

function renderScatterPlot(id, data) {
    const pairs = data.pairs || [];
    const fit = data.fit;

    const scatterTrace = {
        x: pairs.map(p => p.x),
        y: pairs.map(p => p.y),
        mode: 'markers',
        type: 'scatter',
        name: 'Observed pairs',
        text: pairs.map(p => p.boatId),
        marker: { color: '#3a7ec4', size: 7, opacity: 0.75 }
    };

    const traces = [scatterTrace];

    if (fit) {
        // Regression line over the range of data + small margin
        const xs = pairs.map(p => p.x);
        const xMin = Math.min(...xs);
        const xMax = Math.max(...xs);
        const margin = (xMax - xMin) * 0.05 || 0.01;
        const lineX = [xMin - margin, xMax + margin];
        const lineY = lineX.map(x => fit.slope * x + fit.intercept);

        traces.push({
            x: lineX,
            y: lineY,
            mode: 'lines',
            type: 'scatter',
            name: `y = ${fit.slope.toFixed(4)}x ${fit.intercept >= 0 ? '+' : ''}${fit.intercept.toFixed(4)}  R²=${fit.r2.toFixed(3)}`,
            line: { color: '#e03030', width: 2 }
        });

        // Diagonal reference line (y = x)
        traces.push({
            x: lineX,
            y: lineX,
            mode: 'lines',
            type: 'scatter',
            name: 'y = x (reference)',
            line: { color: '#aaa', width: 1, dash: 'dot' }
        });
    }

    const labels = axisLabels(id);
    const layout = {
        title: id,
        xaxis: { title: labels.x },
        yaxis: { title: labels.y },
        legend: { orientation: 'h', y: -0.2 },
        margin: { t: 40, b: 80, l: 60, r: 20 }
    };

    Plotly.newPlot('analysis-plot', traces, layout, { responsive: true });
}

async function loadConversionTable() {
    const id = state.currentAnalysisId;
    if (!id) return;

    const min  = document.getElementById('tbl-min').value  || '0.85';
    const max  = document.getElementById('tbl-max').value  || '1.15';
    const step = document.getElementById('tbl-step').value || '0.01';

    const url = `/api/analyse/${encodeURIComponent(id)}/table?min=${min}&max=${max}&step=${step}`;
    const data = await fetchJson(url);
    if (!data || !Array.isArray(data)) return;

    const tbody = document.getElementById('tbody-conv');
    tbody.innerHTML = '';
    for (const row of data) {
        const barWidth = Math.round(row.weight * 80);
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${row.x.toFixed(4)}</td>
          <td>${row.predicted.toFixed(4)}</td>
          <td>${row.weight.toFixed(3)}</td>
          <td><span class="weight-bar" style="width:${barWidth}px"></span></td>`;
        tbody.appendChild(tr);
    }
}

// --- Initialise ---

loadStats();
loadImporters();
loadList('boats', 0);
loadAnalysisList();
setInterval(loadStats, 30000);
