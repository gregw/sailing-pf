let currentAnalysisId = null;

async function loadAnalysisList() {
    const data = await fetchJson('/api/analyse');
    if (!data) return;

    const sel = document.getElementById('analysis-select');
    while (sel.options.length > 1) sel.remove(1);

    for (const item of data) {
        const opt = document.createElement('option');
        opt.value = item.id;
        const r2str = item.r2 != null ? `  R²=${item.r2.toFixed(3)}` : '  (insufficient data)';
        const trimStr = item.trimmed ? `  ${item.trimmed} trimmed` : '';
        opt.textContent = `${item.id}  (n=${item.n}${r2str}${trimStr})`;
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

    currentAnalysisId = id;
    const data = await fetchJson('/api/analyse/' + encodeURIComponent(id));
    if (!data) return;

    renderScatterPlot(id, data);

    const labels = axisLabels(id);
    document.getElementById('th-conv-x').textContent = labels.x;
    document.getElementById('th-conv-y').textContent = 'Predicted ' + labels.y;

    const MIN_R2 = 0.75;
    if (data.fit) {
        const r2 = data.fit.r2;
        const nKept = data.pairs ? data.pairs.length : 0;
        const nTrimmed = data.trimmedPairs ? data.trimmedPairs.length : 0;
        const nTotal = nKept + nTrimmed;
        const trimStr = nTrimmed > 0 ? `  (${nTrimmed} trimmed)` : '';
        document.getElementById('analysis-summary').textContent =
            `n=${nTotal}${trimStr}  R²=${r2.toFixed(4)}  y = ${data.fit.slope.toFixed(5)}·x ${data.fit.intercept >= 0 ? '+' : ''}${data.fit.intercept.toFixed(5)}`;
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
    let m = id.match(/^([a-z]+)-(\d{4})-to-(\d{4})$/);
    if (m) {
        const sys = m[1].toUpperCase();
        return { x: `${m[2]} ${sys}`, y: `${m[3]} ${sys}` };
    }
    m = id.match(/^([a-z]+)-vs-([a-z]+)-([a-z]+)-(\d{4})$/);
    if (m) {
        const sA = m[1].toUpperCase(), sB = m[2].toUpperCase(), v = variantLabel(m[3]), year = m[4];
        return { x: `${year} ${sA}${v}`, y: `${year} ${sB}${v}` };
    }
    m = id.match(/^([a-z]+)-vs-([a-z]+)-(\d{4})$/);
    if (m) {
        const sA = m[1].toUpperCase(), sB = m[2].toUpperCase(), year = m[3];
        return { x: `${year} ${sA}`, y: `${year} ${sB}` };
    }
    m = id.match(/^([a-z]+)-([a-z]+)-vs-([a-z]+)-(\d{4})$/);
    if (m) {
        const rawSys = m[1].toUpperCase();
        const sys = rawSys === 'ALL' ? 'All Systems' : rawSys;
        const vA = variantLabel(m[2]) || ' Spin';
        const vB = variantLabel(m[3]) || ' Spin', year = m[4];
        return { x: `${year} ${sys}${vA}`, y: `${year} ${sys}${vB}` };
    }
    return { x: 'x (TCF)', y: 'y (TCF)' };
}

function renderScatterPlot(id, data) {
    const pairs = data.pairs || [];
    const trimmedPairs = data.trimmedPairs || [];
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

    if (trimmedPairs.length > 0) {
        traces.push({
            x: trimmedPairs.map(p => p.x),
            y: trimmedPairs.map(p => p.y),
            mode: 'markers',
            type: 'scatter',
            name: 'Trimmed outliers',
            text: trimmedPairs.map(p => p.boatId),
            marker: { color: '#aaa', size: 7, opacity: 0.6, symbol: 'x' }
        });
    }

    if (fit) {
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
    const id = currentAnalysisId;
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

loadAnalysisList();
