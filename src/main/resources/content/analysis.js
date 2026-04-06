let currentAnalysisId = null;
let showAnalysisErrors = false;

function onAnalysisErrorsChange() {
    showAnalysisErrors = document.getElementById('show-analysis-errors').checked;
    if (currentAnalysisId) loadAnalysis();
}

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
    // NS/2H year transition: {sys}-{variant}-{yearA}-to-{yearB}
    let m = id.match(/^([a-z]+)-(nonspin|twohanded)-(\d{4})-to-(\d{4})$/);
    if (m) {
        const sys = m[1].toUpperCase();
        const v   = m[2] === 'nonspin' ? ' NS' : ' 2H';
        return { x: `${m[3]} ${sys}${v}`, y: `${m[4]} ${sys}${v}` };
    }

    // Spin year transition: {sys}-{yearA}-to-{yearB}
    m = id.match(/^([a-z]+)-(\d{4})-to-(\d{4})$/);
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

        const fitLineIdx = traces.length;
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

        if (showAnalysisErrors && fit.se > 0 && fit.ssx > 0) {
            const bandXs = Array.from({length: 51}, (_, i) => xMin - margin + i * (xMax - xMin + 2 * margin) / 50);
            const upper = bandXs.map(x => {
                const yHat   = fit.slope * x + fit.intercept;
                const sePred = fit.se * Math.sqrt(1 + 1 / fit.n + Math.pow(x - fit.xMean, 2) / fit.ssx);
                return yHat + 1.96 * sePred;
            });
            const lower = bandXs.map(x => {
                const yHat   = fit.slope * x + fit.intercept;
                const sePred = fit.se * Math.sqrt(1 + 1 / fit.n + Math.pow(x - fit.xMean, 2) / fit.ssx);
                return yHat - 1.96 * sePred;
            });
            traces.splice(fitLineIdx, 0, {
                x: [...bandXs, ...bandXs.slice().reverse()],
                y: [...upper, ...lower.slice().reverse()],
                type: 'scatter', mode: 'lines', fill: 'toself',
                fillcolor: 'rgba(224,48,48,0.12)',
                line: { color: 'transparent' },
                showlegend: false, hoverinfo: 'skip', name: '95% prediction interval'
            });
        }
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

async function loadNetwork() {
    const data = await fetchJson('/api/analyse/network');
    if (!data || !data.nodes) return;
    renderNetworkGraph(data);
}

function renderNetworkGraph(data) {
    const nodes = data.nodes || [];
    const edges = data.edges || [];

    if (!nodes.length && !edges.length) {
        Plotly.purge('network-plot');
        return;
    }

    // edgeIds[i] = analysisId for trace i, or null if not a clickable edge trace.
    // Built in lock-step with traces[] below.
    const edgeIds = [];

    const targetYear = data.targetYear ||
        Math.max(...nodes.filter(n => n.system !== 'ALL').map(n => n.year),
                 new Date().getFullYear());
    const certAgeYears = data.certAgeYears || 5;
    const yearStart    = targetYear - certAgeYears + 1;
    // Only show targetYear+1 column if actual data nodes exist there
    const hasForwardYear = nodes.some(n => n.system !== 'ALL' && n.year === targetYear + 1);
    const yearEnd      = hasForwardYear ? targetYear + 1 : targetYear;
    const displayYears = [];
    for (let y = yearStart; y <= yearEnd; y++) displayYears.push(y);

    // Layout constants
    const SYS_SPACING  = 2.5;   // vertical gap ORC/AMS → IRC centre within a grid
    const GRID_SPACING = 10.0;  // distance between variant-grid centres
    const TAB_PAD      = 0.8;   // padding above/below ORC/AMS rows within each tab
    const AMS_ORC_BOW  = 0.7;   // x-bow for quadratic-bezier AMS→ORC curve
    const TEXT_DY      = 0.22;  // vertical offset for two text lines inside node dots

    // Tab boundaries — each variant section is a rectangular panel
    function tabTop(nonSpin, twoHanded)    { return gridCenterY(nonSpin, twoHanded) + SYS_SPACING + TAB_PAD; }
    function tabBottom(nonSpin, twoHanded) { return gridCenterY(nonSpin, twoHanded) - SYS_SPACING - TAB_PAD; }

    function gridCenterY(nonSpin, twoHanded) {
        if (nonSpin)   return  GRID_SPACING;   // non-spin top
        if (twoHanded) return -GRID_SPACING;   // two-handed bottom
        return 0;                               // spin middle
    }
    function sysOffsetY(system) {
        if (system === 'ORC') return  SYS_SPACING;
        if (system === 'AMS') return -SYS_SPACING;
        return 0;  // IRC in the middle
    }

    const nodeById = Object.fromEntries(nodes.map(n => [n.id, n]));
    const posMap = {};
    for (const n of nodes) {
        if (n.system === 'ALL') continue;
        if (n.year < yearStart || n.year > yearEnd) continue;
        posMap[n.id] = { x: n.year, y: gridCenterY(n.nonSpin, n.twoHanded) + sysOffsetY(n.system) };
    }

    const sysEdges = [];
    const allEdges = [];
    const weakSysEdges = [];
    const weakAllEdges = [];
    for (const e of edges) {
        const fn = nodeById[e.fromId];
        const tn = nodeById[e.toId];
        if (!fn || !tn) continue;
        if (fn.system === 'ALL' || tn.system === 'ALL') {
            if (fn.year >= yearStart && fn.year <= yearEnd) {
                if (e.weak) weakAllEdges.push({ e, fn, tn });
                else allEdges.push({ e, fn, tn });
            }
        } else if (posMap[e.fromId] && posMap[e.toId]) {
            if (e.weak) weakSysEdges.push({ e, fn, tn });
            else sysEdges.push({ e, fn, tn });
        }
    }

    // Nodes that appear in at least one network edge (not orphans)
    const connectedIds = new Set(sysEdges.flatMap(({ e }) => [e.fromId, e.toId]));

    const traces  = [];
    const labelXs = [], labelYs = [], labelTexts = [];

    // Interpolate a straight line into n+1 evenly-spaced points so Plotly's
    // "closest point" hover detection fires on intermediate points, not just
    // endpoints that coincide with node markers.
    function densify(x1, y1, x2, y2, n) {
        const xs = [], ys = [];
        for (let i = 0; i <= n; i++) {
            const t = i / n;
            xs.push(x1 + (x2 - x1) * t);
            ys.push(y1 + (y2 - y1) * t);
        }
        return { xs, ys };
    }

    // --- Within-grid system edges ---
    // Drawn from node centre to node centre; filled node markers rendered on top visually clip
    // the line endpoints, giving the appearance that edges start/end at the node boundary.
    for (const { e, fn, tn } of sysEdges) {
        const from  = posMap[e.fromId];
        const to    = posMap[e.toId];
        const alpha = Math.max(0.2, e.medianWeight).toFixed(2);
        const curved = fn.system === 'AMS' && tn.system === 'ORC' && fn.year === tn.year;

        let xs, ys;
        if (curved) {
            // Quadratic bezier bowing left.
            // x(t) = year − 2t(1−t)·bow  (P0.x = P2.x = year)
            // y(t) = from.y + (to.y − from.y)·t  (linear, because P1.y = midY exactly)
            const nPts = 24;
            xs = []; ys = [];
            for (let i = 0; i <= nPts; i++) {
                const t = i / nPts;
                xs.push(from.x - 2 * t * (1 - t) * AMS_ORC_BOW);
                ys.push(from.y + (to.y - from.y) * t);
            }
            // Label at leftmost point (t=0.5), shifted above the IRC mid-line
            const midY = (from.y + to.y) / 2;
            labelXs.push(from.x - 0.5 * AMS_ORC_BOW);
            labelYs.push(midY + SYS_SPACING * 0.45);
            labelTexts.push(e.medianWeight.toFixed(2));
        } else if (fn.year === tn.year) {
            // Same-year vertical edge (ORC→IRC or AMS→IRC)
            ({ xs, ys } = densify(from.x, from.y, to.x, to.y, 20));
            // Label ~1 em (≈0.12 data units) to the side of the line
            labelXs.push(from.x + (fn.system === 'AMS' ? -0.12 : 0.12));
            labelYs.push((from.y + to.y) / 2);
            labelTexts.push(e.medianWeight.toFixed(2));
        } else {
            // Year-transition horizontal edge
            ({ xs, ys } = densify(from.x, from.y, to.x, to.y, 20));
            labelXs.push((from.x + to.x) / 2);
            labelYs.push(from.y + 0.4);
            labelTexts.push(e.medianWeight.toFixed(2));
        }

        traces.push({
            x: xs, y: ys, mode: 'lines', type: 'scatter',
            showlegend: false, hoverinfo: 'text',
            hovertext: `${fn.system} ${fn.year} → ${tn.system} ${tn.year}<br>` +
                       `R²=${e.r2.toFixed(3)}  n=${e.n}  weight=${e.medianWeight.toFixed(2)}`,
            line: { color: `rgba(100,100,100,${alpha})`, width: 3, shape: 'linear' }
        });
        edgeIds.push(e.analysisId || null);
    }

    // --- Weak within-grid system edges: dashed, low opacity, still clickable ---
    for (const { e, fn, tn } of weakSysEdges) {
        const from  = posMap[e.fromId];
        const to    = posMap[e.toId];
        const curved = fn.system === 'AMS' && tn.system === 'ORC' && fn.year === tn.year;

        let xs, ys;
        if (curved) {
            const nPts = 24;
            xs = []; ys = [];
            for (let i = 0; i <= nPts; i++) {
                const t = i / nPts;
                xs.push(from.x - 2 * t * (1 - t) * AMS_ORC_BOW);
                ys.push(from.y + (to.y - from.y) * t);
            }
        } else {
            ({ xs, ys } = densify(from.x, from.y, to.x, to.y, 20));
        }

        const nLabel = e.n > 0 ? `n=${e.n}` : 'no data';
        traces.push({
            x: xs, y: ys, mode: 'lines', type: 'scatter',
            showlegend: false, hoverinfo: 'text',
            hovertext: `${fn.system} ${fn.year} → ${tn.system} ${tn.year}<br>` +
                       `${nLabel}  (weak — click to inspect)`,
            line: { color: 'rgba(180,180,180,0.4)', width: 2, dash: 'dot' }
        });
        edgeIds.push(e.analysisId || null);
    }

    // --- Between-tab ALL edges: short connector in the gap between tab panels ---
    for (const { e, fn, tn } of allEdges) {
        const year    = fn.year;
        const sign    = Math.sign(gridCenterY(tn.nonSpin, tn.twoHanded) - gridCenterY(fn.nonSpin, fn.twoHanded));
        const lineFromY = sign > 0 ? tabTop(fn.nonSpin, fn.twoHanded) : tabBottom(fn.nonSpin, fn.twoHanded);
        const lineToY   = sign > 0 ? tabBottom(tn.nonSpin, tn.twoHanded) : tabTop(tn.nonSpin, tn.twoHanded);
        const alpha     = Math.max(0.2, e.medianWeight).toFixed(2);
        const label     = fn.nonSpin ? 'NS→Spin' : '2H→Spin';

        const { xs: allXs, ys: allYs } = densify(year, lineFromY, year, lineToY, 20);
        traces.push({
            x: allXs, y: allYs, mode: 'lines', type: 'scatter',
            showlegend: false, hoverinfo: 'text',
            hovertext: `${label} ${year}<br>` +
                       `R²=${e.r2.toFixed(3)}  n=${e.n}  weight=${e.medianWeight.toFixed(2)}`,
            line: { color: `rgba(110,60,180,${alpha})`, width: 4, dash: 'dot' }
        });
        edgeIds.push(e.analysisId || null);
        labelXs.push(year + 0.12);
        labelYs.push((lineFromY + lineToY) / 2);
        labelTexts.push(e.medianWeight.toFixed(2));
    }

    // --- Weak between-tab ALL edges: dashed, low opacity ---
    for (const { e, fn, tn } of weakAllEdges) {
        const year    = fn.year;
        const sign    = Math.sign(gridCenterY(tn.nonSpin, tn.twoHanded) - gridCenterY(fn.nonSpin, fn.twoHanded));
        const lineFromY = sign > 0 ? tabTop(fn.nonSpin, fn.twoHanded) : tabBottom(fn.nonSpin, fn.twoHanded);
        const lineToY   = sign > 0 ? tabBottom(tn.nonSpin, tn.twoHanded) : tabTop(tn.nonSpin, tn.twoHanded);
        const label     = fn.nonSpin ? 'NS→Spin' : '2H→Spin';

        const { xs: allXs, ys: allYs } = densify(year, lineFromY, year, lineToY, 20);
        const nLabel = e.n > 0 ? `n=${e.n}` : 'no data';
        traces.push({
            x: allXs, y: allYs, mode: 'lines', type: 'scatter',
            showlegend: false, hoverinfo: 'text',
            hovertext: `${label} ${year}<br>${nLabel}  (weak — click to inspect)`,
            line: { color: 'rgba(180,140,220,0.3)', width: 2, dash: 'dot' }
        });
        edgeIds.push(e.analysisId || null);
    }

    // Edge weight labels
    traces.push({
        x: labelXs, y: labelYs, text: labelTexts,
        mode: 'text', type: 'scatter',
        showlegend: false, hoverinfo: 'skip',
        textfont: { size: 18, color: '#555' }
    });
    edgeIds.push(null);

    // --- Node markers (orphans first so connected nodes render on top) ---
    const NODE_COLORS = { IRC: '#3a7ec4', ORC: '#e67e22', AMS: '#27ae60' };
    const displayNodes = nodes.filter(n =>
        n.system !== 'ALL' && n.year >= yearStart && n.year <= yearEnd);
    const orphanNodes    = displayNodes.filter(n => !connectedIds.has(n.id));
    const connectedNodes = displayNodes.filter(n =>  connectedIds.has(n.id));

    // Orphan: empty circle (outline only, same size)
    if (orphanNodes.length) {
        traces.push({
            x: orphanNodes.map(n => posMap[n.id]?.x),
            y: orphanNodes.map(n => posMap[n.id]?.y),
            mode: 'markers', type: 'scatter',
            showlegend: false, hoverinfo: 'text',
            hovertext: orphanNodes.map(n => `${n.label}  (${n.certCount ?? 0} boats — no network edge)`),
            marker: {
                size: 40, color: 'rgba(255,255,255,0)',
                line: { color: orphanNodes.map(n => NODE_COLORS[n.system] || '#555'), width: 3 }
            }
        });
        edgeIds.push(null);
    }

    // Connected: filled circle
    if (connectedNodes.length) {
        traces.push({
            x: connectedNodes.map(n => posMap[n.id]?.x),
            y: connectedNodes.map(n => posMap[n.id]?.y),
            mode: 'markers', type: 'scatter',
            showlegend: false, hoverinfo: 'text',
            hovertext: connectedNodes.map(n => `${n.label}  (${n.certCount ?? 0} boats)`),
            marker: {
                size: 40,
                color: connectedNodes.map(n => NODE_COLORS[n.system] || '#555'),
                line: { color: '#fff', width: 2 }
            }
        });
        edgeIds.push(null);
    }

    // System name text (upper half of dot) and cert count (lower half), rendered last so they
    // sit on top of the marker fills.  Connected nodes use white text; orphans use their system colour.
    function nodeTextTraces(nlist, color) {
        traces.push({
            x: nlist.map(n => posMap[n.id]?.x),
            y: nlist.map(n => (posMap[n.id]?.y ?? 0) + TEXT_DY),
            text: nlist.map(n => n.system),
            mode: 'text', type: 'scatter', showlegend: false, hoverinfo: 'skip',
            textposition: 'middle center', textfont: { size: 14, color }
        });
        edgeIds.push(null);
        traces.push({
            x: nlist.map(n => posMap[n.id]?.x),
            y: nlist.map(n => (posMap[n.id]?.y ?? 0) - TEXT_DY),
            text: nlist.map(n => String(n.certCount ?? 0)),
            mode: 'text', type: 'scatter', showlegend: false, hoverinfo: 'skip',
            textposition: 'middle center', textfont: { size: 14, color }
        });
        edgeIds.push(null);
    }
    if (connectedNodes.length) nodeTextTraces(connectedNodes, '#fff');
    if (orphanNodes.length)
        nodeTextTraces(orphanNodes, orphanNodes.map(n => NODE_COLORS[n.system] || '#555'));

    // Year labels above the top tab
    const yearAnnotations = [];
    const topLabelY = tabTop(true, false) + 0.3;   // just above Non-Spin tab
    for (const year of displayYears) {
        if (year === targetYear) continue;
        yearAnnotations.push({
            x: year, y: topLabelY, text: String(year), showarrow: false,
            font: { size: 26, color: '#888' }, xref: 'x', yref: 'y',
            xanchor: 'center', yanchor: 'bottom'
        });
    }

    // Tab panel rectangles and labels
    const TAB_FILL   = 'rgba(0,0,0,0.025)';
    const TAB_BORDER = { color: '#bbb', width: 1 };
    const tabDefs = [
        { label: 'Non-Spinnaker', ns: true,  th: false },
        { label: 'Spinnaker',     ns: false, th: false },
        { label: '2-Handed',      ns: false, th: true  },
    ];
    const tabShapes = tabDefs.map(t => ({
        type: 'rect', xref: 'paper', yref: 'y',
        x0: 0, x1: 1,
        y0: tabBottom(t.ns, t.th), y1: tabTop(t.ns, t.th),
        line: TAB_BORDER, fillcolor: TAB_FILL
    }));
    const tabLabels = tabDefs.map(t => ({
        x: 0, y: gridCenterY(t.ns, t.th),
        text: `<b>${t.label}</b>`, showarrow: false,
        font: { size: 14, color: '#666' },
        xref: 'paper', yref: 'y', xanchor: 'right', yanchor: 'middle',
        xshift: -8
    }));

    // Target year highlight — vertical stripe across all tabs
    const targetYearShape = {
        type: 'rect', xref: 'x', yref: 'y',
        x0: targetYear - 0.45, x1: targetYear + 0.45,
        y0: tabBottom(false, true) - 0.2,
        y1: tabTop(true, false) + 0.2,
        line: { color: '#c0392b', width: 2 },
        fillcolor: 'rgba(192,57,43,0.04)'
    };

    const layout = {
        font: { size: 14 },
        xaxis: {
            title: 'Certificate year',
            tickvals: displayYears, ticktext: displayYears.map(String),
            showgrid: true, gridcolor: '#eee', zeroline: false,
            range: [yearStart - 0.7, yearEnd + 0.5]
        },
        yaxis: {
            showticklabels: false, showgrid: false, zeroline: false,
            range: [tabBottom(false, true) - 1.5, tabTop(true, false) + 2.5]
        },
        shapes: [...tabShapes, targetYearShape],
        annotations: [
            ...yearAnnotations,
            ...tabLabels,
            { x: targetYear, y: tabTop(true, false) + 0.2,
              text: 'target year', showarrow: false,
              font: { size: 13, color: '#c0392b' },
              xref: 'x', yref: 'y', xanchor: 'center', yanchor: 'bottom' }
        ],
        margin: { t: 20, b: 50, l: 110, r: 20 },
        hovermode: 'closest'
    };

    Plotly.newPlot('network-plot', traces, layout, { responsive: true });

    const plotDiv = document.getElementById('network-plot');
    plotDiv.on('plotly_hover', function(eventData) {
        const pt = eventData.points[0];
        const id = edgeIds[pt.curveNumber];
        plotDiv.style.cursor = id ? 'pointer' : '';
        if (id) {
            const sel = document.getElementById('analysis-select');
            if (sel.value !== id) {
                sel.value = id;
                onAnalysisSelect();
            }
        }
    });
    plotDiv.on('plotly_unhover', function() {
        plotDiv.style.cursor = '';
    });
    plotDiv.on('plotly_click', function(eventData) {
        const pt = eventData.points[0];
        const id = edgeIds[pt.curveNumber];
        if (id) {
            const sel = document.getElementById('analysis-select');
            sel.value = id;
            onAnalysisSelect();
            loadAnalysis();
            document.querySelector('.analysis-controls').scrollIntoView({ behavior: 'smooth' });
        }
    });
}

async function loadHpfQuality() {
    try {
        const resp = await fetch('/api/hpf/quality');
        if (resp.status === 404) { document.getElementById('hpf-quality').style.display = 'none'; return; }
        if (!resp.ok) return;
        renderHpfQuality(await resp.json());
    } catch (e) { /* ignore */ }
}

function fmtTrace(trace) {
    if (!trace || trace.length === 0) return '';
    const f = v => v.toFixed(3);
    if (trace.length <= 10) return trace.map(f).join(' → ');
    return [...trace.slice(0, 3).map(f), '\u2026', ...trace.slice(-3).map(f)].join(' → ');
}

function renderHpfQuality(q) {
    const section = document.getElementById('hpf-quality');
    section.style.display = '';
    const innerStatus = q.innerConverged
        ? `converged in ${q.innerIterations} iterations (max\u0394=${fmt(q.finalMaxDelta)})`
        : `<span style="color:#c62828;font-weight:bold">did not converge</span> after ${q.innerIterations} iterations (max\u0394=${fmt(q.finalMaxDelta)})`;
    const traceStr = fmtTrace(q.outerDeltaTrace);
    const traceLine = traceStr
        ? `<br><span style="font-size:0.85em;color:#666;">\u0394w per cycle: ${traceStr}</span>`
        : '';
    const outerStatus = q.outerConverged
        ? `converged in ${q.outerIterations} cycles${traceLine}`
        : `<span style="color:#c62828;font-weight:bold">did not converge</span> after ${q.outerIterations} cycles (max\u0394w=${fmt(q.finalMaxWeightChange)})${traceLine}`;
    const medRes = q.medianResidual;
    const fitColour = medRes < 0.03 ? '#2e7d32' : medRes < 0.08 ? '#f57f17' : '#c62828';
    const dwPct = q.totalEntries > 0 ? (100 * q.downWeightedEntries / q.totalEntries).toFixed(1) : '0';
    document.getElementById('hpf-quality-content').innerHTML = `
      <table style="border-collapse:collapse;font-size:0.95em">
        <tr><td style="padding:2px 1em"><b>Convergence</b> ${infoBtn('hpf-quality-convergence','Whether the inner (factor) and outer (weight) iteration loops converged within the allowed number of iterations.')}</td>
            <td>Inner: ${innerStatus} | Outer: ${outerStatus}</td></tr>
        <tr><td style="padding:2px 1em"><b>Scale</b> ${infoBtn('hpf-quality-scale','Number of boats, race divisions, and individual race entries used in the most recent HPF optimisation run.')}</td>
            <td>${q.boatsWithHpf} boats, ${q.divisionsUsed} divisions, ${q.totalEntries} entries</td></tr>
        <tr><td style="padding:2px 1em"><b>Fit quality</b> ${infoBtn('hpf-quality-fit','Median absolute residual and spread of residuals across all race entries. Lower values indicate a better fit between predicted and observed factors.')}</td>
            <td style="color:${fitColour}">Median |residual|: ${fmt(medRes)} (residual IQR: ${fmt(q.iqrResidual)}, |residual| P95: ${fmt(q.pct95Residual)})</td></tr>
        <tr><td style="padding:2px 1em"><b>Outliers</b> ${infoBtn('hpf-quality-outliers','Entries assigned reduced weight due to large residuals, and divisions with high internal spread. Outlier down-weighting reduces their influence on the optimised HPF values.')}</td>
            <td>${q.downWeightedEntries} entries down-weighted (${dwPct}%), ${q.highDispersionDivisions} high-dispersion divisions</td></tr>
        <tr><td style="padding:2px 1em"><b>Confidence</b> ${infoBtn('hpf-quality-confidence','Median boat confidence score, reflecting the quantity and quality of race data available per boat. Higher confidence means the HPF estimate is better supported by data.')}</td>
            <td>Median boat confidence: ${fmt(q.medianBoatConfidence)}</td></tr>
      </table>`;
}

function fmt(v) {
    if (v == null) return '?';
    if (Math.abs(v) < 0.001) return v.toExponential(1);
    return v.toFixed(4);
}

loadAnalysisList();
loadNetwork();
loadHpfQuality();
