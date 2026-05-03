'use strict';

// Shared handicap calculator. Exposed as window.HandicapCalc.
//
// HandicapCalc.create(cfg) returns a controller with:
//   setBoats(newBoats)         — render with new boat list (preserves matching entries by id)
//   setHandicapsByMatch(rows)  — fill inputs from [{sailno, name, handicap}] via sail-no / name match,
//                                returns count matched
//   clearAll()                 — clear all inputs and re-render
//   getEnteredHandicaps()      — current entries as [{sailno, name, handicap}]
//   getEnteredValues()         — Map(boatId → handicap)
//   recalc()                   — force recalculation of scaled columns
//
// Boats passed to setBoats must have: { id, name, sailNumber, boatName, pf, rf, bestFit?, color?, pfWeight?, rfWeight? }.
// pf is required; rf and bestFit may be null. pfWeight/rfWeight (when present) weight each anchor's
// contribution to the consensus scale factor used for predictions.
//
// cfg = {
//   section,                        // section <div> wrapping the calculator (show/hide handled here)
//   table,                          // <table> element to render rows into
//   showBestFit,                    // include Best Fit column
//   sessionKey,                     // optional sessionStorage key — when set, entered handicaps
//                                   //   persist across page loads. Saves merge with existing
//                                   //   stored entries for boats not currently shown, so multiple
//                                   //   pages/views with different boat lists can share the same
//                                   //   key without overwriting each other's entries.
//   onChange,                       // optional () => void, fired after every recalc
//   // Optional fetch/load/save controls; pass to wire automatic event handlers:
//   urlInput, fetchBtn, fetchStatus,
//   fileInput, fileStatus,
//   downloadBtn, downloadStatus,
// }

window.HandicapCalc = (function () {

    // --- Sail-number / boat-name normalisation (mirrors importer logic) ---
    function normaliseSailNumber(raw) {
        if (raw == null) return '';
        return String(raw).toUpperCase().replace(/[^A-Z0-9]/g, '');
    }

    const SAIL_PREFIXES = ['JAUS', 'EAUS', 'VAUS', 'SAUS', 'AUS'];

    function stripPrefix(normSail) {
        if (normSail == null || normSail.length === 0) return normSail;
        for (const prefix of SAIL_PREFIXES) {
            if (normSail.startsWith(prefix) && normSail.length > prefix.length
                && /\d/.test(normSail.charAt(prefix.length))) {
                normSail = normSail.slice(prefix.length);
                break;
            }
        }
        while (normSail.length > 1 && normSail.charAt(0) === '0' && /\d/.test(normSail.charAt(1)))
            normSail = normSail.slice(1);
        return normSail;
    }

    function normaliseDesignName(raw) {
        if (raw == null) return '';
        return String(raw).toLowerCase().replace(/[^a-z0-9]/g, '');
    }

    function sailnoMatch(candidate, target) {
        if (candidate == null || target == null) return false;
        const c = stripPrefix(normaliseSailNumber(candidate));
        const t = stripPrefix(normaliseSailNumber(target));
        return c.length > 0 && c === t;
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

    // Mirrors data.js — colour/tooltip for a factor's underlying weight (0..1).
    function weightColor(w) {
        const cw = Math.min(w ?? 0, 1);
        if (cw >= 0.5) {
            const t = (cw - 0.5) * 2;
            return `rgb(${Math.round(120 * (1 - t))},${Math.round(120 + 40 * t)},${Math.round(120 * (1 - t))})`;
        }
        const t = cw * 2;
        return `rgb(${Math.round(220 - 100 * t)},${Math.round(30 + 90 * t)},${Math.round(30 + 90 * t)})`;
    }

    function weightLabel(w) {
        if (w == null) return 'No data';
        if (w >= 0.85) return `Weight: ${w.toFixed(2)} — high confidence`;
        if (w >= 0.6) return `Weight: ${w.toFixed(2)} — moderate confidence`;
        if (w >= 0.35) return `Weight: ${w.toFixed(2)} — low confidence`;
        return `Weight: ${w.toFixed(2)} — very low confidence`;
    }

    function weightFieldFor(ft) {
        return ft === 'pf' ? 'pfWeight' : ft === 'rf' ? 'rfWeight' : null;
    }

    // --- PP pentagon hover popup ---------------------------------------------------
    // Cache: boatId → Promise<profile|null>. null means fetched but no profile available.
    const profileCache = new Map();
    let popupEl = null;
    let popupShowTimer = null;
    let popupHideTimer = null;
    let popupBoatId = null;

    function ensurePopup() {
        if (popupEl) return popupEl;
        popupEl = document.createElement('div');
        popupEl.id = 'pf-pentagon-popup';
        popupEl.style.cssText = [
            'position:fixed', 'z-index:10000', 'display:none',
            'background:#fff', 'border:1px solid #888', 'border-radius:4px',
            'box-shadow:0 2px 8px rgba(0,0,0,0.18)', 'padding:4px',
            'pointer-events:none', 'font-family:sans-serif'
        ].join(';');
        document.body.appendChild(popupEl);
        return popupEl;
    }

    function fetchProfile(boatId) {
        if (profileCache.has(boatId)) return profileCache.get(boatId);
        const p = fetch(`/api/boats/${encodeURIComponent(boatId)}/profile`)
            .then(r => r.ok ? r.json() : null)
            .then(j => (j && j.profile) ? j.profile : null)
            .catch(() => null);
        profileCache.set(boatId, p);
        return p;
    }

    function positionPopup(linkEl) {
        const rect = linkEl.getBoundingClientRect();
        const w = 200, h = 180;
        let left = rect.right + 8;
        let top = rect.top - 4;
        if (left + w > window.innerWidth - 4) left = Math.max(4, rect.left - w - 8);
        if (top + h > window.innerHeight - 4) top = Math.max(4, window.innerHeight - h - 4);
        popupEl.style.left = left + 'px';
        popupEl.style.top = top + 'px';
        popupEl.style.width = w + 'px';
        popupEl.style.height = h + 'px';
    }

    function renderMiniPentagon(container, profile, color) {
        if (typeof Plotly === 'undefined') return;
        const labels = ['Frequency', 'Consistency', 'Diversity', 'Chaotic', 'Stability'];
        const keys = ['frequency', 'consistency', 'diversity', 'chaotic', 'stability'];
        const values = keys.map(k => profile[k] ?? 0);
        const theta = [...labels, labels[0]];
        const r = [...values, values[0]];
        const lineColor = color || 'rgba(31,119,180,0.85)';
        const fillColor = lineColor.startsWith('rgba')
            ? lineColor.replace(/[\d.]+\)$/, '0.18)')
            : lineColor + '2e'; // hex 0x2e ≈ 18% alpha
        const trace = {
            type: 'scatterpolar', r, theta, fill: 'toself',
            fillcolor: fillColor,
            line: {color: lineColor, width: 1.5},
            hoverinfo: 'skip'
        };
        const layout = {
            polar: {
                radialaxis: {visible: true, range: [0, 1], showticklabels: false, tickvals: [0.25, 0.5, 0.75, 1.0]},
                angularaxis: {direction: 'clockwise', tickfont: {size: 9}}
            },
            showlegend: false,
            width: 200, height: 180,
            margin: {t: 14, b: 14, l: 28, r: 28},
            paper_bgcolor: 'rgba(0,0,0,0)'
        };
        Plotly.newPlot(container, [trace], layout, {displayModeBar: false, staticPlot: true});
    }

    function showPentagonPopup(linkEl, boatId, color) {
        clearTimeout(popupHideTimer);
        clearTimeout(popupShowTimer);
        popupBoatId = boatId;
        popupShowTimer = setTimeout(async () => {
            const profile = await fetchProfile(boatId);
            if (popupBoatId !== boatId) return; // user moved on
            if (!profile) {
                linkEl.title = 'Click to view boat details — no performance profile available';
                return;
            }
            linkEl.title = 'Click to view boat details — hover for performance profile';
            const el = ensurePopup();
            el.innerHTML = '';
            const overall = profile.overallScore != null ? profile.overallScore.toFixed(3) : '—';
            const score = document.createElement('div');
            score.style.cssText = 'position:absolute;bottom:4px;left:0;right:0;text-align:center;font-size:0.75rem;color:#555;';
            score.textContent = `PP: ${overall}`;
            const chart = document.createElement('div');
            chart.style.cssText = 'width:200px;height:160px;';
            el.appendChild(chart);
            el.appendChild(score);
            positionPopup(linkEl);
            el.style.display = 'block';
            renderMiniPentagon(chart, profile, color);
        }, 250);
    }

    function hidePentagonPopup() {
        clearTimeout(popupShowTimer);
        popupBoatId = null;
        popupHideTimer = setTimeout(() => {
            if (popupEl) popupEl.style.display = 'none';
        }, 80);
    }

    function create(cfg) {
        const section = cfg.section;
        const table = cfg.table;
        let calcBoats = [];
        let calcSort = {col: 'pf', dir: 'desc'};

        function valueCells() {
            return section.querySelectorAll('.pf-calc-value');
        }

        function inputCells() {
            return section.querySelectorAll('.pf-calc-input');
        }

        function sortCalcBoats() {
            const {col, dir} = calcSort;
            const mul = dir === 'asc' ? 1 : -1;
            if (col === 'name') {
                calcBoats.sort((a, b) => mul * a.name.localeCompare(b.name));
            } else if (col === 'input') {
                calcBoats.sort((a, b) => mul * ((a.pf ?? 0) - (b.pf ?? 0)));
            } else if (col === 'pfDelta' || col === 'rfDelta') {
                const deltaValues = new Map();
                section.querySelectorAll(`.pf-calc-value[data-factor-type="${col}"]`).forEach(td => {
                    deltaValues.set(td.dataset.boatId, parseFloat(td.textContent) || 0);
                });
                calcBoats.sort((a, b) => mul * ((deltaValues.get(a.id) ?? 0) - (deltaValues.get(b.id) ?? 0)));
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

        function render() {
            const enteredValues = new Map();
            inputCells().forEach(inp => {
                if (inp.value !== '') enteredValues.set(inp.dataset.boatId, inp.value);
            });

            if (calcBoats.length === 0) {
                section.style.display = 'none';
                table.innerHTML = '';
                return;
            }

            section.style.display = '';
            table.innerHTML = '';

            const cols = [
                {key: 'name', label: 'Boat', align: 'left'},
                {key: 'input', label: 'Enter handicap', align: 'center'},
                {key: 'pf', label: 'PF', align: 'right'},
                {key: 'pfDelta', label: 'PFΔ', align: 'right'},
                {key: 'rf', label: 'RF', align: 'right'},
                {key: 'rfDelta', label: 'RFΔ', align: 'right'},
            ];
            if (cfg.showBestFit) cols.push({key: 'bestFit', label: 'Best Fit', align: 'right'});

            if (!cols.some(c => c.key === calcSort.col)) calcSort = {col: 'pf', dir: 'desc'};

            sortCalcBoats();

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
                    else calcSort = {col: c.key, dir: c.key === 'name' ? 'asc' : 'desc'};
                    render();
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
                        const tdName = document.createElement('td');
                        const color = b.color || '#888';
                        tdName.style.cssText = `color:${color};font-weight:bold;`;
                        const link = document.createElement('a');
                        link.href = `data.html?tab=boats&boatId=${encodeURIComponent(b.id)}`;
                        link.textContent = b.name;
                        link.style.cssText = 'color:inherit;text-decoration:none;';
                        link.title = 'Click to view boat details — hover for performance profile';
                        const cached = profileCache.get(b.id);
                        if (cached) cached.then(p => {
                            if (p === null) link.title = 'Click to view boat details — no performance profile available';
                        });
                        link.addEventListener('mouseenter', () => showPentagonPopup(link, b.id, color));
                        link.addEventListener('mouseleave', hidePentagonPopup);
                        tdName.appendChild(link);
                        tr.appendChild(tdName);
                    } else if (c.key === 'input') {
                        const tdInput = document.createElement('td');
                        tdInput.style.cssText = 'padding:2px 4px;text-align:center;';
                        const input = document.createElement('input');
                        input.type = 'number';
                        input.step = '0.0001';
                        input.min = '0.1';
                        input.max = '2.0';
                        input.className = 'pf-calc-input';
                        input.dataset.boatId = b.id;
                        input.placeholder = 'enter…';
                        input.style.cssText = 'width:90px;font-family:monospace;text-align:right;';
                        if (enteredValues.has(b.id)) input.value = enteredValues.get(b.id);
                        input.addEventListener('input', recalc);
                        tdInput.appendChild(input);
                        tr.appendChild(tdInput);
                    } else {
                        const td = document.createElement('td');
                        td.className = 'pf-calc-value';
                        td.style.cssText = 'font-family:monospace;padding:2px 8px;text-align:right;';
                        if (c.key === 'pfDelta' || c.key === 'rfDelta') {
                            td.textContent = '';
                            td.dataset.boatId = b.id;
                            td.dataset.factorType = c.key;
                            td.dataset.origValue = '';
                        } else {
                            const v = b[c.key];
                            td.textContent = v != null ? v.toFixed(4) : '—';
                            td.dataset.boatId = b.id;
                            td.dataset.factorType = c.key;
                            td.dataset.origValue = v != null ? String(v) : '';
                            const wField = weightFieldFor(c.key);
                            const w = wField ? b[wField] : null;
                            if (v != null && w != null) {
                                td.style.color = weightColor(w);
                                td.title = weightLabel(w);
                            }
                        }
                        tr.appendChild(td);
                    }
                });
                tbody.appendChild(tr);
            });
            table.appendChild(tbody);

            if (enteredValues.size > 0) recalc();
        }

        function restoreAll() {
            const byId = new Map(calcBoats.map(b => [b.id, b]));
            valueCells().forEach(td => {
                const origStr = td.dataset.origValue;
                td.textContent = origStr ? parseFloat(origStr).toFixed(4) : '—';
                const wField = weightFieldFor(td.dataset.factorType);
                const w = wField ? byId.get(td.dataset.boatId)?.[wField] : null;
                if (origStr && w != null) {
                    td.style.color = weightColor(w);
                    td.title = weightLabel(w);
                } else {
                    td.style.color = '';
                    td.title = '';
                }
            });
            section.querySelectorAll('.pf-calc-value[data-factor-type$="Delta"]').forEach(td => {
                td.textContent = '';
                td.style.color = '';
                td.title = '';
            });
        }

        function scaleSingle(anchor) {
            valueCells().forEach(td => {
                const ft = td.dataset.factorType;
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
            section.querySelectorAll('.pf-calc-value[data-factor-type$="Delta"]').forEach(td => {
                td.textContent = '';
                td.style.color = '';
                td.title = '';
            });
        }

        function scaleMulti(anchors) {
            const anchorIds = new Set(anchors.map(a => a.boat.id));
            const anchorByBoat = new Map(anchors.map(a => [a.boat.id, a]));

            const ftSet = new Set();
            valueCells().forEach(td => ftSet.add(td.dataset.factorType));

            const ftStats = {};
            for (const ft of ftSet) {
                const weightField = (ft === 'pf') ? 'pfWeight' : (ft === 'rf') ? 'rfWeight' : null;
                const ratios = [];
                for (const a of anchors) {
                    const orig = a.boat[ft];
                    if (orig != null && orig !== 0) {
                        const wRaw = weightField ? a.boat[weightField] : null;
                        const w = (wRaw != null && wRaw > 0) ? wRaw : 1;
                        ratios.push({boatId: a.boat.id, r: a.value / orig, w});
                    }
                }
                if (ratios.length === 0) {
                    ftStats[ft] = null;
                    continue;
                }
                const wSum = ratios.reduce((s, x) => s + x.w, 0);
                const R = ratios.reduce((s, x) => s + x.w * x.r, 0) / wSum;
                const S = ratios.length > 1
                    ? Math.sqrt(ratios.reduce((s, x) => s + x.w * (x.r - R) ** 2, 0) / wSum)
                    : 0;
                const cv = R > 0 ? S / R : 0;
                ftStats[ft] = {ratios, R, S, cv, ratioMap: new Map(ratios.map(x => [x.boatId, x.r]))};
            }

            valueCells().forEach(td => {
                const ft = td.dataset.factorType;
                const boatId = td.dataset.boatId;
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
                    const a = anchorByBoat.get(boatId);
                    td.textContent = a.value.toFixed(4);
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
                    td.textContent = (origVal * stats.R).toFixed(4);
                    td.style.color = confidenceColor(stats.cv);
                    td.title = confidenceLabel(stats.cv);
                }
            });

            // Delta columns. Colour uses fitColor(deviation) — the same input as the TCF
            // cell on the same row — so both cells reflect the same fit quality on the
            // same absolute scale (0..5% green→red), independent of which other boats
            // happen to be in the current view.
            const formatDelta = (delta, deviation) => {
                if (Math.abs(delta) < 0.00005) return '0';
                const arrow = delta > 0 ? '↑' : '↓';
                const sign = delta > 0 ? '+' : '−';
                const color = fitColor(deviation);
                return `<span style="font-size:0.75rem;color:${color};font-weight:bold;">${arrow}${sign}${Math.abs(delta).toFixed(4)}</span>`;
            };
            const renderDelta = (ftKey, deltaCol, factorAccessor) => {
                const stats = ftStats[ftKey];
                if (!stats || stats.ratios.length <= 1) return;
                calcBoats.forEach(boat => {
                    const fv = factorAccessor(boat);
                    const cell = section.querySelector(`.pf-calc-value[data-boat-id="${boat.id}"][data-factor-type="${deltaCol}"]`);
                    if (!cell) return;
                    if (fv == null) {
                        cell.textContent = '';
                        cell.style.color = '';
                        cell.title = '';
                        return;
                    }
                    const a = anchorByBoat.get(boat.id);
                    if (a) {
                        const predicted = fv * stats.R;
                        const delta = a.value - predicted;
                        const r = stats.ratioMap.get(boat.id);
                        const deviation = r != null ? Math.abs(r - stats.R) / stats.R : 0;
                        cell.innerHTML = formatDelta(delta, deviation);
                        cell.title = `Entered: ${a.value.toFixed(4)}, Predicted: ${predicted.toFixed(4)}`;
                    } else {
                        cell.textContent = '';
                        cell.style.color = '';
                        cell.title = 'Consensus prediction (no delta)';
                    }
                });
            };
            renderDelta('pf', 'pfDelta', b => b.pf);
            renderDelta('rf', 'rfDelta', b => b.rf);
        }

        function recalc() {
            const anchors = [];
            inputCells().forEach(inp => {
                const v = parseFloat(inp.value);
                if (!isNaN(v)) {
                    const boat = calcBoats.find(b => b.id === inp.dataset.boatId);
                    if (boat) anchors.push({boat, value: v});
                }
            });
            if (anchors.length === 0) restoreAll();
            else if (anchors.length === 1) scaleSingle(anchors[0]);
            else scaleMulti(anchors);
            saveToSession();
            if (cfg.onChange) cfg.onChange();
        }

        function readSession() {
            if (!cfg.sessionKey) return [];
            try {
                const json = sessionStorage.getItem(cfg.sessionKey);
                if (!json) return [];
                const data = JSON.parse(json);
                return Array.isArray(data) ? data : [];
            } catch (e) {
                return [];
            }
        }

        // Merge with existing session entries so boats not currently shown (e.g. from a
        // different race/division on another page) keep their handicap.
        function saveToSession() {
            if (!cfg.sessionKey) return;
            const current = getEnteredHandicaps();
            const remembered = readSession();
            const isCurrent = item => current.some(c =>
                (c.sailno && c.sailno === item.sailno) ||
                (!c.sailno && c.name && c.name === item.name));
            const merged = remembered.filter(r => !isCurrent(r)).concat(current);
            try {
                if (merged.length > 0)
                    sessionStorage.setItem(cfg.sessionKey, JSON.stringify(merged));
                else
                    sessionStorage.removeItem(cfg.sessionKey);
            } catch (e) { /* quota or disabled storage — ignore */
            }
        }

        function loadFromSession() {
            if (!cfg.sessionKey) return;
            const data = readSession();
            if (data.length > 0) setHandicapsByMatch(data);
        }

        function setBoats(newBoats, opts) {
            if (opts && opts.showBestFit !== undefined) cfg.showBestFit = opts.showBestFit;
            calcBoats = (newBoats || []).filter(b => b.pf != null);
            render();
            loadFromSession();
        }

        // Match imported rows to calc boats in passes, strongest criteria first, so that an
        // item with a known name+sailno locks in its boat before another same-sailno item can
        // claim it via sailno-only. Each boat is matched at most once.
        function setHandicapsByMatch(rows) {
            let matched = 0;
            const used = new Set();
            const items = rows.filter(r => r.handicap != null);
            const remaining = [...items];

            function applyTo(boat, item) {
                used.add(boat.id);
                const inp = section.querySelector(`.pf-calc-input[data-boat-id="${boat.id}"]`);
                if (inp) {
                    inp.value = String(item.handicap);
                    matched++;
                }
            }

            // Pass: candidate predicate(item, boat). For each item, if exactly one
            // unused boat matches, claim it. If multiple match and the item carries a
            // division, prefer the one with the same division (if unique).
            function pass(predicate) {
                const leftover = [];
                remaining.forEach(item => {
                    const cands = calcBoats.filter(b => !used.has(b.id) && predicate(item, b));
                    let pick = null;
                    if (cands.length === 1) pick = cands[0];
                    else if (cands.length > 1 && item.division != null) {
                        const dn = normaliseDesignName(item.division);
                        const byDiv = cands.filter(b => normaliseDesignName(b.division) === dn);
                        if (byDiv.length === 1) pick = byDiv[0];
                    }
                    if (pick) applyTo(pick, item);
                    else leftover.push(item);
                });
                remaining.length = 0;
                remaining.push(...leftover);
            }

            // 1. sailno + name (strongest — distinguishes same-sailno boats by name)
            pass((it, b) =>
                it.sailno && it.name &&
                sailnoMatch(b.sailNumber, it.sailno) &&
                normaliseDesignName(b.boatName) === normaliseDesignName(it.name));

            // 2. sailno + division
            pass((it, b) =>
                it.sailno && it.division != null &&
                sailnoMatch(b.sailNumber, it.sailno) &&
                normaliseDesignName(b.division) === normaliseDesignName(it.division));

            // 3. sailno alone (only when unambiguous, or division disambiguates inside pass)
            pass((it, b) =>
                it.sailno && sailnoMatch(b.sailNumber, it.sailno));

            // 4. name alone (last resort for boats whose sail number is missing/garbled)
            pass((it, b) => {
                if (!it.name) return false;
                const tn = normaliseDesignName(it.name);
                return tn !== '' && normaliseDesignName(b.boatName) === tn;
            });

            recalc();
            return matched;
        }

        function clearAll() {
            inputCells().forEach(inp => {
                inp.value = '';
            });
            if (cfg.sessionKey) {
                try {
                    sessionStorage.removeItem(cfg.sessionKey);
                } catch (e) { /* ignore */
                }
            }
            recalc();
        }

        function getEnteredHandicaps() {
            const out = [];
            inputCells().forEach(inp => {
                const v = parseFloat(inp.value);
                if (!isNaN(v)) {
                    const boat = calcBoats.find(b => b.id === inp.dataset.boatId);
                    if (boat) out.push({
                        sailno: boat.sailNumber || '',
                        name: boat.boatName || '',
                        handicap: v
                    });
                }
            });
            return out;
        }

        function getEnteredValues() {
            const m = new Map();
            inputCells().forEach(inp => {
                const v = parseFloat(inp.value);
                if (!isNaN(v)) m.set(inp.dataset.boatId, v);
            });
            return m;
        }

        // Merge a fetched/loaded row set into session storage so entries for boats not
        // currently shown (e.g. other divisions) are remembered and applied later when the
        // visible boat list changes. Existing entries with the same sailno or name are
        // replaced by the incoming row.
        function rememberFetchedRows(rows) {
            if (!cfg.sessionKey) return;
            const incoming = (rows || []).filter(r => r != null && r.handicap != null);
            if (incoming.length === 0) return;
            const existing = readSession();
            const replaced = item => incoming.some(r =>
                (r.sailno && r.sailno === item.sailno) ||
                (!r.sailno && r.name && r.name === item.name));
            const merged = existing.filter(e => !replaced(e)).concat(incoming);
            try {
                sessionStorage.setItem(cfg.sessionKey, JSON.stringify(merged));
            } catch (e) { /* quota or disabled storage — ignore */
            }
        }

        // ---- Wire fetch / load / download buttons ----

        async function doFetch() {
            const url = cfg.urlInput?.value.trim() || '';
            const status = cfg.fetchStatus;
            const btn = cfg.fetchBtn;
            if (!url) {
                if (status) {
                    status.textContent = 'Please enter a URL';
                    status.style.color = '#c62828';
                }
                return;
            }
            if (status) {
                status.textContent = 'Fetching...';
                status.style.color = '#666';
            }
            if (btn) btn.disabled = true;
            try {
                const resp = await fetch('/api/comparison/fetch-handicaps', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({url})
                });
                if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
                const data = await resp.json();
                if (!Array.isArray(data)) throw new Error('Expected array of handicaps');
                rememberFetchedRows(data);
                const matched = setHandicapsByMatch(data);
                if (status) {
                    status.textContent = `Fetched ${data.length} handicaps, matched ${matched} boats`;
                    status.style.color = matched > 0 ? '#2e7d32' : '#c62828';
                }
            } catch (err) {
                if (status) {
                    status.textContent = `Error: ${err.message}`;
                    status.style.color = '#c62828';
                }
            } finally {
                if (btn) btn.disabled = false;
            }
        }

        async function doFile() {
            const fileInput = cfg.fileInput;
            const status = cfg.fileStatus;
            if (!fileInput.files || !fileInput.files[0]) {
                if (status) {
                    status.textContent = 'No file selected';
                    status.style.color = '#c62828';
                }
                return;
            }
            const file = fileInput.files[0];
            if (status) {
                status.textContent = 'Reading file...';
                status.style.color = '#666';
            }
            try {
                const text = await file.text();
                const data = JSON.parse(text);
                if (!Array.isArray(data)) throw new Error('Expected array of handicaps in file');
                rememberFetchedRows(data);
                const matched = setHandicapsByMatch(data);
                if (status) {
                    status.textContent = `Loaded ${data.length} handicaps, matched ${matched} boats`;
                    status.style.color = matched > 0 ? '#2e7d32' : '#c62828';
                }
                fileInput.value = '';
            } catch (err) {
                if (status) {
                    status.textContent = `Error: ${err.message}`;
                    status.style.color = '#c62828';
                }
                fileInput.value = '';
            }
        }

        function doDownload() {
            const status = cfg.downloadStatus;
            const btn = cfg.downloadBtn;
            if (btn) btn.disabled = true;
            if (status) {
                status.textContent = 'Preparing download...';
                status.style.color = '#666';
            }
            try {
                const handicaps = getEnteredHandicaps();
                if (handicaps.length === 0) {
                    if (status) {
                        status.textContent = 'No handicaps entered';
                        status.style.color = '#c62828';
                    }
                    if (btn) btn.disabled = false;
                    return;
                }
                const json = JSON.stringify(handicaps, null, 2);
                const blob = new Blob([json], {type: 'application/json'});
                const url = URL.createObjectURL(blob);
                const link = document.createElement('a');
                link.href = url;
                link.download = `handicaps-${new Date().toISOString().split('T')[0]}.json`;
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
                URL.revokeObjectURL(url);
                if (status) {
                    status.textContent = `Downloaded ${handicaps.length} handicap(s)`;
                    status.style.color = '#2e7d32';
                }
            } catch (err) {
                if (status) {
                    status.textContent = `Error: ${err.message}`;
                    status.style.color = '#c62828';
                }
            } finally {
                if (btn) btn.disabled = false;
            }
        }

        if (cfg.fetchBtn) cfg.fetchBtn.addEventListener('click', doFetch);
        if (cfg.fileInput) cfg.fileInput.addEventListener('change', doFile);
        if (cfg.downloadBtn) cfg.downloadBtn.addEventListener('click', doDownload);

        return {setBoats, setHandicapsByMatch, clearAll, getEnteredHandicaps, getEnteredValues, recalc};
    }

    return {
        create,
        normaliseSailNumber, stripPrefix, normaliseDesignName, sailnoMatch,
    };
})();
