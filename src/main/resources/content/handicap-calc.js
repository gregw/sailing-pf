'use strict';

// Shared handicap calculator. Exposed as window.HandicapCalc.
//
// Supports N "sets" of allocated handicaps (e.g. start-of-series, end-of-series).
// One set is "focused" at any time: load / clear / upload / download and direct
// typing target the focused set. The scaled-PF/RF/Δ preview columns are also
// driven by the focused set. Charts that want to plot every set call getAllSets().
//
// HandicapCalc.create(cfg) returns a controller with:
//   setBoats(newBoats)         — render with new boat list (preserves matching entries by id)
//   setHandicapsByMatch(rows)  — fill focused-set inputs from [{sailno, name, handicap}],
//                                returns count matched
//   clearAll()                 — clear focused-set inputs and re-render
//   getEnteredHandicaps()      — focused-set entries as [{sailno, name, handicap}]
//   getEnteredValues()         — focused-set Map(boatId → handicap)
//   getAllSets()               — [{name, color, focused, show, values: Map(boatId → handicap)}] for charts
//   getShowPf() / getShowRf()  — booleans set by tickboxes in the PF / RF column headers
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
//   onFetchedRows,                  // optional async (rows) => {handled, matched} | null
//                                   //   called after fetch/load before setHandicapsByMatch;
//                                   //   return {handled:true, matched:N} to skip normal matching
//   // Optional fetch/load/save controls; pass to wire automatic event handlers:
//   urlInput, fetchBtn, fetchStatus,
//   fileInput, fileStatus,
//   downloadBtn, downloadStatus,
// }

const HandicapCalc = (function () {

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

    // Palette for allocated-handicap sets. Index 0 keeps the historic dashed-orange so
    // single-set users see no change; subsequent sets cycle through distinct hues.
    const SET_PALETTE = ['#a04020', '#1565c0', '#2e7d32', '#6a1b9a', '#b8860b', '#00838f'];

    function setColor(idx) {
        return SET_PALETTE[idx % SET_PALETTE.length];
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
        // Sized to content — width/height are deliberately unset so the popup can
        // start small with just text and grow once the pentagon is appended.
        popupEl.style.cssText = [
            'position:fixed', 'z-index:10000', 'display:none',
            'background:#fff', 'border:1px solid #888', 'border-radius:4px',
            'box-shadow:0 2px 8px rgba(0,0,0,0.18)', 'padding:6px',
            'pointer-events:none', 'font-family:sans-serif',
            'font-size:0.8rem', 'line-height:1.3', 'color:#222'
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

    // Re-clamps the popup so it stays on-screen after content changes. Reads the
    // popup's actual bounding box (so it must already be display:block) and
    // re-anchors relative to `rect` (the trigger element / cursor point).
    function positionPopupAtRect(rect) {
        if (!popupEl) return;
        const r = popupEl.getBoundingClientRect();
        const w = r.width || 200;
        const h = r.height || 40;
        let left = rect.right + 8;
        let top = rect.top - 4;
        if (left + w > window.innerWidth - 4) left = Math.max(4, rect.left - w - 8);
        if (top + h > window.innerHeight - 4) top = Math.max(4, window.innerHeight - h - 4);
        popupEl.style.left = left + 'px';
        popupEl.style.top = top + 'px';
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

    // Two-stage popup: when `text` is supplied it is rendered immediately, the
    // popup is shown, and a 250 ms timer fetches the performance profile. When
    // the profile resolves the pentagon + PP score are appended to the same
    // popup. Without `text` the popup stays hidden until the profile resolves
    // (legacy behaviour for the calculator boat-name link).
    function schedulePopup(rect, boatId, color, opts) {
        clearTimeout(popupHideTimer);
        clearTimeout(popupShowTimer);
        popupBoatId = boatId;
        const text = opts && opts.text;
        const onResolve = opts && opts.onResolve;

        const el = ensurePopup();
        el.innerHTML = '';
        if (text) {
            const textEl = document.createElement('div');
            textEl.style.cssText = 'padding:2px 4px;';
            textEl.innerHTML = text;
            el.appendChild(textEl);
            el.style.display = 'block';
            positionPopupAtRect(rect);
        }

        popupShowTimer = setTimeout(async () => {
            const profile = await fetchProfile(boatId);
            if (popupBoatId !== boatId) return; // user moved on
            if (onResolve) onResolve(profile);
            if (!profile) return; // popup may already be showing text-only — leave it.
            const chart = document.createElement('div');
            chart.style.cssText = 'width:200px;height:160px;margin:2px auto 0;';
            el.appendChild(chart);
            const overall = profile.overallScore != null ? profile.overallScore.toFixed(3) : '—';
            const score = document.createElement('div');
            score.style.cssText = 'text-align:center;font-size:0.75rem;color:#555;padding:0 4px 2px;';
            score.textContent = `PP: ${overall}`;
            el.appendChild(score);
            el.style.display = 'block';
            renderMiniPentagon(chart, profile, color);
            // Re-clamp now that the popup is taller — keeps it on-screen.
            positionPopupAtRect(rect);
        }, 250);
    }

    function showPentagonPopup(linkEl, boatId, color, text) {
        schedulePopup(linkEl.getBoundingClientRect(), boatId, color, {
            text,
            onResolve: profile => {
                linkEl.title = profile
                    ? 'Click to view boat details — hover for performance profile'
                    : 'Click to view boat details — no performance profile available';
            }
        });
    }

    function showPentagonPopupAt(x, y, boatId, color, text) {
        schedulePopup({left: x, right: x, top: y, bottom: y}, boatId, color, {text});
    }

    function hidePentagonPopup() {
        clearTimeout(popupShowTimer);
        popupBoatId = null;
        popupHideTimer = setTimeout(() => {
            if (popupEl) popupEl.style.display = 'none';
        }, 80);
    }

    const VARIANT_LABELS = {spin: 'Spin', nonSpin: 'NS', twoHanded: '2H'};
    const VARIANT_ORDER = ['spin', 'nonSpin', 'twoHanded'];

    // Decide what to do with one fetched row given the current variant-mode select and the
    // boat's existing variant. Pure helper — also the unit under test.
    //   'skip'     — do not load this handicap (variants disagree under 'filter').
    //   'override' — load handicap AND replace the boat's variant ('set' mode).
    //   'apply'    — load handicap, keep the existing variant.
    function decideVariantAction(mode, itemVariant, currentBoatVariant) {
        if (mode === 'filter' && itemVariant && currentBoatVariant !== itemVariant) return 'skip';
        if (mode === 'set' && itemVariant && itemVariant !== currentBoatVariant) return 'override';
        return 'apply';
    }

    function create(cfg) {
        const section = cfg.section;
        const table = cfg.table;
        let calcBoats = [];
        let calcSort = {col: 'pf', dir: 'desc'};
        // Per-boat variant override: boatId → 'spin'|'nonSpin'|'twoHanded'
        const boatVariants = new Map();

        // Optional "compare" checkbox column — when cfg.compareSelect is true the
        // calculator prepends a tick-column so the consumer can pick exactly N boats
        // (default 2) for pairwise charts. The selection is bounded; ticking past the
        // cap is rejected. The cap and callback are read from cfg so the calculator
        // stays generic across pages.
        const compareEnabled = !!cfg.compareSelect;
        const compareMax = cfg.compareMax || 2;
        const compareSelectedIds = new Set();

        // Sets — one column per set, focused set drives load/clear/typing/scaled-preview.
        // `show` per set (default true) drives whether consumers (charts) plot that set's
        // allocated-corrected dataset; same idea for showPf / showRf at the controller level.
        let sets = [{name: 'Allocated', show: true}];
        let focusedIdx = 0;
        let nextSetN = 2;
        let showPf = true;
        let showRf = true;

        function valueCells() {
            return section.querySelectorAll('.pf-calc-value');
        }

        // All inputs across all sets.
        function inputCells() {
            return section.querySelectorAll('.pf-calc-input');
        }

        // Inputs for a specific set index.
        function setInputCells(idx) {
            return section.querySelectorAll(`.pf-calc-input[data-set-idx="${idx}"]`);
        }

        function focusedInputCells() {
            return setInputCells(focusedIdx);
        }

        function focusedInputFor(boatId) {
            return section.querySelector(
                `.pf-calc-input[data-boat-id="${boatId}"][data-set-idx="${focusedIdx}"]`);
        }

        function addSet() {
            sets.push({name: `Set ${nextSetN++}`, show: true});
            focusedIdx = sets.length - 1;
            render();
            saveToSession();
            recalc();
        }

        function removeSet(idx) {
            if (sets.length <= 1) return;
            // Re-key existing DOM inputs so render's "preserve entered values" pass picks
            // up the surviving columns under their new indices and discards the removed
            // column's values. Without this, removing column 0 would carry its values onto
            // the surviving column (which becomes the new index 0).
            inputCells().forEach(inp => {
                const i = parseInt(inp.dataset.setIdx, 10);
                if (i === idx) inp.value = '';
                else if (i > idx) inp.dataset.setIdx = String(i - 1);
            });
            // Rewrite session storage now (before splice / save) so the removed column's
            // entries — including cross-page boats not currently shown — are dropped, and
            // the surviving columns' entries shift down to match the new indices.
            if (cfg.sessionKey) {
                const remembered = readSession();
                if (remembered && Array.isArray(remembered.sets) && idx < remembered.sets.length) {
                    const surviving = remembered.sets.filter((_, i) => i !== idx);
                    const newFocused = focusedIdx > idx ? focusedIdx - 1
                        : focusedIdx === idx ? Math.min(focusedIdx, surviving.length - 1)
                            : focusedIdx;
                    try {
                        const anyEntries = surviving.some(s => s.entries && s.entries.length > 0);
                        const anyHidden = !showPf || !showRf || surviving.some(s => s.show === false);
                        if (!anyEntries && surviving.length <= 1 && !anyHidden) {
                            sessionStorage.removeItem(cfg.sessionKey);
                        } else {
                            sessionStorage.setItem(cfg.sessionKey, JSON.stringify({
                                version: 2, focused: Math.max(0, newFocused),
                                showPf, showRf, sets: surviving
                            }));
                        }
                    } catch (e) { /* quota or disabled storage — ignore */
                    }
                }
            }
            sets.splice(idx, 1);
            if (focusedIdx >= sets.length) focusedIdx = sets.length - 1;
            else if (focusedIdx > idx) focusedIdx--;
            render();
            saveToSession();
            recalc();
        }

        function setFocus(idx) {
            if (idx < 0 || idx >= sets.length || idx === focusedIdx) return;
            focusedIdx = idx;
            render();
            saveToSession();
            recalc();
        }

        // Delta = entered - predicted. Predicted = boat[ftKey] * R, where R is the
        // weighted mean of (entered/boat[ftKey]) across focused-set anchors. Mirrors
        // scaleMulti so a sort on PFΔ/RFΔ orders by exactly the values shown in the cells.
        // Only anchors get a delta; non-anchors (and the single-anchor case) return null.
        function computeDeltas(ftKey) {
            const wField = ftKey === 'pf' ? 'pfWeight' : 'rfWeight';
            const anchors = [];
            focusedInputCells().forEach(inp => {
                const v = parseFloat(inp.value);
                if (isNaN(v)) return;
                const boat = calcBoats.find(b => b.id === inp.dataset.boatId);
                if (!boat || boat[ftKey] == null || boat[ftKey] === 0) return;
                const wRaw = boat[wField];
                const w = (wRaw != null && wRaw > 0) ? wRaw : 1;
                anchors.push({boat, value: v, w});
            });
            const out = new Map();
            if (anchors.length < 2) return out;
            const wSum = anchors.reduce((s, a) => s + a.w, 0);
            const R = anchors.reduce((s, a) => s + a.w * (a.value / a.boat[ftKey]), 0) / wSum;
            anchors.forEach(a => {
                out.set(a.boat.id, a.value - a.boat[ftKey] * R);
            });
            return out;
        }

        function sortCalcBoats() {
            const {col, dir} = calcSort;
            const mul = dir === 'asc' ? 1 : -1;
            if (col === 'sailno') {
                calcBoats.sort((a, b) => mul * (a.sailNumber || '').localeCompare(b.sailNumber || ''));
            } else if (col === 'variant') {
                calcBoats.sort((a, b) => mul * (boatVariant(a)).localeCompare(boatVariant(b)));
            } else if (col === 'boatName') {
                calcBoats.sort((a, b) => mul * (a.boatName || a.name || '').localeCompare(b.boatName || b.name || ''));
            } else if (col === 'name') {
                calcBoats.sort((a, b) => mul * a.name.localeCompare(b.name));
            } else if (col.startsWith('input:')) {
                // Sort by the values entered in this specific set's input column.
                // Boats with no entry sort to the end regardless of direction.
                const idx = parseInt(col.slice('input:'.length), 10);
                const values = new Map();
                setInputCells(idx).forEach(inp => {
                    const v = parseFloat(inp.value);
                    if (!isNaN(v)) values.set(inp.dataset.boatId, v);
                });
                calcBoats.sort((a, b) => {
                    const va = values.get(a.id);
                    const vb = values.get(b.id);
                    if (va == null && vb == null) return 0;
                    if (va == null) return 1;
                    if (vb == null) return -1;
                    return mul * (va - vb);
                });
            } else if (col === 'pfDelta' || col === 'rfDelta') {
                const deltas = computeDeltas(col === 'pfDelta' ? 'pf' : 'rf');
                calcBoats.sort((a, b) => {
                    const da = deltas.get(a.id);
                    const db = deltas.get(b.id);
                    // Boats without a delta (non-anchors, or all rows when fewer than two
                    // anchors are entered) sort to the end regardless of direction.
                    if (da == null && db == null) return 0;
                    if (da == null) return 1;
                    if (db == null) return -1;
                    return mul * (da - db);
                });
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
            // Capture current entered values per (setIdx, boatId) so we can preserve them
            // across re-renders triggered by sort / focus / add-set / remove-set.
            const entered = new Map();   // key: `${setIdx}|${boatId}` → string
            inputCells().forEach(inp => {
                if (inp.value !== '')
                    entered.set(`${inp.dataset.setIdx}|${inp.dataset.boatId}`, inp.value);
            });

            if (calcBoats.length === 0) {
                section.style.display = 'none';
                table.innerHTML = '';
                return;
            }

            section.style.display = '';

            // Leading columns (before set inputs) — sortable.
            const leadCols = [
                {key: 'sailno', label: 'Sail No', align: 'left'},
                {key: 'boatName', label: 'Boat', align: 'left'},
                {key: 'variant', label: 'Var', align: 'center'},
            ];
            // Static (non-input) columns after set inputs — sortable.
            const staticCols = [
                {key: 'pf', label: 'PF', align: 'right'},
                {key: 'pfDelta', label: 'PFΔ', align: 'right'},
                {key: 'rf', label: 'RF', align: 'right'},
                {key: 'rfDelta', label: 'RFΔ', align: 'right'},
            ];
            if (cfg.showBestFit) staticCols.push({key: 'bestFit', label: 'Best Fit', align: 'right'});

            const validSortKeys = new Set([...leadCols.map(c => c.key), ...staticCols.map(c => c.key)]);
            const inputMatch = /^input:(\d+)$/.exec(calcSort.col);
            const inputValid = inputMatch && parseInt(inputMatch[1], 10) < sets.length;
            if (!validSortKeys.has(calcSort.col) && !inputValid) calcSort = {col: 'pf', dir: 'desc'};

            // Sort BEFORE clearing the table — computeDeltas() reads anchor values from
            // existing input elements, so the DOM must still be intact at this point.
            sortCalcBoats();
            table.innerHTML = '';

            const thead = document.createElement('thead');
            const hdrTr = document.createElement('tr');

            // Compare-checkbox column header (non-sortable). Only present when the page
            // wires up cfg.compareSelect.
            if (compareEnabled) {
                const cmpTh = document.createElement('th');
                cmpTh.style.cssText = 'padding:2px 4px;text-align:center;font-size:0.8rem;color:#555;';
                cmpTh.textContent = 'Cmp';
                cmpTh.title = `Tick up to ${compareMax} boats to compare`;
                hdrTr.appendChild(cmpTh);
            }

            // Sail No + Boat name columns (leading, sortable).
            leadCols.forEach(c => hdrTr.appendChild(makeSortableTh(c)));

            // One header per set: name + focus radio + remove button.
            sets.forEach((s, i) => hdrTr.appendChild(makeSetHeaderTh(s, i)));

            // "+ Add column" header — only shown once at least one set has entries, so
            // adding a second column makes sense (you have something to compare against).
            // Sources for "has entries": current DOM inputs (entered), session storage
            // (covers freshly-loaded calcs whose inputs are populated AFTER render), or
            // multiple existing sets (always allow adding more).
            const stored = readSession();
            const anyEntries = entered.size > 0
                || sets.length > 1
                || (stored && stored.sets && stored.sets.some(s => s.entries && s.entries.length > 0));
            const addTh = document.createElement('th');
            addTh.style.cssText = 'padding:2px 4px;text-align:center;';
            if (anyEntries) {
                const addBtn = document.createElement('button');
                addBtn.type = 'button';
                addBtn.textContent = '+';
                addBtn.title = 'Add another allocated-handicap column';
                addBtn.style.cssText = 'font-size:0.85rem;padding:0 6px;cursor:pointer;line-height:1.4;';
                addBtn.addEventListener('click', addSet);
                addTh.appendChild(addBtn);
            }
            hdrTr.appendChild(addTh);

            // Static columns (PF / PFΔ / RF / RFΔ / Best Fit). PF / RF columns
            // get a "show in chart" tickbox above the sort label so charts can hide each
            // dataset on demand.
            staticCols.forEach(c => {
                if (c.key === 'pf' || c.key === 'rf')
                    hdrTr.appendChild(makeFactorHeaderTh(c));
                else
                    hdrTr.appendChild(makeSortableTh(c));
            });

            thead.appendChild(hdrTr);
            table.appendChild(thead);

            const tbody = document.createElement('tbody');
            calcBoats.forEach(b => {
                const tr = document.createElement('tr');
                makeLeadingCells(b).forEach(td => tr.appendChild(td));
                sets.forEach((_, i) => tr.appendChild(makeInputCell(b, i, entered)));
                // Empty filler aligning with the "+ Add" header.
                const filler = document.createElement('td');
                tr.appendChild(filler);
                staticCols.forEach(c => tr.appendChild(makeStaticCell(b, c)));
                tbody.appendChild(tr);
            });
            table.appendChild(tbody);

            // The bare render places original values in scaled cells; if any focused-set
            // input has a value (preserved across re-render or freshly applied), recalc
            // will overlay scaled / delta values.
            if (Array.from(entered.keys()).some(k => k.startsWith(`${focusedIdx}|`))) recalc();
        }

        function makeSortableTh(c) {
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
            return th;
        }

        // Header for the PF / RF columns — a small "show" tickbox above the sortable label.
        // The tickbox controls whether external charts plot the corresponding corrected-time
        // dataset; toggling fires onChange so consumers can re-render.
        function makeFactorHeaderTh(c) {
            const th = document.createElement('th');
            th.style.cssText = `padding:2px 8px;font-size:0.8rem;color:#555;text-align:${c.align};`;
            const wrap = document.createElement('div');
            wrap.style.cssText = 'display:flex;flex-direction:column;align-items:center;gap:2px;';

            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.checked = c.key === 'pf' ? showPf : showRf;
            cb.title = `Show ${c.label} corrected dataset in chart`;
            cb.style.cssText = 'margin:0;cursor:pointer;';
            cb.addEventListener('change', () => {
                if (c.key === 'pf') showPf = cb.checked;
                else showRf = cb.checked;
                saveToSession();
                if (cfg.onChange) cfg.onChange();
            });
            wrap.appendChild(cb);

            const isActive = c.key === calcSort.col;
            const arrow = isActive ? (calcSort.dir === 'asc' ? ' ↑' : ' ↓') : '';
            const lbl = document.createElement('span');
            lbl.textContent = c.label + arrow;
            lbl.style.cssText = 'cursor:pointer;user-select:none;'
                + (isActive ? 'font-weight:bold;' : '');
            lbl.addEventListener('click', () => {
                if (calcSort.col === c.key) calcSort.dir = (calcSort.dir === 'asc' ? 'desc' : 'asc');
                else calcSort = {col: c.key, dir: c.key === 'name' ? 'asc' : 'desc'};
                render();
            });
            wrap.appendChild(lbl);

            th.appendChild(wrap);
            return th;
        }

        function makeSetHeaderTh(set, idx) {
            const th = document.createElement('th');
            th.style.cssText = 'padding:2px 4px;text-align:center;font-size:0.8rem;color:#555;';
            const wrap = document.createElement('div');
            wrap.style.cssText = 'display:flex;flex-direction:column;align-items:center;gap:2px;';

            // Top row: show tickbox + focus radio + name + (× when removable).
            const topRow = document.createElement('div');
            topRow.style.cssText = 'display:flex;align-items:center;gap:4px;';

            const showCb = document.createElement('input');
            showCb.type = 'checkbox';
            showCb.checked = set.show !== false;
            showCb.title = `Show ${set.name} corrected dataset in chart`;
            showCb.style.cssText = 'margin:0;cursor:pointer;';
            showCb.addEventListener('change', () => {
                set.show = showCb.checked;
                saveToSession();
                if (cfg.onChange) cfg.onChange();
            });
            topRow.appendChild(showCb);

            const radio = document.createElement('input');
            radio.type = 'radio';
            radio.name = `pf-calc-focus-${section.id || ''}`;
            radio.checked = idx === focusedIdx;
            radio.title = `Focus ${set.name} for load / clear / upload / download`;
            radio.style.cssText = 'margin:0;cursor:pointer;';
            radio.addEventListener('change', () => {
                if (radio.checked) setFocus(idx);
            });
            topRow.appendChild(radio);

            const nameSpan = document.createElement('span');
            nameSpan.textContent = set.name;
            nameSpan.style.cssText = `color:${setColor(idx)};font-weight:${idx === focusedIdx ? 'bold' : 'normal'};`;
            topRow.appendChild(nameSpan);

            if (sets.length > 1) {
                const rm = document.createElement('button');
                rm.type = 'button';
                rm.textContent = '×';
                rm.title = `Remove ${set.name}`;
                rm.style.cssText = 'font-size:0.85rem;padding:0 4px;cursor:pointer;line-height:1;background:none;border:1px solid #ccc;color:#888;border-radius:2px;';
                rm.addEventListener('click', () => removeSet(idx));
                topRow.appendChild(rm);
            }
            wrap.appendChild(topRow);

            // Sortable label row — clicking sorts by the values entered in this
            // specific set's column. Each set has its own sort key so a click on
            // column N sorts by column N's entries, not the focused column's.
            const sortKey = `input:${idx}`;
            const sortLbl = document.createElement('span');
            const isActive = calcSort.col === sortKey;
            const arrow = isActive ? (calcSort.dir === 'asc' ? ' ↑' : ' ↓') : '';
            sortLbl.textContent = 'Enter handicap' + arrow;
            sortLbl.style.cssText = 'cursor:pointer;user-select:none;font-size:0.75rem;'
                + (isActive ? 'font-weight:bold;' : '');
            sortLbl.addEventListener('click', () => {
                if (calcSort.col === sortKey) calcSort.dir = (calcSort.dir === 'asc' ? 'desc' : 'asc');
                else calcSort = {col: sortKey, dir: 'desc'};
                render();
            });
            wrap.appendChild(sortLbl);

            th.appendChild(wrap);
            return th;
        }

        function boatVariant(b) {
            return boatVariants.get(b.id) || b.variant || 'spin';
        }

        function applyVariantToPf(b, v) {
            const all = b.pfAll, wAll = b.pfWeightAll;
            if (all) {
                b.pf = all[v] ?? null;
                b.pfWeight = wAll ? (wAll[v] ?? null) : null;
            }
            const rAll = b.rfAll, rwAll = b.rfWeightAll;
            if (rAll) {
                b.rf = rAll[v] ?? null;
                b.rfWeight = rwAll ? (rwAll[v] ?? null) : null;
            }
        }

        function makeVariantCell(b) {
            const td = document.createElement('td');
            td.style.cssText = 'text-align:center;padding:1px 4px;white-space:nowrap;';
            const v = boatVariant(b);
            const hasAll = b.pfAll != null;

            if (hasAll) {
                // Dropdown so the user can switch per-boat variant.
                const sel = document.createElement('select');
                sel.style.cssText = 'font-size:0.75rem;padding:1px 2px;max-width:58px;';
                VARIANT_ORDER.forEach(opt => {
                    if (b.pfAll[opt] == null) return;
                    const o = document.createElement('option');
                    o.value = opt;
                    o.textContent = VARIANT_LABELS[opt];
                    if (opt === v) o.selected = true;
                    sel.appendChild(o);
                });
                sel.addEventListener('change', () => {
                    const newV = sel.value;
                    boatVariants.set(b.id, newV);
                    applyVariantToPf(b, newV);
                    // Refresh the static value cells for this boat.
                    section.querySelectorAll(`.pf-calc-value[data-boat-id="${b.id}"]`).forEach(cell => {
                        const ft = cell.dataset.factorType;
                        if (ft === 'pfDelta' || ft === 'rfDelta') {
                            cell.textContent = '';
                            cell.style.color = '';
                            cell.title = '';
                            return;
                        }
                        const val = b[ft];
                        cell.textContent = val != null ? val.toFixed(4) : '—';
                        cell.dataset.origValue = val != null ? String(val) : '';
                        const wField = weightFieldFor(ft);
                        const w = wField ? b[wField] : null;
                        if (val != null && w != null) {
                            cell.style.color = weightColor(w);
                            cell.title = weightLabel(w);
                        } else {
                            cell.style.color = '';
                            cell.title = '';
                        }
                    });
                    recalc();
                });
                td.appendChild(sel);
            } else {
                // Fixed badge — variant determined by race data, not changeable.
                const badge = document.createElement('span');
                const label = VARIANT_LABELS[v] || v;
                badge.textContent = label;
                badge.style.cssText = 'font-size:0.72rem;padding:1px 4px;border-radius:3px;'
                    + (v === 'twoHanded' ? 'background:#e8f0fe;color:#1a56db;'
                        : v === 'nonSpin' ? 'background:#fef3c7;color:#b45309;'
                            : 'background:#d1fae5;color:#065f46;');
                td.appendChild(badge);
            }
            return td;
        }

        // Build the optional compare-checkbox cell. The checkbox is disabled when
        // the selection is full and this boat isn't already in it, so the user can't
        // tick past the cap. Toggling fires onCompareSelectionChange with the new id set.
        function makeCompareCell(b) {
            const td = document.createElement('td');
            td.style.cssText = 'text-align:center;padding:1px 4px;';
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.className = 'pf-calc-compare';
            cb.dataset.boatId = b.id;
            cb.checked = compareSelectedIds.has(b.id);
            cb.disabled = !cb.checked && compareSelectedIds.size >= compareMax;
            cb.title = cb.disabled
                ? `Tick at most ${compareMax} boats to compare`
                : `Tick to compare this boat with up to ${compareMax - 1} other(s)`;
            cb.addEventListener('change', () => {
                if (cb.checked) {
                    if (compareSelectedIds.size >= compareMax) {
                        cb.checked = false;
                        return;
                    }
                    compareSelectedIds.add(b.id);
                } else {
                    compareSelectedIds.delete(b.id);
                }
                // Re-render so other checkboxes' disabled state updates immediately.
                render();
                if (cfg.onCompareSelectionChange)
                    cfg.onCompareSelectionChange(new Set(compareSelectedIds));
            });
            td.appendChild(cb);
            return td;
        }

        function makeLeadingCells(b) {
            const color = b.color || '#888';

            const tdSailno = document.createElement('td');
            tdSailno.style.cssText = `color:${color};font-weight:bold;font-family:monospace;padding-right:4px;white-space:nowrap;`;
            tdSailno.textContent = b.sailNumber || '—';

            const tdName = document.createElement('td');
            tdName.style.cssText = `color:${color};font-weight:bold;`;
            const link = document.createElement('a');
            link.href = `boats.html?id=${encodeURIComponent(b.id)}`;
            link.textContent = b.boatName || b.name;
            link.style.cssText = 'color:inherit;text-decoration:none;';
            link.title = 'Click to view boat details — hover for performance profile';
            const cached = profileCache.get(b.id);
            if (cached) cached.then(p => {
                if (p === null) link.title = 'Click to view boat details — no performance profile available';
            });
            tdName.appendChild(link);

            // Hover on either cell shows the performance profile popup.
            const designText = b.designName
                ? `<span style="color:#555;font-style:italic">${b.designName.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')}</span>`
                : null;
            tdSailno.addEventListener('mouseenter', () => showPentagonPopup(tdSailno, b.id, color, designText));
            tdSailno.addEventListener('mouseleave', hidePentagonPopup);
            link.addEventListener('mouseenter', () => showPentagonPopup(link, b.id, color, designText));
            link.addEventListener('mouseleave', hidePentagonPopup);

            const cells = [tdSailno, tdName, makeVariantCell(b)];
            if (compareEnabled) cells.unshift(makeCompareCell(b));
            return cells;
        }

        function makeInputCell(b, setIdx, enteredMap) {
            const tdInput = document.createElement('td');
            tdInput.style.cssText = 'padding:2px 4px;text-align:center;';
            const input = document.createElement('input');
            input.type = 'number';
            input.step = '0.0001';
            input.min = '0.1';
            input.max = '2.0';
            input.className = 'pf-calc-input';
            input.dataset.boatId = b.id;
            input.dataset.setIdx = String(setIdx);
            input.placeholder = 'enter…';
            const isFocused = setIdx === focusedIdx;
            input.style.cssText = 'width:90px;font-family:monospace;text-align:right;'
                + (isFocused ? '' : 'background:#f7f7f7;color:#666;');
            const prev = enteredMap.get(`${setIdx}|${b.id}`);
            if (prev != null) {
                const n = parseFloat(prev);
                input.value = isNaN(n) ? prev : n.toFixed(4);
            }
            input.addEventListener('input', recalc);
            input.addEventListener('blur', () => {
                const n = parseFloat(input.value);
                if (!isNaN(n)) input.value = n.toFixed(4);
            });
            input.addEventListener('focus', () => {
                if (setIdx !== focusedIdx) setFocus(setIdx);
            });
            tdInput.appendChild(input);
            return tdInput;
        }

        function makeStaticCell(b, c) {
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
            return td;
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
                // Every value cell shows the per-factor consensus prediction
                // (origVal * R) — including anchor rows, so PF / RF / Best Fit each
                // reflect their own scale. The Δ column carries the residual between
                // entered and predicted for anchor rows.
                td.textContent = (origVal * stats.R).toFixed(4);
                const isAnchor = anchorIds.has(boatId);
                const r = isAnchor ? stats.ratioMap.get(boatId) : null;
                if (isAnchor && r != null) {
                    const deviation = Math.abs(r - stats.R) / stats.R;
                    td.style.color = fitColor(deviation);
                    td.title = fitLabel(deviation);
                } else {
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
            focusedInputCells().forEach(inp => {
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

        // Session schema:
        //   { version: 2, focused: 0, sets: [{name, entries: [{sailno, name, handicap}]}] }
        // Old single-array format auto-migrated to one set named "Allocated".
        function readSession() {
            if (!cfg.sessionKey) return null;
            try {
                const json = sessionStorage.getItem(cfg.sessionKey);
                if (!json) return null;
                const data = JSON.parse(json);
                if (Array.isArray(data))
                    return {version: 2, focused: 0, sets: [{name: 'Allocated', entries: data}]};
                if (data && Array.isArray(data.sets))
                    return data;
                return null;
            } catch (e) {
                return null;
            }
        }

        // For each set, replace any prev entry for a boat that has a value in `current`,
        // and keep every other prev entry untouched — even for boats that ARE currently
        // shown but happen to have an empty input. Only explicit Clear All / column removal
        // wipe entries; manually clearing an input is a no-op for the session, so the value
        // can come back from storage when the calculator is re-rendered.
        //
        // Sailno comparison normalises (uppercase, strip prefixes / leading zeros) so that
        // different formats from fetch sources (e.g. "AUS7232") and stored boat sailNumbers
        // ("7232") don't accumulate as duplicate entries.
        function saveToSession() {
            if (!cfg.sessionKey) return;
            const remembered = readSession();
            const sailKey = s => s ? stripPrefix(normaliseSailNumber(s)) : '';
            const nameKey = n => n ? normaliseDesignName(n) : '';
            const out = {version: 2, focused: focusedIdx, showPf, showRf, sets: []};
            sets.forEach((s, i) => {
                const current = getSetEntries(i);
                const prev = remembered && remembered.sets[i] ? remembered.sets[i].entries : [];
                const isReplaced = item => current.some(c => {
                    const cs = sailKey(c.sailno), is = sailKey(item.sailno);
                    if (cs && is) return cs === is;
                    if (!cs && !is) {
                        const cn = nameKey(c.name), in_ = nameKey(item.name);
                        return cn !== '' && cn === in_;
                    }
                    return false;
                });
                const merged = prev.filter(r => !isReplaced(r)).concat(current);
                out.sets.push({name: s.name, show: s.show !== false, entries: merged});
            });
            const variants = {};
            let hasVariantOverrides = false;
            calcBoats.forEach(b => {
                const v = boatVariants.get(b.id);
                if (v && v !== (b.variant || 'spin')) {
                    variants[b.id] = v;
                    hasVariantOverrides = true;
                }
            });
            if (hasVariantOverrides) out.variants = variants;
            try {
                const anyEntries = out.sets.some(s => s.entries.length > 0);
                const anyHidden = !showPf || !showRf || out.sets.some(s => !s.show);
                if (anyEntries || out.sets.length > 1 || anyHidden || hasVariantOverrides)
                    sessionStorage.setItem(cfg.sessionKey, JSON.stringify(out));
                else
                    sessionStorage.removeItem(cfg.sessionKey);
            } catch (e) { /* quota or disabled storage — ignore */
            }
        }

        function loadFromSession() {
            if (!cfg.sessionKey) return;
            const data = readSession();
            if (!data) return;

            // Reshape `sets` and focus to match what's persisted.
            sets = data.sets.map(s => ({name: s.name || 'Allocated', show: s.show !== false}));
            if (sets.length === 0) sets = [{name: 'Allocated', show: true}];
            focusedIdx = Math.max(0, Math.min(sets.length - 1, data.focused | 0));
            if (typeof data.showPf === 'boolean') showPf = data.showPf;
            if (typeof data.showRf === 'boolean') showRf = data.showRf;
            // Ensure auto-naming continues from the highest existing "Set N".
            nextSetN = Math.max(2, ...sets.map(s => {
                const m = /^Set\s+(\d+)$/.exec(s.name);
                return m ? parseInt(m[1], 10) + 1 : 0;
            }));
            // Restore per-boat variant overrides before render so dropdowns reflect them.
            if (data.variants && typeof data.variants === 'object') {
                Object.entries(data.variants).forEach(([id, v]) => {
                    boatVariants.set(id, v);
                    const boat = calcBoats.find(b => b.id === id);
                    if (boat) applyVariantToPf(boat, v);
                });
            }
            render();

            // Apply each set's entries to its column inputs. applyEntriesToSet may flip
            // per-boat variants when mode is 'set', so re-render if any did — otherwise
            // the variant dropdowns built above would keep showing the pre-import values.
            let variantsChanged = false;
            sets.forEach((_, i) => {
                const entries = data.sets[i] ? data.sets[i].entries : [];
                if (entries && entries.length > 0) {
                    const r = applyEntriesToSet(i, entries);
                    if (r.variantsChanged) variantsChanged = true;
                }
            });
            if (variantsChanged) render();
            recalc();
        }

        function setBoats(newBoats, opts) {
            if (opts && opts.showBestFit !== undefined) cfg.showBestFit = opts.showBestFit;
            const incoming = (newBoats || []);
            // Seed per-boat variant from boat.variant, applying pfAll if provided.
            incoming.forEach(b => {
                const v = b.variant || 'spin';
                if (!boatVariants.has(b.id)) boatVariants.set(b.id, v);
                applyVariantToPf(b, boatVariants.get(b.id));
            });
            calcBoats = incoming.filter(b => b.pf != null);
            // Drop compare ticks for boats that are no longer in the calculator.
            if (compareEnabled) {
                const presentIds = new Set(calcBoats.map(b => b.id));
                let dropped = false;
                for (const id of [...compareSelectedIds])
                    if (!presentIds.has(id)) {
                        compareSelectedIds.delete(id);
                        dropped = true;
                    }
                if (dropped && cfg.onCompareSelectionChange)
                    cfg.onCompareSelectionChange(new Set(compareSelectedIds));
            }
            render();
            loadFromSession();
        }

        // Update variant for all boats whose current variant === oldVariant → newVariant.
        // Applies correct pf/rf from pfAll where available, then recalcs.
        function updateVariant(oldVariant, newVariant) {
            let changed = false;
            calcBoats.forEach(b => {
                if (boatVariant(b) === oldVariant) {
                    boatVariants.set(b.id, newVariant);
                    applyVariantToPf(b, newVariant);
                    changed = true;
                }
            });
            if (changed) {
                render();
                recalc();
            }
        }

        // Match imported rows into a specific set's input column. Same multi-pass logic as
        // before — strongest criteria first so a known name+sailno locks in its boat before
        // a same-sailno-only item can claim it. Each boat is matched at most once per call.
        function applyEntriesToSet(setIdx, rows) {
            let matched = 0;
            let variantsChanged = false;
            const used = new Set();
            const items = rows.filter(r => r.handicap != null);
            const remaining = [...items];

            function applyTo(boat, item) {
                const mode = cfg.variantModeSelect?.value || 'ignore';
                const action = decideVariantAction(mode, item.variant, boatVariant(boat));
                if (action === 'skip') return;
                used.add(boat.id);
                if (action === 'override') {
                    boatVariants.set(boat.id, item.variant);
                    applyVariantToPf(boat, item.variant);
                    variantsChanged = true;
                }
                const inp = section.querySelector(
                    `.pf-calc-input[data-boat-id="${boat.id}"][data-set-idx="${setIdx}"]`);
                if (inp) {
                    inp.value = parseFloat(item.handicap).toFixed(4);
                    matched++;
                }
            }

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
            // 3. sailno alone
            pass((it, b) =>
                it.sailno && sailnoMatch(b.sailNumber, it.sailno));
            // 4. name alone
            pass((it, b) => {
                if (!it.name) return false;
                const tn = normaliseDesignName(it.name);
                return tn !== '' && normaliseDesignName(b.boatName) === tn;
            });

            return {matched, variantsChanged};
        }

        // Public: load rows into the focused set.
        function setHandicapsByMatch(rows) {
            const {matched, variantsChanged} = applyEntriesToSet(focusedIdx, rows);
            if (variantsChanged) render();
            recalc();
            return matched;
        }

        // Clear the focused set entirely — visible inputs *and* any persisted entries for
        // boats not currently shown (e.g. boats from other divisions/races sharing this
        // sessionKey). Other sets are untouched.
        function clearAll() {
            focusedInputCells().forEach(inp => {
                inp.value = '';
            });
            if (cfg.sessionKey) {
                const remembered = readSession();
                if (remembered && Array.isArray(remembered.sets) && remembered.sets[focusedIdx]) {
                    remembered.sets[focusedIdx] = {
                        name: remembered.sets[focusedIdx].name,
                        show: remembered.sets[focusedIdx].show !== false,
                        entries: []
                    };
                    try {
                        const anyEntries = remembered.sets.some(s => s.entries && s.entries.length > 0);
                        const anyHidden = remembered.showPf === false || remembered.showRf === false
                            || remembered.sets.some(s => s.show === false);
                        if (!anyEntries && remembered.sets.length <= 1 && !anyHidden)
                            sessionStorage.removeItem(cfg.sessionKey);
                        else
                            sessionStorage.setItem(cfg.sessionKey, JSON.stringify(remembered));
                    } catch (e) { /* quota or disabled storage — ignore */
                    }
                }
            }
            recalc();
        }

        function getSetEntries(setIdx) {
            const out = [];
            setInputCells(setIdx).forEach(inp => {
                const v = parseFloat(inp.value);
                if (!isNaN(v)) {
                    const boat = calcBoats.find(b => b.id === inp.dataset.boatId);
                    if (boat) out.push({
                        sailno: boat.sailNumber || '',
                        name: boat.boatName || '',
                        handicap: v,
                        variant: boatVariant(boat)
                    });
                }
            });
            return out;
        }

        function getSetValues(setIdx) {
            const m = new Map();
            setInputCells(setIdx).forEach(inp => {
                const v = parseFloat(inp.value);
                if (!isNaN(v)) m.set(inp.dataset.boatId, v);
            });
            return m;
        }

        function getEnteredHandicaps() {
            return getSetEntries(focusedIdx);
        }

        function getEnteredValues() {
            return getSetValues(focusedIdx);
        }

        function getAllSets() {
            return sets.map((s, i) => ({
                name: s.name,
                color: setColor(i),
                focused: i === focusedIdx,
                show: s.show !== false,
                values: getSetValues(i)
            }));
        }

        function getShowPf() {
            return showPf;
        }

        function getShowRf() {
            return showRf;
        }

        // Merge fetched rows into the FOCUSED set's session entries so off-screen boats
        // (e.g. other divisions) are remembered and applied when the boat list changes.
        // Existing entries in that set with the same sailno or name are replaced.
        function rememberFetchedRows(rows) {
            if (!cfg.sessionKey) return;
            const incoming = (rows || []).filter(r => r != null && r.handicap != null);
            if (incoming.length === 0) return;
            const stored = readSession()
                || {version: 2, focused: focusedIdx, showPf, showRf, sets: []};
            // Pad sets to match current length.
            while (stored.sets.length < sets.length) {
                const i = stored.sets.length;
                stored.sets.push({name: sets[i].name, show: sets[i].show !== false, entries: []});
            }
            const target = stored.sets[focusedIdx];
            const replaced = item => incoming.some(r =>
                (r.sailno && r.sailno === item.sailno) ||
                (!r.sailno && r.name && r.name === item.name));
            target.entries = (target.entries || []).filter(e => !replaced(e)).concat(incoming);
            stored.focused = focusedIdx;
            try {
                sessionStorage.setItem(cfg.sessionKey, JSON.stringify(stored));
            } catch (e) { /* quota or disabled storage — ignore */
            }
        }

        // Stamp a source-variant override onto all rows before filter/session logic runs.
        function applySourceVariantOverride(rows) {
            const v = cfg.sourceVariantSelect?.value;
            if (!v || v === 'auto') return;
            rows.forEach(r => {
                r.variant = v;
            });
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
                applySourceVariantOverride(data);
                rememberFetchedRows(data);
                const cbResult = cfg.onFetchedRows ? await cfg.onFetchedRows(data) : null;
                const matched = cbResult?.handled ? cbResult.matched : setHandicapsByMatch(data);
                if (status) {
                    const verb = cbResult?.handled ? 'added' : 'matched';
                    status.textContent = `Fetched ${data.length} handicaps, ${verb} ${matched} boats`;
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
                applySourceVariantOverride(data);
                rememberFetchedRows(data);
                const cbResult = cfg.onFetchedRows ? await cfg.onFetchedRows(data) : null;
                const matched = cbResult?.handled ? cbResult.matched : setHandicapsByMatch(data);
                if (status) {
                    const verb = cbResult?.handled ? 'added' : 'matched';
                    status.textContent = `Loaded ${data.length} handicaps, ${verb} ${matched} boats`;
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

        return {
            setBoats, setHandicapsByMatch, clearAll, getEnteredHandicaps, getEnteredValues,
            getAllSets, getShowPf, getShowRf, recalc, updateVariant,
            // Compare-checkbox accessors (no-op when cfg.compareSelect is false).
            getCompareSelection: () => new Set(compareSelectedIds),
            getBoatVariant: (id) => boatVariants.get(id) || 'spin'
        };
    }

    return {
        create,
        normaliseSailNumber, stripPrefix, normaliseDesignName, sailnoMatch,
        showPentagonPopupAt, hidePentagonPopup,
        decideVariantAction,
    };
})();

if (typeof window !== 'undefined') window.HandicapCalc = HandicapCalc;
if (typeof module !== 'undefined' && module.exports) module.exports = HandicapCalc;
