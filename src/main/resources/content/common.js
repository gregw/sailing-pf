// Shared utilities used by all PF pages

function esc(val) {
    if (val == null) return '';
    return String(val)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function infoBtn(anchor, tip) {
    const escapedTip = tip.replace(/"/g, '&quot;');
    return `<a href="ui-tips.md#${anchor}" class="info-btn" data-tip="${escapedTip}" target="_blank" onclick="event.stopPropagation()">ⓘ</a>`;
}

// Axis title for a "speed factor" (1/factor) x-axis. The formula argument is the
// expression after the colon, e.g. '1/PF' or '1/PF, 1/RF'. Plotly renders axis
// titles inside SVG, where the .info-btn CSS hover tooltip does not fire — so the
// ⓘ is a plain link to the speed-factor section in UI-Tips.
function speedFactorAxisTitle(formula) {
    return `Speed Factor <a href="ui-tips.md#speed-factor" class="info-btn" target="_blank">ⓘ</a>: ${formula}`;
}

// Persist a form control's value (text/select/number/checkbox) in sessionStorage so
// it stays the same as the user navigates between pages in this tab. Restores any
// saved value on call and registers a listener to save future user changes.
//
// `opts.key` is the storage key suffix — defaults to the element id. Pass an explicit
// key to share state between controls that live on different pages but represent the
// same concept (e.g. `hide-empty-boats` and `hide-empty-designs` both pass key
// `hide-empty`). `opts.onChange(el)` runs after every user-driven change, after the
// save — use it when restoration alone isn't enough and downstream logic needs to
// re-run. The element's own inline `onchange=` handler keeps firing too.
function persistControl(id, opts) {
    opts = opts || {};
    const el = document.getElementById(id);
    if (!el) return null;
    const key = 'pf.ctrl.' + (opts.key || id);
    const isCheckbox = el.type === 'checkbox';
    const stored = sessionStorage.getItem(key);
    if (stored !== null) {
        if (isCheckbox) el.checked = (stored === 'true');
        else el.value = stored;
    }
    const evtName = (el.tagName === 'SELECT' || isCheckbox || el.type === 'number')
        ? 'change' : 'input';
    el.addEventListener(evtName, () => {
        const val = isCheckbox ? String(el.checked) : el.value;
        sessionStorage.setItem(key, val);
        if (opts.onChange) opts.onChange(el);
    });
    return el;
}

async function fetchJson(url, options) {
    try {
        const resp = await fetch(url, options);
        if (resp.status === 401) {
            if (!document.getElementById('auth-nudge')) {
                const nudge = document.createElement('div');
                nudge.id = 'auth-nudge';
                nudge.className = 'import-warning-banner';
                nudge.innerHTML = 'Sign in required for this action. ' +
                    '<a href="/auth/protected">Sign in</a> &nbsp; ' +
                    '<button onclick="this.parentElement.remove()">×</button>';
                document.querySelector('main, body').prepend(nudge);
            }
            return null;
        }
        if (!resp.ok) {
            console.error('fetchJson non-OK:', resp.status, url);
            return null;
        }
        return await resp.json();
    } catch (e) {
        console.error('fetchJson failed:', url, e);
        return null;
    }
}

function fmtTime(seconds) {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = Math.round(seconds % 60);
    if (h > 0)
        return `${h}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
    return `${m}:${String(s).padStart(2,'0')}`;
}

/**
 * Push 1st/2nd/3rd podium markers (by smallest pfCorrected) onto the given Plotly traces array.
 * `xs` are x-axis values per finisher (matching the PF-corrected line); `pfCorr` are the
 * corresponding corrected times in MINUTES (already divided by 60). Markers are placed on the
 * PF-corrected line at the three fastest finishers.
 */
// Allocated-handicap podium: ranks `allocPts` (each with name, handicap, correctedMin)
// by corrected time and pushes star/diamond/triangle markers in the allocated-line color.
// `hoverInfo` lets callers suppress the native Plotly tooltip ('none') when they
// render their own popup using `pt.text`. Defaults to 'text' so existing callers
// keep getting Plotly's bubble.
function addAllocPodiumTraces(traces, allocPts, allocXs, allocYs, color = '#a04020', hoverInfo = 'text') {
    const PODIUM_SYMBOLS = ['star', 'diamond', 'triangle-up'];
    const PODIUM_SIZES = [14, 12, 11];
    const PODIUM_LABELS = ['1st', '2nd', '3rd'];
    const ranked = allocPts.map((p, i) => ({i, t: p.correctedMin})).sort((a, b) => a.t - b.t);
    for (let pos = 0; pos < Math.min(3, ranked.length); pos++) {
        const idx = ranked[pos].i;
        const p = allocPts[idx];
        traces.push({
            x: [allocXs[idx]], y: [allocYs[idx]],
            mode: 'markers', type: 'scatter',
            name: PODIUM_LABELS[pos], legendgroup: PODIUM_LABELS[pos], showlegend: false,
            marker: {
                symbol: PODIUM_SYMBOLS[pos], size: PODIUM_SIZES[pos],
                color, line: {color: '#fff', width: 1.5}
            },
            text: [`${PODIUM_LABELS[pos]}: ${esc(p.name)}<br>Allocated: ${p.handicap.toFixed(4)}<br>Corrected: ${fmtTime(p.correctedMin * 60)}`],
            hoverinfo: hoverInfo,
            customdata: p.f ? [{boatId: p.f.boatId}] : undefined
        });
    }
}

function addPodiumTraces(traces, finishers, xs, pfCorr, color = '#2255aa', hoverInfo = 'text') {
    const PODIUM_SYMBOLS = ['star', 'diamond', 'triangle-up'];
    const PODIUM_SIZES = [14, 12, 11];
    const PODIUM_LABELS = ['1st', '2nd', '3rd'];
    const ranked = finishers
        .map((f, i) => ({i, t: pfCorr[i]}))
        .filter(o => o.t != null)
        .sort((a, b) => a.t - b.t);
    for (let p = 0; p < Math.min(3, ranked.length); p++) {
        const idx = ranked[p].i;
        const f = finishers[idx];
        traces.push({
            x: [xs[idx]], y: [pfCorr[idx]],
            mode: 'markers', type: 'scatter',
            name: PODIUM_LABELS[p],
            legendgroup: PODIUM_LABELS[p],
            marker: {
                symbol: PODIUM_SYMBOLS[p], size: PODIUM_SIZES[p],
                color, line: {color: '#fff', width: 1.5}
            },
            text: [`${PODIUM_LABELS[p]}: ${f.sailNumber ? f.sailNumber + ' ' : ''}${esc(f.name || '')}`
            + `<br>PF corrected: ${fmtTime(pfCorr[idx] * 60)}`],
            hoverinfo: hoverInfo,
            customdata: [{boatId: f.boatId}]
        });
    }
}

/**
 * Mix a hex colour towards white. amount is 0..1 (0 = unchanged, 1 = white).
 * Used to derive shaded variants of a base palette colour for grouped series
 * (e.g. divisions of the same race share a hue at descending saturations).
 */
function lightenColor(hex, amount) {
    if (!hex || typeof hex !== 'string' || !hex.startsWith('#') || hex.length !== 7) return hex;
    const t = Math.max(0, Math.min(1, amount));
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    const nr = Math.round(r + (255 - r) * t);
    const ng = Math.round(g + (255 - g) * t);
    const nb = Math.round(b + (255 - b) * t);
    return `#${nr.toString(16).padStart(2, '0')}${ng.toString(16).padStart(2, '0')}${nb.toString(16).padStart(2, '0')}`;
}

/**
 * Build a Plotly trace for a regression / trend line from a slope, intercept, and x range.
 * Used by both the race-division chart and the series chart so they look and behave
 * identically. The trace samples 30 interpolated points so hover fires along the entire
 * visible line — a 2-point line only triggers hover at its endpoints, which is hard to
 * hit. The legend label and hover text both include the slope.
 *
 * opts: {dash, baseWidth, hoverWidth, showlegend}
 */
function trendLineTrace(slope, intercept, xMin, xMax, baseName, color, opts) {
    opts = opts || {};
    const dash = opts.dash || 'dashdot';
    const baseWidth = opts.baseWidth ?? 2.5;
    const hoverWidth = opts.hoverWidth ?? 5;
    const showlegend = opts.showlegend !== false;
    const N = 30;
    const xs = new Array(N);
    const ys = new Array(N);
    for (let i = 0; i < N; i++) {
        const x = xMin + (xMax - xMin) * (i / (N - 1));
        xs[i] = x;
        ys[i] = slope * x + intercept;
    }
    const fullName = `${baseName} (slope ${slope.toFixed(2)})`;
    return {
        x: xs, y: ys,
        mode: 'lines', type: 'scatter',
        name: fullName,
        showlegend,
        line: {dash, color, width: baseWidth},
        text: xs.map(() => fullName),
        hoverinfo: 'text',
        hoverlabel: {namelength: -1},
        meta: {trendLine: true, baseWidth, hoverWidth}
    };
}

// ---- Chart resizing ----
//
// Adds a drag handle to the bottom-right corner of a Plotly chart div so the user can
// resize its height. Heights are persisted per-chart-id in sessionStorage so they
// survive page navigation. Safe to call multiple times for the same chartId (e.g.
// when a dynamically-rendered chart container is rebuilt) — the handle is only added
// once, but the height is re-applied so a freshly-created div picks up the saved size.
const CHART_HEIGHTS_KEY = 'pf.chart.heights';

function initChartResize(chartId, defaultHeight) {
    const chartDiv = document.getElementById(chartId);
    if (!chartDiv) return;

    const storedHeights = JSON.parse(sessionStorage.getItem(CHART_HEIGHTS_KEY) || '{}');
    const currentHeight = storedHeights[chartId] || defaultHeight;
    chartDiv.style.height = currentHeight + 'px';

    if (chartDiv.dataset.resizeWired === 'true') return;
    chartDiv.dataset.resizeWired = 'true';
    chartDiv.style.position = 'relative';

    const handle = document.createElement('div');
    handle.className = 'chart-resize-handle';
    chartDiv.appendChild(handle);

    const minHeight = Math.floor(defaultHeight / 2);

    let isResizing = false;
    let startY = 0;
    let startHeight = 0;

    handle.addEventListener('mousedown', (e) => {
        isResizing = true;
        startY = e.clientY;
        startHeight = chartDiv.offsetHeight;
        e.preventDefault();
        document.body.style.cursor = 'ns-resize';
        document.body.style.userSelect = 'none';
    });

    document.addEventListener('mousemove', (e) => {
        if (!isResizing) return;
        const deltaY = e.clientY - startY;
        const newHeight = Math.max(minHeight, startHeight + deltaY);
        chartDiv.style.height = newHeight + 'px';
        if (window.Plotly) Plotly.Plots.resize(chartDiv);
        const heights = JSON.parse(sessionStorage.getItem(CHART_HEIGHTS_KEY) || '{}');
        heights[chartId] = newHeight;
        sessionStorage.setItem(CHART_HEIGHTS_KEY, JSON.stringify(heights));
    });

    document.addEventListener('mouseup', () => {
        if (isResizing) {
            isResizing = false;
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
        }
    });
}

/** Reference std dev in log space at weight = 1.0.  See .claude/error_bars.md. */
const SIGMA_0 = 0.020;

/**
 * Returns {lower, upper, capped} for a 95% CI around factor at given weight,
 * or null if weight <= 0.  Bars are capped at ±3*SIGMA_0 per the display rules.
 */
function errorBounds(factor, weight) {
    if (!factor || !weight || !isFinite(weight) || weight <= 0) return null;
    const sigma     = SIGMA_0 / Math.sqrt(weight);
    const halfWidth = Math.min(2 * sigma, 3 * SIGMA_0);
    return {
        lower:  Math.exp(Math.log(factor) - halfWidth),
        upper:  Math.exp(Math.log(factor) + halfWidth),
        capped: 2 * sigma > 3 * SIGMA_0
    };
}

// Mark the current page's nav link as active. For dropdown groups, also light up
// the parent trigger so e.g. /comparison.html highlights the "Boats ▾" pill.
(function() {
    const page = location.pathname.split('/').pop() || 'index.html';
    document.querySelectorAll('.site-nav a').forEach(a => {
        const href = a.getAttribute('href').split('/').pop();
        if (href === page) {
            a.classList.add('active');
            const trigger = a.closest('.nav-dropdown')?.querySelector('.nav-dropdown-trigger');
            if (trigger) trigger.classList.add('active');
        }
    });
})();

// Nav dropdowns. On hover-capable devices the menu opens on hover (CSS only) and
// clicking the trigger jumps straight to the first item ("Browse"). On touch the
// click toggles the menu open; outside-click or Escape closes.
(function () {
    const triggers = document.querySelectorAll('.nav-dropdown-trigger');
    if (triggers.length === 0) return;
    const hoverable = window.matchMedia('(hover: hover) and (pointer: fine)').matches;

    function closeAll() {
        document.querySelectorAll('.nav-dropdown.open').forEach(d => {
            d.classList.remove('open');
            d.querySelector('.nav-dropdown-trigger')?.setAttribute('aria-expanded', 'false');
        });
    }

    triggers.forEach(trigger => {
        trigger.addEventListener('click', e => {
            const dd = trigger.closest('.nav-dropdown');
            if (!dd) return;
            e.preventDefault();
            if (hoverable) {
                const first = dd.querySelector('.nav-dropdown-menu a');
                if (first) window.location.href = first.getAttribute('href');
                return;
            }
            const wasOpen = dd.classList.contains('open');
            closeAll();
            if (!wasOpen) {
                dd.classList.add('open');
                trigger.setAttribute('aria-expanded', 'true');
            }
        });
    });

    document.addEventListener('click', e => {
        if (!e.target.closest('.nav-dropdown')) closeAll();
    });
    document.addEventListener('keydown', e => {
        if (e.key === 'Escape') closeAll();
    });
})();

// Auth state — loaded once per page; fires pf:authready when done
window.pfAuth = { authenticated: false, email: null };
(async function loadAuthState() {
    const data = await fetchJson('/auth/status');
    if (!data) return;
    window.pfAuth = { authenticated: data.authenticated, email: data.email || null,
                       devMode: !!data.devMode };
    const nav = document.querySelector('.site-nav');
    if (nav) {
        const widget = document.createElement('span');
        widget.style.marginLeft = 'auto';
        if (data.authenticated && !data.devMode) {
            widget.innerHTML = esc(data.email) +
                ' &nbsp; <a href="/auth/logout">Sign out</a>';
        } else if (!data.authenticated) {
            widget.innerHTML = '<a href="/auth/protected">Sign in</a>';
        }
        nav.appendChild(widget);
    }
    document.dispatchEvent(new CustomEvent('pf:authready'));
})();

// Show an import-running banner on pages that have #import-banner
(function() {
    const banner = document.getElementById('import-banner');
    if (!banner) return;
    let importBannerPoller = null;
    async function checkImportStatus() {
        const data = await fetchJson('/api/importers/status');
        if (!data) return;
        banner.style.display = data.running ? '' : 'none';
        if (data.running && !importBannerPoller) {
            importBannerPoller = setInterval(checkImportStatus, 5000);
        } else if (!data.running && importBannerPoller) {
            clearInterval(importBannerPoller);
            importBannerPoller = null;
        }
    }
    checkImportStatus();
})();
