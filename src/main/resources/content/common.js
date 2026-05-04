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
function addAllocPodiumTraces(traces, allocPts, allocXs, allocYs, color = '#a04020') {
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
            hoverinfo: 'text',
            customdata: p.f ? [{boatId: p.f.boatId}] : undefined
        });
    }
}

function addPodiumTraces(traces, finishers, xs, pfCorr, color = '#2255aa') {
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
            hoverinfo: 'text',
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

// Mark the current page's nav link as active
(function() {
    const page = location.pathname.split('/').pop() || 'index.html';
    document.querySelectorAll('.site-nav a').forEach(a => {
        const href = a.getAttribute('href').split('/').pop();
        if (href === page) a.classList.add('active');
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
