// Shared utilities used by all HPF pages

function esc(val) {
    if (val == null) return '';
    return String(val)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function infoBtn(anchor, tip) {
    const escapedTip = tip.replace(/"/g, '&quot;');
    return `<a href="docs.html#${anchor}" class="info-btn" data-tip="${escapedTip}" target="_blank" onclick="event.stopPropagation()">ⓘ</a>`;
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

// Auth state — loaded once per page; fires hpf:authready when done
window.hpfAuth = { authenticated: false, email: null };
(async function loadAuthState() {
    const data = await fetchJson('/auth/status');
    if (!data) return;
    window.hpfAuth = { authenticated: data.authenticated, email: data.email || null,
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
    document.dispatchEvent(new CustomEvent('hpf:authready'));
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
