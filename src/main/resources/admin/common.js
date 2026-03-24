// Shared utilities used by all HPF pages

function esc(val) {
    if (val == null) return '';
    return String(val)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

async function fetchJson(url, options) {
    try {
        const resp = await fetch(url, options);
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

// Mark the current page's nav link as active
(function() {
    const page = location.pathname.split('/').pop() || 'index.html';
    document.querySelectorAll('.site-nav a').forEach(a => {
        const href = a.getAttribute('href').split('/').pop();
        if (href === page) a.classList.add('active');
    });
})();
