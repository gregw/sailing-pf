function renderJsonTree(val, depth) {
    if (val === null) return '<span class="jt-null">null</span>';
    const t = typeof val;
    if (t === 'boolean') return `<span class="jt-bool">${val}</span>`;
    if (t === 'number')  return `<span class="jt-num">${val}</span>`;
    if (t === 'string')  return `<span class="jt-str">"${esc(val)}"</span>`;
    if (Array.isArray(val)) {
        if (val.length === 0) return '<span class="jt-punct">[]</span>';
        const open = depth < 2 ? ' open' : '';
        const rows = val.map(v => `<div class="jt-row">${renderJsonTree(v, depth + 1)}</div>`).join('');
        return `<details${open}><summary class="jt-sum">[ ${val.length} ]</summary><div class="jt-body">${rows}</div></details>`;
    }
    if (t === 'object') {
        const keys = Object.keys(val);
        if (keys.length === 0) return '<span class="jt-punct">{}</span>';
        const open = depth < 2 ? ' open' : '';
        const preview = keys.slice(0, 4).map(k => esc(k)).join(', ') + (keys.length > 4 ? ', …' : '');
        const rows = keys.map(k =>
            `<div class="jt-row"><span class="jt-key">${esc(k)}</span>: ${renderJsonTree(val[k], depth + 1)}</div>`
        ).join('');
        return `<details${open}><summary class="jt-sum">{ ${preview} }</summary><div class="jt-body">${rows}</div></details>`;
    }
    return esc(String(val));
}

function weightColor(w) {
    const g = Math.round((1 - Math.min(w ?? 0, 1)) * 140);
    return `rgb(${g},${g},${g})`;
}

const COLUMNS = {
    boats: [
        { label: 'ID',     key: 'id' },
        { label: 'Sail',   key: 'sailNumber' },
        { label: 'Name',   key: 'name' },
        { label: 'Design', key: 'designId' },
        { label: 'RF',     key: 'spinRef',
          render: v => v && v.value != null
            ? `<span style="color:${weightColor(v.weight)}">${v.value.toFixed(4)}</span>`
            : '<span style="color:#bbb">—</span>' },
        { label: 'Club',     key: 'clubId' },
        { label: 'Finishes', type: 'action',
          action: item => { setFilter('races', 'boatId', item.id,
                            'Races for ' + (item.name || item.id)); switchTab('races'); } },
        { label: 'Excl',   key: 'excluded', type: 'toggle' },
    ],
    designs: [
        { label: 'ID',     key: 'id' },
        { label: 'Name',   key: 'canonicalName' },
        { label: 'RF',     key: 'spinRef',
          render: v => v && v.value != null
            ? `<span style="color:${weightColor(v.weight)}">${v.value.toFixed(4)}</span>`
            : '<span style="color:#bbb">—</span>' },
        { label: 'Makers', key: 'makerIds', render: v => esc((v || []).join(', ')) },
        { label: 'Boats',  type: 'action',
          action: item => { setFilter('boats', 'designId', item.id,
                            'Boats of design ' + (item.canonicalName || item.id)); switchTab('boats'); } },
        { label: 'Excl',   key: 'excluded', type: 'toggle' },
    ],
    races: [
        { label: 'ID',        key: 'id' },
        { label: 'Club',      key: 'clubId' },
        { label: 'Date',      key: 'date' },
        { label: 'Series',    key: 'seriesName' },
        { label: 'Race',      key: 'name' },
        { label: 'Finishers', key: 'finishers' },
        { label: 'Excl',      key: 'excluded', type: 'toggle' },
    ],
};

const state = {
    pages:  { boats: 0, designs: 0, races: 0 },
    sort:   { boats: 'id', designs: 'id', races: 'date' },
    dir:    { boats: 'asc', designs: 'asc', races: 'desc' },
    searchTimers: {},
    activeTab: 'boats',
    selected:     { boats: new Set(), designs: new Set() },   // IDs of checked rows
    selectedData: { boats: new Map(), designs: new Map() },   // id → item for merge panel
    filter: { boats: null, designs: null, races: null },      // { param, value, label } or null
};

function switchTab(entity) {
    ['boats', 'designs', 'races'].forEach(e => {
        document.getElementById('tab-btn-' + e).classList.toggle('active', e === entity);
        document.getElementById('panel-' + e).classList.toggle('active', e === entity);
    });
    state.activeTab = entity;
    updateFilterBanner(entity);
    updateFilterControls(entity);
    if (document.querySelector('#tbody-' + entity + ' tr') === null) {
        loadList(entity, 0);
    }
}

function setFilter(entity, param, value, label) {
    state.filter[entity] = { param, value, label };
    state.pages[entity] = 0;
    const q = document.getElementById('q-' + entity);
    if (q) q.value = '';
    updateFilterBanner(entity);
    updateFilterControls(entity);
    loadList(entity, 0);
}

function clearFilter(entity) {
    state.filter[entity] = null;
    updateFilterBanner(entity);
    updateFilterControls(entity);
    loadList(entity, 0);
}

function updateFilterBanner(entity) {
    const f   = state.filter[entity];
    const div = document.getElementById('filter-banner-' + entity);
    if (!div) return;
    if (!f) { div.style.display = 'none'; return; }
    div.style.display = '';
    div.innerHTML = esc(f.label) +
        ` <button onclick="clearFilter('${entity}')">× clear</button>`;
}

function updateFilterControls(entity) {
    const active = !!state.filter[entity];
    ['show-excluded-' + entity, 'filter-dupe-sails'].forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.disabled = active;
            if (el.parentElement) el.parentElement.style.opacity = active ? '0.4' : '';
        }
    });
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
    const q    = document.getElementById('q-' + entity).value;
    const sort = state.sort[entity];
    const dir  = state.dir[entity];
    let url = `/api/${entity}?page=${page}&size=50&q=${encodeURIComponent(q)}&sort=${sort}&dir=${dir}`;
    const f = state.filter[entity];
    if (f) url += `&${f.param}=${encodeURIComponent(f.value)}`;
    if (entity === 'boats' && !f) {
        if (document.getElementById('filter-dupe-sails').checked) url += '&dupeSails=true';
    }
    if (!f && document.getElementById('show-excluded-' + entity).checked) url += '&showExcluded=true';
    const data = await fetchJson(url);
    if (!data) return;

    renderHeaders(entity);
    renderTable(entity, data.items);
    renderPager(entity, data);
}

function renderHeaders(entity) {
    const thead  = document.getElementById('thead-' + entity);
    const cols   = COLUMNS[entity];
    const active = state.sort[entity];
    const dir    = state.dir[entity];
    let html = '';
    if (entity === 'boats' || entity === 'designs') html += '<th style="width:2rem"></th>';
    html += cols.map(col => {
        if (col.type === 'toggle' || col.type === 'action') return `<th>${esc(col.label)}</th>`;
        const isActive = col.key === active;
        const arrow    = isActive ? (dir === 'asc' ? ' ↑' : ' ↓') : '';
        return `<th class="sortable${isActive ? ' sort-active' : ''}"
                    onclick="sortBy('${entity}', '${col.key}')">${esc(col.label)}${arrow}</th>`;
    }).join('');
    thead.innerHTML = html;
}

function sortBy(entity, key) {
    if (state.sort[entity] === key) {
        state.dir[entity] = state.dir[entity] === 'asc' ? 'desc' : 'asc';
    } else {
        state.sort[entity] = key;
        state.dir[entity]  = 'asc';
    }
    loadList(entity, 0);
}

function renderTable(entity, items) {
    const tbody = document.getElementById('tbody-' + entity);
    tbody.innerHTML = '';
    const cols = COLUMNS[entity];

    for (const item of items) {
        const tr = document.createElement('tr');
        if (item.excluded) tr.classList.add('excluded');
        if (entity === 'boats' || entity === 'designs') {
            if (state.selected[entity].has(item.id)) tr.classList.add('selected');
            // Checkbox cell — stop propagation so clicking the checkbox doesn't also open detail
            const tdCb = document.createElement('td');
            tdCb.style.textAlign = 'center';
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.checked = state.selected[entity].has(item.id);
            cb.onclick = (e) => { e.stopPropagation(); toggleSelect(entity, item, cb.checked); };
            tdCb.appendChild(cb);
            tr.appendChild(tdCb);
        }
        tr.onclick = () => loadDetail(entity, item.id);
        cols.forEach(col => {
            const td = document.createElement('td');
            if (col.type === 'toggle') {
                td.style.textAlign = 'center';
                const ecb = document.createElement('input');
                ecb.type = 'checkbox';
                ecb.title = 'Exclude from analysis';
                ecb.checked = !!item[col.key];
                ecb.onclick = (e) => {
                    e.stopPropagation();
                    item[col.key] = ecb.checked;
                    tr.classList.toggle('excluded', ecb.checked);
                    toggleExcluded(entity, item.id, ecb.checked);
                };
                td.appendChild(ecb);
            } else if (col.type === 'action') {
                const btn = document.createElement('button');
                btn.className = 'link-btn';
                btn.textContent = col.label;
                btn.onclick = (e) => { e.stopPropagation(); col.action(item); };
                td.appendChild(btn);
            } else {
                const v = item[col.key];
                td.innerHTML = col.render ? col.render(v) : esc(v != null ? String(v) : '');
            }
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    }
}

function renderPager(entity, data) {
    const page = data.page;
    const totalPages = Math.ceil(data.total / data.size) || 1;
    let html = '';
    if (page > 0)
        html += `<button onclick="loadList('${entity}', ${page - 1})">← Prev</button>`;
    html += `<span>Page ${page + 1} of ${totalPages} (${data.total.toLocaleString()} total)</span>`;
    if ((page + 1) * data.size < data.total)
        html += `<button onclick="loadList('${entity}', ${page + 1})">Next →</button>`;

    document.getElementById('pager-' + entity).innerHTML = html;
    const topEl = document.getElementById('pager-top-' + entity);
    if (topEl) topEl.innerHTML = html;
}

async function loadDetail(entity, id) {
    const data = await fetchJson('/api/' + entity + '/' + encodeURIComponent(id));
    if (!data) return;

    const panel = document.getElementById('detail-' + entity);
    const pre   = document.getElementById('pre-' + entity);
    pre.innerHTML = renderJsonTree(data, 0);

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
        if (!f) return `<tr><td>${label}</td><td colspan="3" style="color:#999">—</td></tr>`;
        const barWidth = Math.round(f.weight * 80);
        const genLabel = f.generation === 0 ? 'cert' : `gen ${f.generation}`;
        return `<tr>
          <td>${label}</td>
          <td>${f.value.toFixed(4)}</td>
          <td>${f.weight.toFixed(3)} <span class="weight-bar" style="width:${barWidth}px"></span></td>
          <td style="color:#888;font-size:0.85em">${genLabel}</td>
        </tr>`;
    }
    return `<strong>Reference factors (IRC equivalent, ${ref.currentYear})</strong>
      <table style="width:auto;margin-top:0.4rem;">
        <thead><tr><th>Variant</th><th>Value (TCF)</th><th>Weight</th><th>Gen</th></tr></thead>
        <tbody>
          ${row('Spin',       ref.spin)}
          ${row('Non-spin',   ref.nonSpin)}
          ${row('Two-handed', ref.twoHanded)}
        </tbody>
      </table>`;
}

// ---- Selection and merge (boats and designs) ----

function toggleSelect(entity, item, checked) {
    if (checked) {
        state.selected[entity].add(item.id);
        state.selectedData[entity].set(item.id, item);
    } else {
        state.selected[entity].delete(item.id);
        state.selectedData[entity].delete(item.id);
    }
    updateMergeBar(entity);
}

function updateMergeBar(entity) {
    const n   = state.selected[entity].size;
    const bar = document.getElementById('merge-bar-' + entity);
    bar.style.display = n >= 2 ? '' : 'none';
    const noun = entity === 'boats' ? 'boat' : 'design';
    document.getElementById('merge-bar-count-' + entity).textContent =
        n + ' ' + noun + (n !== 1 ? 's' : '') + ' selected';
}

function clearSelection(entity) {
    state.selected[entity].clear();
    state.selectedData[entity].clear();
    updateMergeBar(entity);
    hideMergePanel(entity);
    document.querySelectorAll('#tbody-' + entity + ' input[type=checkbox]').forEach(cb => cb.checked = false);
    document.querySelectorAll('#tbody-' + entity + ' tr.selected').forEach(tr => tr.classList.remove('selected'));
}

function showMergePanel(entity) {
    const panel = document.getElementById('merge-panel-' + entity);
    const list  = document.getElementById('merge-radio-list-' + entity);
    document.getElementById('merge-status-' + entity).textContent = '';
    list.innerHTML = '';
    const ids = Array.from(state.selected[entity]);
    ids.forEach((id, i) => {
        const item  = state.selectedData[entity].get(id);
        const label = document.createElement('label');
        label.style.display = 'block';
        label.style.margin  = '0.25rem 0';
        const radio = document.createElement('input');
        radio.type  = 'radio';
        radio.name  = 'merge-keep-' + entity;
        radio.value = id;
        if (i === 0) radio.checked = true;
        label.appendChild(radio);
        const desc = entity === 'boats'
            ? ' ' + esc(id) + '  —  sail: ' + esc(item.sailNumber || '') + '  name: ' + esc(item.name || '')
            : ' ' + esc(id) + '  —  ' + esc(item.canonicalName || '');
        label.appendChild(document.createTextNode(desc));
        list.appendChild(label);
    });
    panel.style.display = '';
}

function hideMergePanel(entity) {
    document.getElementById('merge-panel-' + entity).style.display = 'none';
}

async function performMerge(entity) {
    const keepRadio = document.querySelector('#merge-radio-list-' + entity + ' input[name="merge-keep-' + entity + '"]:checked');
    if (!keepRadio) return;
    const keepId   = keepRadio.value;
    const mergeIds = Array.from(state.selected[entity]).filter(id => id !== keepId);
    const statusEl = document.getElementById('merge-status-' + entity);
    statusEl.textContent = 'Merging…';

    const result = await fetchJson('/api/' + entity + '/merge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keepId, mergeIds })
    });

    if (!result) {
        statusEl.textContent = 'Merge failed — see console.';
        return;
    }
    if (entity === 'boats') {
        statusEl.textContent =
            'Merged. Updated ' + result.updatedRaces + ' race(s), ' + result.updatedFinishers + ' finisher record(s).';
    } else {
        statusEl.textContent = 'Merged. Updated ' + result.updatedBoats + ' boat(s).';
    }
    clearSelection(entity);
    hideMergePanel(entity);
    loadList(entity, 0);
}

async function toggleExcluded(entity, id, excluded) {
    const resp = await fetch('/api/' + entity + '/exclude', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id, excluded })
    });
    if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        alert('Failed to update exclusion: ' + (err.error || resp.status));
    }
}

loadList('boats', 0);
