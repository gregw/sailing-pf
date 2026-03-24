const COLUMNS = {
    boats: [
        { label: 'ID',     key: 'id' },
        { label: 'Sail',   key: 'sailNumber' },
        { label: 'Name',   key: 'name' },
        { label: 'Design', key: 'designId' },
        { label: 'Club',   key: 'clubId' },
    ],
    designs: [
        { label: 'ID',     key: 'id' },
        { label: 'Name',   key: 'canonicalName' },
        { label: 'Makers', key: 'makerIds', render: v => esc((v || []).join(', ')) },
    ],
    races: [
        { label: 'ID',        key: 'id' },
        { label: 'Club',      key: 'clubId' },
        { label: 'Date',      key: 'date' },
        { label: 'Series',    key: 'seriesName' },
        { label: 'Race',      key: 'name' },
        { label: 'Finishers', key: 'finishers' },
    ],
};

const state = {
    pages:  { boats: 0, designs: 0, races: 0 },
    sort:   { boats: 'id', designs: 'id', races: 'date' },
    dir:    { boats: 'asc', designs: 'asc', races: 'desc' },
    searchTimers: {},
    activeTab: 'boats',
    selectedBoats: new Set(),   // IDs of checked boat rows
    selectedBoatData: new Map() // id → { id, sailNumber, name } for merge panel
};

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
    const q    = document.getElementById('q-' + entity).value;
    const sort = state.sort[entity];
    const dir  = state.dir[entity];
    const data = await fetchJson(
        `/api/${entity}?page=${page}&size=50&q=${encodeURIComponent(q)}&sort=${sort}&dir=${dir}`
    );
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
    if (entity === 'boats') html += '<th style="width:2rem"></th>';
    html += cols.map(col => {
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
        if (entity === 'boats') {
            if (state.selectedBoats.has(item.id)) tr.classList.add('selected');
            // Checkbox cell — stop propagation so clicking the checkbox doesn't also open detail
            const tdCb = document.createElement('td');
            tdCb.style.textAlign = 'center';
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.checked = state.selectedBoats.has(item.id);
            cb.onclick = (e) => { e.stopPropagation(); toggleBoatSelect(item, cb.checked); };
            tdCb.appendChild(cb);
            tr.appendChild(tdCb);
            tr.onclick = () => loadDetail(entity, item.id);
        } else {
            tr.onclick = () => loadDetail(entity, item.id);
        }
        cols.forEach(col => {
            const td = document.createElement('td');
            const v = item[col.key];
            td.innerHTML = col.render ? col.render(v) : esc(v != null ? String(v) : '');
            tr.appendChild(td);
        });
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
    const pre   = document.getElementById('pre-' + entity);
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
      </table>`;
}

// ---- Boat selection and merge ----

function toggleBoatSelect(item, checked) {
    if (checked) {
        state.selectedBoats.add(item.id);
        state.selectedBoatData.set(item.id, item);
    } else {
        state.selectedBoats.delete(item.id);
        state.selectedBoatData.delete(item.id);
    }
    updateMergeBar();
}

function updateMergeBar() {
    const n = state.selectedBoats.size;
    const bar = document.getElementById('merge-bar');
    bar.style.display = n >= 2 ? '' : 'none';
    document.getElementById('merge-bar-count').textContent =
        n + ' boat' + (n !== 1 ? 's' : '') + ' selected';
}

function clearSelection() {
    state.selectedBoats.clear();
    state.selectedBoatData.clear();
    updateMergeBar();
    hideMergePanel();
    // Uncheck any visible checkboxes
    document.querySelectorAll('#tbody-boats input[type=checkbox]').forEach(cb => cb.checked = false);
    document.querySelectorAll('#tbody-boats tr.selected').forEach(tr => tr.classList.remove('selected'));
}

function showMergePanel() {
    const panel = document.getElementById('merge-panel');
    const list  = document.getElementById('merge-radio-list');
    document.getElementById('merge-status').textContent = '';
    list.innerHTML = '';
    const ids = Array.from(state.selectedBoats);
    ids.forEach((id, i) => {
        const b = state.selectedBoatData.get(id);
        const label = document.createElement('label');
        label.style.display = 'block';
        label.style.margin  = '0.25rem 0';
        const radio = document.createElement('input');
        radio.type  = 'radio';
        radio.name  = 'merge-keep';
        radio.value = id;
        if (i === 0) radio.checked = true;
        label.appendChild(radio);
        label.appendChild(document.createTextNode(
            ' ' + esc(id) + '  —  sail: ' + esc(b.sailNumber || '') + '  name: ' + esc(b.name || '')
        ));
        list.appendChild(label);
    });
    panel.style.display = '';
}

function hideMergePanel() {
    document.getElementById('merge-panel').style.display = 'none';
}

async function performMerge() {
    const keepRadio = document.querySelector('#merge-radio-list input[name="merge-keep"]:checked');
    if (!keepRadio) return;
    const keepId   = keepRadio.value;
    const mergeIds = Array.from(state.selectedBoats).filter(id => id !== keepId);
    const statusEl = document.getElementById('merge-status');
    statusEl.textContent = 'Merging…';

    const result = await fetchJson('/api/boats/merge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keepId, mergeIds })
    });

    if (!result) {
        statusEl.textContent = 'Merge failed — see console.';
        return;
    }
    statusEl.textContent =
        'Merged. Updated ' + result.updatedRaces + ' race(s), ' + result.updatedFinishers + ' finisher record(s).';
    clearSelection();
    hideMergePanel();
    loadList('boats', 0);
}

loadList('boats', 0);
