function renderJsonTree(val, depth) {
    if (val === null) return '<span class="jt-null">null</span>';
    const t = typeof val;
    if (t === 'boolean') return `<span class="jt-bool">${val}</span>`;
    if (t === 'number')  return `<span class="jt-num">${val}</span>`;
    if (t === 'string')  return `<span class="jt-str">"${esc(val)}"</span>`;
    if (Array.isArray(val)) {
        if (val.length === 0) return '<span class="jt-punct">[]</span>';
        const rows = val.map(v => `<div class="jt-row">${renderJsonTree(v, depth + 1)}</div>`).join('');
        return `<details open><summary class="jt-sum">[ ${val.length} ]</summary><div class="jt-body">${rows}</div></details>`;
    }
    if (t === 'object') {
        const keys = Object.keys(val);
        if (keys.length === 0) return '<span class="jt-punct">{}</span>';
        const preview = keys.slice(0, 4).map(k => esc(k)).join(', ') + (keys.length > 4 ? ', …' : '');
        const rows = keys.map(k =>
            `<div class="jt-row"><span class="jt-key">${esc(k)}</span>: ${renderJsonTree(val[k], depth + 1)}</div>`
        ).join('');
        return `<details open><summary class="jt-sum">{ ${preview} }</summary><div class="jt-body">${rows}</div></details>`;
    }
    return esc(String(val));
}

function weightColor(w) {
    const cw = Math.min(w ?? 0, 1);
    if (cw >= 0.5) {
        // Neutral dark gray at 0.5 → dark green at 1.0
        const t = (cw - 0.5) * 2;
        return `rgb(${Math.round(80*(1-t))},${Math.round(80+40*t)},${Math.round(80*(1-t))})`;
    } else {
        // Dark red at 0 → neutral dark gray at 0.5
        const t = cw * 2;
        return `rgb(${Math.round(180-100*t)},${Math.round(80*t)},${Math.round(80*t)})`;
    }
}

const COLUMNS = {
    boats: [
        { label: 'ID',     key: 'id',     anchor: 'col-boat-id',      tip: 'Unique boat identifier derived from sail number, name and design.' },
        { label: 'Sail',   key: 'sailNumber', anchor: 'col-boat-sail', tip: 'Sail number as recorded in source data (SailSys / TopYacht).' },
        { label: 'Name',   key: 'name',   anchor: 'col-boat-name',     tip: 'Boat name as recorded in source data.' },
        { label: 'Design', type: 'action', sortKey: 'designId', anchor: 'col-boat-design',
          tip: 'Design (class) identifier; click to search for this design in the designs tab.',
          render: item => item.designId || '',
          action: item => {
              if (!item.designId) return;
              state.searches['designs'] = item.designId;
              state.pages['designs'] = 0;
              const q = document.getElementById('q-designs');
              if (q) q.value = item.designId;
              switchTab('designs');
              loadList('designs', 0);
          } },
        { label: 'RF',     key: 'spinRef', anchor: 'col-boat-rf',
          tip: 'Reference Factor — IRC-equivalent handicap derived from certificates "standard candles" or median performance against boats with an RF. Colour: green = high confidence, red = low.',
          render: v => v && v.value != null
            ? `<span style="color:${weightColor(v.weight)}">${v.value.toFixed(4)}</span>`
            : '<span style="color:#bbb">—</span>' },
        { label: 'HPF',    key: 'hpf',    anchor: 'col-boat-hpf',
          tip: 'Historical Performance Factor — back-calculated time correction factor optimized over this boat\'s racing history. Colour: green = high confidence, red = low.',
          render: v => v && v.value != null
            ? `<span style="color:${weightColor(v.weight)}">${v.value.toFixed(4)}</span>`
            : '<span style="color:#bbb">—</span>' },
        { label: 'Overall', key: 'profile', sortKey: 'profile', anchor: 'col-boat-profile',
          tip: 'Performance profile overall score — fleet-relative percentile polygon area across Frequency, Consistency, Diversity, NonChaotic and Stability spokes (last 12 months).',
          render: v => v != null
            ? `<span style="color:${weightColor(v)}">${v.toFixed(3)}</span>`
            : '<span style="color:#bbb">—</span>' },
        { label: 'Club',     key: 'clubId', anchor: 'col-boat-club',   tip: 'Home club identifier.' },
        { label: 'Finishes', type: 'action', sortKey: 'finishes', anchor: 'col-boat-finishes',
          tip: 'Number of recorded finishes; click to view this boat\'s races.',
          render: item => item.finishes ? String(item.finishes) : '',
          action: item => { setFilter('races', 'boatId', item.id,
                            'Races for ' + (item.name || item.id)); switchTab('races'); } },
        { label: 'Excl',   key: 'excluded', type: 'toggle', anchor: 'col-boat-excl',
          tip: 'Excluded boats are hidden from analysis and charts.' },
    ],
    designs: [
        { label: 'ID',     key: 'id',           anchor: 'col-design-id',     tip: 'Unique design identifier (normalised class name).' },
        { label: 'Name',   key: 'canonicalName', anchor: 'col-design-name',   tip: 'Canonical design name used in all reports.' },
        { label: 'RF',     key: 'spinRef',       anchor: 'col-design-rf',
          tip: 'Design-level Reference Factor aggregated across all boats of this class. Colors: green = high confidence, red = low',
          render: v => v && v.value != null
            ? `<span style="color:${weightColor(v.weight)}">${v.value.toFixed(4)}</span>`
            : '<span style="color:#bbb">—</span>' },
        { label: 'RF Weight', key: 'spinRef', anchor: 'col-design-rf-weight',
          tip: 'Statistical weight of the design-level RF: higher means more race data and tighter confidence.',
          render: v => v && v.weight != null
            ? `<span style="color:${weightColor(v.weight)}">${v.weight.toFixed(1)}</span>`
            : '<span style="color:#bbb">—</span>' },
        { label: 'Boats',  type: 'action', sortKey: 'boats', anchor: 'col-design-boats',
          tip: 'Number of boats of this design; click to show these boats in the boats table.',
          render: item => item.boats ? String(item.boats) : '',
          action: item => { setFilter('boats', 'designId', item.id,
                            'Boats of design ' + (item.canonicalName || item.id)); switchTab('boats'); } },
        { label: 'Excl',   key: 'excluded', type: 'toggle', anchor: 'col-design-excl',
          tip: 'Excluded designs are hidden from analysis and charts.' },
    ],
    clubs: [
        { label: 'ID',        key: 'id',        anchor: 'col-club-id',    tip: 'Club identifier (website domain).' },
        { label: 'Short',     key: 'shortName', anchor: 'col-club-short', tip: 'Short name used in source data.' },
        { label: 'Name',      key: 'longName',  anchor: 'col-club-name',  tip: 'Full club name.' },
        { label: 'State',     key: 'state',     anchor: 'col-club-state', tip: 'Australian state or territory.' },
        { label: 'Boats',     type: 'action', sortKey: 'boats', anchor: 'col-club-boats',
          tip: 'Number of boats registered at this club; click to show these boats in the boats table.',
          render: item => item.boats != null ? String(item.boats) : '',
          action: item => { setFilter('boats', 'clubId', item.id,
                            'Boats at ' + (item.shortName || item.id)); switchTab('boats'); } },
        { label: 'Series',    type: 'action', sortKey: 'series', anchor: 'col-club-series',
          tip: 'Number of series run by this club; click to show these series in the series table.',
          render: item => item.series != null ? String(item.series) : '',
          action: item => { setFilter('series', 'clubId', item.id,
                            'Series at ' + (item.shortName || item.id)); switchTab('series'); } },
        { label: 'Races',     type: 'action', sortKey: 'races', anchor: 'col-club-races',
          tip: 'Number of races imported from this club; click to show these races in the races table.',
          render: item => item.races != null ? String(item.races) : '',
          action: item => { setFilter('races', 'clubId', item.id,
                            'Races at ' + (item.shortName || item.id)); switchTab('races'); } },
        { label: 'Excl',      key: 'excluded', type: 'toggle', anchor: 'col-club-excl',
          tip: 'Excluded clubs are hidden from analysis.' },
    ],
    series: [
        { label: 'Club',      type: 'action', sortKey: 'club', anchor: 'col-series-club',
          tip: 'Club that runs this series; click to show only series from this club.',
          render: item => item.club || item.clubId || '',
          action: item => setFilter('series', 'clubId', item.clubId, 'Series at ' + (item.club || item.clubId)) },
        { label: 'Name',      key: 'name',      anchor: 'col-series-name',    tip: 'Series name.' },
        { label: 'First',     key: 'firstDate', anchor: 'col-series-first',   tip: 'Date of the first race in this series.' },
        { label: 'Last',      key: 'lastDate',  anchor: 'col-series-last',    tip: 'Date of the last race in this series.' },
        { label: 'Races',     type: 'action', sortKey: 'races', anchor: 'col-series-races',
          tip: 'Number of races in this series; click to show these races in the races table.',
          render: item => item.races != null ? String(item.races) : '',
          action: item => { setFilter('races', 'seriesId', item.id, 'Series: ' + (item.name || item.id)); switchTab('races'); } },
        { label: 'Excl',      key: 'excluded', type: 'toggle', anchor: 'col-series-excl',
          tip: 'Excluded series — all races in this series are excluded from HPF calculations.' },
    ],
    races: [
        { label: 'ID',        key: 'id',        anchor: 'col-race-id',        tip: 'Unique race identifier: clubId–date–number.' },
        { label: 'Date',      key: 'date',      anchor: 'col-race-date',      tip: 'Race date.' },
        { label: 'Club',      key: 'clubId',    anchor: 'col-race-club',      tip: 'Club that ran this race.' },
        { label: 'Series',    type: 'action', sortKey: 'seriesName', anchor: 'col-race-series',
          tip: 'Series this race belongs to; click to show the races of this series.',
          render: item => item.seriesName || '',
          action: item => item.seriesId
            ? setFilter('races', 'seriesId', item.seriesId, 'Series: ' + (item.seriesName || item.seriesId))
            : null },
        { label: 'Race',      key: 'name',      anchor: 'col-race-name',      tip: 'Race name or number within the series.' },
        { label: 'Finishers', key: 'finishers',    anchor: 'col-race-finishers',   tip: 'Total finishers across all divisions in this race.' },
        { label: 'Excl',      key: 'excluded',  type: 'toggle', anchor: 'col-race-excl',
          tip: 'Excluded races are not used in HPF calculations.' },
    ],
};

const state = {
    pages:    { boats: 0, designs: 0, clubs: 0, races: 0, series: 0 },
    sort:     { boats: 'id', designs: 'id', clubs: 'shortName', races: 'date', series: 'firstDate' },
    dir:      { boats: 'asc', designs: 'asc', clubs: 'asc', races: 'desc', series: 'desc' },
    pageSize: 25,
    searchTimers: {},
    searches: { boats: '', designs: '', clubs: '', races: '', series: '' },  // persistent per-tab search terms
    activeTab: 'boats',
    selected:     { boats: new Set(), designs: new Set() },   // IDs of checked rows
    selectedData: { boats: new Map(), designs: new Map() },   // id → item for merge panel
    filter: { boats: null, designs: null, clubs: null, races: null, series: null },
    raceItems:       [],   // current page's race rows for prev/next navigation
    currentRaceIdx:  -1,   // index into raceItems of the currently shown race
    boatItems:       [],   // current page's boat rows for prev/next navigation
    currentBoatIdx:  -1,   // index into boatItems of the currently shown boat
    lastDetailBoat:  null, // most recently loaded boat detail (for edit panel)
};

let currentDivRaceId = null;
let showRaceErrorBars = false;
let showRaceTrendLine = false;
let showRaceRfLine    = true;
let preferredDivision = null;

function isWriteAllowed() { return window.hpfAuth?.authenticated; }

function switchTab(entity) {
    ['clubs', 'boats', 'designs', 'series', 'races'].forEach(e => {
        document.getElementById('tab-btn-' + e).classList.toggle('active', e === entity);
        document.getElementById('panel-' + e).classList.toggle('active', e === entity);
    });
    state.activeTab = entity;
    // Restore persisted search term for this tab
    const q = document.getElementById('q-' + entity);
    if (q && state.searches[entity] !== undefined) q.value = state.searches[entity];
    updateFilterBanner(entity);
    updateFilterControls(entity);
    if (document.querySelector('#tbody-' + entity + ' tr') === null) {
        loadList(entity, 0);
    }
}

function setFilter(entity, param, value, label) {
    state.filter[entity] = { param, value, label };
    state.searches[entity] = '';   // navigation filter clears any persistent search
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
    ['show-excluded-' + entity, 'exclude-nulls-' + entity, 'exclude-empty-' + entity, 'filter-dupe-sails'].forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.disabled = active;
            if (el.parentElement) el.parentElement.style.opacity = active ? '0.4' : '';
        }
    });
}

function debounceSearch(entity) {
    const q = document.getElementById('q-' + entity);
    if (q) state.searches[entity] = q.value;
    clearTimeout(state.searchTimers[entity]);
    state.searchTimers[entity] = setTimeout(() => doSearch(entity), 300);
}

function doSearch(entity) {
    state.pages[entity] = 0;
    document.getElementById('detail-' + entity).classList.remove('visible');
    loadList(entity, 0);
}

function clearSearch(entity) {
    state.searches[entity] = '';
    const q = document.getElementById('q-' + entity);
    if (q) q.value = '';
    doSearch(entity);
}

function setPageSize(size) {
    state.pageSize = parseInt(size);
    ['clubs', 'boats', 'designs', 'series', 'races'].forEach(e => {
        const el = document.getElementById('page-size-' + e);
        if (el) el.value = state.pageSize;
    });
    loadList(state.activeTab, 0);
}

async function loadList(entity, page) {
    state.pages[entity] = page;
    const q    = document.getElementById('q-' + entity).value;
    const sort = state.sort[entity];
    const dir  = state.dir[entity];
    let url = `/api/${entity}?page=${page}&size=${state.pageSize}&q=${encodeURIComponent(q)}&sort=${sort}&dir=${dir}`;
    const f = state.filter[entity];
    if (f) url += `&${f.param}=${encodeURIComponent(f.value)}`;
    if (entity === 'boats' && !f) {
        if (document.getElementById('filter-dupe-sails').checked) url += '&dupeSails=true';
    }
    const showExcludedEl = document.getElementById('show-excluded-' + entity);
    if (!f && showExcludedEl && showExcludedEl.checked) url += '&showExcluded=true';
    const excludeNullsEl = document.getElementById('exclude-nulls-' + entity);
    if (!f && excludeNullsEl && excludeNullsEl.checked) url += '&excludeNulls=true';
    const excludeEmptyEl = document.getElementById('exclude-empty-' + entity);
    if (!f && excludeEmptyEl && excludeEmptyEl.checked) url += '&excludeEmpty=true';
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
        const info = col.anchor ? infoBtn(col.anchor, col.tip || '') : '';
        if (col.type === 'toggle') return `<th>${esc(col.label)}${info}</th>`;
        const sortKey  = col.sortKey || col.key;
        if (col.type === 'action' && !col.sortKey) return `<th>${esc(col.label)}${info}</th>`;
        const isActive = sortKey === active;
        const arrow    = isActive ? (dir === 'asc' ? ' ↑' : ' ↓') : '';
        return `<th class="sortable${isActive ? ' sort-active' : ''}"
                    onclick="sortBy('${entity}', '${sortKey}')">${esc(col.label)}${arrow}${info}</th>`;
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

    if (entity === 'races') state.raceItems = items;
    if (entity === 'boats') state.boatItems = items;

    items.forEach((item, itemIdx) => {
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
        tr.onclick = () => {
            if (entity === 'races') state.currentRaceIdx = itemIdx;
            if (entity === 'boats') { state.currentBoatIdx = itemIdx; }
            loadDetail(entity, item.id);
        };
        cols.forEach(col => {
            const td = document.createElement('td');
            if (col.type === 'toggle') {
                td.style.textAlign = 'center';
                const ecb = document.createElement('input');
                ecb.type = 'checkbox';
                ecb.title = 'Exclude from analysis';
                ecb.checked = !!item[col.key];
                ecb.disabled = !isWriteAllowed();
                ecb.onclick = (e) => {
                    e.stopPropagation();
                    item[col.key] = ecb.checked;
                    tr.classList.toggle('excluded', ecb.checked);
                    toggleExcluded(entity, item.id, ecb.checked, item);
                };
                td.appendChild(ecb);
            } else if (col.type === 'action') {
                const text = col.render ? col.render(item) : col.label;
                if (text && col.action) {
                    const btn = document.createElement('button');
                    btn.className = 'link-btn';
                    btn.textContent = text;
                    btn.onclick = (e) => { e.stopPropagation(); col.action(item); };
                    td.appendChild(btn);
                } else {
                    td.textContent = text || '';
                }
            } else {
                const v = item[col.key];
                td.innerHTML = col.render ? col.render(v) : esc(v != null ? String(v) : '');
            }
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
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
    if (entity === 'series') {
        loadSeriesChart(id);
        return;
    }

    const data = await fetchJson('/api/' + entity + '/' + encodeURIComponent(id));
    if (!data) return;

    const panel = document.getElementById('detail-' + entity);
    const pre   = document.getElementById('pre-' + entity);
    pre.innerHTML = renderJsonTree(data, 0);

    if (entity === 'races') {
        setupRaceDivisionChart(id, data);
    }

    if (entity === 'boats') {
        document.getElementById('ref-factors-boats').innerHTML = '';
        const hpfDiv = document.getElementById('hpf-detail-boats');
        hpfDiv.innerHTML = '<em>Loading…</em>';
        const hpfData = await fetchJson('/api/boats/' + encodeURIComponent(id) + '/hpf');
        hpfDiv.innerHTML = hpfData ? renderBoatHpf(hpfData) : '<em>No HPF data available</em>';

        const aliasDiv = document.getElementById('aliases-boats');
        const aliases = await fetchJson('/api/boats/' + encodeURIComponent(id) + '/aliases');
        aliasDiv.innerHTML = aliases && aliases.length > 0 ? renderBoatAliases(aliases) : '';

        // Store loaded boat data for the edit panel and show the edit button
        state.lastDetailBoat = data;
        document.getElementById('edit-btn-container').style.display = '';
        hideEditPanel();

        updateBoatNav();
    }

    panel.classList.add('visible');
    if (entity === 'races') {
        document.getElementById('division-section-races').scrollIntoView({ behavior: 'smooth', block: 'start' });
    } else if (entity === 'boats') {
        const heading = document.getElementById('hpf-boat-heading');
        if (heading) window.scrollTo(0, heading.getBoundingClientRect().top + window.scrollY);
    } else {
        panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
}

function renderBoatAliases(aliases) {
    if (!aliases || aliases.length === 0) return '';
    const rows = aliases.map(a => {
        const sail = a.sailNumber ? esc(a.sailNumber) : '';
        const name = a.name ? esc(a.name) : '';
        return `<tr><td>${sail}</td><td>${name}</td></tr>`;
    }).join('');
    return `<strong>Aliases</strong>
      <table style="width:auto;margin-top:0.4rem;">
        <thead><tr><th>Sail #</th><th>Name</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
}

function renderBoatHpf(data) {
    const boatHeading = `<div id="hpf-boat-heading" style="font-size:1.05rem;font-weight:bold;margin-bottom:0.4rem;">${data.boatName ? esc(data.boatName) : ''}</div>`;

    function row(label, hpf, rf) {
        if (!hpf && !rf) return `<tr><td>${label}</td><td colspan="7" style="color:#999">—</td></tr>`;
        const rfVal    = rf  ? rf.value.toFixed(4)   : '—';
        const rfWt     = rf  ? rf.weight.toFixed(3)  : '—';
        const rfGen    = rf  && rf.generation != null ? rf.generation : '—';
        const hpfVal   = hpf ? hpf.value.toFixed(4)  : '—';
        const hpfWt    = hpf ? hpf.weight.toFixed(3) : '—';
        const delta    = hpf && hpf.referenceDelta != null
            ? (hpf.referenceDelta >= 0 ? '+' : '') + hpf.referenceDelta.toFixed(4) : '—';
        const races    = hpf ? hpf.raceCount : '—';
        return `<tr>
          <td>${label}</td>
          <td>${rfVal}</td>
          <td>${rfWt}</td>
          <td>${rfGen}</td>
          <td>${hpfVal}</td>
          <td>${hpfWt}</td>
          <td>${delta}</td>
          <td>${races}</td>
        </tr>`;
    }

    let html = boatHeading + `<strong>Performance factors (${data.currentYear})</strong>
      <table style="width:auto;margin-top:0.4rem;">
        <thead><tr>
          <th>Variant${infoBtn('col-hpf-variant','Spin, Non-Spin, or Two-Handed handicap variant.')}</th>
          <th>RF${infoBtn('col-hpf-rf','Reference Factor — IRC-equivalent handicap derived from certificates.')}</th>
          <th>RF Wt${infoBtn('col-ref-weight','RF confidence weight: 1.0 = direct certificate, lower = inferred or multi-hop conversion.')}</th>
          <th>Gen${infoBtn('col-rf-gen','RF generation — the pipeline step that assigned this factor. Lower = earlier/more-direct; higher = later propagation or cross-variant fill.')}</th>
          <th>HPF${infoBtn('col-hpf-value','Historical Performance Factor — back-calculated handicap averaged across this boat\'s race history.')}</th>
          <th>HPF Wt${infoBtn('col-hpf-weight','HPF confidence weight — proportional to number of informative races.')}</th>
          <th>Delta${infoBtn('col-hpf-delta','HPF minus RF. Near zero = race history is consistent with the certificate.')}</th>
          <th>Races${infoBtn('col-hpf-races','Number of races contributing to this HPF estimate.')}</th>
        </tr></thead>
        <tbody>
          ${row('Spin',       data.spin,      data.rfSpin)}
          ${row('Non-spin',   data.nonSpin,   data.rfNonSpin)}
          ${row('Two-handed', data.twoHanded, data.rfTwoHanded)}
        </tbody>
      </table>`;

    if (data.profile) {
        const score = data.profile.overallScore != null ? data.profile.overallScore.toFixed(3) : '—';
        html += `<div style="margin-top:0.75rem;font-weight:bold;font-size:0.9rem;">${data.boatName ? esc(data.boatName) + ' — ' : ''}Performance Profile ${infoBtn('chart-profile','Radar chart: five fleet-relative percentile scores based on the last 12 months. Frequency: how often the boat races. Consistency: how tight the residuals are. Diversity: distinct opponents raced. NonChaotic: whether inconsistency correlates with fleet-wide conditions. Stability: flatness of trend (level=best, declining=worst).')}</div>`;
        html += `<div style="display:inline-block;vertical-align:top;text-align:left;">`;
        html += `  <div id="hpf-profile-chart"></div>`;
        html += `  <div style="text-align:center;font-size:0.85rem;color:#555;margin-top:0.1rem;">Overall: ${score}</div>`;
        html += `</div>`;
        setTimeout(() => renderProfileChart(data.profile), 0);
    }
    html += `<div class="division-nav" id="boat-nav" style="margin:0.75rem 0;">
      <button id="boat-prev-btn" onclick="prevBoat()" disabled>&#8592; Prev</button>
      <span id="boat-nav-label" style="flex:1;text-align:center;font-weight:bold;font-size:1rem;"></span>
      <button id="boat-next-btn" onclick="nextBoat()" disabled>Next &#8594;</button>
    </div>`;

    if (data.residuals && data.residuals.length > 0) {
        html += `<div style="margin-top:0.75rem;font-weight:bold;font-size:0.9rem;">Per-race residuals ${infoBtn('chart-residuals','Scatter plot of back-calculated factor per race over time. Each point is one race division; colour intensity reflects the entry weight used in the HPF optimiser. Points close to zero indicate the boat raced close to its HPF.')}</div>`;
        html += `<label style="font-size:0.85rem;font-weight:normal;"><input type="checkbox" id="residual-last12" onchange="window._residualLast12=this.checked; renderResidualChart(window._lastResiduals)"> Last 12 months only</label>`;
        html += '<div id="hpf-residual-chart"></div>';
        setTimeout(() => {
            // Restore checkbox state across prev/next navigation
            const cb = document.getElementById('residual-last12');
            if (cb && window._residualLast12) cb.checked = true;
            window._lastResiduals = data.residuals;
            renderResidualChart(data.residuals);
        }, 0);
    }
    return html;
}

function renderResidualChart(residuals) {
    const container = document.getElementById('hpf-residual-chart');
    if (!container || typeof Plotly === 'undefined') return;

    const cb = document.getElementById('residual-last12');
    if (cb && cb.checked) {
        const cutoff = new Date();
        cutoff.setFullYear(cutoff.getFullYear() - 1);
        const cutoffStr = cutoff.toISOString().slice(0, 10);
        residuals = residuals.filter(r => r.date >= cutoffStr);
    }

    const spin = residuals.filter(r => !r.nonSpinnaker);
    const nonSpin = residuals.filter(r => r.nonSpinnaker);

    // Negate residuals so faster/better results plot above the zero line.
    // Raw residual = log(elapsed) + log(HPF) - log(T_div); positive means slower than reference.
    function makeTrace(entries, name, baseColor) {
        return {
            x: entries.map(e => e.date),
            y: entries.map(e => -e.residual),
            mode: 'markers',
            type: 'scatter',
            name: name,
            marker: {
                color: entries.map(e => {
                    const a = Math.max(0.6, Math.min(1.0, e.weight));
                    return baseColor.replace('1)', a + ')');
                }),
                size: 8
            },
            text: entries.map(e => {
                const parts = [];
                if (e.seriesName) parts.push(esc(e.seriesName));
                if (e.raceName)   parts.push(esc(e.raceName));
                parts.push(e.division || '—');
                parts.push(`w=${e.weight.toFixed(2)}  r=${(-e.residual).toFixed(4)}`);
                return parts.join('<br>');
            }),
            hoverinfo: 'text+x'
        };
    }

    const traces = [];
    if (spin.length > 0) traces.push(makeTrace(spin, 'Spin', 'rgba(0,100,255,1)'));
    if (nonSpin.length > 0) traces.push(makeTrace(nonSpin, 'Non-spin', 'rgba(255,75,0,1)'));

    // Use a fixed ±0.2 axis so graphs are comparable across boats.
    // If any value falls outside that range, double the height and let Plotly auto-scale.
    const allY = residuals.map(e => -e.residual);
    const maxAbs = allY.length > 0 ? Math.max(...allY.map(v => Math.abs(v))) : 0;
    const overflow = maxAbs > 0.2;

    const layout = {
        title: 'Per-race residuals — faster above zero',
        xaxis: { title: 'Race date' },
        yaxis: {
            title: 'Performance (+ = faster)',
            zeroline: true,
            ...(overflow ? {} : { range: [-0.2, 0.2] })
        },
        shapes: [{ type: 'line', x0: 0, x1: 1, xref: 'paper', y0: 0, y1: 0, line: { color: '#888', width: 1, dash: 'dash' } }],
        height: overflow ? 600 : 300,
        margin: { t: 40, b: 50, l: 60, r: 20 }
    };

    Plotly.newPlot(container, traces, layout, { responsive: true })
        .then(() => Plotly.Plots.resize(container));
}

function renderProfileChart(profile) {
    const container = document.getElementById('hpf-profile-chart');
    if (!container || typeof Plotly === 'undefined') return;

    // Spoke order matches polygon area calculation: Frequency, Consistency, Diversity, NonChaotic, Stability
    const labels = ['Frequency', 'Consistency', 'Diversity', 'NonChaotic', 'Stability'];
    const keys   = ['frequency', 'consistency', 'diversity', 'nonChaotic', 'stability'];
    const values = keys.map(k => profile[k] ?? 0);

    // Close the polygon
    const theta = [...labels, labels[0]];
    const r     = [...values, values[0]];

    const trace = {
        type: 'scatterpolar',
        r,
        theta,
        fill: 'toself',
        fillcolor: 'rgba(31,119,180,0.15)',
        line: { color: 'rgba(31,119,180,0.8)', width: 2 },
        hovertemplate: '%{theta}: %{r:.2f}<extra></extra>'
    };

    const layout = {
        polar: {
            radialaxis: { visible: true, range: [0, 1], tickvals: [0.25, 0.5, 0.75, 1.0] },
            angularaxis: { direction: 'clockwise' }
        },
        showlegend: false,
        width: 360,
        height: 320,
        margin: { t: 30, b: 30, l: 70, r: 70 }
    };

    Plotly.newPlot(container, [trace], layout, { responsive: false });
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

document.addEventListener('hpf:authready', () => {
    loadList(state.activeTab, state.pages[state.activeTab]);
    applyMergeAuthState();
});

let requestEmail = '';

function syncRequestEmail(value) {
    requestEmail = value;
    // Keep all email inputs in sync
    ['merge-email-boats', 'merge-email-designs', 'edit-email'].forEach(id => {
        const el = document.getElementById(id);
        if (el && el !== document.activeElement) el.value = value;
    });
}

function applyMergeAuthState() {
    const w = isWriteAllowed();
    ['boats', 'designs'].forEach(entity => {
        const mergeBtn = document.getElementById('merge-btn-' + entity);
        const reqBtn   = document.getElementById('merge-request-btn-' + entity);
        const confirmBtn = document.getElementById('merge-confirm-' + entity);
        const reqConfirmBtn = document.getElementById('merge-request-confirm-' + entity);
        const emailRow = document.getElementById('merge-email-row-' + entity);
        if (mergeBtn)       mergeBtn.style.display      = w ? '' : 'none';
        if (reqBtn)         reqBtn.style.display         = w ? 'none' : '';
        if (confirmBtn)     confirmBtn.style.display     = w ? '' : 'none';
        if (reqConfirmBtn)  reqConfirmBtn.style.display  = w ? 'none' : '';
        if (emailRow)       emailRow.style.display       = w ? 'none' : '';
        const msgRow = document.getElementById('merge-message-row-' + entity);
        if (msgRow)         msgRow.style.display         = w ? 'none' : '';
    });
    const editSaveBtn  = document.getElementById('edit-save-btn');
    const editReqBtn   = document.getElementById('edit-request-btn');
    const editEmailRow = document.getElementById('edit-email-row');
    const editMsgRow   = document.getElementById('edit-message-row');
    if (editSaveBtn)  editSaveBtn.style.display  = w ? '' : 'none';
    if (editReqBtn)   editReqBtn.style.display   = w ? 'none' : '';
    if (editEmailRow) editEmailRow.style.display = w ? 'none' : '';
    if (editMsgRow)   editMsgRow.style.display   = w ? 'none' : '';
    // Pre-populate email fields with remembered value
    if (!w) {
        ['merge-email-boats', 'merge-email-designs', 'edit-email'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.value = requestEmail;
        });
    }
}

async function performMerge(entity) {
    if (!isWriteAllowed()) return;
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

async function toggleExcluded(entity, id, excluded, item) {
    const body = entity === 'series'
        ? { name: item.name, excluded }
        : { id, excluded };
    const resp = await fetch('/api/' + entity + '/exclude', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });
    if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        alert('Failed to update exclusion: ' + (err.error || resp.status));
    }
}

// ---- Merge request (read-only users) ----

async function requestMerge(entity) {
    const keepRadio = document.querySelector('#merge-radio-list-' + entity + ' input[name="merge-keep-' + entity + '"]:checked');
    if (!keepRadio) return;
    const keepId   = keepRadio.value;
    const mergeIds = Array.from(state.selected[entity]).filter(id => id !== keepId);
    const statusEl = document.getElementById('merge-status-' + entity);
    statusEl.textContent = 'Submitting request…';

    const email = document.getElementById('merge-email-' + entity)?.value.trim() || '';
    const message = document.getElementById('merge-message-' + entity)?.value.trim() || '';
    const result = await fetchJson('/api/' + entity + '/merge-request', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keepId, mergeIds, ...(email && { email }), ...(message && { message }) })
    });

    if (result && result.ok) {
        clearSelection(entity);
    } else {
        statusEl.textContent = 'Failed to record request — see console.';
    }
}

// ---- Edit boat ----

let editingBoatId = null;
let editChoicesLoaded = false;

async function loadEditChoices() {
    if (editChoicesLoaded) return;
    editChoicesLoaded = true;

    const [designsResp, clubsResp] = await Promise.all([
        fetchJson('/api/designs?size=9999&sort=canonicalName&dir=asc'),
        fetchJson('/api/clubs?size=9999&sort=shortName&dir=asc')
    ]);

    const designSel = document.getElementById('edit-boat-design');
    if (designsResp && designsResp.items) {
        const opts = designsResp.items.map(d => {
            const o = document.createElement('option');
            o.value = d.id;
            o.textContent = d.canonicalName || d.id;
            return o;
        });
        // Preserve current selection while replacing options
        const cur = designSel.value;
        designSel.replaceChildren(...opts);
        designSel.value = cur;
    }

    const clubSel = document.getElementById('edit-boat-club');
    if (clubsResp && clubsResp.items) {
        const opts = clubsResp.items.map(c => {
            const o = document.createElement('option');
            o.value = c.id;
            o.textContent = c.shortName ? `${c.shortName} — ${c.id}` : c.id;
            return o;
        });
        const cur = clubSel.value;
        clubSel.replaceChildren(...opts);
        clubSel.value = cur;
    }
}

function showEditPanel() {
    const item = state.lastDetailBoat;
    if (!item) return;
    editingBoatId = item.id;
    document.getElementById('edit-boat-sail').value = item.sailNumber || '';
    document.getElementById('edit-boat-name').value = item.name || '';
    // Populate selects before setting value so the option exists
    loadEditChoices().then(() => {
        document.getElementById('edit-boat-design').value = item.designId || '';
        document.getElementById('edit-boat-club').value = item.clubId || '';
    });
    document.getElementById('edit-boat-design').value = item.designId || '';
    document.getElementById('edit-boat-club').value = item.clubId || '';
    document.getElementById('edit-status-boats').textContent = '';
    document.getElementById('edit-btn-container').style.display = 'none';
    document.getElementById('edit-panel-boats').style.display = '';
    applyMergeAuthState();
}

function hideEditPanel() {
    document.getElementById('edit-panel-boats').style.display = 'none';
    document.getElementById('edit-btn-container').style.display = '';
    editingBoatId = null;
}

async function saveBoatEdit() {
    if (!isWriteAllowed() || !editingBoatId) return;
    const statusEl = document.getElementById('edit-status-boats');
    statusEl.textContent = 'Saving…';

    const body = {
        boatId: editingBoatId,
        sailNumber: document.getElementById('edit-boat-sail').value.trim(),
        name: document.getElementById('edit-boat-name').value.trim(),
        designId: document.getElementById('edit-boat-design').value.trim(),
        clubId: document.getElementById('edit-boat-club').value.trim()
    };

    const result = await fetchJson('/api/boats/edit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });

    if (!result || !result.ok) {
        statusEl.textContent = 'Save failed: ' + ((result && result.error) || 'see console');
        return;
    }

    let msg = 'Saved.';
    if (result.idChanged) msg += ' ID changed to ' + result.newBoatId + '.';
    if (result.updatedRaces > 0) msg += ' Updated ' + result.updatedRaces + ' race(s).';
    statusEl.textContent = msg;
    hideEditPanel();
    loadList('boats', state.pages.boats);
    if (result.newBoatId) loadDetail('boats', result.newBoatId);
}

async function requestBoatEdit() {
    if (!editingBoatId) return;
    const statusEl = document.getElementById('edit-status-boats');
    statusEl.textContent = 'Submitting request…';

    const email = document.getElementById('edit-email')?.value.trim() || '';
    const message = document.getElementById('edit-message')?.value.trim() || '';
    const body = {
        boatId: editingBoatId,
        sailNumber: document.getElementById('edit-boat-sail').value.trim(),
        name: document.getElementById('edit-boat-name').value.trim(),
        designId: document.getElementById('edit-boat-design').value.trim(),
        clubId: document.getElementById('edit-boat-club').value.trim(),
        ...(email && { email }),
        ...(message && { message })
    };

    const result = await fetchJson('/api/boats/edit-request', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });

    if (result && result.ok) {
        hideEditPanel();
    } else {
        statusEl.textContent = 'Failed to record request — see console.';
    }
}

// ---- Race division chart ----

function setupRaceDivisionChart(raceId, raceJson) {
    currentDivRaceId = raceId;
    updateRaceNav();

    const rawDivisions = (raceJson.divisions || []);
    if (rawDivisions.length === 0) {
        document.getElementById('division-section-races').style.display = 'none';
        return;
    }
    // Map null/blank division names to "" (sentinel for API) and display as "Results".
    // Deduplicate by value so multiple null-named divisions collapse to one "" option.
    const fallbackLabel = 'Results';
    const seen = new Set();
    const divisions = [];
    for (const d of rawDivisions) {
        const value = (d.name != null && d.name !== '') ? d.name : '';
        if (seen.has(value)) continue;
        seen.add(value);
        divisions.push({ value, label: value !== '' ? value : fallbackLabel });
    }
    const select = document.getElementById('race-division-select');
    select.innerHTML = '';
    divisions.forEach(({ value, label }) => {
        const opt = document.createElement('option');
        opt.value = value;
        opt.textContent = label;
        select.appendChild(opt);
    });

    const parts = [];
    if (raceJson.clubId)     parts.push(raceJson.clubId);
    if (raceJson.date)       parts.push(raceJson.date);
    if (raceJson.name)       parts.push(raceJson.name);
    document.getElementById('race-div-title').textContent = parts.join(' — ');

    const raceItem = state.currentRaceIdx >= 0 ? state.raceItems[state.currentRaceIdx] : null;
    const seriesName = raceItem?.seriesName || '';
    const raceName   = raceJson.name || '';
    const labelParts = [];
    if (seriesName) labelParts.push(seriesName);
    if (raceName)   labelParts.push(raceName);
    document.getElementById('race-series-race-label').textContent = labelParts.join(' — ');

    const preferred = preferredDivision && divisions.some(d => d.value === preferredDivision)
        ? preferredDivision : divisions[0].value;
    preferredDivision = null;
    select.value = preferred;
    loadRaceDivChart(raceId, preferred);
}

function updateRaceNav() {
    document.getElementById('race-prev-btn').disabled = state.currentRaceIdx <= 0;
    document.getElementById('race-next-btn').disabled =
        state.currentRaceIdx < 0 || state.currentRaceIdx >= state.raceItems.length - 1;
}

function prevRace() {
    if (state.currentRaceIdx > 0) {
        preferredDivision = document.getElementById('race-division-select').value || null;
        state.currentRaceIdx--;
        loadDetail('races', state.raceItems[state.currentRaceIdx].id);
    }
}

function nextRace() {
    if (state.currentRaceIdx < state.raceItems.length - 1) {
        preferredDivision = document.getElementById('race-division-select').value || null;
        state.currentRaceIdx++;
        loadDetail('races', state.raceItems[state.currentRaceIdx].id);
    }
}

function updateBoatNav() {
    const idx = state.currentBoatIdx;
    const n   = state.boatItems.length;
    const prevBtn = document.getElementById('boat-prev-btn');
    const nextBtn = document.getElementById('boat-next-btn');
    const label   = document.getElementById('boat-nav-label');
    if (!prevBtn) return;
    prevBtn.disabled = idx <= 0;
    nextBtn.disabled = idx < 0 || idx >= n - 1;
    if (idx >= 0 && idx < n) {
        const boat = state.boatItems[idx];
        label.textContent = `${boat.name || boat.id}  (${idx + 1} / ${n})`;
    }
}

function prevBoat() {
    if (state.currentBoatIdx > 0) {
        state.currentBoatIdx--;
        loadDetail('boats', state.boatItems[state.currentBoatIdx].id);
    }
}

function nextBoat() {
    if (state.currentBoatIdx < state.boatItems.length - 1) {
        state.currentBoatIdx++;
        loadDetail('boats', state.boatItems[state.currentBoatIdx].id);
    }
}

function onRaceDivisionChange() {
    if (!currentDivRaceId) return;
    loadRaceDivChart(currentDivRaceId, document.getElementById('race-division-select').value);
}

function onRaceRfChange() {
    showRaceRfLine = document.getElementById('race-show-rf').checked;
    onRaceDivisionChange();
}

function onRaceErrorBarsChange() {
    showRaceErrorBars = document.getElementById('race-show-error-bars').checked;
    onRaceDivisionChange();
}

function onRaceTrendChange() {
    showRaceTrendLine = document.getElementById('race-show-trend').checked;
    onRaceDivisionChange();
}

async function loadRaceDivChart(raceId, divisionName) {
    if (!raceId || divisionName == null) return;
    const params = new URLSearchParams({ raceId, divisionName });
    const data = await fetchJson('/api/comparison/division?' + params);
    if (!data || !data.finishers || data.finishers.length === 0) {
        document.getElementById('division-section-races').style.display = '';
        Plotly.purge('race-division-chart');
        return;
    }
    const VARIANT_LABELS = { spin: 'Spin', nonSpin: 'Non-Spin', twoHanded: 'Two-Handed', mixed: 'MIXED' };
    const labelEl = document.getElementById('race-variant-label');
    if (labelEl) labelEl.textContent = 'Variant: ' + (VARIANT_LABELS[data.divisionVariant] ?? data.divisionVariant ?? '');

    // Show note about excluded finishers
    const plotted = data.finishers.filter(f => f.hpf != null && f.elapsed > 0).length;
    const noteEl = document.getElementById('race-division-note');
    if (noteEl) {
        const total = data.totalFinishers ?? data.finishers.length;
        if (plotted < total) {
            const apiExcluded = total - data.finishers.length;
            const noHpf = data.finishers.length - plotted;
            let parts = [];
            if (apiExcluded > 0) parts.push(apiExcluded + ' no data');
            if (noHpf > 0) parts.push(noHpf + ' no HPF');
            noteEl.textContent = 'Showing ' + plotted + ' of ' + total + ' finishers (' + parts.join(', ') + ')';
        } else {
            noteEl.textContent = '';
        }
    }

    renderDivisionChart(data);
}

function renderDivisionChart(data) {
    const finishers = data.finishers.filter(f => f.hpf != null && f.elapsed > 0);
    if (finishers.length === 0) return;

    const xs      = finishers.map(f => f.hpf);
    const names   = finishers.map(f => f.sailNumber ? `${f.sailNumber} ${f.name}` : f.name);
    const elapsed = finishers.map(f => f.elapsed / 60);
    const hpfCorr = finishers.map(f => f.hpfCorrected != null ? f.hpfCorrected / 60 : null);
    const rfCorr  = finishers.map(f => f.rfCorrected  != null ? f.rfCorrected  / 60 : null);

    // Vertical error bars on corrected times: factor uncertainty propagates multiplicatively
    // to the corrected time — e.g. HPF_upper_time = elapsed * hpf_upper / 60
    function yErrArrays(finishers, factorKey, weightKey) {
        const plus  = finishers.map(f => {
            if (!showRaceErrorBars || !f[weightKey] || !f[factorKey]) return 0;
            const b = errorBounds(f[factorKey], f[weightKey]);
            return b ? f.elapsed / 60 * (b.upper - f[factorKey]) : 0;
        });
        const minus = finishers.map(f => {
            if (!showRaceErrorBars || !f[weightKey] || !f[factorKey]) return 0;
            const b = errorBounds(f[factorKey], f[weightKey]);
            return b ? f.elapsed / 60 * (f[factorKey] - b.lower) : 0;
        });
        return { type: 'data', array: plus, arrayminus: minus,
                 visible: showRaceErrorBars, thickness: 1.5, width: 4 };
    }

    function hoverTexts(label, times) {
        return times.map((t, i) =>
            t != null ? `${esc(names[i])}<br>${label}: ${fmtTime(t * 60)}` : '');
    }

    const traces = [
        { x: xs, y: elapsed, mode: 'lines+markers', type: 'scatter', name: 'Elapsed',
          line: { dash: 'dash', color: '#555', width: 1.5 }, marker: { size: 7 },
          text: hoverTexts('Elapsed', elapsed), hoverinfo: 'text' },
        { x: xs, y: hpfCorr, mode: 'lines+markers', type: 'scatter', name: 'HPF corrected',
          line: { dash: 'solid', color: '#2255aa', width: 2 }, marker: { size: 7 },
          error_y: yErrArrays(finishers, 'hpf', 'rfWeight'),
          text: hoverTexts('HPF corrected', hpfCorr), hoverinfo: 'text' },
        ...(showRaceRfLine ? [{ x: xs, y: rfCorr, mode: 'lines+markers', type: 'scatter', name: 'RF corrected',
          line: { dash: 'dot', color: '#c47900', width: 1.5 }, marker: { size: 7 },
          error_y: yErrArrays(finishers, 'rf', 'rfWeight'),
          text: hoverTexts('RF corrected', rfCorr), hoverinfo: 'text' }] : [])
    ];

    if (showRaceTrendLine) {
        // Linear regression of hpfCorrected times vs HPF, excluding nulls
        const pts = finishers.map((f, i) => ({ x: xs[i], y: hpfCorr[i] }))
                             .filter(p => p.y != null);
        if (pts.length >= 2) {
            const n   = pts.length;
            const sx  = pts.reduce((s, p) => s + p.x, 0);
            const sy  = pts.reduce((s, p) => s + p.y, 0);
            const sxx = pts.reduce((s, p) => s + p.x * p.x, 0);
            const sxy = pts.reduce((s, p) => s + p.x * p.y, 0);
            const denom = n * sxx - sx * sx;
            if (denom !== 0) {
                const slope     = (n * sxy - sx * sy) / denom;
                const intercept = (sy - slope * sx) / n;
                const xMin = Math.min(...pts.map(p => p.x));
                const xMax = Math.max(...pts.map(p => p.x));
                traces.push({
                    x: [xMin, xMax],
                    y: [slope * xMin + intercept, slope * xMax + intercept],
                    mode: 'lines', type: 'scatter', name: 'HPF corr trend',
                    line: { dash: 'dashdot', color: '#2255aa', width: 2 },
                    hoverinfo: 'skip'
                });
            }
        }
    }

    // Boat name labels: vertical text at each finisher's lowest time point
    // (lowest time = topmost on chart since y-axis is reversed)
    const annotations = finishers.map((f, i) => {
        const ys = [elapsed[i], hpfCorr[i], rfCorr[i]].filter(v => v != null);
        const minY = Math.min(...ys);
        return {
            x: xs[i], y: minY,
            text: f.name,
            textangle: -90,
            xanchor: 'bottom',
            yanchor: 'bottom',
            yshift: 6,
            showarrow: false,
            cliponaxis: false,
            font: { size: 11 }
        };
    });

    const layout = {
        xaxis: { title: 'HPF' },
        yaxis: { title: 'Time (min)', tickformat: '.1f', autorange: 'reversed' },
        legend: { orientation: 'h', y: -0.18 },
        margin: { t: 120, b: 80, l: 60, r: 20 },
        hovermode: 'closest',
        annotations
    };

    document.getElementById('division-section-races').style.display = '';
    Plotly.react('race-division-chart', traces, layout, { responsive: true });

    // Compare button: include all finishers with a boatId (not just HPF-filtered ones)
    const compareBoats = (data.finishers || [])
        .filter(f => f.boatId)
        .map(f => ({ id: f.boatId, label: f.sailNumber ? `${f.sailNumber} ${f.name}` : f.name }));
    addCompareButton('race-compare-btn-container', compareBoats);
}

// ---- Compare button ----

const COMPARE_COLORS = [
    '#3a7ec4', '#e67e22', '#27ae60', '#8e44ad', '#c0392b',
    '#16a085', '#d35400', '#2c3e50', '#f39c12', '#1abc9c'
];

function addCompareButton(containerId, boats) {
    // boats: array of {id, label}
    const container = document.getElementById(containerId);
    if (!container) return;
    if (boats.length === 0) { container.innerHTML = ''; return; }
    container.innerHTML = '';
    const btn = document.createElement('button');
    btn.textContent = `Compare ${boats.length} boats\u2026`;
    btn.style.marginTop = '0.5rem';
    btn.onclick = () => {
        const items = boats.map((b, i) => ({
            type: 'boat',
            id: b.id,
            label: b.label,
            color: COMPARE_COLORS[i % COMPARE_COLORS.length]
        }));
        sessionStorage.setItem('hpf-comparison-items', JSON.stringify(items));
        window.location.href = '/comparison.html';
    };
    container.appendChild(btn);
}

// ---- Series chart ----

let seriesChartData = null;  // last loaded series chart response

async function loadSeriesChart(seriesId) {
    const section = document.getElementById('series-chart-section');
    const label = document.getElementById('series-chart-label');
    label.textContent = 'Loading series chart…';
    section.style.display = '';

    const data = await fetchJson('/api/series/chart?seriesId=' + encodeURIComponent(seriesId));
    if (!data || !data.races || data.races.length === 0) {
        label.textContent = 'No chart data available for this series.';
        document.getElementById('series-division-select').innerHTML = '';
        Plotly.purge('series-chart');
        return;
    }

    seriesChartData = data;
    label.textContent = (data.seriesName || seriesId) + ' — ' + data.club;

    // Collect unique division names across all races, preserving first-seen order
    const divNameSet = new Set();
    data.races.forEach(r => r.divisions.forEach(d => divNameSet.add(d.name || '')));
    const divNames = [...divNameSet];

    const sel = document.getElementById('series-division-select');
    sel.innerHTML = divNames.map(n => `<option value="${esc(n)}">${esc(n || '—')}</option>`).join('');

    renderSeriesChartForDivision(divNames[0] || '');
    section.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function onSeriesDivisionChange() {
    const divName = document.getElementById('series-division-select').value;
    renderSeriesChartForDivision(divName);
}

function renderSeriesChartForDivision(divName) {
    const data = seriesChartData;
    if (!data) return;

    // Colour palette for races
    const raceColors = [
        '#2255aa', '#c47900', '#2ca02c', '#d62728', '#9467bd',
        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
        '#ff7f0e', '#1f77b4', '#aec7e8', '#ffbb78', '#98df8a'
    ];

    const podiumSymbols = ['star', 'diamond', 'triangle-up'];
    const podiumSizes   = [14, 12, 11];
    const podiumLabels  = ['1st', '2nd', '3rd'];

    const traces = [];
    data.races.forEach((race, raceIdx) => {
        const color = raceColors[raceIdx % raceColors.length];
        const raceLabel = race.raceName || race.date || race.raceId;

        const div = race.divisions.find(d => (d.name || '') === divName);
        if (!div) return;

        const finishers = div.finishers.filter(f => f.hpf != null && f.hpfCorrected != null);
        if (finishers.length === 0) return;

        // Find the indices of the 3 fastest HPF-corrected times
        const sorted = finishers.map((f, i) => ({ i, t: f.hpfCorrected }))
            .sort((a, b) => a.t - b.t);
        const podiumSet = new Set(sorted.slice(0, 3).map(s => s.i));

        const xs = finishers.map(f => f.hpf);
        const ys = finishers.map(f => f.hpfCorrected / 60);
        const texts = finishers.map(f =>
            `${f.sailNumber ? f.sailNumber + ' ' : ''}${esc(f.name || '')}<br>${esc(raceLabel)}<br>HPF corrected: ${fmtTime(f.hpfCorrected)}`
        );

        traces.push({
            x: xs, y: ys,
            mode: 'lines+markers', type: 'scatter',
            name: raceLabel,
            line: { dash: 'solid', color: color, width: 1.5 },
            marker: { size: 5 },
            text: texts,
            hoverinfo: 'text'
        });

        // Add podium markers (1st/2nd/3rd fastest corrected times)
        for (let p = 0; p < Math.min(3, sorted.length); p++) {
            const f = finishers[sorted[p].i];
            traces.push({
                x: [f.hpf], y: [f.hpfCorrected / 60],
                mode: 'markers', type: 'scatter',
                name: podiumLabels[p],
                legendgroup: podiumLabels[p],
                showlegend: raceIdx === 0,
                marker: {
                    symbol: podiumSymbols[p], size: podiumSizes[p],
                    color: color,
                    line: { color: '#fff', width: 1.5 }
                },
                text: [`${podiumLabels[p]}: ${f.sailNumber ? f.sailNumber + ' ' : ''}${esc(f.name || '')}<br>${esc(raceLabel)}<br>HPF corrected: ${fmtTime(f.hpfCorrected)}`],
                hoverinfo: 'text'
            });
        }
    });

    if (traces.length === 0) {
        Plotly.purge('series-chart');
        return;
    }

    const layout = {
        xaxis: { title: 'HPF' },
        yaxis: { title: 'HPF Corrected Time (min)', tickformat: '.1f', autorange: 'reversed' },
        legend: { orientation: 'h', y: -0.25 },
        margin: { t: 30, b: 80, l: 60, r: 20 },
        hovermode: 'closest'
    };

    Plotly.react('series-chart', traces, layout, { responsive: true });

    // Compare button: unique boats across all races in this division
    const seenBoats = new Map();
    data.races.forEach(race => {
        const div = race.divisions.find(d => (d.name || '') === divName);
        if (!div) return;
        div.finishers.forEach(f => {
            if (f.boatId && !seenBoats.has(f.boatId))
                seenBoats.set(f.boatId, f.sailNumber ? `${f.sailNumber} ${f.name}` : f.name);
        });
    });
    addCompareButton('series-compare-btn-container',
        [...seenBoats.entries()].map(([id, label]) => ({ id, label })));
}

// ---- URL param handling (navigation from comparison page) ----

(function applyUrlParams() {
    const p = new URLSearchParams(window.location.search);
    const tab      = p.get('tab');
    const seriesId = p.get('seriesId');
    const raceId   = p.get('raceId');
    if (tab) {
        if (seriesId) {
            state.filter['races'] = { param: 'seriesId', value: seriesId, label: 'Series: ' + seriesId };
            state.pages['races'] = 0;
        }
        switchTab(tab);
        if (!seriesId && raceId) {
            loadList('races', 0).then(() => loadDetail('races', raceId));
        }
    }
})();

loadList('clubs', 0);
