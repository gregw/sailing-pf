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
        { label: 'Design', key: 'designId', anchor: 'col-boat-design', tip: 'Design (class) identifier linking this boat to a design record.' },
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
        { label: 'Ref Time',  key: 'referenceTime', anchor: 'col-race-ref-time', tip: 'T₀: elapsed time a 1.000 HPF boat would have taken. Only available after HPF optimisation.' },
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
    activeTab: 'boats',
    selected:     { boats: new Set(), designs: new Set() },   // IDs of checked rows
    selectedData: { boats: new Map(), designs: new Map() },   // id → item for merge panel
    filter: { boats: null, designs: null, clubs: null, races: null, series: null },
    raceItems:       [],   // current page's race rows for prev/next navigation
    currentRaceIdx:  -1,   // index into raceItems of the currently shown race
    boatItems:       [],   // current page's boat rows for prev/next navigation
    currentBoatIdx:  -1,   // index into boatItems of the currently shown boat
};

let currentDivRaceId = null;
let showRaceErrorBars = false;
let preferredDivision = null;

function isWriteAllowed() { return window.hpfAuth?.authenticated; }

function switchTab(entity) {
    ['boats', 'designs', 'clubs', 'races', 'series'].forEach(e => {
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
    ['show-excluded-' + entity, 'exclude-nulls-' + entity, 'exclude-empty-' + entity, 'filter-dupe-sails'].forEach(id => {
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
    document.getElementById('detail-' + entity).classList.remove('visible');
    loadList(entity, 0);
}

function setPageSize(size) {
    state.pageSize = parseInt(size);
    ['boats', 'designs', 'clubs', 'races', 'series'].forEach(e => {
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
                    toggleExcluded(entity, item.id, ecb.checked);
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
            text: entries.map(e => `${e.division}<br>w=${e.weight.toFixed(2)}<br>r=${(-e.residual).toFixed(4)}`),
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

    Plotly.newPlot(container, traces, layout, { responsive: true });
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
});

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

// ---- Race division chart ----

function setupRaceDivisionChart(raceId, raceJson) {
    currentDivRaceId = raceId;
    updateRaceNav();

    const rawDivisions = (raceJson.divisions || []);
    if (rawDivisions.length === 0) {
        document.getElementById('division-section-races').style.display = 'none';
        return;
    }
    // Map null/blank division names to "" (sentinel for API) and display as handicapSystem or "Results".
    // Deduplicate by value so multiple null-named divisions collapse to one "" option.
    const fallbackLabel = raceJson.handicapSystem || 'Results';
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

function onRaceErrorBarsChange() {
    showRaceErrorBars = document.getElementById('race-show-error-bars').checked;
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
        { x: xs, y: rfCorr,  mode: 'lines+markers', type: 'scatter', name: 'RF corrected',
          line: { dash: 'dot', color: '#c47900', width: 1.5 }, marker: { size: 7 },
          error_y: yErrArrays(finishers, 'rf', 'rfWeight'),
          text: hoverTexts('RF corrected', rfCorr), hoverinfo: 'text' }
    ];

    const layout = {
        xaxis: { title: 'HPF' },
        yaxis: { title: 'Time (min)', tickformat: '.1f', autorange: 'reversed' },
        legend: { orientation: 'h', y: -0.18 },
        margin: { t: 10, b: 80, l: 60, r: 20 },
        hovermode: 'closest'
    };

    document.getElementById('division-section-races').style.display = '';
    Plotly.react('race-division-chart', traces, layout, { responsive: true });
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

loadList('boats', 0);
