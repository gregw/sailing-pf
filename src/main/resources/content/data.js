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
        // Medium gray at 0.5 → bright green at 1.0
        const t = (cw - 0.5) * 2;
        return `rgb(${Math.round(120*(1-t))},${Math.round(120+40*t)},${Math.round(120*(1-t))})`;
    } else {
        // Bright red at 0 → medium gray at 0.5
        const t = cw * 2;
        return `rgb(${Math.round(220-100*t)},${Math.round(30+90*t)},${Math.round(30+90*t)})`;
    }
}

function weightLabel(w) {
    if (w == null) return 'No data';
    if (w >= 0.85) return `Weight: ${w.toFixed(2)} — high confidence`;
    if (w >= 0.6)  return `Weight: ${w.toFixed(2)} — moderate confidence`;
    if (w >= 0.35) return `Weight: ${w.toFixed(2)} — low confidence`;
    return `Weight: ${w.toFixed(2)} — very low confidence`;
}

function weightSpan(value, formattedValue, w) {
    return `<span style="color:${weightColor(w)}" title="${weightLabel(w)}">${formattedValue}</span>`;
}

/**
 * Returns the display bits for a club reference: short name for text, full tooltip,
 * and whether the referenced club is marked excluded. `item.clubShortName`, `clubLongName`
 * and `clubExcluded` are provided by the server on every listing row that carries a clubId.
 */
function clubDisplay(item) {
    const id    = item.clubId || '';
    const short = item.clubShortName || id;
    const long  = item.clubLongName || '';
    const title = id ? (id + (long ? ' — ' + long : '')) : '';
    return { id, short, title, excluded: !!item.clubExcluded };
}

/**
 * Cross-tab navigation: click a club cell outside the Clubs tab to land on the Clubs
 * tab filtered to that single club.
 */
function openClubTab(item) {
    const id = item.clubId;
    if (!id) return;
    setFilter('clubs', 'id', id, 'Club: ' + (item.clubShortName || id));
    switchTab('clubs');
}

/** Shared Club action column used on boats / series / races tabs. */
function clubColumn(anchor, sortKey) {
    return {
        label: 'Club', type: 'action', anchor, sortKey,
        tip: 'Home club (short name). Click to see this club on the Clubs tab; hover for id and full name.',
        render:   item => clubDisplay(item).short,
        title:    item => clubDisplay(item).title,
        btnClass: item => clubDisplay(item).excluded ? 'excluded-link' : '',
        action:   item => openClubTab(item)
    };
}

const COLUMNS = {
    boats: [
        { label: 'ID',     key: 'id',     anchor: 'col-boat-id',      tip: 'Unique boat identifier derived from sail number, name and design.', cls: 'id-col' },
        { label: 'Sail',   key: 'sailNumber', anchor: 'col-boat-sail', tip: 'Sail number as recorded in source data (SailSys / TopYacht).' },
        { label: 'Name',   key: 'name',   anchor: 'col-boat-name',     tip: 'Boat name as recorded in source data.', cls: 'id-col' },
        { label: 'Design', type: 'action', sortKey: 'designId', anchor: 'col-boat-design', cls: 'id-col',
          tip: 'Design (class) identifier; click to search for this design in the designs tab.',
          render:   item => item.designId || '',
          btnClass: item => [
              item.designExcluded ? 'excluded-link' : '',
              item.designIgnored  ? 'ignored-link'  : ''
          ].filter(Boolean).join(' '),
          action: item => {
              if (!item.designId) return;
              state.searches['designs'] = item.designId;
              state.pages['designs'] = 0;
              const q = document.getElementById('q-designs');
              if (q) q.value = item.designId;
              switchTab('designs');
              loadList('designs', 0);
          } },
        { label: 'RF',  anchor: 'col-boat-rf', sortKey: 'spinRef',
          tip: 'Reference Factor for the selected variant — IRC-equivalent handicap derived from certificates or median performance. Colour: green = high confidence, red = low.',
          render: item => {
              const v = boatVariant === 'nonSpin' ? item.nonSpinRef
                      : boatVariant === 'twoHanded' ? item.twoHandedRef : item.spinRef;
              return v && v.value != null
                  ? weightSpan(v.value, v.value.toFixed(4), v.weight)
                  : '<span style="color:#bbb">—</span>';
          } },
        { label: 'PF', anchor: 'col-boat-pf', sortKey: 'pf',
          tip: 'Performance Factor for the selected variant — back-calculated time correction factor optimized over this boat\'s racing history. Colour: green = high confidence, red = low.',
          render: item => {
              const v = boatVariant === 'nonSpin' ? item.pfNonSpin
                      : boatVariant === 'twoHanded' ? item.pfTwoHanded : item.pf;
              return v && v.value != null
                  ? weightSpan(v.value, v.value.toFixed(4), v.weight)
                  : '<span style="color:#bbb">—</span>';
          } },
        { label: 'PP', key: 'profile', sortKey: 'profile', anchor: 'col-boat-profile',
            tip: 'Performance profile score — fleet-relative percentile polygon area across Frequency, Consistency, Diversity, Chaotic and Stability spokes (last 12 months).',
          render: v => v != null ? v.toFixed(3) : '<span style="color:#bbb">—</span>' },
        clubColumn('col-boat-club', 'clubId'),
        { label: 'Finishes', type: 'action', sortKey: 'finishes', anchor: 'col-boat-finishes',
          tip: 'Number of recorded finishes; click to view this boat\'s races.',
          render: item => item.finishes ? String(item.finishes) : '',
          action: item => { setFilter('races', 'boatId', item.id,
                            'Races for ' + (item.name || item.id)); switchTab('races'); } },
    ],
    designs: [
        { label: 'ID',     sortKey: 'id',           anchor: 'col-design-id',     tip: 'Unique design identifier (normalised class name).', cls: 'id-col',
          render: item => {
              const text = esc(item.id != null ? String(item.id) : '');
              return /modified|custom/i.test(item.id || '') ? `<span style="text-decoration:line-through">${text}</span>` : text;
          } },
        { label: 'Name',   sortKey: 'canonicalName', anchor: 'col-design-name',   tip: 'Canonical design name used in all reports.',
          render: item => {
              const text = esc(item.canonicalName != null ? String(item.canonicalName) : '');
              return /modified|custom/i.test(item.id || '') ? `<span style="text-decoration:line-through">${text}</span>` : text;
          } },
        { label: 'RF',     key: 'spinRef',       anchor: 'col-design-rf',
          tip: 'Design-level Reference Factor aggregated across all boats of this class. Colors: green = high confidence, red = low',
          render: v => v && v.value != null
            ? weightSpan(v.value, v.value.toFixed(4), v.weight)
            : '<span style="color:#bbb">—</span>' },
        { label: 'RF Weight', key: 'spinRef', anchor: 'col-design-rf-weight',
          tip: 'Statistical weight of the design-level RF: higher means more race data and tighter confidence.',
          render: v => v && v.weight != null
            ? weightSpan(v.weight, v.weight.toFixed(1), v.weight)
            : '<span style="color:#bbb">—</span>' },
        { label: 'Boats',  type: 'action', sortKey: 'boats', anchor: 'col-design-boats',
          tip: 'Number of boats of this design; click to show these boats in the boats table.',
          render: item => item.boats ? String(item.boats) : '',
          action: item => { setFilter('boats', 'designId', item.id,
                            'Boats of design ' + (item.canonicalName || item.id)); switchTab('boats'); } },
    ],
    clubs: [
        { label: 'ID',        key: 'id',        anchor: 'col-club-id',    tip: 'Club identifier (website domain).', cls: 'id-col' },
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
    ],
    series: [
        clubColumn('col-series-club', 'club'),
        { label: 'Name',      key: 'name',      anchor: 'col-series-name',    tip: 'Series name.', cls: 'id-col' },
        { label: 'First',     key: 'firstDate', anchor: 'col-series-first',   tip: 'Date of the first race in this series.' },
        { label: 'Last',      key: 'lastDate',  anchor: 'col-series-last',    tip: 'Date of the last race in this series.' },
        { label: 'Races',     type: 'action', sortKey: 'races', anchor: 'col-series-races',
          tip: 'Number of races in this series; click to show these races in the races table.',
          render: item => item.races != null ? String(item.races) : '',
          action: item => { setFilter('races', 'seriesId', item.id, 'Series: ' + (item.name || item.id)); switchTab('races'); } },
    ],
    races: [
        { label: 'ID',        key: 'id',        anchor: 'col-race-id',        tip: 'Unique race identifier: clubId–date–number.', cls: 'id-col' },
        { label: 'Date',      key: 'date',      anchor: 'col-race-date',      tip: 'Race date.' },
        clubColumn('col-race-club', 'clubId'),
        { label: 'Series',    type: 'action', sortKey: 'seriesName', anchor: 'col-race-series',
          tip: 'Series this race belongs to; click to jump to it on the Series tab.',
          render:   item => item.seriesName || '',
          btnClass: item => item.seriesExcluded ? 'excluded-link' : '',
          action: item => {
              if (!item.seriesId) return;
              setFilter('series', 'id', item.seriesId,
                  'Series: ' + (item.seriesName || item.seriesId));
              switchTab('series');
              loadDetail('series', item.seriesId);
          } },
        { label: 'Race',      key: 'name',      anchor: 'col-race-name',      tip: 'Race name or number within the series.' },
        { label: 'Finishers', key: 'finishers',    anchor: 'col-race-finishers',   tip: 'Total finishers across all divisions in this race.' },
    ],
};

let boatVariant = 'spin';

// ---- Division-chart "From 0" state (shared across races + series tabs) ----
// Persisted in sessionStorage so switching divisions/tabs keeps the same preference.
const DIV_CHART_X_KEY = 'pf.divChart.xFromZero';
const DIV_CHART_Y_KEY = 'pf.divChart.yFromZero';
const DIV_X_CHECKBOX_IDS = ['race-x-from-zero', 'series-x-from-zero'];
const DIV_Y_CHECKBOX_IDS = ['race-y-from-zero', 'series-y-from-zero'];

function getDivChartXFromZero() {
    const v = sessionStorage.getItem(DIV_CHART_X_KEY);
    return v === null ? false : v === 'true';   // default: X not ticked
}
function getDivChartYFromZero() {
    const v = sessionStorage.getItem(DIV_CHART_Y_KEY);
    return v === null ? true : v === 'true';    // default: Y ticked
}
function setDivChartXFromZero(on) {
    sessionStorage.setItem(DIV_CHART_X_KEY, String(!!on));
    DIV_X_CHECKBOX_IDS.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.checked = !!on;
    });
}
function setDivChartYFromZero(on) {
    sessionStorage.setItem(DIV_CHART_Y_KEY, String(!!on));
    DIV_Y_CHECKBOX_IDS.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.checked = !!on;
    });
}
/** Re-renders whichever division chart is currently visible (races or series tab). */
function reRenderActiveDivisionChart() {
    if (state.activeTab === 'races') onRaceDivisionChange();
    else if (state.activeTab === 'series') onSeriesDivisionChange();
}
function onDivChartXFromZeroChange(cb) {
    setDivChartXFromZero(cb.checked);
    reRenderActiveDivisionChart();
}
function onDivChartYFromZeroChange(cb) {
    setDivChartYFromZero(cb.checked);
    reRenderActiveDivisionChart();
}

function setBoatVariant(v) {
    boatVariant = v;
    const rfCol  = COLUMNS.boats.find(c => c.anchor === 'col-boat-rf');
    const pfCol = COLUMNS.boats.find(c => c.anchor === 'col-boat-pf');
    if (rfCol)  rfCol.sortKey  = v === 'nonSpin' ? 'nonSpinRef'   : v === 'twoHanded' ? 'twoHandedRef' : 'spinRef';
    if (pfCol) pfCol.sortKey = v === 'nonSpin' ? 'pfNonSpin'   : v === 'twoHanded' ? 'pfTwoHanded' : 'pf';
    loadList('boats', 0);
}

// Session-persisted per-tab search terms ("as if just typed") and active tab.
// Cleared only by the [×] button next to the search field, or by an inbound URL
// that names a specific item to display.
const SEARCH_KEY_PREFIX = 'pf.search.';
const ACTIVE_TAB_KEY = 'pf.activeTab';
const TAB_ENTITIES = ['clubs', 'boats', 'designs', 'series', 'races'];

function loadPersistedSearches() {
    const out = {};
    TAB_ENTITIES.forEach(e => out[e] = sessionStorage.getItem(SEARCH_KEY_PREFIX + e) || '');
    return out;
}

const state = {
    pages:    { boats: 0, designs: 0, clubs: 0, races: 0, series: 0 },
    sort:     { boats: 'id', designs: 'id', clubs: 'shortName', races: 'date', series: 'firstDate' },
    dir:      { boats: 'asc', designs: 'asc', clubs: 'asc', races: 'desc', series: 'desc' },
    pageSize: 50,
    hasMore:  { boats: false, designs: false, clubs: false, races: false, series: false },
    loading:  { boats: false, designs: false, clubs: false, races: false, series: false },
    totals:   { boats: 0, designs: 0, clubs: 0, races: 0, series: 0 },
    searchTimers: {},
    searches: loadPersistedSearches(),  // per-tab search terms, persisted across navigation
    activeTab: sessionStorage.getItem(ACTIVE_TAB_KEY) || 'boats',
    selected:     { boats: new Set(), designs: new Set(), clubs: new Set(), series: new Set(), races: new Set() },   // IDs of checked rows
    selectedData: { boats: new Map(), designs: new Map(), clubs: new Map(), series: new Map(), races: new Map() },   // id → item for action panel
    filter: { boats: null, designs: null, clubs: null, races: null, series: null },
    raceItems:       [],   // current page's race rows for prev/next navigation
    currentRaceIdx:  -1,   // index into raceItems of the currently shown race
    boatItems:       [],   // current page's boat rows for prev/next navigation
    currentBoatIdx:  -1,   // index into boatItems of the currently shown boat
    lastDetailBoat:  null, // most recently loaded boat detail (for edit panel)
};

let currentDivRaceId = null;
// Race-division-chart toggles — persisted in sessionStorage so they survive tab
// switches and page reloads within a session. Defaults: RF line on, the rest off.
const RACE_RF_KEY    = 'pf.divChart.showRf';
const RACE_ERR_KEY   = 'pf.divChart.showErrorBars';
const RACE_TREND_KEY = 'pf.divChart.showTrend';
const RACE_DIV_XFACTOR_KEY = 'pf.divChart.xFactor';
function sessionBool(key, dflt) {
    const v = sessionStorage.getItem(key);
    return v === null ? dflt : v === 'true';
}
let showRaceRfLine    = sessionBool(RACE_RF_KEY,    true);
let showRaceErrorBars = sessionBool(RACE_ERR_KEY,   false);
let showRaceTrendLine = sessionBool(RACE_TREND_KEY, false);
const SERIES_OVERALL_TREND_KEY = 'pf.divChart.seriesOverallTrend';
let showSeriesOverallTrend = sessionBool(SERIES_OVERALL_TREND_KEY, false);
// Shared between race-division and series charts; default off so the plot area stays
// constant in height as the user flips between divisions.
const SHOW_LEGEND_KEY = 'pf.divChart.showLegend';
let showLegend = sessionBool(SHOW_LEGEND_KEY, false);
let preferredDivision = null;
let raceDivXFactor = sessionStorage.getItem(RACE_DIV_XFACTOR_KEY) || '---';
let lastRaceDivData = null;

function isWriteAllowed() { return window.pfAuth?.authenticated; }

function switchTab(entity) {
    TAB_ENTITIES.forEach(e => {
        document.getElementById('tab-btn-' + e).classList.toggle('active', e === entity);
        document.getElementById('panel-' + e).classList.toggle('active', e === entity);
    });
    state.activeTab = entity;
    sessionStorage.setItem(ACTIVE_TAB_KEY, entity);
    // Restore persisted search term for this tab
    const q = document.getElementById('q-' + entity);
    if (q && state.searches[entity] !== undefined) q.value = state.searches[entity];
    updateFilterBanner(entity);
    updateFilterControls(entity);
    // Always reload — tbody may be empty (first visit) or hold rows from a prior search
    // that no longer matches the restored search term.
    loadList(entity, 0);
}

function setFilter(entity, param, value, label) {
    state.filter[entity] = { param, value, label };
    state.searches[entity] = '';   // navigation filter clears any persistent search
    sessionStorage.removeItem(SEARCH_KEY_PREFIX + entity);
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
    ['exclude-empty-' + entity, 'filter-dupe-sails'].forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.disabled = active;
            if (el.parentElement) el.parentElement.style.opacity = active ? '0.4' : '';
        }
    });
}

function debounceSearch(entity) {
    const q = document.getElementById('q-' + entity);
    if (q) {
        state.searches[entity] = q.value;
        sessionStorage.setItem(SEARCH_KEY_PREFIX + entity, q.value);
    }
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
    sessionStorage.removeItem(SEARCH_KEY_PREFIX + entity);
    const q = document.getElementById('q-' + entity);
    if (q) q.value = '';
    doSearch(entity);
}

async function loadList(entity, page) {
    if (state.loading[entity]) return;
    state.loading[entity] = true;
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
    if (showExcludedEl && showExcludedEl.checked) url += '&showExcluded=true';
    const hideEmptyEl = document.getElementById('hide-empty-' + entity);
    if (hideEmptyEl && hideEmptyEl.checked) url += '&hideEmpty=true';
    const excludeEmptyEl = document.getElementById('exclude-empty-' + entity);
    if (!f && excludeEmptyEl && excludeEmptyEl.checked) url += '&excludeEmpty=true';
    const data = await fetchJson(url);
    state.loading[entity] = false;
    if (!data) return;

    const append = page > 0;
    if (!append) renderHeaders(entity);
    renderTable(entity, data.items, append);
    state.totals[entity] = data.total;
    state.hasMore[entity] = (page + 1) * data.size < data.total;
    renderScrollStatus(entity);
}

function renderHeaders(entity) {
    const thead  = document.getElementById('thead-' + entity);
    const cols   = COLUMNS[entity];
    const active = state.sort[entity];
    const dir    = state.dir[entity];
    let html = '';
    html += '<th style="width:2rem"></th>';
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
    const container = document.querySelector('#panel-' + entity + ' .table-scroll');
    if (container) container.scrollTop = 0;
    loadList(entity, 0);
}

function renderTable(entity, items, append) {
    const tbody = document.getElementById('tbody-' + entity);
    if (!append) tbody.innerHTML = '';
    const cols = COLUMNS[entity];

    if (entity === 'races') {
        if (append) state.raceItems = state.raceItems.concat(items);
        else state.raceItems = items;
    }
    if (entity === 'boats') {
        if (append) state.boatItems = state.boatItems.concat(items);
        else state.boatItems = items;
    }

    const baseIdx = append ? tbody.children.length : 0;
    items.forEach((item, itemIdx) => {
        const globalIdx = baseIdx + itemIdx;
        const tr = document.createElement('tr');
        if (item.excluded) tr.classList.add('excluded');
        if (item.ignored)  tr.classList.add('ignored');
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
        tr.onclick = () => {
            if (entity === 'races') state.currentRaceIdx = globalIdx;
            if (entity === 'boats') { state.currentBoatIdx = globalIdx; }
            loadDetail(entity, item.id);
        };
        cols.forEach(col => {
            const td = document.createElement('td');
            if (col.cls) td.className = col.cls;
            if (col.type === 'action') {
                const text = col.render ? col.render(item) : col.label;
                const extraClass = col.btnClass ? col.btnClass(item) : '';
                const tooltip = col.title ? col.title(item) : null;
                if (text && col.action) {
                    const btn = document.createElement('button');
                    btn.className = 'link-btn' + (extraClass ? ' ' + extraClass : '');
                    btn.textContent = text;
                    if (tooltip) btn.title = tooltip;
                    else if (col.cls && text) btn.title = text; // Add title for truncated text cells
                    btn.onclick = (e) => {
                        e.stopPropagation();
                        col.action(item);
                    };
                    td.appendChild(btn);
                } else {
                    td.textContent = text || '';
                    if (tooltip) td.title = tooltip;
                    else if (col.cls && text) td.title = text; // Add title for truncated text cells
                    if (extraClass) td.className = ((td.className || '') + ' ' + extraClass).trim();
                }
            } else {
                const v = col.key != null ? item[col.key] : item;
                const rawValue = v != null ? String(v) : '';
                td.innerHTML = col.render ? col.render(v) : esc(rawValue);
                // Add title attribute for cells that may be truncated (those with id-col or similar classes)
                if (col.cls && rawValue) {
                    td.title = rawValue;
                }
            }
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
}

function renderScrollStatus(entity) {
    const loaded = document.getElementById('tbody-' + entity).children.length;
    const total  = state.totals[entity];
    const html   = `<span>${loaded.toLocaleString()} of ${total.toLocaleString()}</span>`;
    const el = document.getElementById('scroll-status-' + entity);
    if (el) el.innerHTML = html;
}

function loadNextPage(entity) {
    if (!state.hasMore[entity] || state.loading[entity]) return;
    loadList(entity, state.pages[entity] + 1);
}

// Attach infinite-scroll listeners to all table-scroll containers, and restore any
// session-persisted chart preferences to their checkboxes.
document.addEventListener('DOMContentLoaded', () => {
    ['clubs', 'boats', 'designs', 'series', 'races'].forEach(entity => {
        const container = document.querySelector('#panel-' + entity + ' .table-scroll');
        if (!container) return;
        container.addEventListener('scroll', () => {
            if (container.scrollTop + container.clientHeight >= container.scrollHeight - 40) {
                loadNextPage(entity);
            }
        });
    });

    // Division-chart From-0 checkboxes (shared across races + series tabs).
    const xFrom = getDivChartXFromZero();
    const yFrom = getDivChartYFromZero();
    DIV_X_CHECKBOX_IDS.forEach(id => { const el = document.getElementById(id); if (el) el.checked = xFrom; });
    DIV_Y_CHECKBOX_IDS.forEach(id => { const el = document.getElementById(id); if (el) el.checked = yFrom; });

    // Race-division-chart toggles.
    const rfCb    = document.getElementById('race-show-rf');
    const errCb   = document.getElementById('race-show-error-bars');
    const trendCb = document.getElementById('race-show-trend');
    if (rfCb)    rfCb.checked    = showRaceRfLine;
    if (errCb)   errCb.checked   = showRaceErrorBars;
    if (trendCb) trendCb.checked = showRaceTrendLine;

    // Series-chart overall-trend toggle.
    const seriesTrendCb = document.getElementById('series-show-overall-trend');
    if (seriesTrendCb) seriesTrendCb.checked = showSeriesOverallTrend;

    // Show-legend toggle (shared across race + series charts).
    ['race-show-legend', 'series-show-legend'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.checked = showLegend;
    });
});

async function loadDetail(entity, id, {scroll = true} = {}) {
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
        const pfDiv = document.getElementById('pf-detail-boats');
        pfDiv.innerHTML = '<em>Loading…</em>';
        const pfData = await fetchJson('/api/boats/' + encodeURIComponent(id) + '/pf');
        pfDiv.innerHTML = pfData ? renderBoatPf(pfData) : '<em>No PF data available</em>';

        const aliasDiv = document.getElementById('aliases-boats');
        const aliases = await fetchJson('/api/boats/' + encodeURIComponent(id) + '/aliases');
        aliasDiv.innerHTML = aliases && aliases.length > 0 ? renderBoatAliases(aliases) : '';

        // Store loaded boat data for possible reuse (edit flow is selection-driven now)
        state.lastDetailBoat = data;

        updateBoatNav();
    }

    panel.classList.add('visible');
    if (scroll) {
        if (entity === 'races') {
            document.getElementById('division-section-races').scrollIntoView({behavior: 'smooth', block: 'start'});
        } else if (entity === 'boats') {
            const heading = document.getElementById('pf-boat-heading');
            if (heading) window.scrollTo(0, heading.getBoundingClientRect().top + window.scrollY);
        } else {
            panel.scrollIntoView({behavior: 'smooth', block: 'start'});
        }
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

function renderBoatPf(data) {
    const boatHeading = `<div id="pf-boat-heading" style="font-size:1.05rem;font-weight:bold;margin-bottom:0.4rem;">${data.boatName ? esc(data.boatName) : ''}</div>`;

    function row(label, pf, rf) {
        if (!pf && !rf) return `<tr><td>${label}</td><td colspan="7" style="color:#999">—</td></tr>`;
        const rfVal    = rf  ? rf.value.toFixed(4)   : '—';
        const rfWt     = rf  ? rf.weight.toFixed(3)  : '—';
        const rfGen    = rf  && rf.generation != null ? rf.generation : '—';
        const pfVal   = pf ? pf.value.toFixed(4)  : '—';
        const pfWt    = pf ? pf.weight.toFixed(3) : '—';
        const delta    = pf && pf.referenceDelta != null
            ? (pf.referenceDelta >= 0 ? '+' : '') + pf.referenceDelta.toFixed(4) : '—';
        const races    = pf ? pf.raceCount : '—';
        return `<tr>
          <td>${label}</td>
          <td>${rfVal}</td>
          <td>${rfWt}</td>
          <td>${rfGen}</td>
          <td>${pfVal}</td>
          <td>${pfWt}</td>
          <td>${delta}</td>
          <td>${races}</td>
        </tr>`;
    }

    let html = boatHeading + `<strong>Performance factors (${data.currentYear})</strong>
      <table style="width:auto;margin-top:0.4rem;">
        <thead><tr>
          <th>Variant${infoBtn('col-pf-variant','Spin, Non-Spin, or Two-Handed handicap variant.')}</th>
          <th>RF${infoBtn('col-pf-rf','Reference Factor — IRC-equivalent handicap derived from certificates.')}</th>
          <th>RF Wt${infoBtn('col-ref-weight','RF confidence weight: 1.0 = direct certificate, lower = inferred or multi-hop conversion.')}</th>
          <th>Gen${infoBtn('col-rf-gen','RF generation — the pipeline step that assigned this factor. Lower = earlier/more-direct; higher = later propagation or cross-variant fill.')}</th>
          <th>PF${infoBtn('col-pf-value','Performance Factor — back-calculated handicap averaged across this boat\'s race history.')}</th>
          <th>PF Wt${infoBtn('col-pf-weight','PF confidence weight — proportional to number of informative races.')}</th>
          <th>Delta${infoBtn('col-pf-delta','PF minus RF. Near zero = race history is consistent with the certificate.')}</th>
          <th>Races${infoBtn('col-pf-races','Number of races contributing to this PF estimate.')}</th>
        </tr></thead>
        <tbody>
          ${row('Spin',       data.spin,      data.rfSpin)}
          ${row('Non-spin',   data.nonSpin,   data.rfNonSpin)}
          ${row('Two-handed', data.twoHanded, data.rfTwoHanded)}
        </tbody>
      </table>`;

    if (data.profile) {
        const score = data.profile.overallScore != null ? data.profile.overallScore.toFixed(3) : '—';
        html += `<div style="margin-top:0.75rem;font-weight:bold;font-size:0.9rem;">${data.boatName ? esc(data.boatName) + ' — ' : ''}Performance Profile ${infoBtn('chart-profile', 'Radar chart: five fleet-relative percentile scores based on the last 12 months. Frequency: how often the boat races. Consistency: how tight the residuals are. Diversity: distinct opponents raced. Chaotic: whether inconsistency correlates with fleet-wide conditions. Stability: flatness of trend (level=best, declining=worst).')}</div>`;
        html += `<div style="display:inline-block;vertical-align:top;text-align:left;">`;
        html += `  <div id="pf-profile-chart"></div>`;
        html += `  <div style="text-align:center;font-size:0.85rem;color:#555;margin-top:0.1rem;">PP: ${score}</div>`;
        html += `</div>`;
        setTimeout(() => renderProfileChart(data.profile), 0);
    }
    html += `<div class="division-nav" id="boat-nav" style="margin:0.75rem 0;">
      <button id="boat-prev-btn" onclick="prevBoat()" disabled>&#8592; Prev</button>
      <button id="boat-next-btn" onclick="nextBoat()" disabled>Next &#8594;</button>
      <span id="boat-nav-label" style="flex:1;text-align:center;font-weight:bold;font-size:1rem;"></span>
    </div>`;

    if (data.residuals && data.residuals.length > 0) {
        html += `<div style="margin-top:0.75rem;font-weight:bold;font-size:0.9rem;">Per-race residuals ${infoBtn('chart-residuals','Scatter plot of back-calculated factor per race over time. Each point is one race division; colour intensity reflects the entry weight used in the PF optimiser. Points close to zero indicate the boat raced close to its PF.')}</div>`;
        html += `<label style="font-size:0.85rem;font-weight:normal;"><input type="checkbox" id="residual-last12" onchange="window._residualLast12=this.checked; renderResidualChart(window._lastResiduals)"> Last 12 months only</label>`;
        html += '<div id="pf-residual-chart"></div>';
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
    const container = document.getElementById('pf-residual-chart');
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
    // Raw residual = log(elapsed) + log(PF) - log(T_div); positive means slower than reference.
    function makeTrace(entries, name, baseColor) {
        return {
            x: entries.map(e => e.date),
            y: entries.map(e => -e.residual),
            mode: 'markers',
            type: 'scatter',
            name: name,
            marker: {
                color: entries.map(e => {
                    const a = (Math.max(0.45, Math.min(1.0, e.weight))).toFixed(2);
                    return baseColor.replace(/[\d.]+\)$/, a + ')');
                }),
                size: entries.map(e => 5 + 4 * Math.min(Math.max(e.weight, 0), 1)),
                symbol: entries.map(e => e.weight < 0.01 ? 'x' : 'circle')
            },
            text: entries.map(e => {
                const parts = [];
                if (e.seriesName) parts.push(esc(e.seriesName));
                if (e.raceName)   parts.push(esc(e.raceName));
                parts.push(e.division || '—');
                parts.push(`w=${e.weight.toFixed(2)}  r=${(-e.residual).toFixed(4)}`);
                return parts.join('<br>');
            }),
            customdata: entries.map(e => ({ raceId: e.raceId, seriesId: e.seriesId })),
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

    container.removeAllListeners && container.removeAllListeners('plotly_click');
    container.on('plotly_click', (eventData) => {
        if (!eventData.points || !eventData.points.length) return;
        const pt = eventData.points[0];
        if (!pt.customdata || !pt.customdata.raceId) return;
        const params = new URLSearchParams({ tab: 'races', raceId: pt.customdata.raceId });
        if (pt.customdata.seriesId) params.set('seriesId', pt.customdata.seriesId);
        window.location.href = 'data.html?' + params;
    });
}

function renderProfileChart(profile) {
    const container = document.getElementById('pf-profile-chart');
    if (!container || typeof Plotly === 'undefined') return;

    // Spoke order matches polygon area calculation: Frequency, Consistency, Diversity, Chaotic, Stability
    const labels = ['Frequency', 'Consistency', 'Diversity', 'Chaotic', 'Stability'];
    const keys = ['frequency', 'consistency', 'diversity', 'chaotic', 'stability'];
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
    // Any change to the selection closes the previously-opened detail panel; the
    // action bar is the new focus for the user until they click another row.
    const panel = document.getElementById('detail-' + entity);
    if (panel) panel.classList.remove('visible');
    updateMergeBar(entity);
}

const ENTITY_NOUNS = {
    boats: 'boat', designs: 'design', clubs: 'club', series: 'series', races: 'race'
};

function updateMergeBar(entity) {
    const n   = state.selected[entity].size;
    const bar = document.getElementById('merge-bar-' + entity);
    if (!bar) return;
    bar.style.display = n >= 1 ? '' : 'none';
    const noun = ENTITY_NOUNS[entity] || entity;
    const label = (entity === 'series') ? (n + ' series selected')
                : (n + ' ' + noun + (n !== 1 ? 's' : '') + ' selected');
    document.getElementById('merge-bar-count-' + entity).textContent = label;

    const items = Array.from(state.selectedData[entity].values());

    const mergeable = (entity === 'boats' || entity === 'designs');
    const w = isWriteAllowed();
    const mergeBtn = document.getElementById('merge-btn-' + entity);
    const reqBtn   = document.getElementById('merge-request-btn-' + entity);
    if (mergeBtn) mergeBtn.style.display = (mergeable && n >= 2 && w)  ? '' : 'none';
    if (reqBtn)   reqBtn.style.display   = (mergeable && n >= 2 && !w) ? '' : 'none';

    // Edit (boats only) — single selection for full edit; 2+ for club-only bulk edit.
    if (entity === 'boats')
    {
        const editBtn = document.getElementById('edit-btn-boats');
        const editReq = document.getElementById('edit-request-btn-boats');
        const editClubBtn = document.getElementById('edit-club-btn-boats');
        const editClubReq = document.getElementById('edit-club-request-btn-boats');
        if (editBtn) editBtn.style.display = (n === 1 && w) ? '' : 'none';
        if (editReq) editReq.style.display = (n === 1 && !w) ? '' : 'none';
        if (editClubBtn) editClubBtn.style.display = (n >= 2 && w) ? '' : 'none';
        if (editClubReq) editClubReq.style.display = (n >= 2 && !w) ? '' : 'none';
    }

    // Ignore / Do Not Ignore (designs only) — mirrors Exclude/Include on the ignored flag.
    if (entity === 'designs')
    {
        const anyIgnored    = items.some(isItemIgnored);
        const anyUnignored  = items.some(it => !isItemIgnored(it));
        const ignoreBtn   = document.getElementById('ignore-btn-designs');
        const unignoreBtn = document.getElementById('unignore-btn-designs');
        if (ignoreBtn)   ignoreBtn.style.display   = (n >= 1 && anyUnignored) ? '' : 'none';
        if (unignoreBtn) unignoreBtn.style.display = (n >= 1 && anyIgnored)   ? '' : 'none';

        // Edit design (single selection) — Edit when authed, Request edit otherwise.
        const editBtn = document.getElementById('edit-btn-designs');
        const editReq = document.getElementById('edit-request-btn-designs');
        if (editBtn) editBtn.style.display = (n === 1 && w)  ? '' : 'none';
        if (editReq) editReq.style.display = (n === 1 && !w) ? '' : 'none';
    }

    // Exclude / Include — visible based on excluded-state of currently-selected items.
    const anyExcluded  = items.some(isItemExcluded);
    const anyIncluded  = items.some(it => !isItemExcluded(it));
    const excludeBtn = document.getElementById('exclude-btn-' + entity);
    const includeBtn = document.getElementById('include-btn-' + entity);
    if (excludeBtn) excludeBtn.style.display = (n >= 1 && anyIncluded) ? '' : 'none';
    if (includeBtn) includeBtn.style.display = (n >= 1 && anyExcluded) ? '' : 'none';
}

/** Returns true if the item is currently excluded (as seen by the last list fetch). */
function isItemExcluded(item) { return !!item.excluded; }

/** Returns true if the design item is currently ignored (as seen by the last list fetch). */
function isItemIgnored(item) { return !!item.ignored; }

function clearSelection(entity) {
    state.selected[entity].clear();
    state.selectedData[entity].clear();
    updateMergeBar(entity);
    hideMergePanel(entity);
    hideExcludePanel(entity);
    if (entity === 'boats') {
        hideEditPanel();
        hideEditClubPanel();
    }
    if (entity === 'designs') { hideIgnorePanel(); hideEditDesignPanel(); }
    const panel = document.getElementById('detail-' + entity);
    if (panel) panel.classList.remove('visible');
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
    const panel = document.getElementById('merge-panel-' + entity);
    if (panel) panel.style.display = 'none';
}

document.addEventListener('pf:authready', () => {
    loadList(state.activeTab, 0);
    applyMergeAuthState();
});

let requestEmail = '';

const ALL_ENTITIES = ['boats', 'designs', 'clubs', 'series', 'races'];

function syncRequestEmail(value) {
    requestEmail = value;
    // Keep all email inputs in sync
    const ids = ['edit-email', 'edit-email-designs'];
    ALL_ENTITIES.forEach(e => {
        ids.push('merge-email-' + e);
        ids.push('exclude-email-' + e);
    });
    ids.forEach(id => {
        const el = document.getElementById(id);
        if (el && el !== document.activeElement) el.value = value;
    });
}

function applyMergeAuthState() {
    const w = isWriteAllowed();
    // Merge panels only exist for boats/designs; exclude panels exist for all entities.
    ['boats', 'designs'].forEach(entity => {
        applyPanelAuthState(entity, 'merge', w);
    });
    ALL_ENTITIES.forEach(entity => applyPanelAuthState(entity, 'exclude', w));
    applyPanelAuthState('designs', 'ignore', w);
    ALL_ENTITIES.forEach(updateMergeBar);

    const editSaveBtn  = document.getElementById('edit-save-btn');
    const editReqBtn   = document.getElementById('edit-request-btn');
    const editEmailRow = document.getElementById('edit-email-row');
    const editMsgRow   = document.getElementById('edit-message-row');
    if (editSaveBtn)  editSaveBtn.style.display  = w ? '' : 'none';
    if (editReqBtn)   editReqBtn.style.display   = w ? 'none' : '';
    if (editEmailRow) editEmailRow.style.display = w ? 'none' : '';
    if (editMsgRow)   editMsgRow.style.display   = w ? 'none' : '';

    // Edit-club panel (bulk boats)
    const ecSave = document.getElementById('edit-club-save-btn');
    const ecReq = document.getElementById('edit-club-request-save-btn');
    const ecEmail = document.getElementById('edit-club-email-row');
    const ecMsg = document.getElementById('edit-club-message-row');
    if (ecSave) ecSave.style.display = w ? '' : 'none';
    if (ecReq) ecReq.style.display = w ? 'none' : '';
    if (ecEmail) ecEmail.style.display = w ? 'none' : '';
    if (ecMsg) ecMsg.style.display = w ? 'none' : '';

    // Edit-design panel: same auth pattern but with design-scoped ids.
    const edSave = document.getElementById('edit-design-save-btn');
    const edReq  = document.getElementById('edit-design-request-btn');
    const edEmail = document.getElementById('edit-design-email-row');
    const edMsg   = document.getElementById('edit-design-message-row');
    if (edSave)  edSave.style.display  = w ? '' : 'none';
    if (edReq)   edReq.style.display   = w ? 'none' : '';
    if (edEmail) edEmail.style.display = w ? 'none' : '';
    if (edMsg)   edMsg.style.display   = w ? 'none' : '';

    // Pre-populate email fields with remembered value
    if (!w) {
        const ids = ['edit-email', 'edit-email-designs', 'edit-club-email'];
        ALL_ENTITIES.forEach(e => {
            ids.push('merge-email-' + e);
            ids.push('exclude-email-' + e);
        });
        ids.forEach(id => {
            const el = document.getElementById(id);
            if (el) el.value = requestEmail;
        });
    }
}

/** Show/hide the auth vs request confirm buttons and email/message rows in a panel. */
function applyPanelAuthState(entity, kind, writeAllowed) {
    const confirmBtn    = document.getElementById(kind + '-confirm-' + entity);
    const reqConfirmBtn = document.getElementById(kind + '-request-confirm-' + entity);
    const emailRow      = document.getElementById(kind + '-email-row-' + entity);
    const msgRow        = document.getElementById(kind + '-message-row-' + entity);
    if (confirmBtn)    confirmBtn.style.display    = writeAllowed ? '' : 'none';
    if (reqConfirmBtn) reqConfirmBtn.style.display = writeAllowed ? 'none' : '';
    if (emailRow)      emailRow.style.display      = writeAllowed ? 'none' : '';
    if (msgRow)        msgRow.style.display        = writeAllowed ? 'none' : '';
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

// ---- Exclude / Include (selection-based, all entities) ----

let excludeIntent = {};   // per-entity: true = exclude, false = include

function showExcludePanel(entity, doExclude) {
    excludeIntent[entity] = doExclude;
    const panel = document.getElementById('exclude-panel-' + entity);
    const list  = document.getElementById('exclude-list-' + entity);
    const title = document.getElementById('exclude-panel-title-' + entity);
    document.getElementById('exclude-status-' + entity).textContent = '';
    // Only act on items that need the transition (not already in target state)
    const targetItems = Array.from(state.selectedData[entity].values())
        .filter(it => isItemExcluded(it) !== doExclude);
    const verb = doExclude ? 'Exclude' : 'Include';
    const noun = entity === 'series' ? 'series'
               : (ENTITY_NOUNS[entity] + (targetItems.length !== 1 ? 's' : ''));
    title.textContent = verb + ' ' + targetItems.length + ' ' + noun + '?';
    list.innerHTML = '';
    targetItems.forEach(it => {
        const line = document.createElement('div');
        line.style.fontFamily = 'monospace';
        line.style.fontSize   = '0.9rem';
        line.textContent = describeItem(entity, it);
        list.appendChild(line);
    });
    panel.style.display = '';
    // Auth-dependent visibility of confirm buttons / email + message rows
    applyPanelAuthState(entity, 'exclude', isWriteAllowed());
}

function hideExcludePanel(entity) {
    const panel = document.getElementById('exclude-panel-' + entity);
    if (panel) panel.style.display = 'none';
}

function describeItem(entity, item) {
    switch (entity) {
        case 'boats':   return item.id + '  —  sail: ' + (item.sailNumber || '') + '  name: ' + (item.name || '');
        case 'designs': return item.id + '  —  ' + (item.canonicalName || '');
        case 'clubs':   return item.id + '  —  ' + (item.shortName || '') + (item.longName ? ' (' + item.longName + ')' : '');
        case 'series':  return item.id + '  —  ' + (item.name || '');
        case 'races':   return item.id + '  —  ' + (item.date || '') + '  ' + (item.name || '');
        default:        return item.id;
    }
}

/** Build the body payload for a POST to /api/{entity}/exclude or /exclude-request. */
function buildExcludeBody(entity, intent) {
    const items = Array.from(state.selectedData[entity].values())
        .filter(it => isItemExcluded(it) !== intent);
    const payload = { excluded: intent };
    if (entity === 'series') payload.names = items.map(it => it.name).filter(Boolean);
    else                     payload.ids   = items.map(it => it.id).filter(Boolean);
    return payload;
}

async function performExclude(entity) {
    if (!isWriteAllowed()) return;
    const intent = !!excludeIntent[entity];
    const statusEl = document.getElementById('exclude-status-' + entity);
    const body = buildExcludeBody(entity, intent);
    const keyCount = (body.ids || body.names || []).length;
    if (keyCount === 0) { hideExcludePanel(entity); return; }
    statusEl.textContent = (intent ? 'Excluding' : 'Including') + '…';

    const result = await fetchJson('/api/' + entity + '/exclude', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });
    if (!result || !result.ok) {
        statusEl.textContent = 'Failed — see console.';
        return;
    }
    clearSelection(entity);
    hideExcludePanel(entity);
    loadList(entity, 0);
}

async function requestExclude(entity) {
    const intent = !!excludeIntent[entity];
    const statusEl = document.getElementById('exclude-status-' + entity);
    const email = document.getElementById('exclude-email-' + entity)?.value.trim() || '';
    const message = document.getElementById('exclude-message-' + entity)?.value.trim() || '';
    const body = buildExcludeBody(entity, intent);
    const keyCount = (body.ids || body.names || []).length;
    if (keyCount === 0) { hideExcludePanel(entity); return; }
    if (email) body.email = email;
    if (message) body.message = message;
    statusEl.textContent = 'Submitting request…';
    const result = await fetchJson('/api/' + entity + '/exclude-request', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });
    if (result && result.ok) {
        statusEl.textContent = 'Request recorded.';
        clearSelection(entity);
        hideExcludePanel(entity);
    } else {
        statusEl.textContent = 'Failed to record request — see console.';
    }
}

// ---- Ignore / Do-Not-Ignore (designs only) ----

let ignoreIntent = true;

function showIgnorePanel(doIgnore) {
    ignoreIntent = doIgnore;
    const panel = document.getElementById('ignore-panel-designs');
    const list  = document.getElementById('ignore-list-designs');
    const title = document.getElementById('ignore-panel-title-designs');
    const warn  = document.getElementById('ignore-warning-designs');
    document.getElementById('ignore-status-designs').textContent = '';
    // Only act on designs whose state needs to flip.
    const targets = Array.from(state.selectedData.designs.values())
        .filter(it => isItemIgnored(it) !== doIgnore);
    const n = targets.length;
    const verb = doIgnore ? 'Ignore' : 'Do not ignore';
    title.textContent = verb + ' ' + n + ' design' + (n !== 1 ? 's' : '') + '?';
    list.innerHTML = '';
    targets.forEach(it => {
        const line = document.createElement('div');
        line.style.fontFamily = 'monospace';
        line.style.fontSize   = '0.9rem';
        const boats = it.boats != null ? it.boats : 0;
        line.textContent = it.id + '  —  ' + (it.canonicalName || '') + '   (' + boats + ' boat' + (boats !== 1 ? 's' : '') + ')';
        list.appendChild(line);
    });
    if (doIgnore) {
        const total = targets.reduce((acc, it) => acc + (it.boats || 0), 0);
        warn.textContent = total > 0
            ? 'This will strip the design from ' + total + ' boat' + (total !== 1 ? 's' : '')
                + '; where a designless record already exists at the same sail+name, the two will be merged.'
            : '';
        warn.style.display = total > 0 ? '' : 'none';
    } else {
        warn.textContent = 'Un-ignoring a design does not restore boats that were already de-designed by a previous ignore.';
        warn.style.display = '';
    }
    panel.style.display = '';
    applyPanelAuthState('designs', 'ignore', isWriteAllowed());
}

function hideIgnorePanel() {
    const panel = document.getElementById('ignore-panel-designs');
    if (panel) panel.style.display = 'none';
}

function buildIgnoreBody() {
    const ids = Array.from(state.selectedData.designs.values())
        .filter(it => isItemIgnored(it) !== ignoreIntent)
        .map(it => it.id)
        .filter(Boolean);
    return { ids, ignored: ignoreIntent };
}

async function performIgnore() {
    if (!isWriteAllowed()) return;
    const statusEl = document.getElementById('ignore-status-designs');
    const body = buildIgnoreBody();
    if (body.ids.length === 0) { hideIgnorePanel(); return; }
    statusEl.textContent = (ignoreIntent ? 'Ignoring' : 'Un-ignoring') + '…';
    const result = await fetchJson('/api/designs/ignore', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });
    if (!result || !result.ok) {
        statusEl.textContent = 'Failed — see console.';
        return;
    }
    clearSelection('designs');
    hideIgnorePanel();
    loadList('designs', 0);
}

async function requestIgnore() {
    const statusEl = document.getElementById('ignore-status-designs');
    const email = document.getElementById('ignore-email-designs')?.value.trim() || '';
    const message = document.getElementById('ignore-message-designs')?.value.trim() || '';
    const body = buildIgnoreBody();
    if (body.ids.length === 0) { hideIgnorePanel(); return; }
    if (email) body.email = email;
    if (message) body.message = message;
    statusEl.textContent = 'Submitting request…';
    const result = await fetchJson('/api/designs/ignore-request', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });
    if (result && result.ok) {
        statusEl.textContent = 'Request recorded.';
        clearSelection('designs');
        hideIgnorePanel();
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
        const noClubOpt = document.createElement('option');
        noClubOpt.value = '';
        noClubOpt.textContent = '— No Club —';
        const opts = clubsResp.items.map(c => {
            const o = document.createElement('option');
            o.value = c.id;
            o.textContent = c.shortName ? `${c.shortName} — ${c.id}` : c.id;
            return o;
        });
        const cur = clubSel.value;
        clubSel.replaceChildren(noClubOpt, ...opts);
        clubSel.value = cur;
    }
}

/** Opens the edit panel for the single currently-selected boat. */
function showEditPanel() {
    const ids = Array.from(state.selected.boats);
    if (ids.length !== 1) return;
    const item = state.selectedData.boats.get(ids[0]);
    if (!item) return;
    editingBoatId = item.id;
    const title = document.getElementById('edit-panel-title');
    if (title) title.textContent = isWriteAllowed() ? ('Edit Boat ' + item.id)
                                                    : ('Request edit for Boat ' + item.id);
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
    document.getElementById('edit-panel-boats').style.display = '';
    applyMergeAuthState();
}

function hideEditPanel() {
    const panel = document.getElementById('edit-panel-boats');
    if (panel) panel.style.display = 'none';
    editingBoatId = null;
}

async function showEditClubPanel() {
    const panel = document.getElementById('edit-club-panel-boats');
    const sel = document.getElementById('edit-club-select');
    const ids = Array.from(state.selected.boats);
    const title = document.getElementById('edit-club-panel-title');
    if (title) title.textContent = `Edit Club for ${ids.length} selected boat${ids.length !== 1 ? 's' : ''}`;
    document.getElementById('edit-club-status').textContent = '';

    // Populate club selector with No Club + all clubs
    const clubsResp = await fetchJson('/api/clubs?limit=500');
    const noClubOpt = document.createElement('option');
    noClubOpt.value = '';
    noClubOpt.textContent = '— No Club —';
    const opts = (clubsResp && clubsResp.items ? clubsResp.items : []).map(c => {
        const o = document.createElement('option');
        o.value = c.id;
        o.textContent = c.shortName ? `${c.shortName} — ${c.id}` : c.id;
        return o;
    });
    sel.replaceChildren(noClubOpt, ...opts);

    // Pre-select common club if all selected boats share one
    const clubs = ids.map(id => (state.selectedData.boats.get(id) || {}).clubId || '');
    const common = clubs.every(c => c === clubs[0]) ? clubs[0] : '';
    sel.value = common;

    applyMergeAuthState();
    panel.style.display = '';
}

function hideEditClubPanel() {
    const panel = document.getElementById('edit-club-panel-boats');
    if (panel) panel.style.display = 'none';
}

async function saveBoatClubBulk() {
    if (!isWriteAllowed()) return;
    const ids = Array.from(state.selected.boats);
    const clubId = document.getElementById('edit-club-select').value.trim() || null;
    const statusEl = document.getElementById('edit-club-status');
    statusEl.textContent = `Saving ${ids.length} boat${ids.length !== 1 ? 's' : ''}…`;

    let saved = 0, failed = 0;
    for (const boatId of ids) {
        const result = await fetchJson('/api/boats/edit', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({boatId, clubId})
        });
        if (result && result.ok) saved++;
        else failed++;
    }

    statusEl.textContent = failed === 0
        ? `Saved ${saved} boat${saved !== 1 ? 's' : ''}.`
        : `${saved} saved, ${failed} failed — see console.`;
    clearSelection('boats');
    loadList('boats', 0);
}

async function requestBoatClubBulk() {
    const ids = Array.from(state.selected.boats);
    const clubId = document.getElementById('edit-club-select').value.trim() || null;
    const email = document.getElementById('edit-club-email')?.value.trim() || '';
    const message = document.getElementById('edit-club-message')?.value.trim() || '';
    const statusEl = document.getElementById('edit-club-status');
    statusEl.textContent = `Submitting request for ${ids.length} boat${ids.length !== 1 ? 's' : ''}…`;

    let sent = 0, failed = 0;
    for (const boatId of ids) {
        const result = await fetchJson('/api/boats/edit-request', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({boatId, clubId, ...(email && {email}), ...(message && {message})})
        });
        if (result && result.ok) sent++;
        else failed++;
    }

    statusEl.textContent = failed === 0
        ? `Request submitted for ${sent} boat${sent !== 1 ? 's' : ''}.`
        : `${sent} submitted, ${failed} failed — see console.`;
    if (failed === 0) clearSelection('boats');
}

async function saveBoatEdit() {
    if (!isWriteAllowed() || !editingBoatId) return;
    const statusEl = document.getElementById('edit-status-boats');
    statusEl.textContent = 'Saving…';

    const clubVal = document.getElementById('edit-boat-club').value.trim();
    const body = {
        boatId: editingBoatId,
        sailNumber: document.getElementById('edit-boat-sail').value.trim(),
        name: document.getElementById('edit-boat-name').value.trim(),
        designId: document.getElementById('edit-boat-design').value.trim(),
        clubId: clubVal || null
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
    clearSelection('boats');
    loadList('boats', 0);
    if (result.newBoatId) loadDetail('boats', result.newBoatId);
}

async function requestBoatEdit() {
    if (!editingBoatId) return;
    const statusEl = document.getElementById('edit-status-boats');
    statusEl.textContent = 'Submitting request…';

    const email = document.getElementById('edit-email')?.value.trim() || '';
    const message = document.getElementById('edit-message')?.value.trim() || '';
    const clubValReq = document.getElementById('edit-boat-club').value.trim();
    const body = {
        boatId: editingBoatId,
        sailNumber: document.getElementById('edit-boat-sail').value.trim(),
        name: document.getElementById('edit-boat-name').value.trim(),
        designId: document.getElementById('edit-boat-design').value.trim(),
        clubId: clubValReq || null,
        ...(email && { email }),
        ...(message && { message })
    };

    const result = await fetchJson('/api/boats/edit-request', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });

    if (result && result.ok) {
        statusEl.textContent = 'Request recorded.';
        clearSelection('boats');
    } else {
        statusEl.textContent = 'Failed to record request — see console.';
    }
}

// ---- Edit design ----

let editingDesignId = null;

function showEditDesignPanel() {
    const ids = Array.from(state.selected.designs);
    if (ids.length !== 1) return;
    const item = state.selectedData.designs.get(ids[0]);
    if (!item) return;
    editingDesignId = item.id;
    const title = document.getElementById('edit-panel-title-designs');
    title.textContent = isWriteAllowed()
        ? ('Edit Design ' + item.id)
        : ('Request edit for Design ' + item.id);
    document.getElementById('edit-design-id').value   = item.id || '';
    document.getElementById('edit-design-name').value = item.canonicalName || '';
    const boats = item.boats != null ? item.boats : 0;
    const warn = document.getElementById('edit-design-warning');
    warn.textContent = boats > 0
        ? 'Changing the ID will rename ' + boats + ' boat' + (boats !== 1 ? 's' : '')
          + ' of this design; an alias entry will be added so future imports still resolve correctly.'
        : '';
    document.getElementById('edit-status-designs').textContent = '';
    document.getElementById('edit-panel-designs').style.display = '';
    applyPanelAuthState('designs', 'edit-design', isWriteAllowed());
}

function hideEditDesignPanel() {
    const panel = document.getElementById('edit-panel-designs');
    if (panel) panel.style.display = 'none';
    editingDesignId = null;
}

function buildDesignEditBody() {
    const newId = document.getElementById('edit-design-id').value.trim();
    const newName = document.getElementById('edit-design-name').value.trim();
    return { designId: editingDesignId, newId, canonicalName: newName };
}

async function saveDesignEdit() {
    if (!isWriteAllowed() || !editingDesignId) return;
    const statusEl = document.getElementById('edit-status-designs');
    statusEl.textContent = 'Saving…';
    const result = await fetchJson('/api/designs/edit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildDesignEditBody())
    });
    if (!result || !result.ok) {
        statusEl.textContent = 'Save failed: ' + ((result && result.error) || 'see console');
        return;
    }
    let msg = result.noop ? 'No change.' : 'Saved.';
    if (result.idChanged)   msg += ' ID → ' + result.newDesignId + '.';
    if (result.nameChanged) msg += ' Name updated.';
    if (result.updatedBoats > 0)
        msg += ' ' + result.updatedBoats + ' boat' + (result.updatedBoats !== 1 ? 's' : '') + ' re-keyed.';
    if (result.updatedRaces > 0)
        msg += ' ' + result.updatedRaces + ' race(s).';
    statusEl.textContent = msg;
    clearSelection('designs');
    loadList('designs', 0);
}

async function requestDesignEdit() {
    if (!editingDesignId) return;
    const statusEl = document.getElementById('edit-status-designs');
    const email = document.getElementById('edit-email-designs')?.value.trim() || '';
    const message = document.getElementById('edit-message-designs')?.value.trim() || '';
    const body = buildDesignEditBody();
    if (email) body.email = email;
    if (message) body.message = message;
    statusEl.textContent = 'Submitting request…';
    const result = await fetchJson('/api/designs/edit-request', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });
    if (result && result.ok) {
        statusEl.textContent = 'Request recorded.';
        clearSelection('designs');
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
    // Add an "All" option (sentinel "__all__") when there's more than one division.
    if (divisions.length > 1) divisions.push({value: '__all__', label: 'All'});
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
    loadRaceHandicapCalc(raceId, preferred);
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
    const divName = document.getElementById('race-division-select').value;
    loadRaceDivChart(currentDivRaceId, divName);
    applyRaceCalcDivision(divName);
}

function onRaceRfChange() {
    showRaceRfLine = document.getElementById('race-show-rf').checked;
    sessionStorage.setItem(RACE_RF_KEY, String(showRaceRfLine));
    onRaceDivisionChange();
}

function onRaceErrorBarsChange() {
    showRaceErrorBars = document.getElementById('race-show-error-bars').checked;
    sessionStorage.setItem(RACE_ERR_KEY, String(showRaceErrorBars));
    onRaceDivisionChange();
}

function onRaceTrendChange() {
    showRaceTrendLine = document.getElementById('race-show-trend').checked;
    sessionStorage.setItem(RACE_TREND_KEY, String(showRaceTrendLine));
    onRaceDivisionChange();
}

function onRaceDivXFactorChange() {
    raceDivXFactor = document.getElementById('race-div-xfactor').value;
    sessionStorage.setItem(RACE_DIV_XFACTOR_KEY, raceDivXFactor);
    if (lastRaceDivData) renderDivisionChart(lastRaceDivData);
}

function onSeriesOverallTrendChange(cb) {
    showSeriesOverallTrend = cb.checked;
    sessionStorage.setItem(SERIES_OVERALL_TREND_KEY, String(showSeriesOverallTrend));
    onSeriesDivisionChange();
}

function onShowLegendChange(cb) {
    showLegend = cb.checked;
    sessionStorage.setItem(SHOW_LEGEND_KEY, String(showLegend));
    ['race-show-legend', 'series-show-legend'].forEach(id => {
        const el = document.getElementById(id);
        if (el && el !== cb) el.checked = showLegend;
    });
    const update = legendLayoutSettings();
    ['race-division-chart', 'series-chart'].forEach(id => {
        const div = document.getElementById(id);
        if (div && div.data) Plotly.relayout(id, update);
    });
}

// Returns layout fragments controlling the legend. When shown, the legend is overlaid
// in the chart's top-right corner so toggling it does not change the plot area's height.
function legendLayoutSettings() {
    if (showLegend) {
        return {
            showlegend: true,
            legend: {
                x: 0.99, y: 0.99,
                xanchor: 'right', yanchor: 'top',
                bgcolor: 'rgba(255,255,255,0.85)',
                bordercolor: '#ccc',
                borderwidth: 1
            }
        };
    }
    return {showlegend: false};
}

async function loadRaceDivChart(raceId, divisionName) {
    if (!raceId || divisionName == null) return;
    const params = new URLSearchParams({ raceId, divisionName });
    const data = await fetchJson('/api/comparison/division?' + params);
    if (!data || !data.finishers || data.finishers.length === 0) {
        document.getElementById('division-section-races').style.display = 'none';
        Plotly.purge('race-division-chart');
        return;
    }
    const VARIANT_LABELS = { spin: 'Spin', nonSpin: 'Non-Spin', twoHanded: 'Two-Handed', mixed: 'MIXED' };
    const labelEl = document.getElementById('race-variant-label');
    if (labelEl) labelEl.textContent = 'Variant: ' + (VARIANT_LABELS[data.divisionVariant] ?? data.divisionVariant ?? '');

    // Show note about excluded finishers
    const plotted = data.finishers.filter(f => f.pf != null && f.elapsed > 0).length;
    const noteEl = document.getElementById('race-division-note');
    if (noteEl) {
        const total = data.totalFinishers ?? data.finishers.length;
        if (plotted < total) {
            const apiExcluded = total - data.finishers.length;
            const noPf = data.finishers.length - plotted;
            let parts = [];
            if (apiExcluded > 0) parts.push(apiExcluded + ' no data');
            if (noPf > 0) parts.push(noPf + ' no PF');
            noteEl.textContent = 'Showing ' + plotted + ' of ' + total + ' finishers (' + parts.join(', ') + ')';
        } else {
            noteEl.textContent = '';
        }
    }

    renderDivisionChart(data);
}

function renderDivisionChart(data) {
    lastRaceDivData = data;
    const allFinishers = (data.finishers || []).filter(f => f.pf != null && f.elapsed > 0);
    if (allFinishers.length === 0) return;

    // Allocated handicaps from the race-tab calculator (one calculator covers all
    // race boats, so this map is global across divisions).
    const allocByBoat = new Map();
    document.querySelectorAll('#pf-calc .pf-calc-input').forEach(inp => {
        const v = parseFloat(inp.value);
        if (!isNaN(v)) allocByBoat.set(inp.dataset.boatId, v);
    });

    // Group by division. Backend includes a "division" field on each finisher when
    // called with __all__; for a single-division request all finishers share the
    // same key and we end up with one group.
    const groups = new Map();
    allFinishers.forEach(f => {
        const k = f.division || '';
        if (!groups.has(k)) groups.set(k, []);
        groups.get(k).push(f);
    });
    const groupEntries = [...groups.entries()];
    const isMulti = groupEntries.length > 1;

    // X-factor selector options reflect data across all groups.
    const anyRf = allFinishers.some(f => f.rf != null && f.rfCorrected != null);
    const anyAlloc = allFinishers.some(f => allocByBoat.has(f.boatId));
    const xSelect = document.getElementById('race-div-xfactor');
    if (xSelect) {
        const opts = ['---', 'PF',
            ...(anyRf ? ['RF'] : []),
            ...(anyAlloc ? ['Allocated'] : [])
        ];
        if (!opts.includes(raceDivXFactor)) raceDivXFactor = '---';
        if (xSelect.options.length !== opts.length ||
            [...xSelect.options].map(o => o.value).join() !== opts.join()) {
            xSelect.innerHTML = opts.map(o => `<option value="${o}">${o}</option>`).join('');
        }
        xSelect.value = raceDivXFactor;
    }

    // Vertical error bars on corrected times: factor uncertainty propagates multiplicatively
    // to the corrected time — e.g. PF_upper_time = elapsed * pf_upper / 60
    function yErrArrays(rows, factorKey, weightKey) {
        const plus = rows.map(f => {
            if (!showRaceErrorBars || !f[weightKey] || !f[factorKey]) return 0;
            const b = errorBounds(f[factorKey], f[weightKey]);
            return b ? f.elapsed / 60 * (b.upper - f[factorKey]) : 0;
        });
        const minus = rows.map(f => {
            if (!showRaceErrorBars || !f[weightKey] || !f[factorKey]) return 0;
            const b = errorBounds(f[factorKey], f[weightKey]);
            return b ? f.elapsed / 60 * (f[factorKey] - b.lower) : 0;
        });
        return { type: 'data', array: plus, arrayminus: minus,
                 visible: showRaceErrorBars, thickness: 1.5, width: 4 };
    }

    function hoverTexts(label, times, labels) {
        return times.map((t, i) =>
            t != null ? `${esc(labels[i])}<br>${label}: ${fmtTime(t * 60)}` : '');
    }

    function buildTrendTrace(plotPts, name, color) {
        const pts = plotPts.filter(p => p.x != null && p.y != null);
        if (pts.length < 2) return null;
        const n = pts.length;
        const sx = pts.reduce((s, p) => s + p.x, 0);
        const sy = pts.reduce((s, p) => s + p.y, 0);
        const sxx = pts.reduce((s, p) => s + p.x * p.x, 0);
        const sxy = pts.reduce((s, p) => s + p.x * p.y, 0);
        const denom = n * sxx - sx * sx;
        if (denom === 0) return null;
        const slope = (n * sxy - sx * sy) / denom;
        const intercept = (sy - slope * sx) / n;
        const xMin = Math.min(...pts.map(p => p.x));
        const xMax = Math.max(...pts.map(p => p.x));
        return {
            x: [xMin, xMax],
            y: [slope * xMin + intercept, slope * xMax + intercept],
            mode: 'lines', type: 'scatter', name,
            line: {dash: 'dashdot', color, width: 1.5},
            showlegend: false,
            hoverinfo: 'skip'
        };
    }

    // Build all traces + annotations for one division group. With "All" selected,
    // a separate group per division keeps lines from joining across divisions and
    // gives each its own trend line; lighten shifts the colour palette to a paler
    // shade for later divisions so divisions of the same race share a hue.
    function buildGroupTraces(groupFinishers, lighten, divLabel) {
        const finishers = groupFinishers;
        const elapsedColor = lightenColor('#555', lighten);
        const pfColor = lightenColor('#2255aa', lighten);
        const rfColor = lightenColor('#c47900', lighten);
        const allocColor = lightenColor('#a04020', lighten);
        const suffix = divLabel ? ` — ${divLabel}` : '';

        const rfFinishers = finishers
            .map(f => ({f, rf: f.rf, rfCorrMin: f.rfCorrected != null ? f.rfCorrected / 60 : null}))
            .filter(o => o.rf != null && o.rfCorrMin != null)
            .sort((a, b) => a.rf - b.rf);
        const allocPts = finishers
            .filter(f => allocByBoat.has(f.boatId))
            .map(f => ({
                f,
                name: f.sailNumber ? `${f.sailNumber} ${f.name}` : f.name,
                handicap: allocByBoat.get(f.boatId),
                correctedMin: f.elapsed * allocByBoat.get(f.boatId) / 60
            }))
            .sort((a, b) => a.handicap - b.handicap);

        const traces = [];
        let annotations = [];
        let xAxisTitle;

        if (raceDivXFactor === '---') {
            // Natural mode: elapsed always at x=PF; corrected traces use their own factor.
            const xs = finishers.map(f => f.pf);
            const names = finishers.map(f => f.sailNumber ? `${f.sailNumber} ${f.name}` : f.name);
            const elapsed = finishers.map(f => f.elapsed / 60);
            const pfCorr = finishers.map(f => f.pfCorrected != null ? f.pfCorrected / 60 : null);

            const rfXs = rfFinishers.map(o => o.rf);
            const rfCorr = rfFinishers.map(o => o.rfCorrMin);
            const rfNames = rfFinishers.map(o => o.f.sailNumber ? `${o.f.sailNumber} ${o.f.name}` : o.f.name);
            const rfCustom = rfFinishers.map(o => ({boatId: o.f.boatId}));

            const boatCustom = finishers.map(f => ({boatId: f.boatId}));

            traces.push(
                {
                    x: xs, y: elapsed,
                    mode: 'lines+markers', type: 'scatter', name: 'Elapsed' + suffix,
                    legendgroup: 'elapsed' + suffix,
                    line: {dash: 'dash', color: elapsedColor, width: 1.5}, marker: {size: 7},
                    text: hoverTexts('Elapsed', elapsed, names),
                    hoverinfo: 'text', customdata: boatCustom
                },
                {
                    x: xs, y: pfCorr, mode: 'lines+markers', type: 'scatter', name: 'PF corrected' + suffix,
                    legendgroup: 'pf' + suffix,
                    line: {dash: 'solid', color: pfColor, width: 2}, marker: {size: 7},
                    error_y: yErrArrays(finishers, 'pf', 'rfWeight'),
                    text: hoverTexts('PF corrected', pfCorr, names), hoverinfo: 'text', customdata: boatCustom
                }
            );
            if (showRaceRfLine && rfFinishers.length > 0) {
                traces.push({
                    x: rfXs, y: rfCorr, mode: 'lines+markers', type: 'scatter', name: 'RF corrected' + suffix,
                    legendgroup: 'rf' + suffix,
                    line: {dash: 'dot', color: rfColor, width: 1.5}, marker: {size: 7},
                    error_y: yErrArrays(rfFinishers.map(o => o.f), 'rf', 'rfWeight'),
                    text: hoverTexts('RF corrected', rfCorr, rfNames), hoverinfo: 'text', customdata: rfCustom
                });
            }

            addPodiumTraces(traces, finishers, xs, pfCorr, pfColor);

            if (allocPts.length > 0) {
                const allocXs = allocPts.map(p => p.handicap);
                const allocYs = allocPts.map(p => p.correctedMin);
                traces.push({
                    x: allocXs, y: allocYs,
                    mode: 'lines+markers', type: 'scatter',
                    name: 'Allocated handicap corrected' + suffix,
                    legendgroup: 'alloc' + suffix,
                    line: {dash: 'longdash', color: allocColor, width: 2},
                    marker: {size: 8, symbol: 'square'},
                    text: allocPts.map(p =>
                        `${esc(p.name)}<br>Allocated: ${p.handicap.toFixed(4)}`
                        + `<br>Corrected: ${fmtTime(p.correctedMin * 60)}`),
                    hoverinfo: 'text',
                    customdata: allocPts.map(p => ({boatId: p.f.boatId}))
                });
                addAllocPodiumTraces(traces, allocPts, allocXs, allocYs, allocColor);
            }

            if (showRaceTrendLine) {
                const elapsedTrend = buildTrendTrace(
                    finishers.map((f, i) => ({x: xs[i], y: elapsed[i]})), 'Elapsed trend' + suffix, elapsedColor);
                if (elapsedTrend) traces.push(elapsedTrend);
                const pfTrend = buildTrendTrace(
                    finishers.map((f, i) => ({x: xs[i], y: pfCorr[i]})), 'PF corr trend' + suffix, pfColor);
                if (pfTrend) traces.push(pfTrend);
                if (showRaceRfLine && rfFinishers.length > 0) {
                    const rfTrend = buildTrendTrace(
                        rfFinishers.map((o, i) => ({x: rfXs[i], y: rfCorr[i]})),
                        'RF corr trend' + suffix, rfColor);
                    if (rfTrend) traces.push(rfTrend);
                }
                if (allocPts.length > 0) {
                    const allocTrend = buildTrendTrace(
                        allocPts.map(p => ({x: p.handicap, y: p.correctedMin})),
                        'Allocated corr trend' + suffix, allocColor);
                    if (allocTrend) traces.push(allocTrend);
                }
            }

            annotations = finishers.map((f, i) => {
                const ys = [elapsed[i], pfCorr[i]].filter(v => v != null);
                return {
                    x: xs[i], y: Math.max(...ys), text: f.name, textangle: -90,
                    xanchor: 'center', yanchor: 'bottom', yshift: 6,
                    showarrow: false, cliponaxis: false, font: {size: 11}
                };
            });

            xAxisTitle = showRaceRfLine && rfFinishers.length > 0 ? 'PF / RF' : 'PF';

        } else {
            // Common-factor mode: all traces use the same x-axis factor, so all dots for
            // a given boat form a vertical line.
            const getX = f => {
                if (raceDivXFactor === 'RF') return f.rf;
                if (raceDivXFactor === 'Allocated') return allocByBoat.get(f.boatId);
                return f.pf;
            };
            const plotFinishers = finishers
                .filter(f => getX(f) != null)
                .sort((a, b) => getX(a) - getX(b));
            if (plotFinishers.length === 0) {
                xAxisTitle = raceDivXFactor === 'Allocated' ? 'Allocated Handicap' : raceDivXFactor;
                return {traces, annotations, xAxisTitle};
            }

            const xs = plotFinishers.map(getX);
            const names = plotFinishers.map(f => f.sailNumber ? `${f.sailNumber} ${f.name}` : f.name);
            const elapsed = plotFinishers.map(f => f.elapsed / 60);
            const pfCorr = plotFinishers.map(f => f.pfCorrected != null ? f.pfCorrected / 60 : null);
            const rfCorr = plotFinishers.map(f => f.rfCorrected != null ? f.rfCorrected / 60 : null);
            const allocCorr = plotFinishers.map(f => {
                const h = allocByBoat.get(f.boatId);
                return h != null ? f.elapsed * h / 60 : null;
            });
            const allocFiltered = plotFinishers
                .map((f, i) => ({
                    f, x: xs[i], y: allocCorr[i],
                    name: names[i],
                    handicap: allocByBoat.get(f.boatId),
                    correctedMin: allocCorr[i]
                }))
                .filter(p => p.y != null);
            const boatCustom = plotFinishers.map(f => ({boatId: f.boatId}));

            traces.push(
                {
                    x: xs, y: elapsed, mode: 'lines+markers', type: 'scatter', name: 'Elapsed' + suffix,
                    legendgroup: 'elapsed' + suffix,
                    line: {dash: 'dash', color: elapsedColor, width: 1.5}, marker: {size: 7},
                    text: hoverTexts('Elapsed', elapsed, names), hoverinfo: 'text', customdata: boatCustom
                },
                {
                    x: xs, y: pfCorr, mode: 'lines+markers', type: 'scatter', name: 'PF corrected' + suffix,
                    legendgroup: 'pf' + suffix,
                    line: {dash: 'solid', color: pfColor, width: 2}, marker: {size: 7},
                    error_y: yErrArrays(plotFinishers, 'pf', 'rfWeight'),
                    text: hoverTexts('PF corrected', pfCorr, names), hoverinfo: 'text', customdata: boatCustom
                }
            );
            if (showRaceRfLine && rfCorr.some(v => v != null)) {
                traces.push({
                    x: xs, y: rfCorr, mode: 'lines+markers', type: 'scatter', name: 'RF corrected' + suffix,
                    legendgroup: 'rf' + suffix,
                    line: {dash: 'dot', color: rfColor, width: 1.5}, marker: {size: 7},
                    error_y: yErrArrays(plotFinishers, 'rf', 'rfWeight'),
                    text: hoverTexts('RF corrected', rfCorr, names), hoverinfo: 'text', customdata: boatCustom
                });
            }
            if (allocFiltered.length > 0) {
                traces.push({
                    x: allocFiltered.map(p => p.x),
                    y: allocFiltered.map(p => p.y),
                    mode: 'lines+markers', type: 'scatter',
                    name: 'Allocated handicap corrected' + suffix,
                    legendgroup: 'alloc' + suffix,
                    line: {dash: 'longdash', color: allocColor, width: 2},
                    marker: {size: 8, symbol: 'square'},
                    text: allocFiltered.map(p =>
                        `${esc(p.name)}<br>Allocated: ${p.handicap.toFixed(4)}<br>Corrected: ${fmtTime(p.y * 60)}`),
                    hoverinfo: 'text',
                    customdata: allocFiltered.map(p => ({boatId: p.f.boatId}))
                });
            }

            addPodiumTraces(traces, plotFinishers, xs, pfCorr, pfColor);
            if (allocFiltered.length > 0) {
                addAllocPodiumTraces(traces, allocFiltered,
                    allocFiltered.map(p => p.x), allocFiltered.map(p => p.y), allocColor);
            }

            if (showRaceTrendLine) {
                const elapsedTrend = buildTrendTrace(
                    plotFinishers.map((f, i) => ({x: xs[i], y: elapsed[i]})),
                    'Elapsed trend' + suffix, elapsedColor);
                if (elapsedTrend) traces.push(elapsedTrend);
                const pfTrend = buildTrendTrace(
                    plotFinishers.map((f, i) => ({x: xs[i], y: pfCorr[i]})),
                    'PF corr trend' + suffix, pfColor);
                if (pfTrend) traces.push(pfTrend);
                if (showRaceRfLine && rfCorr.some(v => v != null)) {
                    const rfTrend = buildTrendTrace(
                        plotFinishers.map((f, i) => ({x: xs[i], y: rfCorr[i]})),
                        'RF corr trend' + suffix, rfColor);
                    if (rfTrend) traces.push(rfTrend);
                }
                if (allocCorr.some(v => v != null)) {
                    const allocTrend = buildTrendTrace(
                        plotFinishers.map((f, i) => ({x: xs[i], y: allocCorr[i]})),
                        'Allocated corr trend' + suffix, allocColor);
                    if (allocTrend) traces.push(allocTrend);
                }
            }

            annotations = plotFinishers.map((f, i) => {
                const ys = [elapsed[i], pfCorr[i]].filter(v => v != null);
                return {
                    x: xs[i], y: Math.max(...ys), text: f.name, textangle: -90,
                    xanchor: 'center', yanchor: 'bottom', yshift: 6,
                    showarrow: false, cliponaxis: false, font: {size: 11}
                };
            });

            xAxisTitle = raceDivXFactor === 'Allocated' ? 'Allocated Handicap' : raceDivXFactor;
        }
        return {traces, annotations, xAxisTitle};
    }

    let allTraces = [];
    let allAnnotations = [];
    let xAxisTitle = 'PF';
    groupEntries.forEach(([divName, groupFinishers], divIdx) => {
        const lighten = isMulti ? Math.min(0.5, divIdx * 0.18) : 0;
        const divLabel = isMulti ? (divName || 'Results') : null;
        const r = buildGroupTraces(groupFinishers, lighten, divLabel);
        allTraces = allTraces.concat(r.traces);
        allAnnotations = allAnnotations.concat(r.annotations);
        xAxisTitle = r.xAxisTitle;
    });

    const layout = {
        xaxis: {
            title: xAxisTitle,
            rangemode: getDivChartXFromZero() ? 'tozero' : 'normal'
        },
        yaxis: { title: 'Time (min)', tickformat: '.1f',
                 rangemode: getDivChartYFromZero() ? 'tozero' : 'normal' },
        ...legendLayoutSettings(),
        margin: {t: 30, b: 50, l: 60, r: 20},
        hovermode: 'closest',
        annotations: allAnnotations
    };

    document.getElementById('division-section-races').style.display = '';
    Plotly.react('race-division-chart', allTraces, layout, {responsive: true});

    const raceDivDiv = document.getElementById('race-division-chart');
    raceDivDiv.removeAllListeners && raceDivDiv.removeAllListeners('plotly_click');
    raceDivDiv.on('plotly_click', (eventData) => {
        if (!eventData.points || !eventData.points.length) return;
        const pt = eventData.points[0];
        if (!pt.customdata || !pt.customdata.boatId) return;
        window.location.href = 'data.html?' +
            new URLSearchParams({ tab: 'boats', boatId: pt.customdata.boatId });
    });
}

// ---- Handicap calculator on the races tab ----
//
// Calculator is initialised lazily once the first race is selected. Allocated handicaps
// persist in sessionStorage under ALLOCATED_HANDICAPS_KEY and are shared with the series
// tab and the boat-comparison page, so entries follow the user across views.

const ALLOCATED_HANDICAPS_KEY = 'pf.allocated.handicaps';

let raceCalcController = null;

function raceCalc() {
    if (raceCalcController) return raceCalcController;
    raceCalcController = HandicapCalc.create({
        section: document.getElementById('pf-calc'),
        table: document.querySelector('#pf-calc table'),
        showBestFit: false,
        sessionKey: ALLOCATED_HANDICAPS_KEY,
        urlInput: document.getElementById('handicap-url'),
        fetchBtn: document.getElementById('fetch-handicaps-btn'),
        fetchStatus: document.getElementById('fetch-status'),
        fileInput: document.getElementById('handicap-file'),
        fileStatus: document.getElementById('file-status'),
        downloadBtn: document.getElementById('download-handicaps-btn'),
        downloadStatus: document.getElementById('download-status'),
        onChange: () => {
            if (lastRaceDivData) renderDivisionChart(lastRaceDivData);
        }
    });
    const clearBtn = document.getElementById('clear-handicaps-btn');
    if (clearBtn) clearBtn.addEventListener('click', () => raceCalcController.clearAll());
    return raceCalcController;
}

// Cached /api/comparison/race-boats response so division changes can re-filter
// without a network round trip.
let lastRaceBoatsResponse = null;

async function loadRaceHandicapCalc(raceId, divName) {
    if (!raceId) return;
    lastRaceBoatsResponse = await fetchJson('/api/comparison/race-boats?raceId=' + encodeURIComponent(raceId));
    applyRaceCalcDivision(divName);
}

function applyRaceCalcDivision(divName) {
    const data = lastRaceBoatsResponse;
    if (!data || !data.boats) return;
    const allDivisions = !divName || divName === '__all__';
    const boats = data.boats
        .filter(b => allDivisions || (b.division || '') === divName)
        .map(b => ({
            id: b.id,
            name: b.sailNumber ? `${b.sailNumber} ${b.name}` : b.name,
            sailNumber: b.sailNumber || null,
            boatName: b.name || null,
            division: b.division || null,
            pf: b.pf,
            rf: b.rf,
            bestFit: null
        }));
    raceCalc().setBoats(boats);
    // Re-render division chart so the allocated line picks up any applied entries.
    if (lastRaceDivData) renderDivisionChart(lastRaceDivData);
}

// ---- Series chart ----

let seriesChartData = null;  // last loaded series chart response
let seriesCurrentDivision = null;  // division name currently displayed
let seriesPfCalcController = null;

function seriesPfCalc() {
    if (seriesPfCalcController) return seriesPfCalcController;
    seriesPfCalcController = HandicapCalc.create({
        section: document.getElementById('series-pf-calc'),
        table: document.querySelector('#series-pf-calc table'),
        showBestFit: false,
        sessionKey: ALLOCATED_HANDICAPS_KEY,
        urlInput: document.getElementById('series-handicap-url'),
        fetchBtn: document.getElementById('series-fetch-handicaps-btn'),
        fetchStatus: document.getElementById('series-fetch-status'),
        fileInput: document.getElementById('series-handicap-file'),
        fileStatus: document.getElementById('series-file-status'),
        downloadBtn: document.getElementById('series-download-handicaps-btn'),
        downloadStatus: document.getElementById('series-download-status'),
        onChange: () => {
            // Re-render only the chart; the calc has already updated its own DOM.
            if (seriesChartData && seriesCurrentDivision != null)
                renderSeriesChartForDivision(seriesCurrentDivision, {refreshCalc: false});
        }
    });
    const clearBtn = document.getElementById('series-clear-handicaps-btn');
    if (clearBtn) clearBtn.addEventListener('click', () => seriesPfCalcController.clearAll());
    return seriesPfCalcController;
}

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
        seriesPfCalc().setBoats([]);
        return;
    }

    seriesChartData = data;
    label.textContent = (data.seriesName || seriesId) + ' — ' + data.club;

    // Collect unique division names across all races, preserving first-seen order
    const divNameSet = new Set();
    data.races.forEach(r => r.divisions.forEach(d => divNameSet.add(d.name || '')));
    const divNames = [...divNameSet];

    const sel = document.getElementById('series-division-select');
    const opts = divNames.map(n => `<option value="${esc(n)}">${esc(n || '—')}</option>`);
    if (divNames.length > 1) opts.push(`<option value="__all__">All</option>`);
    sel.innerHTML = opts.join('');

    renderSeriesChartForDivision(divNames[0] || '');
    section.scrollIntoView({behavior: 'smooth', block: 'start'});
}

function onSeriesDivisionChange() {
    const divName = document.getElementById('series-division-select').value;
    renderSeriesChartForDivision(divName);
}

// Build a unique-by-boatId list of {id, name, sailNumber, boatName, pf, rf} for the calc.
// When divName is "__all__", aggregates boats from every division in the series.
function buildSeriesCalcBoats(data, divName) {
    const allDivisions = divName === '__all__';
    const seen = new Map();
    data.races.forEach(race => {
        const divs = allDivisions ? race.divisions
            : race.divisions.filter(d => (d.name || '') === divName);
        divs.forEach(div => {
            div.finishers.forEach(f => {
                if (!f.boatId || f.pf == null || seen.has(f.boatId)) return;
                seen.set(f.boatId, {
                    id: f.boatId,
                    name: f.sailNumber ? `${f.sailNumber} ${f.name || ''}`.trim() : (f.name || f.boatId),
                    sailNumber: f.sailNumber || null,
                    boatName: f.name || null,
                    pf: f.pf,
                    rf: f.rf != null ? f.rf : null,
                    bestFit: null
                });
            });
        });
    });
    return [...seen.values()];
}

// Finishers for a given race under the current division selection. When divName is
// "__all__" combines finishers from every division.
function getRaceFinishers(race, divName) {
    if (divName === '__all__')
        return (race.divisions || []).flatMap(d => d.finishers || []);
    const div = (race.divisions || []).find(d => (d.name || '') === divName);
    return div ? (div.finishers || []) : [];
}

function renderSeriesChartForDivision(divName, opts) {
    const data = seriesChartData;
    if (!data) return;
    seriesCurrentDivision = divName;

    if (!opts || opts.refreshCalc !== false) {
        seriesPfCalc().setBoats(buildSeriesCalcBoats(data, divName));
    }
    const allocByBoat = seriesPfCalc().getEnteredValues();

    // Colour palette for races
    const raceColors = [
        '#2255aa', '#c47900', '#2ca02c', '#d62728', '#9467bd',
        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
        '#ff7f0e', '#1f77b4', '#aec7e8', '#ffbb78', '#98df8a'
    ];

    const podiumSymbols = ['star', 'diamond', 'triangle-up'];
    const podiumSizes = [14, 12, 11];
    const podiumLabels = ['1st', '2nd', '3rd'];

    // In "All" mode each (race, division) gets its own line so divisions don't join.
    // Divisions of the same race share the race's hue, lightening with division index.
    const allDivisions = divName === '__all__';
    const seriesDivOrder = allDivisions ? (() => {
        const seen = new Set();
        const ordered = [];
        data.races.forEach(r => (r.divisions || []).forEach(d => {
            const n = d.name || '';
            if (!seen.has(n)) {
                seen.add(n);
                ordered.push(n);
            }
        }));
        return ordered;
    })() : [];

    const traces = [];
    const allocatedLegendShownFor = new Set();
    const podiumLegendShownFor = new Set();
    data.races.forEach((race, raceIdx) => {
        const baseColor = raceColors[raceIdx % raceColors.length];
        const raceLabel = race.raceName || race.date || race.raceId;

        const divsToPlot = allDivisions
            ? (race.divisions || [])
            : (race.divisions || []).filter(d => (d.name || '') === divName);

        divsToPlot.forEach(div => {
            const groupDivName = div.name || '';
            const lighten = allDivisions
                ? Math.min(0.5, seriesDivOrder.indexOf(groupDivName) * 0.18)
                : 0;
            const color = lightenColor(baseColor, lighten);
            const groupKey = allDivisions ? `race-${raceIdx}-div-${groupDivName}` : `race-${raceIdx}`;
            const traceName = allDivisions
                ? `${raceLabel} — ${groupDivName || 'Results'}`
                : raceLabel;

            const finishers = (div.finishers || [])
                .filter(f => f.pf != null && f.pfCorrected != null)
                .slice()
                .sort((a, b) => a.pf - b.pf);
            if (finishers.length === 0) return;

            const sorted = finishers.map((f, i) => ({i, t: f.pfCorrected}))
                .sort((a, b) => a.t - b.t);

            const xs = finishers.map(f => f.pf);
            const ys = finishers.map(f => f.pfCorrected / 60);
            const texts = finishers.map(f =>
                `${f.sailNumber ? f.sailNumber + ' ' : ''}${esc(f.name || '')}<br>${esc(raceLabel)}`
                + (allDivisions ? `<br>Division: ${esc(groupDivName || 'Results')}` : '')
                + `<br>PF corrected: ${fmtTime(f.pfCorrected)}`
            );
            const boatCustom = finishers.map(f => ({boatId: f.boatId}));

            traces.push({
                x: xs, y: ys,
                mode: 'lines+markers', type: 'scatter',
                name: traceName,
                legendgroup: groupKey,
                line: {dash: 'solid', color: color, width: 1.5},
                marker: {size: 5},
                text: texts,
                hoverinfo: 'text',
                customdata: boatCustom
            });

            // Allocated-handicap corrected line for this (race, division)
            const allocPts = finishers
                .filter(f => allocByBoat.has(f.boatId) && f.elapsed != null && f.elapsed > 0)
                .map(f => ({
                    f,
                    handicap: allocByBoat.get(f.boatId),
                    correctedMin: f.elapsed * allocByBoat.get(f.boatId) / 60
                }))
                .sort((a, b) => a.f.pf - b.f.pf);
            if (allocPts.length > 0) {
                const allocLegendKey = allDivisions ? `allocated-${groupDivName}` : 'allocated';
                traces.push({
                    x: allocPts.map(p => p.f.pf),
                    y: allocPts.map(p => p.correctedMin),
                    mode: 'lines+markers', type: 'scatter',
                    name: allDivisions ? `Allocated — ${groupDivName || 'Results'}` : 'Allocated corrected',
                    legendgroup: allocLegendKey,
                    showlegend: !allocatedLegendShownFor.has(allocLegendKey),
                    line: {dash: 'dash', color: color, width: 1.5},
                    marker: {size: 5, symbol: 'square'},
                    text: allocPts.map(p =>
                        `${p.f.sailNumber ? p.f.sailNumber + ' ' : ''}${esc(p.f.name || '')}<br>${esc(raceLabel)}`
                        + (allDivisions ? `<br>Division: ${esc(groupDivName || 'Results')}` : '')
                        + `<br>Allocated: ${p.handicap.toFixed(4)}`
                        + `<br>Corrected: ${fmtTime(p.correctedMin * 60)}`),
                    hoverinfo: 'text',
                    customdata: allocPts.map(p => ({boatId: p.f.boatId}))
                });
                allocatedLegendShownFor.add(allocLegendKey);
            }

            // Podium markers (1st/2nd/3rd fastest corrected times in this group)
            for (let p = 0; p < Math.min(3, sorted.length); p++) {
                const f = finishers[sorted[p].i];
                const podiumKey = podiumLabels[p];
                traces.push({
                    x: [f.pf], y: [f.pfCorrected / 60],
                    mode: 'markers', type: 'scatter',
                    name: podiumKey,
                    legendgroup: podiumKey,
                    showlegend: !podiumLegendShownFor.has(podiumKey),
                    marker: {
                        symbol: podiumSymbols[p], size: podiumSizes[p],
                        color: color,
                        line: {color: '#fff', width: 1.5}
                    },
                    text: [`${podiumKey}: ${f.sailNumber ? f.sailNumber + ' ' : ''}${esc(f.name || '')}`
                    + `<br>${esc(raceLabel)}`
                    + (allDivisions ? `<br>Division: ${esc(groupDivName || 'Results')}` : '')
                    + `<br>PF corrected: ${fmtTime(f.pfCorrected)}`],
                    hoverinfo: 'text',
                    customdata: [{boatId: f.boatId}]
                });
                podiumLegendShownFor.add(podiumKey);
            }
        });
    });

    if (traces.length === 0) {
        Plotly.purge('series-chart');
        return;
    }

    if (showSeriesOverallTrend) {
        // One trend line per division. In "All" mode: one trend per division (combining
        // its races across the series); in single-division mode: one trend.
        const trendDivs = allDivisions ? seriesDivOrder : [divName];
        trendDivs.forEach((dn, i) => {
            const lighten = allDivisions ? Math.min(0.4, i * 0.18) : 0;
            const trend = computeSeriesOverallTrend(data, dn);
            if (trend) {
                if (allDivisions) {
                    trend.name = `Overall ${dn || 'Results'} — ${trend.name}`;
                    trend.line.color = lightenColor('#333', lighten);
                }
                traces.push(trend);
            }
            const allocTrend = computeSeriesAllocatedTrend(data, dn, allocByBoat);
            if (allocTrend) {
                if (allDivisions) {
                    allocTrend.name = `Allocated ${dn || 'Results'} — ${allocTrend.name}`;
                    allocTrend.line.color = lightenColor('#a04020', lighten);
                }
                traces.push(allocTrend);
            }
        });
    }

    const layout = {
        xaxis: {title: 'PF', rangemode: getDivChartXFromZero() ? 'tozero' : 'normal'},
        yaxis: {
            title: 'PF Corrected Time (min)', tickformat: '.1f',
            rangemode: getDivChartYFromZero() ? 'tozero' : 'normal'
        },
        ...legendLayoutSettings(),
        margin: {t: 30, b: 50, l: 60, r: 20},
        hovermode: 'closest'
    };

    Plotly.react('series-chart', traces, layout, {responsive: true});

    const seriesChartDiv = document.getElementById('series-chart');
    seriesChartDiv.removeAllListeners && seriesChartDiv.removeAllListeners('plotly_click');
    seriesChartDiv.on('plotly_click', (eventData) => {
        if (!eventData.points || !eventData.points.length) return;
        const pt = eventData.points[0];
        if (!pt.customdata || !pt.customdata.boatId) return;
        window.location.href = 'data.html?' +
            new URLSearchParams({tab: 'boats', boatId: pt.customdata.boatId});
    });
}

/**
 * Computes the series overall trend trace for the given division: the line with the
 * average OLS slope of each race's (PF, PF-corrected-minutes) points, anchored at the
 * median of all PF values and the median of all PF-corrected-minutes values across the
 * division. Returns null if there is insufficient data (fewer than two races with at
 * least two qualifying finishers each).
 */
function computeSeriesOverallTrend(data, divName) {
    const slopes = [];
    const allX = [];
    const allY = [];
    data.races.forEach(race => {
        const finishers = getRaceFinishers(race, divName)
            .filter(f => f.pf != null && f.pfCorrected != null);
        if (finishers.length < 2) return;
        const xs = finishers.map(f => f.pf);
        const ys = finishers.map(f => f.pfCorrected / 60);
        allX.push(...xs);
        allY.push(...ys);
        const s = olsSlope(xs, ys);
        if (s != null && isFinite(s)) slopes.push(s);
    });
    if (slopes.length === 0 || allX.length === 0) return null;

    const avgSlope = slopes.reduce((a, b) => a + b, 0) / slopes.length;
    const medX = median(allX);
    const medY = median(allY);
    const xMin = Math.min(...allX);
    const xMax = Math.max(...allX);
    // Line: y = avgSlope * (x - medX) + medY, across the observed X range.
    return {
        x: [xMin, xMax],
        y: [avgSlope * (xMin - medX) + medY, avgSlope * (xMax - medX) + medY],
        mode: 'lines', type: 'scatter',
        name: `Overall trend (slope ${avgSlope.toFixed(2)})`,
        line: { dash: 'dot', color: '#333', width: 3 },
        hoverinfo: 'skip'
    };
}

/**
 * Same as computeSeriesOverallTrend, but uses allocated-handicap corrected times
 * (elapsed * allocatedHandicap) as Y, plotted against PF on X. Returns null when no
 * allocated handicaps are entered, or when fewer than two races have at least two
 * allocated finishers.
 */
function computeSeriesAllocatedTrend(data, divName, allocByBoat) {
    if (!allocByBoat || allocByBoat.size === 0) return null;
    const slopes = [];
    const allX = [];
    const allY = [];
    data.races.forEach(race => {
        const finishers = getRaceFinishers(race, divName).filter(f =>
            f.pf != null && f.elapsed != null && f.elapsed > 0 && allocByBoat.has(f.boatId));
        if (finishers.length < 2) return;
        const xs = finishers.map(f => f.pf);
        const ys = finishers.map(f => f.elapsed * allocByBoat.get(f.boatId) / 60);
        allX.push(...xs);
        allY.push(...ys);
        const s = olsSlope(xs, ys);
        if (s != null && isFinite(s)) slopes.push(s);
    });
    if (slopes.length === 0 || allX.length === 0) return null;

    const avgSlope = slopes.reduce((a, b) => a + b, 0) / slopes.length;
    const medX = median(allX);
    const medY = median(allY);
    const xMin = Math.min(...allX);
    const xMax = Math.max(...allX);
    return {
        x: [xMin, xMax],
        y: [avgSlope * (xMin - medX) + medY, avgSlope * (xMax - medX) + medY],
        mode: 'lines', type: 'scatter',
        name: `Allocated trend (slope ${avgSlope.toFixed(2)})`,
        line: {dash: 'dashdot', color: '#a04020', width: 3},
        hoverinfo: 'skip'
    };
}

function olsSlope(xs, ys) {
    const n = xs.length;
    if (n < 2) return null;
    let sx = 0, sy = 0;
    for (let i = 0; i < n; i++) { sx += xs[i]; sy += ys[i]; }
    const mx = sx / n, my = sy / n;
    let num = 0, den = 0;
    for (let i = 0; i < n; i++) {
        const dx = xs[i] - mx;
        num += dx * (ys[i] - my);
        den += dx * dx;
    }
    return den > 0 ? num / den : null;
}

function median(arr) {
    const s = arr.slice().sort((a, b) => a - b);
    const n = s.length;
    if (n === 0) return 0;
    return (n % 2) ? s[(n - 1) >> 1] : (s[n / 2 - 1] + s[n / 2]) / 2;
}

// ---- Chart resizing functionality ----

const CHART_HEIGHTS_KEY = 'pf.chart.heights';

function initChartResize(chartId, defaultHeight) {
    const chartDiv = document.getElementById(chartId);
    if (!chartDiv) return;

    // Make chart container position relative for absolute positioning of handle
    chartDiv.style.position = 'relative';

    // Create resize handle
    const handle = document.createElement('div');
    handle.className = 'chart-resize-handle';
    chartDiv.appendChild(handle);

    // Get stored height or use default
    const storedHeights = JSON.parse(sessionStorage.getItem(CHART_HEIGHTS_KEY) || '{}');
    const currentHeight = storedHeights[chartId] || defaultHeight;
    chartDiv.style.height = currentHeight + 'px';

    // Minimum height is half the default
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

        // Store the new height
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

// Initialize resize handles when charts are rendered
function initChartResizers() {
    // Race division chart (default 600px, min 300px)
    initChartResize('race-division-chart', 600);

    // Series chart (default 500px, min 250px)
    initChartResize('series-chart', 500);
}

// Call this after DOM is ready
document.addEventListener('DOMContentLoaded', initChartResizers);

// ---- Debugging tools (remove in production) ----

function installDevTools() {
    if (window.pfDevTools) return;
    const s = window.pfDevTools = {
        enabled: true,
        traces: {},
        init() {
            this.enabled = true;
            this.log('--- Performance Forum Dev Tools ---');
            this.log('Version: ' + (window.PF_DEV_VERSION || 'dev build'));
            this.log('Env: ' + (window.PF_ENV || 'unknown'));
            this.log('User: ' + (window.pfAuth?.user?.email || 'guest'));
            this.log('---');
            this.patchFetch();
            this.patchXhr();
            this.patchConsole();
            this.patchTable();
            this.patchPlotly();
            this.patchHandicapCalc();
        },
        log(...args) {
            if (!this.enabled) return;
            console.log('[PF DevTools]', ...args);
        },
        group(name) {
            if (!this.enabled) return;
            console.groupCollapsed('[PF DevTools] ' + name);
        },
        groupEnd() {
            if (!this.enabled) return;
            console.groupEnd();
        },
        time(label) {
            if (!this.enabled) return;
            console.time('[PF DevTools] ' + label);
        },
        timeEnd(label) {
            if (!this.enabled) return;
            console.timeEnd('[PF DevTools] ' + label);
        },
        patchFetch() {
            if (!this.enabled) return;
            const orig = window.fetch;
            const dev = (...args) => {
                const start = performance.now();
                return orig(...args).then(res => {
                    const url = args[0];
                    const clone = res.clone();
                    clone.json().then(data => {
                        if (data && data.traceId) {
                            this.log('Fetch: ' + url);
                            this.log(' → ' + data.traceId);
                            this.log(' → ', data);
                        }
                    });
                    this.log('Fetch: ' + url + ' (' + res.status + ')');
                    this.log(' → Duration: ' + (performance.now() - start).toFixed(2) + 'ms');
                    return res;
                });
            };
            window.fetch = dev;
        },
        patchXhr() {
            if (!this.enabled) return;
            const origOpen = XMLHttpRequest.prototype.open;
            const origSend = XMLHttpRequest.prototype.send;
            const devOpen = function (...args) {
                this._url = args[1];
                this._method = args[0];
                return origOpen.apply(this, args);
            };
            const devSend = function (...args) {
                const xhr = this;
                const start = performance.now();
                const url = this._url;
                const method = this._method;
                const logResponse = (res) => {
                    if (res && res.traceId) {
                        console.log('XHR: ' + url);
                        console.log(' → ' + res.traceId);
                        console.log(' → ', res);
                    }
                };
                const onload = function () {
                    logResponse(xhr.response);
                    console.log('XHR: ' + url + ' (' + xhr.status + ')');
                    console.log(' → Duration: ' + (performance.now() - start).toFixed(2) + 'ms');
                };
                if (this.addEventListener) {
                    this.addEventListener('load', onload);
                } else {
                    this.onload = onload;
                }
                return origSend.apply(this, args);
            };
            XMLHttpRequest.prototype.open = devOpen;
            XMLHttpRequest.prototype.send = devSend;
        },
        patchConsole() {
            if (!this.enabled) return;
            const origLog = console.log;
            console.log = (...args) => {
                origLog.apply(console, args);
                if (args.length > 0 && typeof args[0] === 'string' && args[0].startsWith('Fetch: ')) {
                    const url = args[0].substring(7);
                    const traceId = args[1];
                    const data = args[2];
                    if (traceId && data) {
                        this.traces[traceId] = data;
                    }
                }
            };
        },
        patchTable() {
            if (!this.enabled) return;
            const origTable = console.table;
            console.table = (...args) => {
                origTable.apply(console, args);
                if (args.length > 0 && typeof args[0] === 'object') {
                    const data = args[0];
                    if (data && data.traceId) {
                        this.log('Table: ' + data.traceId);
                        this.log(' → ', data);
                    }
                }
            };
        },
        patchPlotly() {
            if (!this.enabled || typeof Plotly === 'undefined') return;
            const origNewPlot = Plotly.newPlot;
            Plotly.newPlot = function (...args) {
                const traceIds = args[1]?.map(t => t.name).filter(Boolean);
                const divId = args[0]?.id;
                const key = divId + (traceIds ? '|' + traceIds.join('|') : '');
                if (key) {
                    this.traces[key] = {divId, traceIds};
                }
                return origNewPlot.apply(this, args);
            };
            const origReact = Plotly.react;
            Plotly.react = function (...args) {
                const traceIds = args[1]?.map(t => t.name).filter(Boolean);
                const divId = args[0]?.id;
                const key = divId + (traceIds ? '|' + traceIds.join('|') : '');
                if (key) {
                    this.traces[key] = {divId, traceIds};
                }
                return origReact.apply(this, args);
            };
        },
        patchHandicapCalc() {
            if (!this.enabled || typeof HandicapCalc === 'undefined') return;
            const origCreate = HandicapCalc.create;
            HandicapCalc.create = function (...args) {
                const instance = origCreate.apply(this, args);
                const origSetBoats = instance.setBoats;
                instance.setBoats = function (boats) {
                    const traceId = 'handicap-calc';
                    if (boats && boats.length > 0) {
                        const boatIds = boats.map(b => b.id).join(',');
                        this.traces[traceId] = {boatIds};
                    }
                    return origSetBoats.apply(this, arguments);
                };
                return instance;
            };
        }
    };
    s.init();
}

// ---- Debugging tools UI ----

function showDevTools() {
    const panel = document.getElementById('dev-tools-panel');
    panel.style.display = '';
    panel.scrollIntoView({behavior: 'smooth', block: 'start'});
    installDevTools();
}

function hideDevTools() {
    const panel = document.getElementById('dev-tools-panel');
    panel.style.display = 'none';
}

function toggleDevTools() {
    const panel = document.getElementById('dev-tools-panel');
    if (panel.style.display === 'none' || panel.style.display === '') {
        showDevTools();
    } else {
        hideDevTools();
    }
}

// ---- Global event listeners (for debug UI) ----

document.addEventListener('DOMContentLoaded', () => {
    const toggleBtn = document.getElementById('dev-tools-toggle');
    if (toggleBtn) {
        toggleBtn.addEventListener('click', toggleDevTools);
    }
});

// ---- Initial load ----
//
// Order of precedence:
//   1. URL contains a specific target (raceId, boatId, seriesId, clubId, designId):
//      switch to the matching tab, CLEAR that tab's persisted search, load the list,
//      and load the detail for the target.
//   2. URL contains only `tab=<entity>`: switch to that tab and restore its persisted
//      search.
//   3. No URL args: switch to the persisted active tab (default 'boats') and restore
//      its persisted search.
function initFromUrlOrSession() {
    const params = new URLSearchParams(location.search);
    const idParamToEntity = {
        raceId: 'races',
        boatId: 'boats',
        seriesId: 'series',
        clubId: 'clubs',
        designId: 'designs',
    };
    let detailEntity = null;
    let detailId = null;
    for (const [param, entity] of Object.entries(idParamToEntity)) {
        const v = params.get(param);
        if (v) {
            detailEntity = entity;
            detailId = v;
            break;
        }
    }

    if (detailEntity && detailId) {
        // Specific target: clear search for that tab, then switch + load detail.
        state.searches[detailEntity] = '';
        sessionStorage.removeItem(SEARCH_KEY_PREFIX + detailEntity);
        switchTab(detailEntity);
        loadDetail(detailEntity, detailId);
        return;
    }

    const tabArg = params.get('tab');
    const initialTab = (tabArg && TAB_ENTITIES.includes(tabArg)) ? tabArg : state.activeTab;
    switchTab(initialTab);
}

initFromUrlOrSession();
