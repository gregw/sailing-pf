const DAYS = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY'];
const DAY_LABELS = {
    MONDAY: 'Mon', TUESDAY: 'Tue', WEDNESDAY: 'Wed', THURSDAY: 'Thu',
    FRIDAY: 'Fri', SATURDAY: 'Sat', SUNDAY: 'Sun'
};
const DISPLAY_NAMES = {
    'sailsys-races':    'Import SailSys Races',
    'orc':              'Import ORC Certificates',
    'ams':              'Import AMS Certificates',
    'topyacht':         'Import TopYacht Races',
    'bwps':             'Import BWPS Races',
    'analysis':         'Analyse Certificates',
    'reference-factors':'Calculate Reference Factors',
    'build-indexes':    'Build Indexes',
    'hpf-optimise':     'HPF Optimise'
};

let currentEntries = [];
let statusPoller = null;
let prevRunningName = null;
let prevRunningMode = null;

function isWriteAllowed() { return window.hpfAuth?.authenticated; }

function applyAuthState() {
    const ok = isWriteAllowed();
    document.querySelectorAll(
        '#importers-body button, [onclick="saveSchedule()"], [onclick="stopSchedule()"]'
    ).forEach(b => {
        b.disabled = !ok;
        b.title = ok ? '' : 'Sign in to use this action';
    });
}

function displayName(name) {
    return DISPLAY_NAMES[name] || name;
}

async function loadImporters() {
    const data = await fetchJson('/api/importers');
    if (!data) return;

    currentEntries = data.entries || [];
    buildTable(currentEntries);
    buildDayPicker((data.schedule && data.schedule.days) ? data.schedule.days : []);
    if (data.schedule && data.schedule.time) {
        const t = data.schedule.time;
        document.getElementById('schedule-time').value =
            typeof t === 'string' ? t.substring(0, 5) : t;
    }
    const yearInput = document.getElementById('target-irc-year');
    yearInput.value = data.targetIrcYear != null ? data.targetIrcYear : '';

    if (data.hpfConfig) {
        document.getElementById('hpf-lambda').value = data.hpfConfig.lambda;
        document.getElementById('hpf-convergence').value = data.hpfConfig.convergenceThreshold;
        document.getElementById('hpf-max-inner').value = data.hpfConfig.maxInnerIterations;
        document.getElementById('hpf-max-outer').value = data.hpfConfig.maxOuterIterations;
        document.getElementById('hpf-outlier-k').value = data.hpfConfig.outlierK;
        document.getElementById('hpf-asymmetry').value = data.hpfConfig.asymmetryFactor;
        document.getElementById('hpf-outer-damping').value = data.hpfConfig.outerDampingFactor;
        document.getElementById('hpf-outer-convergence').value = data.hpfConfig.outerConvergenceThreshold;
    }

    const anyRunning = currentEntries.some(e => e.status === 'running');
    if (anyRunning) startStatusPoller();
    applyAuthState();
}

function buildTable(entries) {
    const tbody = document.getElementById('importers-body');
    tbody.innerHTML = '';
    for (const entry of entries) {
        tbody.appendChild(buildRow(entry));
    }
}

function taskTip(name) {
    const tips = {
        'sailsys-races':      'Fetches race results from the SailSys API (or local file cache).',
        'orc':                'Downloads ORC certificate data from data.orc.org.',
        'ams':                'Scrapes AMS certificate data from raceyachts.org.',
        'topyacht':           'Scrapes race results from TopYacht club result pages.',
        'bwps':               'Imports BWPS (BlueSail) race results from the CYCA.',
        'analysis':           'Builds the ConversionGraph from paired handicap observations.',
        'reference-factors':  'Computes IRC-equivalent reference factors for all boats.',
        'build-indexes':      'Rebuilds navigation indexes (boat→races, design→boats, etc.).',
        'hpf-optimise':       'Runs the HPF optimiser to produce Historical Performance Factors.',
    };
    return tips[name] || name;
}

function buildRow(entry) {
    const tr = document.createElement('tr');
    tr.id = 'row-' + entry.name + '-' + entry.mode;
    tr.dataset.name = entry.name;
    tr.dataset.mode = entry.mode;
    const isRunning = entry.status === 'running';
    const isSailSysApi = entry.name === 'sailsys-races';
    const key = entry.name + '-' + entry.mode;
    const defaultStart = (entry.nextStartId != null) ? entry.nextStartId : 1;
    const startInput = isSailSysApi
        ? `<input type="number" id="start-${esc(key)}" value="${defaultStart}" min="1" style="width:5em"
               title="SailSys race ID to start importing from">`
        : '';
    const runStopBtns = isSailSysApi
        ? `<button id="run-btn-${esc(key)}"
                   onclick="runImporter('${esc(entry.name)}','${esc(entry.mode)}')"
                   title="Run this task now"
                   ${isRunning ? 'style="display:none"' : ''}>Run</button>
           <button id="stop-btn-${esc(key)}"
                   onclick="stopImport()"
                   title="Request the running task to stop"
                   ${isRunning ? '' : 'style="display:none"'}>Stop</button>`
        : `<button onclick="runImporter('${esc(entry.name)}','${esc(entry.mode)}')"
               title="Run this task now">Run</button>`;
    tr.innerHTML = `
      <td class="order-col">
        <button class="order-btn" title="Move up in run order"
                onclick="moveRow('${esc(entry.name)}','${esc(entry.mode)}','up')">↑</button>
        <button class="order-btn" title="Move down in run order"
                onclick="moveRow('${esc(entry.name)}','${esc(entry.mode)}','down')">↓</button>
      </td>
      <td>${esc(displayName(entry.name))} ${infoBtn('task-' + entry.name, taskTip(entry.name))}</td>
      <td><span class="badge ${isRunning ? 'badge-running' : 'badge-idle'}"
               id="badge-${esc(key)}">${esc(entry.status)}</span></td>
      <td>${startInput}${runStopBtns}</td>
      <td style="text-align:center"><input type="checkbox" id="sched-${esc(key)}"
               title="Include in the automatic scheduled run"
               ${entry.includeInSchedule ? 'checked' : ''}></td>
      <td style="text-align:center"><input type="checkbox" id="start-${esc(key)}-startup"
               title="Run this task automatically when the server starts"
               ${entry.runAtStartup ? 'checked' : ''}></td>`;
    return tr;
}

function moveRow(name, mode, dir) {
    const tbody = document.getElementById('importers-body');
    const row = document.getElementById('row-' + name + '-' + mode);
    if (dir === 'up' && row.previousElementSibling) {
        tbody.insertBefore(row, row.previousElementSibling);
    } else if (dir === 'down' && row.nextElementSibling) {
        tbody.insertBefore(row.nextElementSibling, row);
    }
}

function buildDayPicker(scheduledDays) {
    const container = document.getElementById('schedule-days');
    container.innerHTML = DAYS.map(d =>
        `<label title="Run schedule on ${d.charAt(0) + d.slice(1).toLowerCase()}s">` +
        `<input type="checkbox" value="${d}" ${scheduledDays.includes(d) ? 'checked' : ''}> ${DAY_LABELS[d]}</label>`
    ).join('');
}

document.addEventListener('hpf:authready', applyAuthState);

async function runImporter(name, mode) {
    if (!isWriteAllowed()) return;
    const key = name + '-' + mode;
    const startInput = document.getElementById('start-' + key);
    const startId = startInput ? parseInt(startInput.value, 10) || 1 : 1;
    const resp = await fetch(
        '/api/importers/' + name + '/run?mode=' + encodeURIComponent(mode) + '&startId=' + startId,
        { method: 'POST' }
    );
    const data = await resp.json().catch(() => ({}));
    if (resp.status === 409) {
        alert('An import is already running');
    } else if (resp.status === 202) {
        setBadge(name, mode, 'running');
        setAllRunButtonsDisabled(true);
        startStatusPoller();
    } else {
        alert('Unexpected response: ' + resp.status);
    }
}

async function stopImport() {
    if (!isWriteAllowed()) return;
    await fetch('/api/importers/stop', { method: 'POST' });
}

async function stopSchedule() {
    if (!isWriteAllowed()) return;
    await fetch('/api/importers/stop', { method: 'POST' });
    setStopScheduleVisible(false);
}

async function saveSchedule() {
    if (!isWriteAllowed()) return;
    const days = [...document.querySelectorAll('#schedule-days input:checked')].map(cb => cb.value);
    const time = document.getElementById('schedule-time').value;
    const importers = [...document.querySelectorAll('#importers-body tr')].map(tr => {
        const name = tr.dataset.name;
        const mode = tr.dataset.mode;
        return {
            name,
            mode,
            includeInSchedule: document.getElementById('sched-' + name + '-' + mode).checked,
            runAtStartup: document.getElementById('start-' + name + '-' + mode + '-startup').checked
        };
    });
    const yearVal = parseInt(document.getElementById('target-irc-year').value, 10);
    const targetIrcYear = yearVal > 0 ? yearVal : null;
    const hpfLambda = parseFloat(document.getElementById('hpf-lambda').value) || null;
    const hpfConvergenceThreshold = parseFloat(document.getElementById('hpf-convergence').value) || null;
    const hpfMaxInnerIterations = parseInt(document.getElementById('hpf-max-inner').value, 10) || null;
    const hpfMaxOuterIterations = parseInt(document.getElementById('hpf-max-outer').value, 10) || null;
    const hpfOutlierK = parseFloat(document.getElementById('hpf-outlier-k').value) || null;
    const hpfAsymmetryFactor = parseFloat(document.getElementById('hpf-asymmetry').value) || null;
    const hpfOuterDampingFactor = parseFloat(document.getElementById('hpf-outer-damping').value) || null;
    const hpfOuterConvergenceThreshold = parseFloat(document.getElementById('hpf-outer-convergence').value) || null;
    const resp = await fetch('/api/schedule', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ days, time, importers, targetIrcYear,
            hpfLambda, hpfConvergenceThreshold, hpfMaxInnerIterations, hpfMaxOuterIterations,
            hpfOutlierK, hpfAsymmetryFactor, hpfOuterDampingFactor, hpfOuterConvergenceThreshold })
    });
    if (!resp.ok) {
        const err = await resp.json().catch(() => ({ error: 'unknown error' }));
        alert('Failed: ' + (err.error || resp.status));
    }
}

function setAllRunButtonsDisabled(disabled) {
    for (const entry of currentEntries) {
        const key = entry.name + '-' + entry.mode;
        const row = document.getElementById('row-' + key);
        if (row) {
            row.querySelectorAll('button:not([id^="stop-btn"])').forEach(btn => {
                if (!btn.classList.contains('order-btn'))
                    btn.disabled = disabled;
            });
        }
    }
}

function setStopScheduleVisible(visible) {
    const btn = document.getElementById('stop-schedule-btn');
    if (btn) btn.style.display = visible ? '' : 'none';
}

function startStatusPoller() {
    if (statusPoller) return;
    prevRunningName = null;
    prevRunningMode = null;
    statusPoller = setInterval(async () => {
        const data = await fetchJson('/api/importers/status');
        if (!data) return;
        if (!data.running) {
            clearInterval(statusPoller);
            statusPoller = null;
            prevRunningName = null;
            prevRunningMode = null;
            setStopScheduleVisible(false);
            await loadImporters();
        } else {
            if (prevRunningName && (prevRunningName !== data.name || prevRunningMode !== data.mode)) {
                setBadge(prevRunningName, prevRunningMode, 'idle');
                const prevKey = prevRunningName + '-' + prevRunningMode;
                const prevRunBtn  = document.getElementById('run-btn-'  + prevKey);
                const prevStopBtn = document.getElementById('stop-btn-' + prevKey);
                if (prevRunBtn)  prevRunBtn.style.display  = '';
                if (prevStopBtn) prevStopBtn.style.display = 'none';
            }
            prevRunningName = data.name;
            prevRunningMode = data.mode;

            setBadge(data.name, data.mode, 'running');
            setAllRunButtonsDisabled(true);
            setStopScheduleVisible(!!data.scheduledRun);

            if (data.currentId != null) {
                const key = data.name + '-' + data.mode;
                const startInput = document.getElementById('start-' + key);
                if (startInput) startInput.value = data.currentId + 1;
                const runBtn  = document.getElementById('run-btn-'  + key);
                const stopBtn = document.getElementById('stop-btn-' + key);
                if (runBtn)  runBtn.style.display  = 'none';
                if (stopBtn) stopBtn.style.display = '';
            }
        }
    }, 2000);
}

function setBadge(name, mode, status) {
    const badge = document.getElementById('badge-' + name + '-' + mode);
    if (!badge) return;
    badge.textContent = status;
    badge.className = 'badge ' + (status === 'running' ? 'badge-running' : 'badge-idle');
}

loadImporters();
