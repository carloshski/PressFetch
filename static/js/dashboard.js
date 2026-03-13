let selectedTrusts = {};
let dataCache = {};
let freshnessData = {};
let comparisonCharts = [];
const ALL_DATASETS = ['rtt', 'ae', 'cancer', 'diagnostics', 'ambulance', 'workforce', 'community', 'maternity'];

const DATASET_LABELS = {
    rtt: 'RTT Waiting Times',
    ae: 'A&E Performance',
    cancer: 'Cancer Waiting Times',
    diagnostics: 'Diagnostics (DM01)',
    ambulance: 'Ambulance Response Times',
    workforce: 'NHS Workforce',
    community: 'Community Services',
    maternity: 'Maternity Services'
};

const CHART_COLORS = [
    'rgb(0, 94, 184)',
    'rgb(40, 167, 69)',
    'rgb(220, 53, 69)',
    'rgb(255, 193, 7)',
    'rgb(111, 66, 193)',
    'rgb(23, 162, 184)',
    'rgb(253, 126, 20)',
    'rgb(108, 117, 125)'
];

document.addEventListener('DOMContentLoaded', () => {
    loadStatus();
    loadFreshness();
    renderSelectedTrusts();
    loadFavourites();
    document.getElementById('stat-trusts').textContent = '0';
});

async function loadStatus() {
    try {
        const resp = await fetch('/api/status');
        const data = await resp.json();
        document.getElementById('stat-trusts').textContent = Object.keys(selectedTrusts).length;
        document.getElementById('stat-raw').textContent = data.file_counts?.raw_files || 0;
        document.getElementById('stat-processed').textContent = data.file_counts?.processed_files || 0;
        document.getElementById('stat-ops').textContent = data.recent_operations || 0;
    } catch (e) {
        console.error('Status load failed:', e);
    }
}

async function loadFreshness() {
    try {
        const resp = await fetch('/api/freshness');
        freshnessData = await resp.json();
        renderFreshnessBar();
        ALL_DATASETS.forEach(ds => renderFreshnessTag(ds));
    } catch (e) {
        console.error('Freshness load failed:', e);
    }
}

function renderFreshnessBar() {
    const bar = document.getElementById('freshness-bar');
    if (!bar) return;
    let html = '';
    for (const [ds, info] of Object.entries(freshnessData)) {
        const label = DATASET_LABELS[ds] || ds;
        if (!info.last_updated) {
            html += `<div class="freshness-item"><span class="dot none"></span><span class="ds-name">${label}</span><span class="ds-time">No data</span></div>`;
        } else {
            const cls = info.stale ? 'stale' : '';
            const dotCls = info.stale ? 'stale' : 'fresh';
            const ageText = info.age_hours < 1 ? 'Just now' :
                info.age_hours < 24 ? `${Math.round(info.age_hours)}h ago` :
                `${Math.round(info.age_hours / 24)}d ago`;
            html += `<div class="freshness-item ${cls}"><span class="dot ${dotCls}"></span><span class="ds-name">${label}</span><span class="ds-time">${ageText}</span></div>`;
        }
    }
    bar.innerHTML = html;
}

function renderFreshnessTag(dataset) {
    const tag = document.getElementById(`${dataset}-freshness`);
    if (!tag) return;
    const info = freshnessData[dataset];
    if (!info || !info.last_updated) {
        tag.innerHTML = '';
        return;
    }
    const warn = info.stale ? ' <span class="stale-warn">(Data is over 7 days old)</span>' : '';
    tag.innerHTML = `Last updated: ${info.last_updated}${warn}`;
}

const searchInput = document.getElementById('trust-search');
const searchResults = document.getElementById('search-results');
let searchTimeout = null;

searchInput.addEventListener('input', () => {
    clearTimeout(searchTimeout);
    const q = searchInput.value.trim();
    if (q.length < 2) { searchResults.style.display = 'none'; return; }
    searchTimeout = setTimeout(() => searchTrusts(q), 300);
});

searchInput.addEventListener('blur', () => {
    setTimeout(() => { searchResults.style.display = 'none'; }, 200);
});

searchInput.addEventListener('focus', () => {
    if (searchInput.value.trim().length >= 2) searchTrusts(searchInput.value.trim());
});

async function searchTrusts(query) {
    try {
        const resp = await fetch(`/api/trusts/search?q=${encodeURIComponent(query)}`);
        const results = await resp.json();
        searchResults.innerHTML = '';
        if (results.length === 0) {
            searchResults.innerHTML = '<div class="result-item"><span class="name">No results found</span></div>';
        } else {
            results.forEach(t => {
                const div = document.createElement('div');
                div.className = 'result-item';
                div.innerHTML = `<span class="code">${t.code}</span><span class="name">${t.name}</span>`;
                div.addEventListener('mousedown', (e) => {
                    e.preventDefault();
                    addTrust(t.code, t.name);
                    searchInput.value = '';
                    searchResults.style.display = 'none';
                });
                searchResults.appendChild(div);
            });
        }
        searchResults.style.display = 'block';
    } catch (e) {
        console.error('Search failed:', e);
    }
}

function addTrust(code, name) {
    if (selectedTrusts[code]) return;
    selectedTrusts[code] = name;
    renderSelectedTrusts();
    document.getElementById('stat-trusts').textContent = Object.keys(selectedTrusts).length;
}

function removeTrust(code) {
    delete selectedTrusts[code];
    renderSelectedTrusts();
    document.getElementById('stat-trusts').textContent = Object.keys(selectedTrusts).length;
}

function renderSelectedTrusts() {
    const container = document.getElementById('selected-trusts');
    container.innerHTML = '';
    for (const [code, name] of Object.entries(selectedTrusts)) {
        const tag = document.createElement('span');
        tag.className = 'trust-tag';
        tag.innerHTML = `<strong>${code}</strong> ${name} <span class="remove" onclick="removeTrust('${code}')">&times;</span>`;
        container.appendChild(tag);
    }
}

function clearAll() {
    selectedTrusts = {};
    renderSelectedTrusts();
    document.getElementById('stat-trusts').textContent = '0';
}

function getSelectedCodes() {
    return Object.keys(selectedTrusts);
}

function switchTab(tab) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    document.querySelector(`.tab[onclick="switchTab('${tab}')"]`).classList.add('active');
    document.getElementById(`tab-${tab}`).classList.add('active');
}

function saveFavourite() {
    const codes = getSelectedCodes();
    if (codes.length === 0) { alert('Select at least one trust first.'); return; }
    const name = prompt('Give this favourite group a name:');
    if (!name || !name.trim()) return;
    const favs = getFavourites();
    favs[name.trim()] = { ...selectedTrusts };
    localStorage.setItem('nhs_favourites', JSON.stringify(favs));
    loadFavourites();
}

function loadFavourite(name) {
    if (!name) return;
    const favs = getFavourites();
    if (!favs[name]) return;
    selectedTrusts = { ...favs[name] };
    renderSelectedTrusts();
    document.getElementById('stat-trusts').textContent = Object.keys(selectedTrusts).length;
    document.getElementById('favourites-select').value = '';
}

function deleteFavourite() {
    const sel = document.getElementById('favourites-select');
    const name = sel.value;
    if (!name) { alert('Select a favourite from the dropdown first.'); return; }
    if (!confirm(`Delete favourite "${name}"?`)) return;
    const favs = getFavourites();
    delete favs[name];
    localStorage.setItem('nhs_favourites', JSON.stringify(favs));
    loadFavourites();
}

function getFavourites() {
    try {
        return JSON.parse(localStorage.getItem('nhs_favourites') || '{}');
    } catch { return {}; }
}

function loadFavourites() {
    const sel = document.getElementById('favourites-select');
    const favs = getFavourites();
    sel.innerHTML = '<option value="">Load Favourites...</option>';
    for (const name of Object.keys(favs)) {
        const codes = Object.keys(favs[name]);
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = `${name} (${codes.length} trust${codes.length !== 1 ? 's' : ''})`;
        sel.appendChild(opt);
    }
}

function createProgressOverlay() {
    let overlay = document.getElementById('progress-overlay');
    if (overlay) overlay.remove();

    overlay = document.createElement('div');
    overlay.id = 'progress-overlay';
    Object.assign(overlay.style, {
        position: 'fixed', top: '0', left: '0', width: '100%', height: '100%',
        background: 'rgba(0,0,0,0.55)', zIndex: '99999',
        display: 'flex', justifyContent: 'center', alignItems: 'center'
    });

    const modal = document.createElement('div');
    Object.assign(modal.style, {
        background: '#fff', borderRadius: '12px', padding: '32px',
        maxWidth: '460px', width: '90%',
        boxShadow: '0 8px 32px rgba(0,0,0,0.3)', textAlign: 'center'
    });

    const titleEl = document.createElement('h3');
    titleEl.id = 'progress-title';
    Object.assign(titleEl.style, { fontSize: '18px', color: '#005eb8', marginBottom: '20px' });

    const barWrap = document.createElement('div');
    Object.assign(barWrap.style, {
        width: '100%', height: '14px', background: '#e9ecef',
        borderRadius: '7px', overflow: 'hidden', marginBottom: '8px'
    });

    const bar = document.createElement('div');
    bar.id = 'progress-bar';
    Object.assign(bar.style, {
        height: '100%', width: '0%',
        background: 'linear-gradient(90deg, #005eb8, #0088ff)',
        borderRadius: '7px', transition: 'width 0.4s ease'
    });
    barWrap.appendChild(bar);

    const pctEl = document.createElement('div');
    pctEl.id = 'progress-pct';
    Object.assign(pctEl.style, { fontSize: '15px', fontWeight: '700', color: '#005eb8', marginBottom: '16px' });
    pctEl.textContent = '0%';

    const stepsEl = document.createElement('div');
    stepsEl.id = 'progress-steps';
    Object.assign(stepsEl.style, { textAlign: 'left', marginBottom: '16px' });

    const noteEl = document.createElement('p');
    noteEl.id = 'progress-note';
    Object.assign(noteEl.style, { fontSize: '12px', color: '#999', marginTop: '4px' });

    modal.appendChild(titleEl);
    modal.appendChild(barWrap);
    modal.appendChild(pctEl);
    modal.appendChild(stepsEl);
    modal.appendChild(noteEl);
    overlay.appendChild(modal);
    document.body.appendChild(overlay);
    return overlay;
}

function showProgress(title, note) {
    createProgressOverlay();
    document.getElementById('progress-title').textContent = title || 'Fetching NHS Data...';
    document.getElementById('progress-note').textContent = note || 'This may take a minute as we download live data from NHS England.';

    const steps = document.getElementById('progress-steps');
    steps.innerHTML = '';
    ALL_DATASETS.forEach(ds => {
        const row = document.createElement('div');
        row.id = 'pstep-' + ds;
        Object.assign(row.style, {
            display: 'flex', alignItems: 'center', gap: '8px',
            padding: '4px 0', fontSize: '13px', color: '#ccc'
        });
        const icon = document.createElement('span');
        icon.className = 'step-icon';
        Object.assign(icon.style, { width: '18px', textAlign: 'center', flexShrink: '0' });
        icon.innerHTML = '&#9675;';
        row.appendChild(icon);
        row.appendChild(document.createTextNode(DATASET_LABELS[ds] || ds));
        steps.appendChild(row);
    });
}

function updateProgress(completedCount, currentDs, failed) {
    const total = ALL_DATASETS.length;
    const pct = Math.round((completedCount / total) * 100);
    const bar = document.getElementById('progress-bar');
    if (bar) bar.style.width = pct + '%';
    const pctEl = document.getElementById('progress-pct');
    if (pctEl) pctEl.textContent = pct + '%';

    ALL_DATASETS.forEach((ds, i) => {
        const el = document.getElementById('pstep-' + ds);
        if (!el) return;
        const icon = el.querySelector('.step-icon');
        if (!icon) return;
        if (failed && failed[ds]) {
            el.style.color = '#dc3545';
            el.style.fontWeight = 'normal';
            icon.innerHTML = '&#10007;';
        } else if (i < completedCount) {
            el.style.color = '#28a745';
            el.style.fontWeight = 'normal';
            icon.innerHTML = '&#10003;';
        } else if (ds === currentDs) {
            el.style.color = '#005eb8';
            el.style.fontWeight = '600';
            icon.innerHTML = '&#9654;';
        } else {
            el.style.color = '#ccc';
            el.style.fontWeight = 'normal';
            icon.innerHTML = '&#9675;';
        }
    });
}

function hideProgress() {
    const overlay = document.getElementById('progress-overlay');
    if (overlay) overlay.remove();
}

async function fetchDataForSelected() {
    const codes = getSelectedCodes();
    if (codes.length === 0) { alert('Please select at least one Trust.'); return; }

    showProgress('Fetching NHS Data...', 'This may take a minute as we download live data from NHS England.');

    let step = 0;
    const progressInterval = setInterval(() => {
        if (step < ALL_DATASETS.length) {
            updateProgress(step, ALL_DATASETS[step]);
            step++;
        }
    }, 2500);

    try {
        const resp = await fetch('/api/run-custom', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ods_codes: codes })
        });
        const data = await resp.json();
        clearInterval(progressInterval);

        const datasets = data.datasets || {};
        const failedDs = {};
        ALL_DATASETS.forEach(ds => {
            if (datasets[ds] && !datasets[ds].success) failedDs[ds] = true;
        });
        updateProgress(ALL_DATASETS.length, null, failedDs);
        document.getElementById('progress-title').textContent = 'Loading dashboard...';
        const bar = document.getElementById('progress-bar');
        bar.style.width = '100%';
        bar.style.background = 'linear-gradient(90deg, #e9ecef 25%, #005eb8 50%, #e9ecef 75%)';
        bar.style.backgroundSize = '200% 100%';
        bar.style.animation = 'shimmer 1.5s infinite';
        document.getElementById('progress-note').textContent = 'Processing data and updating views...';

        await Promise.all([loadAllData(), loadFreshness()]);
        loadStatus();
        hideProgress();
    } catch (e) {
        clearInterval(progressInterval);
        document.getElementById('progress-title').textContent = 'Error';
        document.getElementById('progress-note').textContent = e.message;
        const bar = document.getElementById('progress-bar');
        bar.style.width = '100%';
        bar.style.background = '#dc3545';
        setTimeout(hideProgress, 3000);
    }
}

async function loadAllData() {
    const codes = getSelectedCodes();
    if (codes.length === 0) return;
    const codesParam = codes.join(',');

    try {
        const fetches = ALL_DATASETS.map(ds => fetch(`/api/data/${ds}?ods_codes=${codesParam}`));
        fetches.push(fetch(`/api/summary?ods_codes=${codesParam}`));
        fetches.push(fetch(`/api/comparison?ods_codes=${codesParam}`));
        const responses = await Promise.all(fetches);
        const results = await Promise.all(responses.map(r => r.json()));

        ALL_DATASETS.forEach((ds, i) => {
            dataCache[ds] = results[i];
        });
        const summaryData = results[ALL_DATASETS.length];
        const comparisonData = results[ALL_DATASETS.length + 1];

        ALL_DATASETS.forEach(ds => renderDatasetTab(ds, dataCache[ds]));
        renderSummary(summaryData);
        renderComparison(comparisonData);
    } catch (e) {
        console.error('Data load failed:', e);
    }
}

function renderSummary(data) {
    const container = document.getElementById('summary-content');
    if (!data || Object.keys(data).length === 0) {
        container.innerHTML = '<p class="muted">No data available yet. Use "Fetch Data for Selected Trusts" to get started.</p>';
        return;
    }

    let html = '';
    for (const [dataset, providerMetrics] of Object.entries(data)) {
        const label = DATASET_LABELS[dataset] || dataset.toUpperCase();
        html += `<div class="summary-panel"><h3>${label}</h3>`;
        for (const [code, metrics] of Object.entries(providerMetrics)) {
            for (const [key, val] of Object.entries(metrics)) {
                if (key === 'record_count') continue;
                const displayKey = key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
                html += `<div class="summary-row"><span class="label">${code} - ${displayKey}</span><span class="value">${formatNum(val)}</span></div>`;
            }
            if (Object.keys(metrics).length === 1 && metrics.record_count !== undefined) {
                html += `<div class="summary-row"><span class="label">${code} - Records</span><span class="value">${formatNum(metrics.record_count)}</span></div>`;
            }
        }
        html += `</div>`;
    }
    container.innerHTML = html;
}

function renderComparison(data) {
    const container = document.getElementById('compare-content');
    comparisonCharts.forEach(c => c.destroy());
    comparisonCharts = [];

    if (!data || Object.keys(data).length === 0) {
        container.innerHTML = '<p class="muted">No comparison data available. Select multiple trusts and fetch data first.</p>';
        return;
    }

    const codes = getSelectedCodes();
    if (codes.length < 2) {
        container.innerHTML = '<p class="muted">Select at least 2 trusts to compare performance side by side.</p>';
        return;
    }

    let html = '';
    for (const [dataset, info] of Object.entries(data)) {
        if (info.error) continue;
        const label = DATASET_LABELS[dataset] || dataset;
        html += `<div class="compare-section"><h4>${label}</h4>`;

        if (info.metrics && Object.keys(info.metrics).length > 0) {
            html += `<table class="compare-table"><thead><tr><th>Trust</th>`;
            const firstMetrics = Object.values(info.metrics)[0];
            const metricKeys = Object.keys(firstMetrics).filter(k => k !== 'record_count');
            metricKeys.forEach(k => {
                html += `<th>${k.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}</th>`;
            });
            html += `</tr></thead><tbody>`;
            for (const [code, m] of Object.entries(info.metrics)) {
                html += `<tr><td class="trust-code">${code}</td>`;
                metricKeys.forEach(k => {
                    html += `<td>${formatNum(m[k])}</td>`;
                });
                html += `</tr>`;
            }
            html += `</tbody></table>`;
        }

        if (info.has_timeseries && info.trust_series && Object.keys(info.trust_series).length > 1) {
            const chartId = `compare-chart-${dataset}`;
            html += `<div class="compare-chart-wrap"><canvas id="${chartId}"></canvas></div>`;
        }

        html += `</div>`;
    }

    if (!html) {
        container.innerHTML = '<p class="muted">No comparison data available for the selected trusts.</p>';
        return;
    }

    container.innerHTML = html;

    requestAnimationFrame(() => {
        for (const [dataset, info] of Object.entries(data)) {
            if (!info.has_timeseries || !info.trust_series || Object.keys(info.trust_series).length < 2) continue;
            const chartId = `compare-chart-${dataset}`;
            const canvas = document.getElementById(chartId);
            if (!canvas) continue;

            const allLabels = new Set();
            for (const s of Object.values(info.trust_series)) {
                (s.labels || []).forEach(l => allLabels.add(l));
            }
            const sortedLabels = Array.from(allLabels).sort();

            const datasets = [];
            let colorIdx = 0;
            for (const [code, s] of Object.entries(info.trust_series)) {
                const labelMap = {};
                (s.labels || []).forEach((l, i) => { labelMap[l] = s.values[i]; });
                const values = sortedLabels.map(l => labelMap[l] !== undefined ? labelMap[l] : null);
                datasets.push({
                    label: `${code} - ${selectedTrusts[code] || code}`,
                    data: values,
                    borderColor: CHART_COLORS[colorIdx % CHART_COLORS.length],
                    backgroundColor: 'transparent',
                    tension: 0.3,
                    pointRadius: sortedLabels.length > 20 ? 0 : 3,
                    borderWidth: 2
                });
                colorIdx++;
            }

            const chart = new Chart(canvas, {
                type: 'line',
                data: { labels: sortedLabels, datasets: datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { intersect: false, mode: 'index' },
                    plugins: {
                        legend: { position: 'bottom', labels: { font: { size: 11 } } },
                        title: { display: true, text: info.value_label || DATASET_LABELS[dataset], font: { size: 13 } },
                        tooltip: {
                            backgroundColor: 'rgba(0,0,0,0.8)',
                            callbacks: {
                                label: ctx => `${ctx.dataset.label}: ${Number(ctx.raw).toLocaleString()}`
                            }
                        }
                    },
                    scales: {
                        x: { grid: { display: false }, ticks: { font: { size: 10 }, maxRotation: 45, maxTicksLimit: 12 } },
                        y: { beginAtZero: false, grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { font: { size: 11 }, callback: v => Number(v).toLocaleString() } }
                    }
                }
            });
            comparisonCharts.push(chart);
        }
    });
}

function renderDatasetTab(dataset, data) {
    const empty = document.getElementById(`${dataset}-empty`);
    const table = document.getElementById(`${dataset}-table`);
    const summary = document.getElementById(`${dataset}-summary`);

    if (!empty || !table) return;

    if (!data || !data.rows || data.rows.length === 0) {
        empty.style.display = 'block';
        table.style.display = 'none';
        if (summary) summary.innerHTML = '';
        return;
    }

    empty.style.display = 'none';
    table.style.display = 'table';

    if (data.metrics && summary) {
        let cards = '';
        for (const [code, m] of Object.entries(data.metrics)) {
            if (dataset === 'rtt') {
                cards += metricCard(formatNum(m.total_pathways), `${code} - Total Incomplete Pathways`);
                const pct18 = m.total_pathways > 0 ? ((m.within_18_weeks / m.total_pathways) * 100).toFixed(1) : 0;
                const cls = pct18 >= 92 ? 'green' : pct18 >= 80 ? 'amber' : 'red';
                cards += metricCard(`${pct18}%`, `${code} - Within 18 Weeks`, cls);
            } else if (dataset === 'ae') {
                cards += metricCard(formatNum(m.type1_attendances), `${code} - Type 1 Attendances`);
                cards += metricCard(formatNum(m.emergency_admissions), `${code} - Emergency Admissions`);
                const cls = m.dta_12h_waits > 0 ? 'red' : 'green';
                cards += metricCard(formatNum(m.dta_12h_waits), `${code} - 12h+ DTA Waits`, cls);
            } else if (dataset === 'cancer') {
                cards += metricCard(formatNum(m.total_treated), `${code} - Total Treated`);
                const compPct = m.total_treated > 0 ? ((m.within_standard / m.total_treated) * 100).toFixed(1) : 0;
                const cls = compPct >= 85 ? 'green' : compPct >= 75 ? 'amber' : 'red';
                cards += metricCard(`${compPct}%`, `${code} - Compliance`, cls);
            } else if (dataset === 'diagnostics') {
                cards += metricCard(formatNum(m.total_waiting), `${code} - Total Waiting`);
                cards += metricCard(formatNum(m.waiting_over_6_weeks), `${code} - Over 6 Weeks`, m.waiting_over_6_weeks > 0 ? 'amber' : 'green');
            } else if (dataset === 'workforce') {
                cards += metricCard(formatNum(m.total_fte), `${code} - Total FTE`);
                cards += metricCard(formatNum(m.total_headcount), `${code} - Total Headcount`);
            } else {
                for (const [key, val] of Object.entries(m)) {
                    if (key === 'record_count') {
                        cards += metricCard(formatNum(val), `${code} - Records`);
                    } else {
                        const displayKey = key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
                        cards += metricCard(formatNum(val), `${code} - ${displayKey}`);
                    }
                }
            }
        }
        summary.innerHTML = cards;
    }

    renderTable(dataset, data);
}

function renderTable(dataset, data) {
    const table = document.getElementById(`${dataset}-table`);
    if (!table || !data || !data.rows) return;

    const cols = data.columns || [];
    const displayCols = cols.filter(c => !['data_source', 'processing_date'].includes(c));

    const fromVal = (document.getElementById(`${dataset}-date-from`) || {}).value || '';
    const toVal = (document.getElementById(`${dataset}-date-to`) || {}).value || '';

    let rows = data.rows;
    if (fromVal || toVal) {
        const periodCol = findPeriodCol(displayCols);
        if (periodCol) {
            rows = rows.filter(row => {
                const val = (row[periodCol] || '').toString().toLowerCase();
                if (fromVal && val < fromVal.toLowerCase()) return false;
                if (toVal && val > toVal.toLowerCase() + '\uffff') return false;
                return true;
            });
        }
    }

    let thead = '<tr>';
    displayCols.forEach(c => { thead += `<th>${c}</th>`; });
    thead += '</tr>';
    table.querySelector('thead').innerHTML = thead;

    const maxRows = 500;
    const rowsToShow = rows.slice(0, maxRows);
    let tbody = '';
    rowsToShow.forEach(row => {
        tbody += '<tr>';
        displayCols.forEach(c => {
            const val = row[c] !== null && row[c] !== undefined ? row[c] : '';
            tbody += `<td>${val}</td>`;
        });
        tbody += '</tr>';
    });
    if (rows.length > maxRows) {
        tbody += `<tr><td colspan="${displayCols.length}" class="muted">Showing first ${maxRows} of ${rows.length} rows. Download CSV for full data.</td></tr>`;
    } else if (rows.length === 0) {
        tbody += `<tr><td colspan="${displayCols.length}" class="muted">No rows match the selected date range.</td></tr>`;
    }
    table.querySelector('tbody').innerHTML = tbody;
}

function findPeriodCol(cols) {
    for (const c of cols) {
        const cl = c.toLowerCase();
        if (cl === 'period' || cl === 'month' || cl === 'date' || cl === 'year' || cl === 'reporting period' || cl === 'month_year') return c;
    }
    for (const c of cols) {
        const cl = c.toLowerCase();
        if (cl.includes('period') || cl.includes('month') || cl.includes('date')) return c;
    }
    return null;
}

function applyDateFilter(dataset) {
    if (dataCache[dataset]) {
        renderTable(dataset, dataCache[dataset]);
    }
}

function clearDateFilter(dataset) {
    const from = document.getElementById(`${dataset}-date-from`);
    const to = document.getElementById(`${dataset}-date-to`);
    if (from) from.value = '';
    if (to) to.value = '';
    if (dataCache[dataset]) {
        renderTable(dataset, dataCache[dataset]);
    }
}

function metricCard(value, label, cls) {
    return `<div class="metric-card ${cls || ''}"><div class="metric-value">${value}</div><div class="metric-label">${label}</div></div>`;
}

function formatNum(n) {
    if (n === undefined || n === null || isNaN(n)) return '-';
    return Number(n).toLocaleString();
}

function downloadCSV(dataset) {
    const codes = getSelectedCodes().join(',');
    const from = (document.getElementById(`${dataset}-date-from`) || {}).value || '';
    const to = (document.getElementById(`${dataset}-date-to`) || {}).value || '';
    let url = `/api/download/${dataset}?ods_codes=${codes}`;
    if (from) url += `&date_from=${encodeURIComponent(from)}`;
    if (to) url += `&date_to=${encodeURIComponent(to)}`;
    window.open(url, '_blank');
}

function openReport() {
    const params = new URLSearchParams();
    for (const [code, name] of Object.entries(selectedTrusts)) {
        params.append('t', `${code}:${name}`);
    }
    const qs = params.toString();
    window.open('/report' + (qs ? '?' + qs : ''), '_blank');
}

async function runPipeline() {
    const codes = getSelectedCodes();
    if (codes.length === 0) { alert('Please select at least one Trust first.'); return; }

    const btn = document.getElementById('run-btn');
    const result = document.getElementById('pipeline-result');

    btn.disabled = true;
    result.style.display = 'none';

    showProgress('Running Full Pipeline...', 'Downloading and processing live data from NHS England across all 8 datasets.');

    let step = 0;
    const progressInterval = setInterval(() => {
        if (step < ALL_DATASETS.length) {
            updateProgress(step, ALL_DATASETS[step]);
            step++;
        }
    }, 2500);

    try {
        const resp = await fetch('/api/run-custom', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ods_codes: codes })
        });
        const data = await resp.json();
        clearInterval(progressInterval);

        const datasets = data.datasets || {};
        const failedDs = {};
        ALL_DATASETS.forEach(ds => {
            if (datasets[ds] && !datasets[ds].success) failedDs[ds] = true;
        });
        updateProgress(ALL_DATASETS.length, null, failedDs);

        document.getElementById('progress-title').textContent = 'Loading dashboard...';
        const pbar = document.getElementById('progress-bar');
        pbar.style.width = '100%';
        pbar.style.background = 'linear-gradient(90deg, #e9ecef 25%, #005eb8 50%, #e9ecef 75%)';
        pbar.style.backgroundSize = '200% 100%';
        pbar.style.animation = 'shimmer 1.5s infinite';
        document.getElementById('progress-note').textContent = 'Processing data and updating views...';

        result.style.display = 'block';
        let text = '';
        for (const [name, ds] of Object.entries(datasets)) {
            const icon = ds.success ? '\u2705' : '\u274c';
            const records = ds.record_count || ds.total_record_count || 0;
            const providers = (ds.providers || []).join(', ');
            text += `${icon} ${(DATASET_LABELS[name] || name).toUpperCase()}: ${ds.success ? records + ' records' + (providers ? ' (' + providers + ')' : '') : ds.error || 'Failed'}\n`;
        }
        text += `\nDuration: ${(data.pipeline_duration_seconds || 0).toFixed(1)}s`;
        result.textContent = text;

        await Promise.all([loadAllData(), loadFreshness()]);
        loadStatus();
        hideProgress();
    } catch (e) {
        clearInterval(progressInterval);
        result.style.display = 'block';
        result.textContent = 'Error: ' + e.message;
        document.getElementById('progress-title').textContent = 'Error';
        document.getElementById('progress-note').textContent = e.message;
        setTimeout(hideProgress, 3000);
    }

    btn.disabled = false;
}
