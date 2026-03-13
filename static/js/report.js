let selectedTrusts = {};
let chartInstances = [];

if (window._urlTrusts && Object.keys(window._urlTrusts).length > 0) {
    Object.assign(selectedTrusts, window._urlTrusts);
}

const DATASET_LABELS = {
    rtt: 'Referral to Treatment (RTT)',
    ae: 'A&E Attendances & Emergency Admissions',
    cancer: 'Cancer Waiting Times',
    diagnostics: 'Diagnostics (DM01)',
    ambulance: 'Ambulance Response Times (AmbSYS)',
    workforce: 'NHS Workforce Statistics',
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

const searchInput = document.getElementById('trust-search');
const searchResults = document.getElementById('search-results');
let searchTimeout = null;

if (Object.keys(selectedTrusts).length > 0) {
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => {
            renderTags();
            generateReport();
        });
    } else {
        renderTags();
        generateReport();
    }
}

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
            searchResults.innerHTML = '<div class="result-item"><span>No results found</span></div>';
        } else {
            results.forEach(t => {
                const div = document.createElement('div');
                div.className = 'result-item';
                div.innerHTML = `<span class="code">${t.code}</span>${t.name}`;
                div.addEventListener('mousedown', (e) => {
                    e.preventDefault();
                    selectedTrusts[t.code] = t.name;
                    renderTags();
                    searchInput.value = '';
                    searchResults.style.display = 'none';
                });
                searchResults.appendChild(div);
            });
        }
        searchResults.style.display = 'block';
    } catch (e) { console.error(e); }
}

function renderTags() {
    const container = document.getElementById('selected-trusts');
    container.innerHTML = '';
    for (const [code, name] of Object.entries(selectedTrusts)) {
        const tag = document.createElement('span');
        tag.className = 'trust-tag';
        tag.innerHTML = `<strong>${code}</strong> ${name} <span class="remove" onclick="delete selectedTrusts['${code}']; renderTags();">&times;</span>`;
        container.appendChild(tag);
    }
}

async function generateReport() {
    const codes = Object.keys(selectedTrusts);
    if (codes.length === 0) { alert('Please select at least one trust.'); return; }

    const spinner = document.getElementById('report-spinner');
    spinner.textContent = 'Generating report...';
    spinner.classList.add('active');

    chartInstances.forEach(c => c.destroy());
    chartInstances = [];

    try {
        const codesParam = codes.join(',');
        const [summaryResp, trendsResp] = await Promise.all([
            fetch(`/api/summary?ods_codes=${codesParam}`),
            fetch(`/api/trends?ods_codes=${codesParam}`)
        ]);
        const [summaryData, trendsData] = await Promise.all([
            summaryResp.json(), trendsResp.json()
        ]);

        renderReport(summaryData, trendsData, codes);
    } catch (e) {
        document.getElementById('report-body').innerHTML = `<div class="report-placeholder"><p>Error generating report: ${e.message}</p></div>`;
    }
    spinner.classList.remove('active');
}

function renderReport(summary, trends, codes) {
    const body = document.getElementById('report-body');
    const trustNames = codes.map(c => `${c} - ${selectedTrusts[c] || c}`).join(', ');
    const now = new Date();
    const dateStr = now.toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' });
    const timeStr = now.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });

    let html = `
        <div class="report-header">
            <h1>NHS Trust Performance Report</h1>
            <div class="subtitle">${trustNames}</div>
            <div class="meta">Generated ${dateStr} at ${timeStr}</div>
        </div>
    `;

    html += `<div class="report-section"><h2>Performance Overview</h2><div class="kpi-grid">`;
    let hasKpis = false;

    if (summary.rtt) {
        for (const [code, m] of Object.entries(summary.rtt)) {
            const pct = m.total_pathways > 0 ? ((m.within_18_weeks / m.total_pathways) * 100).toFixed(1) : 0;
            const cls = pct >= 92 ? 'green' : pct >= 80 ? 'amber' : 'red';
            html += kpiCard(`${pct}%`, `${code} RTT Within 18 Weeks`, `${fmt(m.total_pathways)} total pathways`, cls);
            hasKpis = true;
        }
    }
    if (summary.ae) {
        for (const [code, m] of Object.entries(summary.ae)) {
            const cls = m.dta_12h_waits > 0 ? 'red' : 'green';
            html += kpiCard(fmt(m.type1_attendances), `${code} Type 1 A&E`, `${fmt(m.dta_12h_waits)} waits over 12h`, cls);
            hasKpis = true;
        }
    }
    if (summary.cancer) {
        for (const [code, m] of Object.entries(summary.cancer)) {
            const pct = m.total_treated > 0 ? ((m.within_standard / m.total_treated) * 100).toFixed(1) : 0;
            const cls = pct >= 85 ? 'green' : pct >= 75 ? 'amber' : 'red';
            html += kpiCard(`${pct}%`, `${code} Cancer Compliance`, `${fmt(m.total_treated)} treated`, cls);
            hasKpis = true;
        }
    }
    if (summary.diagnostics) {
        for (const [code, m] of Object.entries(summary.diagnostics)) {
            html += kpiCard(fmt(m.total_waiting || m.record_count), `${code} Diagnostics`, m.waiting_over_6_weeks ? `${fmt(m.waiting_over_6_weeks)} over 6 weeks` : '');
            hasKpis = true;
        }
    }
    if (summary.workforce) {
        for (const [code, m] of Object.entries(summary.workforce)) {
            html += kpiCard(fmt(m.total_fte), `${code} Workforce FTE`, m.total_headcount ? `${fmt(m.total_headcount)} headcount` : '');
            hasKpis = true;
        }
    }
    if (summary.ambulance) {
        for (const [code, m] of Object.entries(summary.ambulance)) {
            const firstKey = Object.keys(m).find(k => k !== 'record_count');
            html += kpiCard(fmt(m.record_count), `${code} Ambulance Records`, firstKey ? `${firstKey}: ${m[firstKey]}` : '');
            hasKpis = true;
        }
    }
    for (const ds of ['community', 'maternity']) {
        if (summary[ds]) {
            for (const [code, m] of Object.entries(summary[ds])) {
                const firstKey = Object.keys(m).find(k => k !== 'record_count');
                const val = firstKey ? m[firstKey] : m.record_count;
                const label = firstKey ? firstKey.replace(/_/g, ' ') : 'Records';
                html += kpiCard(fmt(val), `${code} ${DATASET_LABELS[ds]}`, `${fmt(m.record_count)} records`);
                hasKpis = true;
            }
        }
    }

    if (!hasKpis) {
        html += `<div class="snapshot-note">No performance data available yet. Fetch data from the dashboard first.</div>`;
    }
    html += `</div></div>`;

    for (const [dataset, trendInfo] of Object.entries(trends)) {
        const label = DATASET_LABELS[dataset] || dataset;
        html += `<div class="report-section"><h2>${label} Trends</h2>`;

        if (trendInfo.type === 'timeseries' && trendInfo.series && Object.keys(trendInfo.series).length > 0) {
            for (const [key, seriesData] of Object.entries(trendInfo.series)) {
                const chartId = `chart-${dataset}-${key}`;
                html += `<div class="chart-container"><h3>${seriesData.label}</h3><canvas id="${chartId}"></canvas></div>`;
            }
        } else if (trendInfo.type === 'snapshot') {
            html += `<div class="snapshot-note">This dataset shows a point-in-time snapshot rather than a time series trend.</div>`;
            if (trendInfo.metrics) {
                html += `<div class="kpi-grid">`;
                for (const [code, m] of Object.entries(trendInfo.metrics)) {
                    for (const [k, v] of Object.entries(m)) {
                        html += kpiCard(fmt(v), `${code} - ${k.replace(/_/g, ' ')}`);
                    }
                }
                html += `</div>`;
            }
        } else if (trendInfo.type === 'error') {
            html += `<div class="snapshot-note">Could not load trend data: ${trendInfo.error}</div>`;
        } else {
            html += `<div class="snapshot-note">No trend data available for this dataset.</div>`;
        }
        html += `</div>`;
    }

    html += `<div class="report-footer">NHS England Data Explorer &mdash; Report generated ${dateStr} at ${timeStr}</div>`;

    body.innerHTML = html;

    requestAnimationFrame(() => {
        for (const [dataset, trendInfo] of Object.entries(trends)) {
            if (trendInfo.type === 'timeseries' && trendInfo.series) {
                for (const [key, seriesData] of Object.entries(trendInfo.series)) {
                    const chartId = `chart-${dataset}-${key}`;
                    const canvas = document.getElementById(chartId);
                    if (!canvas) continue;
                    createTrendChart(canvas, seriesData);
                }
            }
        }
    });
}

function createTrendChart(canvas, seriesData) {
    const labels = seriesData.labels || [];
    const values = seriesData.values || [];
    const cleanValues = values.map(v => (v === null || v === undefined || isNaN(v)) ? 0 : v);

    const chart = new Chart(canvas, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: seriesData.label,
                data: cleanValues,
                borderColor: CHART_COLORS[0],
                backgroundColor: 'rgba(0, 94, 184, 0.08)',
                fill: true,
                tension: 0.3,
                pointRadius: labels.length > 20 ? 0 : 3,
                pointHoverRadius: 5,
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { intersect: false, mode: 'index' },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    titleFont: { size: 12 },
                    bodyFont: { size: 12 },
                    callbacks: {
                        label: function(ctx) {
                            return `${seriesData.label}: ${Number(ctx.raw).toLocaleString()}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: {
                        font: { size: 11 },
                        maxRotation: 45,
                        maxTicksLimit: 12
                    }
                },
                y: {
                    beginAtZero: false,
                    grid: { color: 'rgba(0,0,0,0.05)' },
                    ticks: {
                        font: { size: 11 },
                        callback: function(value) { return Number(value).toLocaleString(); }
                    }
                }
            }
        }
    });
    chartInstances.push(chart);
}

function kpiCard(value, label, detail, cls) {
    return `<div class="kpi-card ${cls || ''}"><div class="kpi-value">${value}</div><div class="kpi-label">${label}</div>${detail ? `<div class="kpi-detail">${detail}</div>` : ''}</div>`;
}

function fmt(n) {
    if (n === undefined || n === null || isNaN(n)) return '-';
    return Number(n).toLocaleString();
}
