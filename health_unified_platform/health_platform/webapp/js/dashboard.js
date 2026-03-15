/**
 * Health App — Dashboard Page
 * Fetches /v1/mobile/sync and renders metric cards, alerts, daily summary.
 */

const dashboardGrid = document.getElementById('dashboard-grid');
const dashboardSummary = document.getElementById('dashboard-summary');
const dashboardAlerts = document.getElementById('dashboard-alerts');

const METRIC_CONFIG = {
  sleep_score: { label: 'Søvn', unit: '', key: 'sleep_score', icon: '&#x1F634;' },
  readiness_score: { label: 'Readiness', unit: '', key: 'readiness_score', icon: '&#x26A1;' },
  steps: { label: 'Skridt', unit: '', key: 'steps', icon: '&#x1F6B6;' },
  activity_score: { label: 'Aktivitet', unit: '', key: 'activity_score', icon: '&#x1F3C3;' },
  resting_heart_rate: { label: 'Hvilepuls', unit: 'bpm', key: 'resting_heart_rate', icon: '&#x2764;' },
  weight: { label: 'Vægt', unit: 'kg', key: 'weight_kg', icon: '&#x2696;' },
  daily_stress: { label: 'Stress', unit: '', key: 'day_summary', icon: '&#x1F9E0;' },
};

function scoreClass(metricName, value) {
  const num = parseFloat(value);
  if (isNaN(num)) return '';
  if (metricName === 'steps') {
    if (num >= 8000) return 'score-good';
    if (num >= 5000) return 'score-ok';
    return 'score-low';
  }
  if (metricName === 'resting_heart_rate') {
    if (num <= 55) return 'score-good';
    if (num <= 65) return 'score-ok';
    return 'score-low';
  }
  if (metricName.includes('score')) {
    if (num >= 80) return 'score-good';
    if (num >= 65) return 'score-ok';
    return 'score-low';
  }
  return '';
}

function formatValue(val, unit) {
  if (val === null || val === undefined) return '—';
  const num = parseFloat(val);
  if (isNaN(num)) return String(val);
  if (unit === 'kg') return num.toFixed(1);
  return num.toLocaleString('da-DK');
}

function renderMetricCard(name, rows) {
  const cfg = METRIC_CONFIG[name];
  if (!cfg || !rows || rows.length === 0) return '';

  const latest = rows[rows.length - 1];
  const value = latest[cfg.key];
  const cls = scoreClass(name, value);

  return `<div class="card metric-card">
    <div class="label">${cfg.icon} ${escapeHtml(cfg.label)}</div>
    <div class="stat-value ${cls}">${formatValue(value, cfg.unit)}</div>
    <div class="metric-sub">${cfg.unit ? escapeHtml(cfg.unit) : ''}</div>
  </div>`;
}

function renderAlerts(alerts) {
  if (!alerts || alerts.length === 0) {
    dashboardAlerts.innerHTML = '';
    return;
  }
  let html = '<div class="label">Alerts</div>';
  for (const a of alerts) {
    html += `<div class="card alert-card">
      <span class="alert-date">${escapeHtml(a.day)}</span>
      <span class="alert-text">${escapeHtml(a.anomaly_metrics || '')}</span>
    </div>`;
  }
  dashboardAlerts.innerHTML = html;
}

function renderSummary(summaries) {
  if (!summaries || summaries.length === 0) {
    dashboardSummary.innerHTML = '';
    return;
  }
  const latest = summaries[summaries.length - 1];
  dashboardSummary.innerHTML = `<div class="card summary-card">
    <div class="label">Dagsoverblik — ${escapeHtml(latest.day)}</div>
    <div class="summary-text">${md(latest.summary_text || '')}</div>
  </div>`;
}

async function loadDashboard() {
  dashboardGrid.innerHTML = '<div class="loading">Henter data...</div>';
  try {
    const r = await apiFetch('/v1/mobile/sync?since=' + new Date(Date.now() - 30 * 86400000).toISOString());
    if (!r.ok) throw new Error('sync failed');
    const data = await r.json();

    // Metric cards
    let cardsHtml = '';
    for (const [name, cfg] of Object.entries(METRIC_CONFIG)) {
      if (data.metrics && data.metrics[name]) {
        cardsHtml += renderMetricCard(name, data.metrics[name]);
      }
    }
    dashboardGrid.innerHTML = cardsHtml || '<div class="empty-state"><p>Ingen data endnu.</p></div>';

    renderAlerts(data.alerts);
    renderSummary(data.daily_summaries);
    dashboardLoaded = true;
  } catch (_) {
    dashboardGrid.innerHTML = '<div class="empty-state"><p>Kunne ikke hente data. Tjek forbindelsen.</p></div>';
  }
}

// Load on tab switch (called from app.js) — guard prevents redundant fetches
let dashboardLoaded = false;

function onDashboardVisible() {
  if (!dashboardLoaded) loadDashboard();
}

// Initial load if dashboard is default tab
if (document.getElementById('page-dashboard').classList.contains('active')) {
  loadDashboard();
}
