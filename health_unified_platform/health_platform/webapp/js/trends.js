/**
 * Health App — Trends Page
 * Renders line charts for key metrics using Chart.js.
 */

let trendsChart = null;
let trendsData = null;

const TREND_METRICS = [
  { key: 'sleep_score', label: 'Søvn Score', col: 'sleep_score', color: '#5c8a5c' },
  { key: 'readiness_score', label: 'Readiness', col: 'readiness_score', color: '#c8a45c' },
  { key: 'steps', label: 'Skridt', col: 'steps', color: '#6ca6cd' },
  { key: 'resting_heart_rate', label: 'Hvilepuls', col: 'resting_heart_rate', color: '#c85c5c' },
  { key: 'weight', label: 'Vægt (kg)', col: 'weight_kg', color: '#a07cc8' },
];

const trendsSelect = document.getElementById('trends-metric-select');
const trendsCanvas = document.getElementById('trends-chart');

function buildChart(metricKey) {
  const cfg = TREND_METRICS.find(m => m.key === metricKey);
  if (!cfg || !trendsData || !trendsData[metricKey]) return;

  const rows = trendsData[metricKey];
  const labels = rows.map(r => r.day || r.datetime || '');
  const values = rows.map(r => {
    const v = r[cfg.col];
    return v !== null && v !== undefined ? parseFloat(v) : null;
  });

  if (trendsChart) trendsChart.destroy();

  trendsChart = new Chart(trendsCanvas.getContext('2d'), {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: cfg.label,
        data: values,
        borderColor: cfg.color,
        backgroundColor: cfg.color + '33',
        fill: true,
        tension: 0.3,
        pointRadius: 2,
        pointHoverRadius: 5,
        borderWidth: 2,
        spanGaps: true,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#1a1a1a',
          borderColor: '#333',
          borderWidth: 1,
          titleColor: '#d4d0c8',
          bodyColor: '#d4d0c8',
        }
      },
      scales: {
        x: {
          ticks: { color: '#7a7568', maxTicksLimit: 7, font: { size: 11 } },
          grid: { color: 'rgba(51,51,51,0.5)' },
        },
        y: {
          ticks: { color: '#7a7568', font: { size: 11 } },
          grid: { color: 'rgba(51,51,51,0.5)' },
        }
      },
      interaction: { intersect: false, mode: 'index' },
    }
  });
}

async function loadTrends() {
  if (trendsData) return; // Already loaded
  try {
    const r = await apiFetch('/v1/mobile/sync?since=' + new Date(Date.now() - 30 * 86400000).toISOString());
    if (!r.ok) throw new Error('sync failed');
    const data = await r.json();
    trendsData = data.metrics || {};

    // Build selector options
    trendsSelect.innerHTML = '';
    for (const m of TREND_METRICS) {
      if (trendsData[m.key] && trendsData[m.key].length > 0) {
        const opt = document.createElement('option');
        opt.value = m.key;
        opt.textContent = m.label;
        trendsSelect.appendChild(opt);
      }
    }

    // Render first available
    if (trendsSelect.options.length > 0) {
      buildChart(trendsSelect.value);
    }
  } catch (_) {
    document.getElementById('trends-chart-wrap').innerHTML =
      '<div class="empty-state"><p>Kunne ikke hente data.</p></div>';
  }
}

trendsSelect.addEventListener('change', () => buildChart(trendsSelect.value));

// Called from app.js when trends tab activates
function onTrendsVisible() {
  loadTrends();
}
