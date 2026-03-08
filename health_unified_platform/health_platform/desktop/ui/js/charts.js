/**
 * Chart.js wrappers for HealthReporting dashboard.
 * Creates sparklines and trend overlay charts.
 */

// Chart.js global defaults for dark mode
Chart.defaults.color = 'rgba(235,235,245,0.6)';
Chart.defaults.borderColor = 'rgba(84,84,88,0.35)';
Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'SF Pro Text', system-ui, sans-serif";

const COLORS = {
    blue: '#0a84ff',
    green: '#30d158',
    yellow: '#ffd60a',
    red: '#ff453a',
    teal: '#64d2ff',
    purple: '#bf5af2',
    labelSecondary: 'rgba(235,235,245,0.6)',
    gridLine: 'rgba(84,84,88,0.2)',
};

/**
 * Create a sparkline chart (small, no axes, no labels).
 * @param {string} canvasId - Canvas element ID
 * @param {number[]} values - Data values
 * @param {string} color - Line color
 */
function createSparkline(canvasId, values, color) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !values.length) return null;

    const ctx = canvas.getContext('2d');

    // Gradient fill
    const gradient = ctx.createLinearGradient(0, 0, 0, 40);
    gradient.addColorStop(0, color + '40');
    gradient.addColorStop(1, color + '00');

    return new Chart(ctx, {
        type: 'line',
        data: {
            labels: values.map((_, i) => i),
            datasets: [{
                data: values,
                borderColor: color,
                backgroundColor: gradient,
                borderWidth: 1.5,
                pointRadius: 0,
                tension: 0.4,
                fill: true,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: { enabled: false },
            },
            scales: {
                x: { display: false },
                y: { display: false },
            },
            animation: { duration: 600 },
        }
    });
}

/**
 * Create the 30-day trend overlay chart (sleep + readiness).
 * @param {string} canvasId - Canvas element ID
 * @param {string[]} days - Date labels
 * @param {number[]} sleepData - Sleep score values
 * @param {number[]} readinessData - Readiness score values
 */
function createTrendChart(canvasId, days, sleepData, readinessData) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return null;

    const ctx = canvas.getContext('2d');

    // Short date labels (MM/DD)
    const labels = days.map(d => {
        const parts = d.split('-');
        return parts[1] + '/' + parts[2];
    });

    return new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Sleep Score',
                    data: sleepData,
                    borderColor: COLORS.blue,
                    backgroundColor: COLORS.blue + '15',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    tension: 0.3,
                    fill: true,
                },
                {
                    label: 'Readiness',
                    data: readinessData,
                    borderColor: COLORS.green,
                    backgroundColor: COLORS.green + '15',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    tension: 0.3,
                    fill: true,
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false,
            },
            plugins: {
                legend: {
                    position: 'top',
                    align: 'end',
                    labels: {
                        boxWidth: 12,
                        padding: 16,
                        font: { size: 12 },
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(28,28,30,0.95)',
                    titleFont: { size: 12 },
                    bodyFont: { size: 13 },
                    padding: 10,
                    cornerRadius: 8,
                    displayColors: true,
                }
            },
            scales: {
                x: {
                    grid: { color: COLORS.gridLine },
                    ticks: {
                        font: { size: 11 },
                        maxRotation: 0,
                        maxTicksLimit: 10,
                    }
                },
                y: {
                    min: 40,
                    max: 100,
                    grid: { color: COLORS.gridLine },
                    ticks: {
                        font: { size: 11 },
                        stepSize: 10,
                    }
                }
            },
            animation: { duration: 800 },
        }
    });
}

/**
 * Get the appropriate color for a score value.
 * @param {number} score
 * @returns {string} CSS color
 */
function scoreColor(score) {
    if (score >= 85) return COLORS.green;
    if (score >= 70) return COLORS.blue;
    if (score >= 55) return COLORS.yellow;
    return COLORS.red;
}
