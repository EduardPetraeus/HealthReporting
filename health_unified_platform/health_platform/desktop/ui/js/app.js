/**
 * HealthReporting Desktop — Navigation and state management.
 * Communicates with Python DesktopAPI via window.pywebview.api.
 */

let currentPage = 'dashboard';
let isBusy = false;
let sparklineCharts = {};
let trendChart = null;

// --- Navigation ---

function navigate(page) {
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));

    const pageEl = document.getElementById('page-' + page);
    const navEl = document.getElementById('nav-' + page);
    if (pageEl) pageEl.classList.add('active');
    if (navEl) navEl.classList.add('active');

    currentPage = page;

    if (page === 'dashboard') loadDashboard();
    if (page === 'chat') loadChatHistory();
}

// --- Dashboard ---

async function loadDashboard() {
    const container = document.getElementById('dashboard-content');
    if (!container) return;

    container.innerHTML = '<div class="loading">Loading dashboard</div>';

    try {
        const data = await window.pywebview.api.get_dashboard_data();

        if (data.error === 'no_database') {
            container.innerHTML = `
                <div class="no-data">
                    <div class="icon">&#x1F4CA;</div>
                    <h2>No Database Found</h2>
                    <p>Run with --dev flag to create a dev database with synthetic data:</p>
                    <code>python -m health_platform.desktop --dev</code>
                </div>`;
            return;
        }

        renderDashboard(data, container);
    } catch (err) {
        container.innerHTML = `
            <div class="no-data">
                <div class="icon">&#x26A0;</div>
                <h2>Connection Error</h2>
                <p>${escapeHtml(String(err))}</p>
            </div>`;
    }
}

function renderDashboard(data, container) {
    let html = '';

    const hs = data.health_score || {};
    const scoreVal = hs.score != null ? Math.round(hs.score) : '--';
    const status = hs.status || 'unknown';
    const comp = hs.components || {};

    html += `
    <div class="health-score-card">
        <div class="score-circle ${status}">
            <span class="score-value">${scoreVal}</span>
            <span class="score-label">Health</span>
        </div>
        <div class="score-details">
            <h2>Health Score</h2>
            <div class="score-status">${status} ${hs.day ? '— ' + hs.day : ''}</div>
            <div class="score-components">
                <span class="score-component">Sleep <span class="comp-value">${comp.sleep ?? '--'}</span></span>
                <span class="score-component">Readiness <span class="comp-value">${comp.readiness ?? '--'}</span></span>
                <span class="score-component">Activity <span class="comp-value">${comp.activity ?? '--'}</span></span>
            </div>
        </div>
    </div>`;

    const kpis = data.kpis || {};
    html += '<div class="kpi-grid">';
    html += renderKpiCard(kpis.sleep_score, 'sleep', 'Sleep Score');
    html += renderKpiCard(kpis.readiness_score, 'readiness', 'Readiness');
    html += renderKpiCard(kpis.steps, 'steps', 'Steps');
    html += renderKpiCard(kpis.resting_hr, 'hr', 'Resting HR');
    html += '</div>';

    html += `
    <div class="trend-card">
        <h3>30-Day Trend — Sleep &amp; Readiness</h3>
        <div class="trend-chart"><canvas id="trend-canvas"></canvas></div>
    </div>`;

    const alerts = data.alerts || [];
    if (alerts.length > 0) {
        html += '<div class="alerts-section"><h3>Alerts</h3>';
        for (const alert of alerts) {
            const icon = alert.type === 'warning' ? '&#x26A0;' : '&#x2139;';
            html += `<div class="alert-item ${alert.type}">
                <span class="alert-icon">${icon}</span>
                <span>${escapeHtml(alert.message)}</span>
            </div>`;
        }
        html += '</div>';
    }

    html += `
    <div class="quick-actions">
        <button class="quick-action" onclick="quickChat('Hvordan har jeg sovet den seneste uge?')">Sleep Analysis</button>
        <button class="quick-action" onclick="quickChat('Hvad er min readiness og energi?')">Energy Check</button>
        <button class="quick-action" onclick="quickChat('Giv mig et overblik over min sundhed')">Health Overview</button>
        <button class="quick-action" onclick="quickChat('Hvordan er min stress og recovery?')">Stress Report</button>
    </div>`;

    container.innerHTML = html;

    requestAnimationFrame(() => {
        renderSparklines(data.sparklines || {});
        renderTrend(data.trends || {});
    });
}

function renderKpiCard(kpi, id, fallbackLabel) {
    if (!kpi) kpi = {};
    const label = kpi.label || fallbackLabel;
    const value = kpi.value != null ? kpi.value.toLocaleString() : '--';
    const unit = kpi.unit || '';
    const day = kpi.day || '';

    return `
    <div class="kpi-card">
        <div class="kpi-header">
            <span class="kpi-label">${escapeHtml(label)}</span>
            <span class="kpi-day">${day}</span>
        </div>
        <div class="kpi-value">${value} <span class="kpi-unit">${unit}</span></div>
        <div class="kpi-sparkline"><canvas id="spark-${id}"></canvas></div>
    </div>`;
}

function renderSparklines(sparklines) {
    Object.values(sparklineCharts).forEach(c => c && c.destroy());
    sparklineCharts = {};

    const configs = [
        { key: 'sleep_score', id: 'spark-sleep', color: COLORS.blue },
        { key: 'readiness_score', id: 'spark-readiness', color: COLORS.green },
        { key: 'steps', id: 'spark-steps', color: COLORS.teal },
        { key: 'resting_hr', id: 'spark-hr', color: COLORS.purple },
    ];

    for (const cfg of configs) {
        const series = sparklines[cfg.key] || [];
        const values = series.map(s => s.value);
        if (values.length > 0) {
            sparklineCharts[cfg.key] = createSparkline(cfg.id, values, cfg.color);
        }
    }
}

function renderTrend(trends) {
    if (trendChart) { trendChart.destroy(); trendChart = null; }
    const days = trends.days || [];
    const sleep = trends.sleep || [];
    const readiness = trends.readiness || [];
    if (days.length > 0) {
        trendChart = createTrendChart('trend-canvas', days, sleep, readiness);
    }
}

// --- Quick Actions ---

function quickChat(question) {
    navigate('chat');
    setTimeout(() => {
        const input = document.getElementById('chat-input');
        if (input) { input.value = question; sendChat(); }
    }, 100);
}

// --- Chat ---

let streamingEl = null;
let streamBuffer = '';
let chatHistoryLoaded = false;

async function sendChat() {
    const input = document.getElementById('chat-input');
    if (!input || !input.value.trim() || isBusy) return;

    isBusy = true;
    const question = input.value.trim();
    input.value = '';

    const chatEl = document.getElementById('chat-messages');
    addChatMsg(chatEl, question, 'user');
    showTyping(chatEl);

    try {
        await window.pywebview.api.chat(question);
    } catch (err) {
        hideTyping();
        if (!streamingEl) {
            addChatMsg(chatEl, 'Error: ' + String(err), 'bot error');
        }
    } finally {
        isBusy = false;
    }
}

function appendStreamChunk(text) {
    const chatEl = document.getElementById('chat-messages');
    if (!streamingEl) {
        hideTyping();
        streamingEl = document.createElement('div');
        streamingEl.className = 'msg bot';
        streamingEl.innerHTML = '<div class="bot-label">Health Assistant</div><div class="stream-content"></div>';
        chatEl.appendChild(streamingEl);
        streamBuffer = '';
    }
    streamBuffer += text;
    const contentEl = streamingEl.querySelector('.stream-content');
    if (contentEl) contentEl.innerHTML = md(streamBuffer);
    requestAnimationFrame(() => chatEl.scrollTop = chatEl.scrollHeight);
}

function finishStream() {
    if (streamingEl) {
        const contentEl = streamingEl.querySelector('.stream-content');
        if (contentEl) contentEl.innerHTML = md(streamBuffer);
    }
    streamingEl = null;
    streamBuffer = '';
}

async function loadChatHistory() {
    if (chatHistoryLoaded) return;
    chatHistoryLoaded = true;
    try {
        const history = await window.pywebview.api.get_chat_history();
        if (!history || history.length === 0) return;
        const chatEl = document.getElementById('chat-messages');
        for (const msg of history) {
            addChatMsg(chatEl, msg.content, msg.role === 'user' ? 'user' : 'bot');
        }
    } catch (err) {
        console.error('Failed to load chat history:', err);
    }
}

async function clearChatHistory() {
    try {
        await window.pywebview.api.clear_chat_history();
        const chatEl = document.getElementById('chat-messages');
        while (chatEl.children.length > 1) {
            chatEl.removeChild(chatEl.lastChild);
        }
        chatHistoryLoaded = false;
    } catch (err) {
        console.error('Failed to clear history:', err);
    }
}

function addChatMsg(container, text, cls) {
    const d = document.createElement('div');
    d.className = 'msg ' + cls;
    if (cls.startsWith('bot')) {
        d.innerHTML = '<div class="bot-label">Health Assistant</div>' + md(text);
    } else {
        d.textContent = text;
    }
    container.appendChild(d);
    requestAnimationFrame(() => container.scrollTop = container.scrollHeight);
}

function showTyping(container) {
    const d = document.createElement('div');
    d.className = 'typing';
    d.id = 'typing';
    d.innerHTML = '<span></span><span></span><span></span>';
    container.appendChild(d);
    requestAnimationFrame(() => container.scrollTop = container.scrollHeight);
}

function hideTyping() {
    const t = document.getElementById('typing');
    if (t) t.remove();
}

// --- Data Explorer ---

async function runExplorer() {
    const metric = document.getElementById('explorer-metric').value;
    const computation = document.getElementById('explorer-computation').value;
    const dateRange = document.getElementById('explorer-range').value;
    const resultEl = document.getElementById('explorer-result');

    resultEl.textContent = 'Loading...';

    try {
        const data = await window.pywebview.api.query_metric(metric, computation, dateRange);
        resultEl.textContent = data.result || data.error || 'No data';
    } catch (err) {
        resultEl.textContent = 'Error: ' + String(err);
    }
}

async function populateMetricDropdown() {
    try {
        const metrics = await window.pywebview.api.list_metrics();
        const select = document.getElementById('explorer-metric');
        if (!select || !metrics) return;
        select.innerHTML = '';
        for (const m of metrics) {
            const opt = document.createElement('option');
            opt.value = m;
            opt.textContent = m.replace(/_/g, ' ');
            select.appendChild(opt);
        }
    } catch (err) {
        console.error('Failed to load metrics:', err);
    }
}

// --- Markdown renderer ---

function md(text) {
    if (!text) return '';
    let html = escapeHtml(text);
    html = html.replace(/^(-{3,}|\*{3,})$/gm, '<div class="divider"></div>');
    html = html.replace(/^### (.+)$/gm, '<h3>$1</h3>');
    html = html.replace(/^## (.+)$/gm, '<h2>$1</h2>');
    html = html.replace(/^# (.+)$/gm, '<h1>$1</h1>');
    html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    html = html.replace(/(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/g, '<em>$1</em>');
    html = renderTables(html);
    html = html.replace(/^- (.+)$/gm, '<li>$1</li>');
    html = html.replace(/((?:<li>.*<\/li>\n?)+)/g, '<ul>$1</ul>');
    html = html.replace(/\n\n+/g, '</p><p>');
    html = html.replace(/([^>])\n([^<])/g, '$1<br>$2');
    html = '<p>' + html + '</p>';
    html = html.replace(/<p>\s*<\/p>/g, '');
    html = html.replace(/<p>\s*(<(?:h[123]|div|table|ul|ol)>)/g, '$1');
    html = html.replace(/(<\/(?:h[123]|div|table|ul|ol)>)\s*<\/p>/g, '$1');
    return html;
}

function renderTables(html) {
    const lines = html.split('\n');
    let result = [], tableLines = [], inTable = false;
    for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed.startsWith('|') && trimmed.endsWith('|')) {
            inTable = true;
            tableLines.push(trimmed);
        } else {
            if (inTable) { result.push(buildTable(tableLines)); tableLines = []; inTable = false; }
            result.push(line);
        }
    }
    if (inTable) result.push(buildTable(tableLines));
    return result.join('\n');
}

function buildTable(lines) {
    if (lines.length < 2) return lines.join('\n');
    const parseRow = line => line.split('|').filter((_, i, a) => i > 0 && i < a.length - 1).map(c => c.trim());
    const header = parseRow(lines[0]);
    let dataStart = 1;
    if (lines[1] && /^[\s|:-]+$/.test(lines[1].replace(/-/g, ''))) dataStart = 2;

    let t = '<table><thead><tr>';
    for (const h of header) t += '<th>' + h + '</th>';
    t += '</tr></thead><tbody>';
    for (let i = dataStart; i < lines.length; i++) {
        const cells = parseRow(lines[i]);
        t += '<tr>';
        for (let j = 0; j < cells.length; j++) t += '<td>' + colorizeScore(cells[j], header[j]) + '</td>';
        t += '</tr>';
    }
    t += '</tbody></table>';
    return '<div class="table-wrap">' + t + '</div>';
}

function colorizeScore(val, header) {
    const num = parseFloat(val);
    if (isNaN(num)) return val;
    const h = (header || '').toLowerCase();
    if (h.includes('score')) {
        if (num >= 80) return '<span class="score-good">' + val + '</span>';
        if (num >= 65) return '<span class="score-ok">' + val + '</span>';
        if (num > 0) return '<span class="score-low">' + val + '</span>';
    }
    if (h.includes('step')) {
        if (num >= 8000) return '<span class="score-good">' + val + '</span>';
        if (num >= 5000) return '<span class="score-ok">' + val + '</span>';
        if (num > 0) return '<span class="score-low">' + val + '</span>';
    }
    return val;
}

function escapeHtml(t) {
    const d = document.createElement('div');
    d.textContent = t;
    return d.innerHTML;
}

// --- Init ---

window.addEventListener('pywebviewready', () => {
    loadDashboard();
    populateMetricDropdown();

    window.pywebview.api.get_status().then(status => {
        const el = document.getElementById('db-status');
        if (el) {
            el.textContent = status.db_exists
                ? `DB: ${status.db_size_mb} MB`
                : 'No database';
        }
    });
});

// Keyboard shortcuts
document.addEventListener('keydown', (e) => {
    if (e.metaKey && e.key === '1') navigate('dashboard');
    if (e.metaKey && e.key === '2') navigate('chat');
    if (e.metaKey && e.key === '3') navigate('reports');
    if (e.metaKey && e.key === '4') navigate('explorer');
});
