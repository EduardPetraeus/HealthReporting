"""Mobile-friendly chat UI for the Health API.

Serves a single-page chat interface at the root URL.
Authentication is handled via a login screen — token stored in browser sessionStorage.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["ui"])

CHAT_HTML = r"""<!DOCTYPE html>
<html lang="da">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>Health Assistant</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
:root {
  --bg: #0d1117; --surface: #161b22; --border: #30363d;
  --text: #e6edf3; --muted: #8b949e; --accent: #58a6ff;
  --user-bg: #1f6feb; --bot-bg: #161b22;
  --table-header: #1c2128; --table-row: #0d1117;
  --green: #3fb950; --yellow: #d29922; --red: #f85149;
}
body {
  font-family: -apple-system, BlinkMacSystemFont, 'SF Pro', system-ui, sans-serif;
  background: var(--bg); color: var(--text);
  height: 100dvh; display: flex; flex-direction: column;
  -webkit-font-smoothing: antialiased;
}

/* Header */
header {
  padding: 14px 20px; border-bottom: 1px solid var(--border);
  display: flex; align-items: center; gap: 10px;
  background: var(--surface); flex-shrink: 0;
}
header .dot { width:10px; height:10px; border-radius:50%; background:var(--green); flex-shrink:0; }
header h1 { font-size:17px; font-weight:600; }
header .status { font-size:12px; color:var(--muted); margin-left:auto; }

/* Chat area */
#chat {
  flex:1; overflow-y:auto; padding:16px 12px;
  display:flex; flex-direction:column; gap:14px;
  scroll-behavior: smooth;
}

/* Messages */
.msg { max-width:92%; animation: fadeIn 0.25s ease; }
@keyframes fadeIn { from{opacity:0;transform:translateY(6px)} to{opacity:1;transform:none} }

.msg.user {
  align-self: flex-end; background: var(--user-bg);
  padding: 10px 16px; border-radius: 18px 18px 4px 18px;
  font-size: 15px; line-height: 1.4; color: #fff;
}

.msg.bot {
  align-self: flex-start; background: var(--bot-bg);
  border: 1px solid var(--border); border-radius: 4px 18px 18px 18px;
  padding: 14px 16px; font-size: 14px; line-height: 1.55;
}

.msg.bot .bot-icon {
  display: inline-flex; align-items: center; gap: 6px;
  font-size: 12px; font-weight: 600; color: var(--accent);
  margin-bottom: 10px;
}
.msg.bot .bot-icon::before {
  content: ''; display: inline-block; width: 18px; height: 18px;
  background: var(--accent); border-radius: 50%; opacity: 0.2;
}
.msg.error { background: #2d1214; border-color: var(--red); }

/* Markdown rendering inside bot messages */
.msg.bot h1, .msg.bot h2, .msg.bot h3 {
  font-size: 16px; font-weight: 700; margin: 12px 0 6px;
  color: #fff;
}
.msg.bot h1:first-child, .msg.bot h2:first-child, .msg.bot h3:first-child,
.msg.bot .bot-icon + h1, .msg.bot .bot-icon + h2, .msg.bot .bot-icon + h3 {
  margin-top: 0;
}
.msg.bot p { margin: 6px 0; }
.msg.bot strong { color: #fff; font-weight: 600; }
.msg.bot ul, .msg.bot ol { margin: 6px 0 6px 20px; }
.msg.bot li { margin: 3px 0; }

/* Tables */
.msg.bot table {
  width: 100%; border-collapse: collapse; margin: 10px 0;
  font-size: 13px;
}
.msg.bot thead th {
  background: var(--table-header); padding: 8px 12px;
  text-align: left; font-weight: 600; color: var(--muted);
  text-transform: uppercase; font-size: 11px; letter-spacing: 0.5px;
  border-bottom: 2px solid var(--border);
}
.msg.bot tbody td {
  padding: 7px 12px; border-bottom: 1px solid var(--border);
  font-variant-numeric: tabular-nums;
}
.msg.bot tbody tr:hover { background: rgba(88,166,255,0.04); }
.msg.bot tbody tr:last-child td { border-bottom: none; }

/* Score badges */
.score-good { color: var(--green); font-weight: 600; }
.score-ok { color: var(--yellow); font-weight: 600; }
.score-low { color: var(--red); font-weight: 600; }

/* Quick buttons */
.quick-btns {
  display: flex; gap: 6px; padding: 8px 12px; overflow-x: auto;
  -webkit-overflow-scrolling: touch; flex-shrink: 0;
}
.quick-btns button {
  padding: 7px 14px; border-radius: 18px; border: 1px solid var(--border);
  background: var(--surface); color: var(--text); font-size: 13px;
  white-space: nowrap; cursor: pointer; flex-shrink: 0;
  transition: all 0.15s;
}
.quick-btns button:active { background: var(--accent); border-color: var(--accent); color:#fff; }

/* Input area */
#input-area {
  padding: 10px 12px; border-top: 1px solid var(--border);
  background: var(--surface); display: flex; gap: 8px; flex-shrink: 0;
}
#input-area input {
  flex:1; padding: 12px 16px; border-radius: 24px;
  border: 1px solid var(--border); background: var(--bg);
  color: var(--text); font-size: 16px; outline: none;
}
#input-area input:focus { border-color: var(--accent); }
#input-area button {
  width: 44px; height: 44px; border-radius: 50%;
  background: var(--accent); border: none; cursor: pointer;
  display: flex; align-items: center; justify-content: center;
  flex-shrink: 0; transition: transform 0.1s;
}
#input-area button:active { transform: scale(0.9); }
#input-area button svg { fill: #fff; width: 20px; height: 20px; }

/* Typing indicator */
.typing { display:flex; gap:4px; padding:10px 16px; align-self:flex-start; }
.typing span {
  width:7px; height:7px; border-radius:50%; background:var(--muted);
  animation: blink 1.4s infinite both;
}
.typing span:nth-child(2) { animation-delay:0.2s; }
.typing span:nth-child(3) { animation-delay:0.4s; }
@keyframes blink { 0%,80%,100%{opacity:.3} 40%{opacity:1} }

/* Login overlay */
#login {
  position:fixed; inset:0; background:var(--bg); z-index:10;
  display:flex; flex-direction:column; align-items:center;
  justify-content:center; gap:20px; padding:24px;
}
#login.hidden { display:none; }
#login h2 { font-size:24px; font-weight:700; }
#login p { color:var(--muted); font-size:14px; text-align:center; max-width:300px; line-height:1.5; }
#login input {
  width:100%; max-width:320px; padding:14px 16px; border-radius:12px;
  border:1px solid var(--border); background:var(--surface);
  color:var(--text); font-size:16px; text-align:center;
}
#login button {
  padding:14px 40px; border-radius:12px; border:none;
  background:var(--accent); color:#fff; font-size:16px;
  font-weight:600; cursor:pointer;
}
#login .error-msg { color:var(--red); font-size:13px; min-height:18px; }
</style>
</head>
<body>

<div id="login">
  <h2>Health Assistant</h2>
  <p>Connect to your health data on Mac Mini via Tailscale.</p>
  <input id="token-input" type="password" placeholder="API token" autocomplete="off">
  <button onclick="doLogin()">Connect</button>
  <div class="error-msg" id="login-error"></div>
</div>

<header>
  <div class="dot" id="status-dot"></div>
  <h1>Health Assistant</h1>
  <span class="status" id="status-text">Connected</span>
</header>

<div id="chat">
  <div class="msg bot">
    <div class="bot-icon">Health Assistant</div>
    Ask me about your health data — sleep, steps, readiness, weight, stress, or workouts.
    <br><br>You can type in Danish or English.
  </div>
</div>

<div class="quick-btns">
  <button onclick="ask('how did I sleep?')">Sleep</button>
  <button onclick="ask('readiness')">Readiness</button>
  <button onclick="ask('steps')">Steps</button>
  <button onclick="ask('weight')">Weight</button>
  <button onclick="ask('stress')">Stress</button>
  <button onclick="ask('workout')">Workouts</button>
  <button onclick="ask('profil')">Profile</button>
  <button onclick="ask('sleep trend')">Trend</button>
</div>

<div id="input-area">
  <input id="q" type="text" placeholder="Ask about your health..." enterkeyhint="send"
    autocomplete="off" autocapitalize="sentences">
  <button onclick="send()" aria-label="Send">
    <svg viewBox="0 0 24 24"><path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/></svg>
  </button>
</div>

<script>
const API = window.location.origin;
let token = sessionStorage.getItem('health_token') || '';

/* --- Login --- */
function doLogin() {
  const inp = document.getElementById('token-input');
  token = inp.value.trim();
  if (!token) return;
  fetch(API + '/v1/chat', {
    method:'POST',
    headers:{'Authorization':'Bearer '+token,'Content-Type':'application/json'},
    body: JSON.stringify({question:'sleep',format:'markdown'})
  }).then(r => {
    if (r.ok) {
      sessionStorage.setItem('health_token', token);
      document.getElementById('login').classList.add('hidden');
    } else {
      document.getElementById('login-error').textContent = 'Invalid token';
    }
  }).catch(() => {
    document.getElementById('login-error').textContent = 'Cannot reach server';
  });
}
document.getElementById('token-input').addEventListener('keydown', e => {
  if (e.key === 'Enter') doLogin();
});
if (token) document.getElementById('login').classList.add('hidden');

/* --- Markdown to HTML renderer --- */
function md(text) {
  if (!text) return '';
  let html = escapeHtml(text);

  // Headers: ## Title → <h3>Title</h3>
  html = html.replace(/^### (.+)$/gm, '<h3>$1</h3>');
  html = html.replace(/^## (.+)$/gm, '<h2>$1</h2>');
  html = html.replace(/^# (.+)$/gm, '<h1>$1</h1>');

  // Bold: **text** → <strong>text</strong>
  html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');

  // Italic: *text* → <em>text</em>
  html = html.replace(/(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/g, '<em>$1</em>');

  // Tables: | col | col |
  html = renderTables(html);

  // Bullet lists: - item
  html = html.replace(/^- (.+)$/gm, '<li>$1</li>');
  html = html.replace(/((?:<li>.*<\/li>\n?)+)/g, '<ul>$1</ul>');

  // Paragraphs: double newline
  html = html.replace(/\n\n+/g, '</p><p>');
  html = '<p>' + html + '</p>';

  // Clean up empty paragraphs and whitespace around block elements
  html = html.replace(/<p>\s*<\/p>/g, '');
  html = html.replace(/<p>\s*(<h[123]>)/g, '$1');
  html = html.replace(/(<\/h[123]>)\s*<\/p>/g, '$1');
  html = html.replace(/<p>\s*(<table>)/g, '$1');
  html = html.replace(/(<\/table>)\s*<\/p>/g, '$1');
  html = html.replace(/<p>\s*(<ul>)/g, '$1');
  html = html.replace(/(<\/ul>)\s*<\/p>/g, '$1');

  return html;
}

function renderTables(html) {
  const lines = html.split('\n');
  let result = [];
  let tableLines = [];
  let inTable = false;

  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.startsWith('|') && trimmed.endsWith('|')) {
      inTable = true;
      tableLines.push(trimmed);
    } else {
      if (inTable) {
        result.push(buildTable(tableLines));
        tableLines = [];
        inTable = false;
      }
      result.push(line);
    }
  }
  if (inTable) result.push(buildTable(tableLines));
  return result.join('\n');
}

function buildTable(lines) {
  if (lines.length < 2) return lines.join('\n');

  const parseRow = line => line.split('|').filter((_,i,a) => i > 0 && i < a.length-1).map(c => c.trim());
  const header = parseRow(lines[0]);

  // Skip separator line (|---|---|)
  let dataStart = 1;
  if (lines[1] && /^[\s|:-]+$/.test(lines[1].replace(/-/g,''))) dataStart = 2;

  let t = '<table><thead><tr>';
  for (const h of header) t += '<th>' + h + '</th>';
  t += '</tr></thead><tbody>';

  for (let i = dataStart; i < lines.length; i++) {
    const cells = parseRow(lines[i]);
    t += '<tr>';
    for (let j = 0; j < cells.length; j++) {
      const val = cells[j];
      t += '<td>' + colorizeScore(val, header[j]) + '</td>';
    }
    t += '</tr>';
  }
  t += '</tbody></table>';
  return t;
}

function colorizeScore(val, header) {
  // Auto-colorize score values based on common health metric ranges
  const num = parseFloat(val);
  if (isNaN(num)) return val;

  const h = (header || '').toLowerCase();
  if (h.includes('score')) {
    if (num >= 80) return '<span class="score-good">' + val + '</span>';
    if (num >= 65) return '<span class="score-ok">' + val + '</span>';
    if (num > 0) return '<span class="score-low">' + val + '</span>';
  }
  if (h.includes('steps') || h.includes('skridt')) {
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

/* --- Chat logic --- */
const chatEl = document.getElementById('chat');
const inputEl = document.getElementById('q');

function addMsg(text, cls) {
  const d = document.createElement('div');
  d.className = 'msg ' + cls;
  if (cls.startsWith('bot')) {
    d.innerHTML = '<div class="bot-icon">Health Assistant</div>' + md(text);
  } else {
    d.textContent = text;
  }
  chatEl.appendChild(d);
  chatEl.scrollTop = chatEl.scrollHeight;
}

function showTyping() {
  const d = document.createElement('div');
  d.className = 'typing'; d.id = 'typing';
  d.innerHTML = '<span></span><span></span><span></span>';
  chatEl.appendChild(d);
  chatEl.scrollTop = chatEl.scrollHeight;
}
function hideTyping() {
  const t = document.getElementById('typing');
  if (t) t.remove();
}

async function ask(question) {
  if (!question.trim()) return;
  addMsg(question, 'user');
  inputEl.value = '';
  inputEl.focus();
  showTyping();

  try {
    const r = await fetch(API + '/v1/chat', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({question: question, format: 'markdown'})
    });
    hideTyping();
    if (r.ok) {
      const data = await r.json();
      addMsg(data.answer, 'bot');
    } else if (r.status === 401) {
      sessionStorage.removeItem('health_token');
      document.getElementById('login').classList.remove('hidden');
      addMsg('Session expired. Please log in again.', 'bot error');
    } else {
      addMsg('Error: ' + r.status, 'bot error');
    }
  } catch(e) {
    hideTyping();
    addMsg('Cannot reach server. Is Mac Mini online?', 'bot error');
  }
}

function send() { ask(inputEl.value); }
inputEl.addEventListener('keydown', e => { if (e.key === 'Enter') send(); });

/* --- Server health check --- */
fetch(API + '/health').then(r => r.json()).then(d => {
  document.getElementById('status-dot').style.background =
    d.status === 'healthy' ? 'var(--green)' : 'var(--red)';
  document.getElementById('status-text').textContent =
    d.status === 'healthy' ? 'Connected' : 'Degraded';
}).catch(() => {
  document.getElementById('status-dot').style.background = 'var(--red)';
  document.getElementById('status-text').textContent = 'Offline';
});
</script>
</body>
</html>"""


@router.get("/", response_class=HTMLResponse)
async def chat_ui(request: Request):
    """Serve the mobile chat interface."""
    return HTMLResponse(content=CHAT_HTML)
