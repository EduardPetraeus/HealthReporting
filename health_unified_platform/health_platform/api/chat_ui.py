"""Mobile-friendly chat UI for the Health API.

Serves a single-page chat interface at the root URL.
Authentication is handled via a login screen — token stored in browser sessionStorage.
Design follows Apple Human Interface Guidelines for iOS dark mode.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["ui"])

CHAT_HTML = r"""<!DOCTYPE html>
<html lang="da">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, interactive-widget=resizes-content">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="Health">
<link rel="apple-touch-icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect fill='%23000' width='100' height='100' rx='22'/><text y='68' x='50' text-anchor='middle' font-size='52'>&#x2764;&#xFE0F;</text></svg>">
<title>Health</title>
<style>
/* ===== Apple iOS Dark Mode System Colors ===== */
:root {
  /* Backgrounds — true black base like iOS */
  --bg-primary: #000000;
  --bg-secondary: #1c1c1e;
  --bg-tertiary: #2c2c2e;
  --bg-elevated: #1c1c1e;
  --bg-grouped: #1c1c1e;

  /* Labels */
  --label-primary: #ffffff;
  --label-secondary: rgba(235,235,245,0.6);
  --label-tertiary: rgba(235,235,245,0.3);
  --label-quaternary: rgba(235,235,245,0.18);

  /* Separators */
  --separator: rgba(84,84,88,0.65);
  --separator-opaque: #38383a;

  /* Fills */
  --fill-primary: rgba(120,120,128,0.36);
  --fill-secondary: rgba(120,120,128,0.32);
  --fill-tertiary: rgba(118,118,128,0.24);

  /* System accent colors */
  --system-blue: #0a84ff;
  --system-green: #30d158;
  --system-yellow: #ffd60a;
  --system-orange: #ff9f0a;
  --system-red: #ff453a;
  --system-teal: #64d2ff;
  --system-indigo: #5e5ce6;

  /* App-specific */
  --user-bubble: #0a84ff;
  --bot-bubble: #1c1c1e;
}

* { margin:0; padding:0; box-sizing:border-box; -webkit-tap-highlight-color: transparent; }

body {
  font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Text', 'SF Pro Display', system-ui, sans-serif;
  background: var(--bg-primary); color: var(--label-primary);
  height: 100dvh; display: flex; flex-direction: column;
  -webkit-font-smoothing: antialiased;
  -webkit-text-size-adjust: 100%;
}

/* ===== Header — frosted glass like iOS navbar ===== */
header {
  padding: 12px 16px calc(12px + env(safe-area-inset-top, 0px)) 16px;
  display: flex; align-items: center; gap: 10px;
  background: rgba(28,28,30,0.72);
  -webkit-backdrop-filter: saturate(180%) blur(20px);
  backdrop-filter: saturate(180%) blur(20px);
  border-bottom: 0.5px solid var(--separator);
  flex-shrink: 0; z-index: 5;
}
header .dot {
  width: 8px; height: 8px; border-radius: 50%;
  background: var(--system-green); flex-shrink: 0;
  box-shadow: 0 0 6px rgba(48,209,88,0.5);
}
header h1 {
  font-size: 17px; font-weight: 600;
  letter-spacing: -0.4px;
}
header .status {
  font-size: 13px; color: var(--label-secondary);
  margin-left: auto; font-weight: 400;
}

/* ===== Chat area ===== */
#chat {
  flex: 1; overflow-y: auto; padding: 16px 16px 8px;
  display: flex; flex-direction: column; gap: 12px;
  overscroll-behavior: contain;
}

/* ===== Messages ===== */
.msg {
  max-width: 88%; animation: msgIn 0.3s cubic-bezier(0.16,1,0.3,1);
  word-wrap: break-word;
}
@keyframes msgIn {
  from { opacity:0; transform: translateY(8px) scale(0.97); }
  to { opacity:1; transform: none; }
}

.msg.user {
  align-self: flex-end; background: var(--user-bubble);
  padding: 10px 14px; border-radius: 18px 18px 4px 18px;
  font-size: 16px; line-height: 1.4; color: #fff;
  font-weight: 400;
}

.msg.bot {
  align-self: flex-start; background: var(--bot-bubble);
  border-radius: 4px 18px 18px 18px;
  padding: 12px 14px; font-size: 15px; line-height: 1.5;
  color: var(--label-primary);
}

.msg.bot .bot-label {
  font-size: 12px; font-weight: 500; color: var(--label-tertiary);
  margin-bottom: 6px; letter-spacing: 0.3px; text-transform: uppercase;
}

.msg.error {
  background: rgba(255,69,58,0.12);
  border: 0.5px solid rgba(255,69,58,0.3);
}

/* ===== Markdown inside bot messages ===== */
.msg.bot h1, .msg.bot h2, .msg.bot h3 {
  font-size: 15px; font-weight: 600; margin: 14px 0 4px;
  color: var(--label-primary); letter-spacing: -0.2px;
}
.msg.bot h1:first-child, .msg.bot h2:first-child, .msg.bot h3:first-child,
.msg.bot .bot-label + h1, .msg.bot .bot-label + h2, .msg.bot .bot-label + h3 {
  margin-top: 0;
}
.msg.bot p { margin: 4px 0; }
.msg.bot strong { color: var(--label-primary); font-weight: 600; }
.msg.bot em { color: var(--label-secondary); font-style: italic; }
.msg.bot ul, .msg.bot ol { margin: 4px 0 4px 18px; }
.msg.bot li { margin: 2px 0; }
.msg.bot hr { border: none; border-top: 0.5px solid var(--separator); margin: 10px 0; }

/* ===== Tables — Apple-style grouped list look ===== */
.msg.bot .table-wrap {
  overflow-x: auto; -webkit-overflow-scrolling: touch;
  margin: 8px -4px; border-radius: 10px;
}
.msg.bot table {
  width: 100%; border-collapse: separate; border-spacing: 0;
  font-size: 13px; background: var(--bg-tertiary); border-radius: 10px;
  overflow: hidden;
}
.msg.bot thead th {
  padding: 8px 10px; text-align: left;
  font-weight: 600; color: var(--label-secondary);
  font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px;
  background: var(--bg-tertiary);
  border-bottom: 0.5px solid var(--separator);
}
.msg.bot tbody td {
  padding: 8px 10px;
  border-bottom: 0.5px solid var(--separator);
  font-variant-numeric: tabular-nums;
}
.msg.bot tbody tr:last-child td { border-bottom: none; }

/* Score colors */
.score-good { color: var(--system-green); font-weight: 600; }
.score-ok { color: var(--system-yellow); font-weight: 600; }
.score-low { color: var(--system-red); font-weight: 600; }

/* ===== Quick action chips — inside chat, not fixed ===== */
.quick-chips {
  display: flex; flex-wrap: wrap; gap: 8px;
  margin-top: 8px;
}
.quick-chips button {
  padding: 8px 16px; min-height: 36px; border-radius: 18px;
  border: none;
  background: var(--fill-tertiary);
  color: var(--system-blue); font-size: 15px; font-weight: 500;
  cursor: pointer; transition: all 0.15s;
  -webkit-tap-highlight-color: transparent;
}
.quick-chips button:active {
  background: var(--system-blue); color: #fff;
  transform: scale(0.96);
}

/* ===== Input area — frosted glass ===== */
#input-area {
  padding: 8px 12px calc(8px + env(safe-area-inset-bottom, 0px)) 12px;
  background: rgba(28,28,30,0.72);
  -webkit-backdrop-filter: saturate(180%) blur(20px);
  backdrop-filter: saturate(180%) blur(20px);
  border-top: 0.5px solid var(--separator);
  display: flex; gap: 8px; flex-shrink: 0;
  align-items: flex-end;
}
#input-area input {
  flex: 1; padding: 10px 16px; border-radius: 20px;
  border: 0.5px solid var(--separator-opaque);
  background: var(--fill-tertiary);
  color: var(--label-primary); font-size: 16px; outline: none;
  font-family: inherit; transition: border-color 0.2s;
}
#input-area input::placeholder { color: var(--label-tertiary); }
#input-area input:focus { border-color: var(--system-blue); }
#input-area button {
  width: 36px; height: 36px; border-radius: 50%;
  background: var(--system-blue); border: none; cursor: pointer;
  display: flex; align-items: center; justify-content: center;
  flex-shrink: 0; transition: all 0.15s;
}
#input-area button:active { transform: scale(0.88); opacity: 0.8; }
#input-area button svg { fill: #fff; width: 18px; height: 18px; }

/* ===== Typing indicator ===== */
.typing {
  display: flex; gap: 5px; padding: 12px 16px;
  align-self: flex-start;
  background: var(--bot-bubble); border-radius: 4px 18px 18px 18px;
}
.typing span {
  width: 7px; height: 7px; border-radius: 50%;
  background: var(--label-tertiary);
  animation: pulse 1.4s infinite both;
}
.typing span:nth-child(2) { animation-delay: 0.2s; }
.typing span:nth-child(3) { animation-delay: 0.4s; }
@keyframes pulse {
  0%, 80%, 100% { opacity: 0.3; transform: scale(0.85); }
  40% { opacity: 1; transform: scale(1); }
}

/* ===== Login — centered, minimal ===== */
#login {
  position: fixed; inset: 0; background: var(--bg-primary); z-index: 10;
  display: flex; flex-direction: column; align-items: center;
  justify-content: center; gap: 24px; padding: 32px;
}
#login.hidden { display: none; }
#login .icon {
  width: 80px; height: 80px; border-radius: 22px;
  background: linear-gradient(135deg, var(--system-red), var(--system-orange));
  display: flex; align-items: center; justify-content: center;
  font-size: 38px; box-shadow: 0 8px 32px rgba(255,69,58,0.3);
}
#login h2 {
  font-size: 28px; font-weight: 700; letter-spacing: -0.5px;
}
#login p {
  color: var(--label-secondary); font-size: 15px;
  text-align: center; max-width: 280px; line-height: 1.45;
}
#login input {
  width: 100%; max-width: 300px; padding: 14px 16px; border-radius: 12px;
  border: 0.5px solid var(--separator-opaque);
  background: var(--bg-secondary);
  color: var(--label-primary); font-size: 16px; text-align: center;
  font-family: inherit; outline: none;
}
#login input:focus { border-color: var(--system-blue); }
#login button {
  width: 100%; max-width: 300px;
  padding: 16px; border-radius: 14px; border: none;
  background: var(--system-blue); color: #fff; font-size: 17px;
  font-weight: 600; cursor: pointer; letter-spacing: -0.2px;
  transition: all 0.15s;
}
#login button:active { transform: scale(0.97); opacity: 0.85; }
#login .error-msg {
  color: var(--system-red); font-size: 14px; min-height: 20px;
  font-weight: 500;
}

/* ===== Retry button in error messages ===== */
.retry-btn {
  margin-top: 8px; padding: 8px 20px; border-radius: 16px;
  border: none; background: var(--fill-tertiary);
  color: var(--system-blue); font-size: 15px; font-weight: 500;
  cursor: pointer; transition: all 0.15s;
}
.retry-btn:active { background: var(--system-blue); color: #fff; transform: scale(0.96); }

/* ===== Horizontal rule as subtle gradient ===== */
.msg.bot .divider {
  height: 0.5px; margin: 12px 0;
  background: linear-gradient(to right, transparent, var(--separator), transparent);
}
</style>
</head>
<body>

<div id="login">
  <div class="icon"><span>&#x2764;&#xFE0F;</span></div>
  <h2>Health</h2>
  <p>Din personlige sundhedsassistent. Forbind via Tailscale.</p>
  <input id="token-input" type="password" placeholder="Indsæt API token"
    autocomplete="off" enterkeyhint="go" spellcheck="false" autocorrect="off">
  <button onclick="doLogin()">Forbind</button>
  <div class="error-msg" id="login-error"></div>
</div>

<header>
  <div class="dot" id="status-dot"></div>
  <h1>Health</h1>
  <span class="status" id="status-text">Forbundet</span>
</header>

<div id="chat">
  <div class="msg bot" id="welcome">
    <div class="bot-label">Health Assistant</div>
    Hej! Jeg er din personlige sundhedsassistent. Stil mig et spørgsmål, eller vælg et emne nedenfor.
    <div class="quick-chips">
      <button onclick="ask('Hvordan har jeg sovet den seneste uge?')">Søvn</button>
      <button onclick="ask('Hvad er min readiness og energi?')">Energi</button>
      <button onclick="ask('Hvor mange skridt har jeg gået?')">Skridt</button>
      <button onclick="ask('Giv mig et overblik over min sundhed')">Overblik</button>
      <button onclick="ask('Hvordan er min stress og recovery?')">Stress</button>
      <button onclick="ask('Vis mine træninger')">Træning</button>
      <button onclick="ask('Hvad vejer jeg og hvordan er trenden?')">Vægt</button>
    </div>
  </div>
</div>

<div id="input-area">
  <input id="q" type="text" placeholder="Spørg om din sundhed..." enterkeyhint="send"
    autocomplete="off" autocapitalize="sentences" autocorrect="on">
  <button onclick="send()" aria-label="Send">
    <svg viewBox="0 0 24 24"><path d="M3.478 2.405a.75.75 0 00-.926.94l2.432 7.905H13.5a.75.75 0 010 1.5H4.984l-2.432 7.905a.75.75 0 00.926.94l18.04-8.01a.75.75 0 000-1.36L3.478 2.405z"/></svg>
  </button>
</div>

<script>
const API = window.location.origin;
let token = localStorage.getItem('health_token') || sessionStorage.getItem('health_token') || '';
let isBusy = false;  // Prevent parallel requests

/* --- Login --- */
function doLogin() {
  const inp = document.getElementById('token-input');
  const btn = inp.parentElement.querySelector('button');
  token = inp.value.trim();
  if (!token) return;
  btn.textContent = '...';
  btn.disabled = true;
  fetch(API + '/health').then(r => r.json()).then(d => {
    if (d.status) {
      // Server reachable — now validate token
      return fetch(API + '/v1/profile', {
        headers: {'Authorization': 'Bearer ' + token}
      });
    }
  }).then(r => {
    btn.textContent = 'Forbind';
    btn.disabled = false;
    if (r && r.ok) {
      localStorage.setItem('health_token', token);
      document.getElementById('login').classList.add('hidden');
    } else {
      document.getElementById('login-error').textContent = 'Ugyldigt token';
    }
  }).catch(() => {
    btn.textContent = 'Forbind';
    btn.disabled = false;
    document.getElementById('login-error').textContent = 'Kan ikke nå serveren';
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

  // Horizontal rules: --- or ***
  html = html.replace(/^(-{3,}|\*{3,})$/gm, '<div class="divider"></div>');

  // Headers
  html = html.replace(/^### (.+)$/gm, '<h3>$1</h3>');
  html = html.replace(/^## (.+)$/gm, '<h2>$1</h2>');
  html = html.replace(/^# (.+)$/gm, '<h1>$1</h1>');

  // Bold
  html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');

  // Italic
  html = html.replace(/(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/g, '<em>$1</em>');

  // Tables
  html = renderTables(html);

  // Bullet lists
  html = html.replace(/^- (.+)$/gm, '<li>$1</li>');
  html = html.replace(/((?:<li>.*<\/li>\n?)+)/g, '<ul>$1</ul>');

  // Numbered lists
  html = html.replace(/^\d+\. (.+)$/gm, '<li>$1</li>');

  // Paragraphs
  html = html.replace(/\n\n+/g, '</p><p>');
  html = html.replace(/([^>])\n([^<])/g, '$1<br>$2');
  html = '<p>' + html + '</p>';

  // Clean up
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
      inTable = true; tableLines.push(trimmed);
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
  const parseRow = line => line.split('|').filter((_,i,a) => i > 0 && i < a.length-1).map(c => c.trim());
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
  if (h.includes('score') || h.includes('søvn')) {
    if (num >= 80) return '<span class="score-good">' + val + '</span>';
    if (num >= 65) return '<span class="score-ok">' + val + '</span>';
    if (num > 0) return '<span class="score-low">' + val + '</span>';
  }
  if (h.includes('step') || h.includes('skridt')) {
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
    d.innerHTML = '<div class="bot-label">Health Assistant</div>' + md(text);
  } else {
    d.textContent = text;
  }
  chatEl.appendChild(d);
  requestAnimationFrame(() => { chatEl.scrollTop = chatEl.scrollHeight; });
}

function showTyping() {
  const d = document.createElement('div');
  d.className = 'typing'; d.id = 'typing';
  d.innerHTML = '<span></span><span></span><span></span>';
  chatEl.appendChild(d);
  requestAnimationFrame(() => { chatEl.scrollTop = chatEl.scrollHeight; });
}
function hideTyping() {
  const t = document.getElementById('typing');
  if (t) t.remove();
}

async function ask(question) {
  if (!question.trim() || isBusy) return;
  isBusy = true;
  const lastQ = question;
  addMsg(question, 'user');
  inputEl.value = '';
  // Remove welcome chips after first use
  const w = document.getElementById('welcome');
  if (w) { const c = w.querySelector('.quick-chips'); if (c) c.remove(); }
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
      localStorage.removeItem('health_token');
      document.getElementById('login').classList.remove('hidden');
      addMsg('Session udløbet. Log ind igen.', 'bot error');
    } else {
      addRetryMsg('Serverfejl. Prøv igen.', lastQ);
    }
  } catch(e) {
    hideTyping();
    addRetryMsg('Kan ikke nå serveren. Tjek din forbindelse.', lastQ);
  } finally {
    isBusy = false;
  }
}

function addRetryMsg(text, retryQuestion) {
  const d = document.createElement('div');
  d.className = 'msg bot error';
  d.innerHTML = '<div class="bot-label">Health Assistant</div><p>' + escapeHtml(text) +
    '</p><button class="retry-btn" onclick="ask(\'' + escapeHtml(retryQuestion).replace(/'/g, "\\'") +
    '\')">Prøv igen</button>';
  chatEl.appendChild(d);
  requestAnimationFrame(() => { chatEl.scrollTop = chatEl.scrollHeight; });
}

function send() { ask(inputEl.value); }
inputEl.addEventListener('keydown', e => { if (e.key === 'Enter') send(); });

/* --- iOS keyboard resize --- */
if (window.visualViewport) {
  let pending = false;
  window.visualViewport.addEventListener('resize', () => {
    if (pending) return;
    pending = true;
    requestAnimationFrame(() => {
      document.body.style.height = window.visualViewport.height + 'px';
      chatEl.scrollTop = chatEl.scrollHeight;
      pending = false;
    });
  });
  window.visualViewport.addEventListener('scroll', () => {
    requestAnimationFrame(() => {
      document.body.style.height = window.visualViewport.height + 'px';
    });
  });
}

/* --- Server health check --- */
fetch(API + '/health').then(r => r.json()).then(d => {
  const dot = document.getElementById('status-dot');
  const txt = document.getElementById('status-text');
  if (d.status === 'healthy') {
    dot.style.background = 'var(--system-green)';
    dot.style.boxShadow = '0 0 6px rgba(48,209,88,0.5)';
    txt.textContent = 'Forbundet';
  } else {
    dot.style.background = 'var(--system-red)';
    dot.style.boxShadow = '0 0 6px rgba(255,69,58,0.5)';
    txt.textContent = 'Degraderet';
  }
}).catch(() => {
  document.getElementById('status-dot').style.background = 'var(--system-red)';
  document.getElementById('status-text').textContent = 'Offline';
});
</script>
</body>
</html>"""


@router.get("/", response_class=HTMLResponse)
async def chat_ui(request: Request):
    """Serve the mobile chat interface."""
    return HTMLResponse(
        content=CHAT_HTML,
        headers={
            "Content-Security-Policy": (
                "default-src 'self'; "
                "script-src 'unsafe-inline'; "
                "style-src 'unsafe-inline'; "
                "img-src 'self' data:;"
            ),
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
        },
    )
