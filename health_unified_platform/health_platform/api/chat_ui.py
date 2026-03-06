"""Mobile-friendly chat UI for the Health API.

Serves a single-page chat interface at the root URL.
Authentication is handled via a login screen — token stored in browser sessionStorage.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["ui"])

CHAT_HTML = """<!DOCTYPE html>
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
  --user-bg: #1f6feb; --bot-bg: #21262d;
}
body {
  font-family: -apple-system, BlinkMacSystemFont, 'SF Pro', system-ui, sans-serif;
  background: var(--bg); color: var(--text);
  height: 100dvh; display: flex; flex-direction: column;
  -webkit-font-smoothing: antialiased;
}
header {
  padding: 16px 20px; border-bottom: 1px solid var(--border);
  display: flex; align-items: center; gap: 12px;
  background: var(--surface);
}
header .dot { width:10px; height:10px; border-radius:50%; background:#3fb950; }
header h1 { font-size:17px; font-weight:600; }
header .status { font-size:12px; color:var(--muted); margin-left:auto; }

#chat {
  flex:1; overflow-y:auto; padding:16px 12px;
  display:flex; flex-direction:column; gap:12px;
  scroll-behavior: smooth;
}
.msg {
  max-width: 85%; padding: 10px 14px; border-radius: 16px;
  font-size: 15px; line-height: 1.45; white-space: pre-wrap;
  word-wrap: break-word; animation: fadeIn 0.2s ease;
}
@keyframes fadeIn { from{opacity:0;transform:translateY(8px)} to{opacity:1;transform:none} }
.msg.user {
  align-self: flex-end; background: var(--user-bg);
  border-bottom-right-radius: 4px; color: #fff;
}
.msg.bot {
  align-self: flex-start; background: var(--bot-bg);
  border-bottom-left-radius: 4px; border: 1px solid var(--border);
  font-family: 'SF Mono', 'Menlo', monospace; font-size: 13px;
}
.msg.bot .label {
  font-size: 11px; color: var(--accent); margin-bottom: 4px;
  font-family: -apple-system, system-ui, sans-serif; font-weight: 600;
}
.msg.error { background: #3d1f1f; border-color: #f85149; }

#input-area {
  padding: 12px; border-top: 1px solid var(--border);
  background: var(--surface); display: flex; gap: 8px;
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
  flex-shrink: 0;
}
#input-area button:active { transform: scale(0.92); }
#input-area button svg { fill: #fff; width: 20px; height: 20px; }

.quick-btns {
  display: flex; gap: 6px; padding: 8px 12px; overflow-x: auto;
  -webkit-overflow-scrolling: touch;
}
.quick-btns button {
  padding: 6px 14px; border-radius: 16px; border: 1px solid var(--border);
  background: var(--surface); color: var(--text); font-size: 13px;
  white-space: nowrap; cursor: pointer; flex-shrink: 0;
}
.quick-btns button:active { background: var(--accent); border-color: var(--accent); }

.typing { display:flex; gap:4px; padding:8px 14px; align-self:flex-start; }
.typing span {
  width:8px; height:8px; border-radius:50%; background:var(--muted);
  animation: blink 1.4s infinite both;
}
.typing span:nth-child(2) { animation-delay:0.2s; }
.typing span:nth-child(3) { animation-delay:0.4s; }
@keyframes blink { 0%,80%,100%{opacity:.3} 40%{opacity:1} }

/* Login overlay */
#login {
  position:fixed; inset:0; background:var(--bg); z-index:10;
  display:flex; flex-direction:column; align-items:center;
  justify-content:center; gap:20px; padding:20px;
}
#login.hidden { display:none; }
#login h2 { font-size:22px; }
#login p { color:var(--muted); font-size:14px; text-align:center; max-width:300px; }
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
#login .error-msg { color:#f85149; font-size:13px; }
</style>
</head>
<body>

<div id="login">
  <h2>Health Assistant</h2>
  <p>Enter your API token to connect to your health data.</p>
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
    <div class="label">Health Assistant</div>
    Ask me about your health data. Try: sleep, steps, readiness, weight, stress, or workouts.
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
    autocomplete="off" autocapitalize="off">
  <button onclick="send()" aria-label="Send">
    <svg viewBox="0 0 24 24"><path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/></svg>
  </button>
</div>

<script>
const API = window.location.origin;
let token = sessionStorage.getItem('health_token') || '';

function doLogin() {
  token = document.getElementById('token-input').value.trim();
  if (!token) return;
  // Test token
  fetch(API + '/v1/chat', {
    method:'POST',
    headers:{'Authorization':'Bearer '+token,'Content-Type':'application/json'},
    body: JSON.stringify({question:'sleep',format:'plain'})
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

// Auto-login if token exists
if (token) {
  document.getElementById('login').classList.add('hidden');
}

const chatEl = document.getElementById('chat');
const inputEl = document.getElementById('q');

function addMsg(text, cls) {
  const d = document.createElement('div');
  d.className = 'msg ' + cls;
  if (cls === 'bot') {
    d.innerHTML = '<div class="label">Health Assistant</div>' + escapeHtml(text);
  } else {
    d.textContent = text;
  }
  chatEl.appendChild(d);
  chatEl.scrollTop = chatEl.scrollHeight;
  return d;
}

function escapeHtml(t) {
  const d = document.createElement('div');
  d.textContent = t;
  return d.innerHTML;
}

function showTyping() {
  const d = document.createElement('div');
  d.className = 'typing';
  d.id = 'typing';
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
  showTyping();

  try {
    const r = await fetch(API + '/v1/chat', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({question: question, format: 'plain'})
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

// Check server health
fetch(API + '/health').then(r => r.json()).then(d => {
  document.getElementById('status-dot').style.background =
    d.status === 'healthy' ? '#3fb950' : '#f85149';
  document.getElementById('status-text').textContent =
    d.status === 'healthy' ? 'Connected' : 'Degraded';
}).catch(() => {
  document.getElementById('status-dot').style.background = '#f85149';
  document.getElementById('status-text').textContent = 'Offline';
});
</script>
</body>
</html>"""


@router.get("/", response_class=HTMLResponse)
async def chat_ui(request: Request):
    """Serve the mobile chat interface."""
    return HTMLResponse(content=CHAT_HTML)
