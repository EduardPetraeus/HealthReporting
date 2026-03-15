/**
 * Health App — Chat Page
 * SSE streaming chat with the Health AI assistant.
 * Ported from chat_ui.py, uses shared md.js renderer and app.js apiFetch.
 */

let isBusy = false;
let sessionId = crypto.randomUUID();

const chatEl = document.getElementById('chat-messages');
const chatInput = document.getElementById('chat-input');

/* --- Messages --- */

function addMsg(text, cls) {
  const d = document.createElement('div');
  d.className = 'chat-msg ' + cls;
  if (cls.startsWith('bot')) {
    d.innerHTML = '<div class="chat-bot-label">Health Assistant</div>' + md(text);
  } else {
    d.textContent = text;
  }
  chatEl.appendChild(d);
  requestAnimationFrame(() => { chatEl.scrollTop = chatEl.scrollHeight; });
}

function createBotMessage() {
  const d = document.createElement('div');
  d.className = 'chat-msg bot';
  d.innerHTML = '<div class="chat-bot-label">Health Assistant</div>';
  chatEl.appendChild(d);
  requestAnimationFrame(() => { chatEl.scrollTop = chatEl.scrollHeight; });
  return d;
}

function updateBotMessage(el, text) {
  el.innerHTML = '<div class="chat-bot-label">Health Assistant</div>' + md(text);
  requestAnimationFrame(() => { chatEl.scrollTop = chatEl.scrollHeight; });
}

function showTyping() {
  const d = document.createElement('div');
  d.className = 'chat-typing'; d.id = 'typing';
  d.innerHTML = '<span></span><span></span><span></span>';
  chatEl.appendChild(d);
  requestAnimationFrame(() => { chatEl.scrollTop = chatEl.scrollHeight; });
}

function hideTyping() {
  const t = document.getElementById('typing');
  if (t) t.remove();
}

function addRetryMsg(text, retryQuestion) {
  const d = document.createElement('div');
  d.className = 'chat-msg bot error';
  d.innerHTML = '<div class="chat-bot-label">Health Assistant</div><p>' + escapeHtml(text) + '</p>';
  const btn = document.createElement('button');
  btn.className = 'chat-retry-btn';
  btn.textContent = 'Prøv igen';
  btn.addEventListener('click', () => ask(retryQuestion));
  d.appendChild(btn);
  chatEl.appendChild(d);
  requestAnimationFrame(() => { chatEl.scrollTop = chatEl.scrollHeight; });
}

/* --- Send / SSE streaming --- */

async function ask(question) {
  if (!question.trim() || isBusy) return;
  isBusy = true;
  const lastQ = question;
  addMsg(question, 'user');
  chatInput.value = '';

  // Remove welcome chips after first use
  const chips = document.getElementById('chat-welcome-chips');
  if (chips) chips.remove();

  showTyping();

  try {
    const response = await fetch(API + '/v1/chat/stream', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ question: question, format: 'markdown', session_id: sessionId })
    });

    if (response.status === 401) {
      hideTyping();
      localStorage.removeItem('health_token');
      document.getElementById('login').classList.remove('hidden');
      addMsg('Session udløbet. Log ind igen.', 'bot error');
      return;
    }

    if (!response.ok) {
      hideTyping();
      addRetryMsg('Serverfejl. Prøv igen.', lastQ);
      return;
    }

    hideTyping();
    const msgDiv = createBotMessage();
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let fullText = '';

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        const chunk = decoder.decode(value, { stream: true });
        for (const line of chunk.split(/\r?\n/)) {
          if (line.startsWith('data: ')) {
            try {
              const data = JSON.parse(line.slice(6));
              if (data.text) {
                fullText += data.text;
                updateBotMessage(msgDiv, fullText);
              }
            } catch (_) { /* incomplete JSON chunk */ }
          }
        }
      }
    } finally {
      reader.cancel().catch(() => {});
    }

    if (!fullText) {
      updateBotMessage(msgDiv, 'Intet svar modtaget.');
    }
  } catch (_) {
    hideTyping();
    // Fallback to non-streaming endpoint
    try {
      const r = await apiFetch('/v1/chat', {
        method: 'POST',
        body: JSON.stringify({ question: lastQ, format: 'markdown', session_id: sessionId })
      });
      if (r.ok) {
        const data = await r.json();
        addMsg(data.answer, 'bot');
      } else {
        addRetryMsg('Kan ikke nå serveren. Tjek din forbindelse.', lastQ);
      }
    } catch (_) {
      addRetryMsg('Kan ikke nå serveren. Tjek din forbindelse.', lastQ);
    }
  } finally {
    isBusy = false;
  }
}

function chatSend() { ask(chatInput.value); }

chatInput.addEventListener('keydown', e => { if (e.key === 'Enter') chatSend(); });
document.getElementById('chat-send-btn').addEventListener('click', chatSend);
