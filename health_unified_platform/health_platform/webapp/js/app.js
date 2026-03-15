/**
 * Health App — App Shell Logic
 * Auth (localStorage token), tab navigation, API client helper, health check.
 */

const API = window.location.origin;
let token = localStorage.getItem('health_token') || '';

/* ===== Auth / Login ===== */

function doLogin() {
  const inp = document.getElementById('token-input');
  const btn = document.getElementById('login-btn');
  token = inp.value.trim();
  if (!token) return;

  btn.textContent = '...';
  btn.disabled = true;

  fetch(API + '/health')
    .then(r => r.json())
    .then(d => {
      if (d.status) {
        return fetch(API + '/v1/profile', {
          headers: { 'Authorization': 'Bearer ' + token }
        });
      }
    })
    .then(r => {
      btn.textContent = 'Forbind';
      btn.disabled = false;
      if (r && r.ok) {
        localStorage.setItem('health_token', token);
        document.getElementById('login').classList.add('hidden');
        // Trigger dashboard load after login
        if (typeof onDashboardVisible === 'function') onDashboardVisible();
      } else {
        document.getElementById('login-error').textContent = 'Ugyldigt token';
      }
    })
    .catch(() => {
      btn.textContent = 'Forbind';
      btn.disabled = false;
      document.getElementById('login-error').textContent = 'Kan ikke nå serveren';
    });
}

document.getElementById('token-input').addEventListener('keydown', e => {
  if (e.key === 'Enter') doLogin();
});
document.getElementById('login-btn').addEventListener('click', doLogin);

// Auto-hide login if token exists
if (token) {
  document.getElementById('login').classList.add('hidden');
}

/* ===== Tab Navigation ===== */

function switchTab(tabId) {
  // Deactivate all tabs and pages
  document.querySelectorAll('#tab-bar .tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));

  // Activate selected
  const tab = document.querySelector(`#tab-bar .tab[data-tab="${tabId}"]`);
  const page = document.getElementById('page-' + tabId);
  if (tab) tab.classList.add('active');
  if (page) page.classList.add('active');

  // Notify page-specific loaders
  if (tabId === 'dashboard' && typeof onDashboardVisible === 'function') onDashboardVisible();
  if (tabId === 'trends' && typeof onTrendsVisible === 'function') onTrendsVisible();
}

// Bind tab clicks
document.querySelectorAll('#tab-bar .tab').forEach(tab => {
  tab.addEventListener('click', () => switchTab(tab.dataset.tab));
});

/* ===== API Client Helper ===== */

async function apiFetch(path, options = {}) {
  const headers = {
    'Authorization': 'Bearer ' + token,
    'Content-Type': 'application/json',
    ...options.headers
  };

  const response = await fetch(API + path, { ...options, headers });

  if (response.status === 401) {
    localStorage.removeItem('health_token');
    document.getElementById('login').classList.remove('hidden');
    throw new Error('Unauthorized');
  }

  return response;
}

/* ===== Health Check / Status Dot ===== */

function checkHealth() {
  fetch(API + '/health')
    .then(r => r.json())
    .then(d => {
      const dot = document.getElementById('status-dot');
      const txt = document.getElementById('status-text');
      if (d.status === 'healthy') {
        dot.style.background = 'var(--status-green)';
        dot.style.boxShadow = '0 0 6px rgba(92,138,92,0.5)';
        txt.textContent = 'Forbundet';
      } else {
        dot.style.background = 'var(--status-red)';
        dot.style.boxShadow = '0 0 6px rgba(200,92,92,0.5)';
        txt.textContent = 'Degraderet';
      }
    })
    .catch(() => {
      document.getElementById('status-dot').style.background = 'var(--status-red)';
      document.getElementById('status-text').textContent = 'Offline';
    });
}

checkHealth();

/* ===== iOS Keyboard Resize ===== */

if (window.visualViewport) {
  let pending = false;
  window.visualViewport.addEventListener('resize', () => {
    if (pending) return;
    pending = true;
    requestAnimationFrame(() => {
      document.getElementById('app').style.height = window.visualViewport.height + 'px';
      pending = false;
    });
  });
  window.visualViewport.addEventListener('scroll', () => {
    requestAnimationFrame(() => {
      document.getElementById('app').style.height = window.visualViewport.height + 'px';
    });
  });
}
