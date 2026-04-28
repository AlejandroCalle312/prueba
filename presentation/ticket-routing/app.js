'use strict';

const configuredApiBase = (window.APP_CONFIG?.apiBase || '').trim();
const API_BASE = configuredApiBase && !/^%%.+%%$/.test(configuredApiBase)
  ? configuredApiBase.replace(/\/$/, '')
  : window.location.origin;

const $ = (sel) => document.querySelector(sel);

const dom = {
  btnLoad: $('#btn-load'),
  emptyMsg: $('#empty-msg'),
  loadingMsg: $('#loading-msg'),
  summaryBar: $('#summary-bar'),
  results: $('#results'),
  ownerBars: $('#owner-bars'),
  resolverCards: $('#resolver-cards'),
  btnMonthFilter: $('#btn-month-filter'),
  monthDropdown: $('#month-dropdown'),
  monthOptions: $('#month-options'),
  btnMonthSelectAll: $('#btn-month-select-all'),
  btnMonthClearAll: $('#btn-month-clear-all'),
};

const state = { months: [], selectedMonths: [] };

// ── Helpers ──────────────────────────────────────────────────────────────

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || `HTTP ${response.status}`);
  }
  return response.json();
}

function monthToLabel(month) {
  const [y, m] = month.split('-');
  const names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return `${names[parseInt(m, 10) - 1]} ${y}`;
}

// ── Month dropdown ───────────────────────────────────────────────────────

function renderMonths() {
  if (!dom.monthOptions) return;
  dom.monthOptions.innerHTML = '';
  state.months.forEach((month) => {
    const selected = state.selectedMonths.includes(month);
    const label = document.createElement('label');
    label.className = `dropdown-option${selected ? ' selected' : ''}`;
    label.innerHTML = `<input type="checkbox" ${selected ? 'checked' : ''}/> ${monthToLabel(month)}`;
    label.querySelector('input').addEventListener('change', (e) => {
      if (e.target.checked) {
        state.selectedMonths.push(month);
      } else {
        state.selectedMonths = state.selectedMonths.filter((m) => m !== month);
      }
      renderMonths();
    });
    dom.monthOptions.appendChild(label);
  });
  if (dom.btnMonthFilter) {
    const count = state.selectedMonths.length;
    if (count === 0 || count === state.months.length) {
      dom.btnMonthFilter.textContent = 'All Months ▾';
    } else if (count === 1) {
      dom.btnMonthFilter.textContent = monthToLabel(state.selectedMonths[0]) + ' ▾';
    } else {
      dom.btnMonthFilter.textContent = `${count} months selected ▾`;
    }
  }
}

// ── Render ────────────────────────────────────────────────────────────────

function renderResolverCards(resolverRouting) {
  if (!dom.resolverCards) return;
  dom.resolverCards.innerHTML = '';

  (resolverRouting || []).forEach((r) => {
    const card = document.createElement('div');
    card.className = 'resolver-card';

    const maxTickets = r.suggestedOwners.length ? r.suggestedOwners[0].tickets : 1;
    const ownerRows = r.suggestedOwners.map((o) => {
      const pct = Math.round(o.tickets / maxTickets * 100);
      const conf = o.confidence != null ? o.confidence : 0;
      const confClass = conf >= 80 ? 'conf-high' : conf >= 60 ? 'conf-mid' : 'conf-low';
      const m = o.methods || {};
      const methodParts = [];
      if (m.escalation) methodParts.push(`<span class="method-tag method-esc">${m.escalation} esc</span>`);
      if (m.description) methodParts.push(`<span class="method-tag method-desc">${m.description} desc</span>`);
      if (m.it_service) methodParts.push(`<span class="method-tag method-svc">${m.it_service} svc</span>`);
      const methodHtml = methodParts.length ? `<div class="method-tags">${methodParts.join('')}</div>` : '';
      return `
        <div class="owner-bar-row">
          <span class="owner-name">${o.group}</span>
          <div class="owner-bar-wrap">
            <div class="owner-bar" style="width: ${pct}%"></div>
            <span class="owner-count">${o.tickets} <small>(${o.pct}%)</small></span>
          </div>
          <span class="confidence-badge ${confClass}" title="Confidence level">${conf}%</span>
          ${methodHtml}
        </div>`;
    }).join('');

    const selfNote = r.selfResolved > 0 ? `<span class="resolver-note">Self-resolved: ${r.selfResolved}</span>` : '';
    const unclNote = r.unclassified > 0 ? `<span class="resolver-note">Unclassified: ${r.unclassified}</span>` : '';

    card.innerHTML = `
      <div class="resolver-card__header">
        <h3>${r.resolver}</h3>
        <span class="resolver-card__total">${r.totalTickets} tickets</span>
      </div>
      <div class="resolver-card__bars">${ownerRows}</div>
      <div class="resolver-card__footer">${selfNote}${unclNote}</div>
    `;
    dom.resolverCards.appendChild(card);
  });
}

function renderResults(data) {
  const s = data.summary || {};

  // Summary bar
  if (dom.summaryBar) {
    const rm = s.routingMethod || {};
    dom.summaryBar.innerHTML = `
      <div class="score-metric">
        <span class="label">Front-line Resolved</span>
        <span class="value-sm">${s.totalFrontLineResolved || 0}</span>
      </div>
      <div class="score-metric">
        <span class="label">Classified</span>
        <span class="value-sm">${s.classifiedTickets || 0}</span>
      </div>
      <div class="score-metric">
        <span class="label">Unclassified</span>
        <span class="value-sm">${s.unclassifiedTickets || 0}</span>
      </div>
      <div class="score-metric">
        <span class="label">Re-routable</span>
        <span class="value-sm">${s.reroutableTickets || 0} (${s.reroutePct || 0}%)</span>
      </div>
      <div class="routing-method-summary">
        <span class="label">Prediction basis</span>
        <div class="method-tags-summary">
          <span class="method-tag method-esc">${rm.escalation || 0} escalation</span>
          <span class="method-tag method-desc">${rm.description || 0} description</span>
          <span class="method-tag method-svc">${rm.it_service || 0} it_service</span>
        </div>
      </div>
    `;
    dom.summaryBar.classList.remove('hidden');
  }

  // Owner bars
  if (dom.ownerBars) {
    dom.ownerBars.innerHTML = '';
    const owners = data.ownerRanking || [];
    const maxTickets = owners.length ? owners[0].tickets : 1;
    owners.forEach((o) => {
      const pct = Math.round(o.tickets / maxTickets * 100);
      const row = document.createElement('div');
      row.className = 'owner-bar-row';
      row.innerHTML = `
        <span class="owner-name">${o.group}</span>
        <div class="owner-bar-wrap">
          <div class="owner-bar" style="width: ${pct}%"></div>
          <span class="owner-count">${o.tickets}</span>
        </div>
      `;
      dom.ownerBars.appendChild(row);
    });
  }



  if (dom.results) dom.results.classList.remove('hidden');

  // Resolver breakdown cards
  renderResolverCards(data.resolverRouting);
}

// ── Load ─────────────────────────────────────────────────────────────────

async function loadAnalysis() {
  if (dom.emptyMsg) dom.emptyMsg.classList.add('hidden');
  if (dom.results) dom.results.classList.add('hidden');
  if (dom.loadingMsg) dom.loadingMsg.classList.remove('hidden');

  try {
    const monthsParam = state.selectedMonths.length
      ? state.selectedMonths.join(',')
      : state.months.join(',');
    const url = `${API_BASE}/api/ticket-lifecycle/routing-analysis?months=${encodeURIComponent(monthsParam)}`;
    const data = await fetchJson(url);
    renderResults(data);
  } catch (err) {
    if (dom.emptyMsg) {
      dom.emptyMsg.textContent = `Error: ${err.message}`;
      dom.emptyMsg.classList.remove('hidden');
    }
  } finally {
    if (dom.loadingMsg) dom.loadingMsg.classList.add('hidden');
  }
}

// ── Init ─────────────────────────────────────────────────────────────────

async function init() {
  // Load available months from the score-engine endpoint (reuse)
  try {
    const months = await fetchJson(`${API_BASE}/api/ticket-lifecycle/months`);
    state.months = months;
    renderMonths();
  } catch {
    // fallback: generate last 12 months
    const now = new Date();
    for (let i = 11; i >= 0; i--) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      state.months.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`);
    }
    renderMonths();
  }
}

// ── Events ───────────────────────────────────────────────────────────────

if (dom.btnLoad) dom.btnLoad.addEventListener('click', loadAnalysis);

if (dom.btnMonthFilter && dom.monthDropdown) {
  dom.btnMonthFilter.addEventListener('click', () => {
    dom.monthDropdown.classList.toggle('hidden');
  });
  document.addEventListener('click', (e) => {
    if (!dom.monthDropdown.contains(e.target) && e.target !== dom.btnMonthFilter) {
      dom.monthDropdown.classList.add('hidden');
    }
  });
}

if (dom.btnMonthSelectAll) {
  dom.btnMonthSelectAll.addEventListener('click', () => {
    state.selectedMonths = [...state.months];
    renderMonths();
  });
}
if (dom.btnMonthClearAll) {
  dom.btnMonthClearAll.addEventListener('click', () => {
    state.selectedMonths = [];
    renderMonths();
  });
}

init();
