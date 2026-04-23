'use strict';

const configuredApiBase = (window.APP_CONFIG?.apiBase || '').trim();
const API_BASE = configuredApiBase && !/^%%.+%%$/.test(configuredApiBase)
  ? configuredApiBase.replace(/\/$/, '')
  : window.location.origin;

const $ = (sel) => document.querySelector(sel);

const dom = {
  btnLoadScores: $('#btn-load-scores'),
  scoreEmpty: $('#score-empty'),
  scoreLoading: $('#score-loading'),
  scoreSummary: $('#score-summary'),
  scoreResults: $('#score-results'),
  scoreBody: $('#score-body'),
  scorePriorityBreakdown: $('#score-priority-breakdown'),
  btnMonthFilter: $('#btn-month-filter'),
  scoreMonthDropdown: $('#score-month-dropdown'),
  scoreMonthOptions: $('#score-month-options'),
  btnMonthSelectAll: $('#btn-month-select-all'),
  btnMonthClearAll: $('#btn-month-clear-all'),
  btnGroupFilter: $('#btn-group-filter'),
  scoreGroupDropdown: $('#score-group-dropdown'),
  scoreGroupOptions: $('#score-group-options'),
  btnGroupSelectAll: $('#btn-group-select-all'),
  btnGroupClearAll: $('#btn-group-clear-all'),
  scoreGroupSearch: $('#score-group-search'),
};

const state = { months: [] };

const scoreState = {
  selectedMonths: [],
  allGroups: [],
  selectedGroups: [],
  lastData: null,
};

// ── Helpers ──────────────────────────────────────────────────────────────

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || `HTTP ${response.status}`);
  }
  return response.json();
}

function formatDuration(totalSeconds) {
  const seconds = Math.max(0, Number(totalSeconds) || 0);
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const parts = [];
  if (days) parts.push(`${days}d`);
  if (hours) parts.push(`${hours}h`);
  parts.push(`${minutes}m`);
  return parts.join(' ');
}

function monthToLabel(month) {
  const [y, m] = month.split('-');
  const names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return `${names[parseInt(m, 10) - 1]} ${y}`;
}

function confidenceBadgeClass(confidence) {
  if (confidence === 'high') return 'confidence-high';
  if (confidence === 'medium') return 'confidence-medium';
  return 'confidence-low';
}

// ── Month dropdown ───────────────────────────────────────────────────────

function renderScoreMonths() {
  if (!dom.scoreMonthOptions) return;
  dom.scoreMonthOptions.innerHTML = '';
  state.months.forEach((month) => {
    const selected = scoreState.selectedMonths.includes(month);
    const label = document.createElement('label');
    label.className = `dropdown-option${selected ? ' selected' : ''}`;
    label.innerHTML = `<input type="checkbox" ${selected ? 'checked' : ''}/> ${monthToLabel(month)}`;
    label.querySelector('input').addEventListener('change', (e) => {
      if (e.target.checked) {
        scoreState.selectedMonths.push(month);
      } else {
        scoreState.selectedMonths = scoreState.selectedMonths.filter((m) => m !== month);
      }
      renderScoreMonths();
    });
    dom.scoreMonthOptions.appendChild(label);
  });
  if (dom.btnMonthFilter) {
    const count = scoreState.selectedMonths.length;
    if (count === 0 || count === state.months.length) {
      dom.btnMonthFilter.textContent = 'All Months ▾';
    } else if (count === 1) {
      dom.btnMonthFilter.textContent = monthToLabel(scoreState.selectedMonths[0]) + ' ▾';
    } else {
      dom.btnMonthFilter.textContent = `${count} months selected ▾`;
    }
  }
}

// ── Group dropdown ───────────────────────────────────────────────────────

function renderGroupFilter(searchTerm) {
  if (!dom.scoreGroupOptions) return;
  dom.scoreGroupOptions.innerHTML = '';
  const filter = (searchTerm || '').toLowerCase();
  scoreState.allGroups.forEach((name) => {
    if (filter && !name.toLowerCase().includes(filter)) return;
    const selected = scoreState.selectedGroups.includes(name);
    const label = document.createElement('label');
    label.className = `dropdown-option${selected ? ' selected' : ''}`;
    label.innerHTML = `<input type="checkbox" ${selected ? 'checked' : ''}/> ${name}`;
    label.querySelector('input').addEventListener('change', (e) => {
      if (e.target.checked) {
        scoreState.selectedGroups.push(name);
      } else {
        scoreState.selectedGroups = scoreState.selectedGroups.filter((g) => g !== name);
      }
      renderGroupFilter(dom.scoreGroupSearch ? dom.scoreGroupSearch.value : '');
      applyGroupFilter();
    });
    dom.scoreGroupOptions.appendChild(label);
  });
  if (dom.btnGroupFilter) {
    const count = scoreState.selectedGroups.length;
    if (count === 0 || count === scoreState.allGroups.length) {
      dom.btnGroupFilter.textContent = 'All Groups ▾';
    } else if (count === 1) {
      dom.btnGroupFilter.textContent = scoreState.selectedGroups[0] + ' ▾';
    } else {
      dom.btnGroupFilter.textContent = `${count} groups selected ▾`;
    }
  }
}

function applyGroupFilter() {
  if (!scoreState.lastData) return;
  const data = scoreState.lastData;
  const filtered = scoreState.selectedGroups.length > 0
    ? data.groups.filter((g) => scoreState.selectedGroups.includes(g.assignmentGroup))
    : data.groups;
  renderScoreEngine({ ...data, groups: filtered }, true);
}

// ── Render ───────────────────────────────────────────────────────────────

function renderScoreEngine(data, isFilterOnly) {
  if (!data || !data.groups || !data.groups.length) {
    if (dom.scoreEmpty) {
      dom.scoreEmpty.classList.remove('hidden');
      dom.scoreEmpty.textContent = 'No data available for the selected months.';
    }
    if (dom.scoreResults) dom.scoreResults.classList.add('hidden');
    if (dom.scoreSummary) dom.scoreSummary.classList.add('hidden');
    return;
  }

  if (dom.scoreEmpty) dom.scoreEmpty.classList.add('hidden');

  if (dom.scoreSummary && !isFilterOnly) {
    const summary = data.summary || {};
    const monthsLabel = Array.isArray(summary.monthsAnalyzed)
      ? summary.monthsAnalyzed.join(', ')
      : 'All months';
    dom.scoreSummary.innerHTML = `
      <div class="score-metric">
        <span class="label">Resolved Tickets</span>
        <span class="value-sm">${summary.totalTicketsAnalyzed || 0}</span>
      </div>
      <div class="score-metric">
        <span class="label">Scored Groups</span>
        <span class="value-sm">${summary.groupCount || 0}</span>
      </div>
      <div class="score-metric">
        <span class="label">Months</span>
        <span class="value-sm">${monthsLabel}</span>
      </div>
    `;
    dom.scoreSummary.classList.remove('hidden');
  }

  const forecastMap = {};
  (data.forecast || []).forEach((f) => {
    forecastMap[f.assignmentGroup] = f;
  });

  if (dom.scoreBody) {
    dom.scoreBody.innerHTML = '';
    const top10 = data.groups.slice(0, 10);
    const rest = data.groups.slice(10);
    const grandTotal = (scoreState.lastData || data).summary?.totalTicketsAnalyzed || (scoreState.lastData || data).groups.reduce((s, g) => s + g.ticketsResolved, 0) || 1;

    top10.forEach((group) => {
      const forecast = forecastMap[group.assignmentGroup] || {};
      const sharePct = Math.round(group.ticketsResolved / grandTotal * 1000) / 10;
      const confidenceClass = confidenceBadgeClass(forecast.confidence);
      const trendIcon = forecast.trend === 'up' ? '&#9650;' : forecast.trend === 'down' ? '&#9660;' : '&#9654;';
      const trendClass = forecast.trend === 'up' ? 'trend-up' : forecast.trend === 'down' ? 'trend-down' : 'trend-stable';
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td><span class="rank-badge rank-${group.rank <= 3 ? group.rank : 'default'}">${group.rank}</span></td>
        <td><strong>${group.assignmentGroup}</strong></td>
        <td>${group.ticketsReceived || group.ticketsResolved}</td>
        <td>${group.ticketsResolved}</td>
        <td>${group.resolutionRatePct || 0}%</td>
        <td>${sharePct}%</td>
        <td>${formatDuration(group.avgResolutionSeconds)}</td>
        <td>${formatDuration(group.medianResolutionSeconds)}</td>
        <td>
          <div class="speed-bar-wrap">
            <div class="speed-bar" style="width: ${Math.max(group.speedScore, 2)}%"></div>
            <span>${group.speedScore}</span>
          </div>
        </td>
        <td>
          <span class="confidence-badge ${confidenceClass}">${forecast.forecastSharePct || 0}%</span>
          <span class="${trendClass}">${trendIcon}</span>
          <small class="confidence-label">${forecast.confidence || 'low'}</small>
        </td>
      `;
      dom.scoreBody.appendChild(tr);
    });

    if (rest.length) {
      const othersReceived = rest.reduce((s, g) => s + (g.ticketsReceived || g.ticketsResolved), 0);
      const othersResolved = rest.reduce((s, g) => s + g.ticketsResolved, 0);
      const othersRate = othersReceived > 0 ? Math.round(othersResolved * 1000 / othersReceived) / 10 : 0;
      const othersAvgSecs = othersResolved > 0 ? Math.round(rest.reduce((s, g) => s + g.avgResolutionSeconds * g.ticketsResolved, 0) / othersResolved) : 0;
      const othersMedianSecs = othersResolved > 0 ? Math.round(rest.reduce((s, g) => s + g.medianResolutionSeconds * g.ticketsResolved, 0) / othersResolved) : 0;
      const othersSpeed = rest.length > 0 ? Math.round(rest.reduce((s, g) => s + g.speedScore, 0) / rest.length * 10) / 10 : 0;
      const othersSharePct = Math.round(othersResolved / grandTotal * 1000) / 10;
      const tr = document.createElement('tr');
      tr.className = 'others-row';
      tr.innerHTML = `
        <td><span class="rank-badge rank-default">…</span></td>
        <td><strong>Others (${rest.length} groups)</strong></td>
        <td>${othersReceived}</td>
        <td>${othersResolved}</td>
        <td>${othersRate}%</td>
        <td>${othersSharePct}%</td>
        <td>${formatDuration(othersAvgSecs)}</td>
        <td>${formatDuration(othersMedianSecs)}</td>
        <td>
          <div class="speed-bar-wrap">
            <div class="speed-bar" style="width: ${Math.max(othersSpeed, 2)}%"></div>
            <span>${othersSpeed}</span>
          </div>
        </td>
        <td>—</td>
      `;
      dom.scoreBody.appendChild(tr);
    }
  }

  if (dom.scorePriorityBreakdown) {
    dom.scorePriorityBreakdown.innerHTML = '';
    const topGroups = [...data.groups].sort((a, b) => b.ticketsResolved - a.ticketsResolved).slice(0, 3);
    if (topGroups.length) {
      const title = document.createElement('h3');
      title.textContent = 'Priority Breakdown (Top 3 by Tickets Resolved)';
      title.className = 'priority-title';
      dom.scorePriorityBreakdown.appendChild(title);

      const grid = document.createElement('div');
      grid.className = 'priority-grid';
      topGroups.forEach((group) => {
        const pb = group.priorityBreakdown || {};
        const card = document.createElement('div');
        card.className = 'priority-card';
        card.innerHTML = `
          <h4>${group.assignmentGroup}</h4>
          <div class="priority-bars">
            ${['P1', 'P2', 'P3', 'P4', 'P5'].map((p) => {
              const count = pb[p] || 0;
              const pct = group.ticketsResolved > 0 ? Math.round((count / group.ticketsResolved) * 100) : 0;
              return `<div class="priority-row">
                <span class="priority-label priority-${p.toLowerCase()}">${p}</span>
                <div class="priority-bar-track"><div class="priority-bar-fill priority-fill-${p.toLowerCase()}" style="width: ${pct}%"></div></div>
                <span class="priority-count">${count} (${pct}%)</span>
              </div>`;
            }).join('')}
          </div>
        `;
        grid.appendChild(card);
      });
      dom.scorePriorityBreakdown.appendChild(grid);
    }
  }

  if (dom.scoreResults) dom.scoreResults.classList.remove('hidden');
}

// ── Load ─────────────────────────────────────────────────────────────────

async function loadScoreEngine() {
  const monthsToUse = scoreState.selectedMonths.length ? scoreState.selectedMonths : state.months;

  if (!monthsToUse.length) {
    if (dom.scoreEmpty) {
      dom.scoreEmpty.classList.remove('hidden');
      dom.scoreEmpty.textContent = 'No months available yet. Wait for data to load.';
    }
    return;
  }

  if (dom.scoreEmpty) dom.scoreEmpty.classList.add('hidden');
  if (dom.scoreLoading) dom.scoreLoading.classList.remove('hidden');
  if (dom.scoreResults) dom.scoreResults.classList.add('hidden');
  if (dom.scoreSummary) dom.scoreSummary.classList.add('hidden');

  try {
    const query = new URLSearchParams();
    query.set('months', monthsToUse.join(','));
    const data = await fetchJson(`${API_BASE}/api/ticket-lifecycle/score-engine?${query.toString()}`);
    scoreState.lastData = data;
    scoreState.allGroups = (data.groups || []).map((g) => g.assignmentGroup);
    scoreState.selectedGroups = [];
    renderGroupFilter();
    renderScoreEngine(data);
  } catch (error) {
    if (dom.scoreEmpty) {
      dom.scoreEmpty.classList.remove('hidden');
      dom.scoreEmpty.textContent = `Error loading scores: ${error.message}`;
    }
    console.error(error);
  } finally {
    if (dom.scoreLoading) dom.scoreLoading.classList.add('hidden');
  }
}

// ── Events ───────────────────────────────────────────────────────────────

function bindEvents() {
  if (dom.btnLoadScores) {
    dom.btnLoadScores.addEventListener('click', loadScoreEngine);
  }

  // Month dropdown
  if (dom.btnMonthFilter) {
    dom.btnMonthFilter.addEventListener('click', (e) => {
      e.stopPropagation();
      dom.scoreMonthDropdown.classList.toggle('hidden');
    });
    dom.scoreMonthDropdown.addEventListener('click', (e) => { e.stopPropagation(); });
    document.addEventListener('click', () => { dom.scoreMonthDropdown.classList.add('hidden'); });
  }
  if (dom.btnMonthSelectAll) {
    dom.btnMonthSelectAll.addEventListener('click', () => {
      scoreState.selectedMonths = [...state.months];
      renderScoreMonths();
    });
  }
  if (dom.btnMonthClearAll) {
    dom.btnMonthClearAll.addEventListener('click', () => {
      scoreState.selectedMonths = [];
      renderScoreMonths();
    });
  }

  // Group dropdown
  if (dom.btnGroupFilter) {
    dom.btnGroupFilter.addEventListener('click', (e) => {
      e.stopPropagation();
      dom.scoreGroupDropdown.classList.toggle('hidden');
    });
    dom.scoreGroupDropdown.addEventListener('click', (e) => { e.stopPropagation(); });
    document.addEventListener('click', () => { dom.scoreGroupDropdown.classList.add('hidden'); });
  }
  if (dom.btnGroupSelectAll) {
    dom.btnGroupSelectAll.addEventListener('click', () => {
      scoreState.selectedGroups = [...scoreState.allGroups];
      renderGroupFilter();
      applyGroupFilter();
    });
  }
  if (dom.btnGroupClearAll) {
    dom.btnGroupClearAll.addEventListener('click', () => {
      scoreState.selectedGroups = [];
      renderGroupFilter();
      applyGroupFilter();
    });
  }
  if (dom.scoreGroupSearch) {
    dom.scoreGroupSearch.addEventListener('input', () => {
      renderGroupFilter(dom.scoreGroupSearch.value);
    });
  }
}

// ── Init ─────────────────────────────────────────────────────────────────

async function init() {
  bindEvents();
  try {
    state.months = await fetchJson(`${API_BASE}/api/ticket-lifecycle/available-months`);
    renderScoreMonths();
  } catch (error) {
    if (dom.scoreEmpty) {
      dom.scoreEmpty.textContent = `Error loading months: ${error.message}`;
    }
    console.error(error);
  }
}

document.addEventListener('DOMContentLoaded', init);
