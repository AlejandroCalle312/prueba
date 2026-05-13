'use strict';

const configuredApiBase = (window.APP_CONFIG?.apiBase || '').trim();
const API_BASE = configuredApiBase && !/^%%.+%%$/.test(configuredApiBase)
  ? configuredApiBase.replace(/\/$/, '')
  : window.location.origin;
const COLORS = {
  English: '#1c7ed6',
  German: '#f08c00',
  Other: '#0ca678',
};
const LANG_ORDER = ['English', 'German', 'Other'];
const DETAIL_LIMIT = 5000;

const state = {
  rows: [],
  details: [],
  filteredRows: [],
  filteredDetails: [],
  availableMonths: [],
  selectedMonths: new Set(),
  selectedLanguage: null,
  summary: { total_tickets: 0, totals_by_language: {}, scope: '--' },
  charts: {
    month: null,
    donut: null,
    sparkEnglish: null,
    sparkGerman: null,
    sparkOther: null,
  },
};

const dom = {
  state: document.getElementById('state'),
  bringDataBtn: document.getElementById('bring-data-btn'),
  clearFiltersBtn: document.getElementById('clear-filters-btn'),
  monthToggle: document.getElementById('month-filter-toggle'),
  monthSummary: document.getElementById('month-filter-summary'),
  monthList: document.getElementById('month-filter-list'),
  totalPill: document.getElementById('total-pill'),
  scopePill: document.getElementById('scope-pill'),
  englishCount: document.getElementById('english-count'),
  germanCount: document.getElementById('german-count'),
  otherCount: document.getElementById('other-count'),
  englishShare: document.getElementById('english-share'),
  germanShare: document.getElementById('german-share'),
  otherShare: document.getElementById('other-share'),
  detailBody: document.getElementById('detail-body'),
  monthCanvas: document.getElementById('month-language-chart'),
  donutCanvas: document.getElementById('distribution-chart'),
  sparkEnglish: document.getElementById('english-spark'),
  sparkGerman: document.getElementById('german-spark'),
  sparkOther: document.getElementById('other-spark'),
  languageCards: document.querySelectorAll('.kpi-card[data-language]'),
};

function setStateText(message, isError = false) {
  dom.state.textContent = message;
  dom.state.classList.toggle('error', isError);
}

function monthSortDesc(a, b) {
  return b.localeCompare(a);
}

function asMonthLabel(setValues) {
  if (setValues.size === 0) {
    return 'All months';
  }
  if (setValues.size === 1) {
    return [...setValues][0];
  }
  return `${setValues.size} selected`;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function sumTickets(rows) {
  return rows.reduce((acc, row) => acc + Number(row.ticket_count || 0), 0);
}

function groupByLanguage(rows) {
  const totals = { English: 0, German: 0, Other: 0 };
  rows.forEach((row) => {
    const language = LANG_ORDER.includes(row.language) ? row.language : 'Other';
    totals[language] += Number(row.ticket_count || 0);
  });
  return totals;
}

function groupByMonthAndLanguage(rows) {
  const map = new Map();
  rows.forEach((row) => {
    const month = String(row.month || '');
    const language = LANG_ORDER.includes(row.language) ? row.language : 'Other';
    if (!map.has(month)) {
      map.set(month, { English: 0, German: 0, Other: 0 });
    }
    map.get(month)[language] += Number(row.ticket_count || 0);
  });
  return map;
}

function normalizeLanguage(language) {
  return LANG_ORDER.includes(language) ? language : 'Other';
}

function applyLanguageFilter(rows) {
  if (!state.selectedLanguage) {
    return rows;
  }
  return rows.filter((row) => normalizeLanguage(row.language) === state.selectedLanguage);
}

function setLanguageFilter(language) {
  const normalized = normalizeLanguage(language);
  state.selectedLanguage = state.selectedLanguage === normalized ? null : normalized;
  renderAll();
}

function destroyChart(chart) {
  if (chart) {
    chart.destroy();
  }
}

function renderMonthFilter() {
  dom.monthList.innerHTML = '';

  const allRow = document.createElement('label');
  allRow.className = 'multi-item multi-item--all';
  const allCheck = document.createElement('input');
  allCheck.type = 'checkbox';
  allCheck.checked = state.selectedMonths.size === 0;
  allCheck.addEventListener('change', () => {
    state.selectedMonths.clear();
    renderAll();
  });
  const allText = document.createElement('span');
  allText.textContent = 'All months';
  allRow.appendChild(allCheck);
  allRow.appendChild(allText);
  dom.monthList.appendChild(allRow);

  state.availableMonths.forEach((month) => {
    const row = document.createElement('label');
    row.className = 'multi-item';
    const check = document.createElement('input');
    check.type = 'checkbox';
    check.checked = state.selectedMonths.has(month);
    check.addEventListener('change', () => {
      if (check.checked) {
        state.selectedMonths.add(month);
      } else {
        state.selectedMonths.delete(month);
      }
      renderAll();
    });

    const text = document.createElement('span');
    text.textContent = month;
    row.appendChild(check);
    row.appendChild(text);
    dom.monthList.appendChild(row);
  });

  dom.monthSummary.textContent = asMonthLabel(state.selectedMonths);
}

function applyMonthFilter(rows) {
  let filtered = rows;
  if (state.selectedMonths.size > 0) {
    filtered = filtered.filter((row) => state.selectedMonths.has(String(row.month || '')));
  }
  return filtered;
}

function applyDetailFilters(rows) {
  let filtered = applyMonthFilter(rows);
  return applyLanguageFilter(filtered);
}

function renderKpiCards(filteredRows) {
  const totals = groupByLanguage(filteredRows);
  const total = totals.English + totals.German + totals.Other;

  dom.englishCount.textContent = totals.English.toLocaleString();
  dom.germanCount.textContent = totals.German.toLocaleString();
  dom.otherCount.textContent = totals.Other.toLocaleString();

  dom.englishShare.textContent = `${total > 0 ? ((totals.English / total) * 100).toFixed(1) : '0.0'}%`;
  dom.germanShare.textContent = `${total > 0 ? ((totals.German / total) * 100).toFixed(1) : '0.0'}%`;
  dom.otherShare.textContent = `${total > 0 ? ((totals.Other / total) * 100).toFixed(1) : '0.0'}%`;

  dom.languageCards.forEach((card) => {
    const cardLanguage = normalizeLanguage(card.dataset.language || 'Other');
    card.classList.toggle('is-active', state.selectedLanguage === cardLanguage);
  });

  dom.totalPill.textContent = `Total tickets: ${total.toLocaleString()}`;
  dom.scopePill.textContent = `Scope: ${state.summary.scope || 'it_hub'}`;
}

function renderSpark(canvas, monthLabels, values, color, chartRefKey) {
  destroyChart(state.charts[chartRefKey]);
  state.charts[chartRefKey] = new Chart(canvas, {
    type: 'line',
    data: {
      labels: monthLabels,
      datasets: [
        {
          data: values,
          borderColor: color,
          backgroundColor: color + '22',
          fill: true,
          tension: 0.35,
          pointRadius: 2,
          pointHoverRadius: 3,
          borderWidth: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { enabled: true } },
      scales: {
        x: { display: false, grid: { display: false } },
        y: { display: false, grid: { display: false }, beginAtZero: true },
      },
    },
  });
}

function renderCharts(filteredRows) {
  const byMonth = groupByMonthAndLanguage(filteredRows);
  const monthLabels = [...byMonth.keys()].sort();
  const englishData = monthLabels.map((month) => byMonth.get(month).English || 0);
  const germanData = monthLabels.map((month) => byMonth.get(month).German || 0);
  const otherData = monthLabels.map((month) => byMonth.get(month).Other || 0);

  renderSpark(dom.sparkEnglish, monthLabels, englishData, COLORS.English, 'sparkEnglish');
  renderSpark(dom.sparkGerman, monthLabels, germanData, COLORS.German, 'sparkGerman');
  renderSpark(dom.sparkOther, monthLabels, otherData, COLORS.Other, 'sparkOther');

  destroyChart(state.charts.month);
  state.charts.month = new Chart(dom.monthCanvas, {
    type: 'bar',
    data: {
      labels: monthLabels,
      datasets: [
        { label: 'English', data: englishData, backgroundColor: COLORS.English },
        { label: 'German', data: germanData, backgroundColor: COLORS.German },
        { label: 'Other', data: otherData, backgroundColor: COLORS.Other },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      onClick: (_evt, elements) => {
        if (!elements || elements.length === 0) {
          return;
        }
        const first = elements[0];
        const language = LANG_ORDER[first.datasetIndex] || null;
        if (language) {
          setLanguageFilter(language);
        }
      },
      plugins: {
        legend: { position: 'bottom' },
      },
      scales: {
        x: { stacked: true, grid: { display: false } },
        y: { stacked: true, beginAtZero: true },
      },
    },
  });

  const totals = groupByLanguage(filteredRows);
  destroyChart(state.charts.donut);
  state.charts.donut = new Chart(dom.donutCanvas, {
    type: 'doughnut',
    data: {
      labels: LANG_ORDER,
      datasets: [
        {
          data: [totals.English, totals.German, totals.Other],
          backgroundColor: [COLORS.English, COLORS.German, COLORS.Other],
          borderWidth: 0,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      onClick: (_evt, elements) => {
        if (!elements || elements.length === 0) {
          return;
        }
        const first = elements[0];
        const language = LANG_ORDER[first.index] || null;
        if (language) {
          setLanguageFilter(language);
        }
      },
      plugins: { legend: { position: 'bottom' } },
    },
  });
}

function renderTable(filteredRows) {
  dom.detailBody.innerHTML = '';
  const sorted = [...filteredRows].sort((a, b) => {
    const monthCmp = String(b.month || '').localeCompare(String(a.month || ''));
    if (monthCmp !== 0) {
      return monthCmp;
    }
    const langCmp = String(a.language || '').localeCompare(String(b.language || ''));
    if (langCmp !== 0) {
      return langCmp;
    }
    return String(a.ticket_key || '').localeCompare(String(b.ticket_key || ''));
  });

  if (sorted.length === 0) {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td colspan="7">No ticket detail rows for selected filters.</td>';
    dom.detailBody.appendChild(tr);
    return;
  }

  sorted.forEach((row) => {
    const ticketKey = row.ticket_key || '';
    const ticketUrl = row.ticket_url || '';
    const ticketCell = ticketUrl
      ? `<a href="${escapeHtml(ticketUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(ticketKey)}</a>`
      : escapeHtml(ticketKey);

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(row.month || '')}</td>
      <td>${ticketCell}</td>
      <td>${escapeHtml(row.language || 'Other')}</td>
      <td>${escapeHtml(row.status || 'Unknown')}</td>
      <td>${Number(row.de_score || 0)}</td>
      <td>${Number(row.en_score || 0)}</td>
      <td>${escapeHtml(row.description_preview || '')}</td>
    `;
    dom.detailBody.appendChild(tr);
  });
}

function renderAll() {
  state.filteredRows = applyMonthFilter(state.rows);
  state.filteredDetails = applyDetailFilters(state.details);
  renderMonthFilter();
  renderKpiCards(state.filteredRows);
  renderCharts(state.filteredRows);
  renderTable(state.filteredDetails);

  const openedCount = Number(state.summary.opened_unique_tickets || 0);
  const aggregatedCount = Number(state.summary.language_aggregated_total || 0);
  const integrityOk = Boolean(state.summary.integrity_opened_vs_aggregated);
  const detailsReturned = state.filteredDetails.length;
  setStateText(
    `Loaded ${state.filteredRows.length} aggregated rows and ${detailsReturned} ticket detail rows. ` +
    `Opened=${openedCount}, Aggregated=${aggregatedCount}, Integrity=${integrityOk ? 'OK' : 'MISMATCH'}.` +
    `${state.selectedLanguage ? ` Language filter=${state.selectedLanguage}.` : ''}`
  );
}

function buildDataUrl() {
  const params = new URLSearchParams();
  params.set('detailLimit', String(DETAIL_LIMIT));
  const months = [...state.selectedMonths].sort();
  if (months.length > 0) {
    params.set('months', months.join(','));
  }
  return `${API_BASE}/api/tickets-per-language?${params.toString()}`;
}

async function fetchMonths() {
  const res = await fetch(`${API_BASE}/api/tickets-per-language/months`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

async function fetchData() {
  const res = await fetch(buildDataUrl());
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

async function bringData() {
  dom.bringDataBtn.disabled = true;
  setStateText('Loading data...');
  try {
    const payload = await fetchData();
    state.rows = Array.isArray(payload.rows) ? payload.rows : [];
    state.details = Array.isArray(payload.details) ? payload.details : [];
    state.summary = payload.summary || {};
    renderAll();
  } catch (err) {
    setStateText(`Failed to load data: ${err.message || String(err)}`, true);
  } finally {
    dom.bringDataBtn.disabled = false;
  }
}

function clearFilters() {
  state.selectedMonths.clear();
  state.selectedLanguage = null;
  renderAll();
  setStateText('Filters reset. Click Bring Data to run query.');
}

function wireEvents() {
  dom.bringDataBtn.addEventListener('click', bringData);
  dom.clearFiltersBtn.addEventListener('click', clearFilters);

  dom.monthToggle.addEventListener('click', () => {
    const open = dom.monthToggle.getAttribute('aria-expanded') !== 'true';
    dom.monthToggle.setAttribute('aria-expanded', open ? 'true' : 'false');
    dom.monthList.hidden = !open;
  });

  document.addEventListener('click', (event) => {
    if (!dom.monthToggle.contains(event.target) && !dom.monthList.contains(event.target)) {
      dom.monthToggle.setAttribute('aria-expanded', 'false');
      dom.monthList.hidden = true;
    }
  });

  dom.languageCards.forEach((card) => {
    card.addEventListener('click', () => {
      setLanguageFilter(card.dataset.language || 'Other');
    });
  });
}

async function init() {
  wireEvents();
  try {
    const months = await fetchMonths();
    state.availableMonths = Array.isArray(months) ? months.sort(monthSortDesc) : [];
    renderMonthFilter();
    if (state.availableMonths.length > 0) {
      state.selectedMonths.add(state.availableMonths[0]);
      renderMonthFilter();
    }
    await bringData();
  } catch (err) {
    setStateText(`Failed to initialize: ${err.message || String(err)}`, true);
  }
}

init();
