/**
 * SRF-AXSA — Tickets by Hour Dashboard
 * Vanilla JS + Chart.js (no build step)
 *
 * Features:
 *  - Fetch available months on load and populate the month picker
 *  - Multi-month selection using chip/toggle UI
 *  - Grouped bar chart rendered with Chart.js
 *  - URL query-param persistence for shareable links
 *  - Debounced API calls during rapid selection changes
 *  - Summary metric cards (total, peak hour, avg)
 *  - Manual cache-refresh control
 *  - Full error and empty-state handling
 */

'use strict';

/* ── Configuration ─────────────────────────────────────────────────────────── */

const API_BASE = window.APP_CONFIG?.apiBase ?? 'https://api-srf-axsa.azurewebsites.net';
const DEBOUNCE_MS = 400;
const MAX_MONTHS = 12;
const PRIORITY_ASSIGNMENT_GROUP_KEYWORD = 'service management center';
const JIRA_TICKET_PREFIX = 'ITHUB-';
const DEFAULT_METRIC_MODE = 'open';
const METRIC_TITLES = {
  open: 'Ticket Open Events by Hour of Day',
  entry_smc_first: 'First Entry to SMC by Hour of Day',
  closed: 'Ticket Close Events by Hour of Day',
  assignment_transitions: 'Assignment Transitions by Hour of Day',
};

/** Series colours — must stay in sync with CSS --series-N variables */
const SERIES_COLORS = [
  '#0078D4', '#6B5B95', '#00B4D8', '#E67E22',
  '#2ECC71', '#E74C3C', '#1ABC9C', '#9B59B6',
  '#F39C12', '#16A085', '#D35400', '#8E44AD',
];

/* ── State ─────────────────────────────────────────────────────────────────── */

const state = {
  availableMonths: /** @type {string[]} */ ([]),
  selectedMonths:  /** @type {string[]} */ ([]),
  assignmentGroups: /** @type {string[]} */ ([]),
  assignmentGroupSearch: '',
  selectedAssignmentGroup: '',
  data:            /** @type {Array<{hour:number,count:number,month:string,year:number}>} */ ([]),
  metricMode: DEFAULT_METRIC_MODE,
  metricMeta: {
    metricMode: DEFAULT_METRIC_MODE,
    unit: 'tickets',
    dataQuality: 'legacy',
    scope: 'local_event_time',
    reportingTimezone: { label: 'Madrid/Switzerland', iana: 'Europe/Madrid' },
  },
  loading: false,
  error: null,
  lastUpdated: null,
  chart: null,
  selectedHourDetail: null,
};

/* ── DOM references ────────────────────────────────────────────────────────── */

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

const dom = {
  monthPicker:     $('#month-picker'),
  selectedChips:   $('#selected-chips'),
  chartCanvas:     $('#chart-canvas'),
  loadingOverlay:  $('#loading-overlay'),
  emptyOverlay:    $('#empty-overlay'),
  errorOverlay:    $('#error-overlay'),
  errorMsg:        $('#error-message'),
  metricTotal:     $('#metric-total'),
  metricPeak:      $('#metric-peak'),
  metricAvg:       $('#metric-avg'),
  metricPeakSub:   $('#metric-peak-sub'),
  metricAvgSub:    $('#metric-avg-sub'),
  lastUpdated:     $('#last-updated'),
  btnRefresh:      $('#btn-refresh'),
  btnClearAll:     $('#btn-clear-all'),
  btnCopyUrl:      $('#btn-copy-url'),
  shareUrl:        $('#share-url'),
  toastContainer:  $('#toast-container'),
  selectionCount:  $('#selection-count'),
  assignmentGroupSearch: $('#assignment-group-search'),
  assignmentGroupList: $('#assignment-group-list'),
  selectedAssignmentGroup: $('#selected-assignment-group'),
  hourDetailPanel: $('#hour-detail-panel'),
  selectedHour: $('#selected-hour'),
  selectedHourCount: $('#selected-hour-count'),
  selectedHourTicketList: $('#selected-hour-ticket-list'),
  selectedTicketUrl: $('#selected-ticket-url'),
  selectedHourUniqueCount: $('#selected-hour-unique-count'),
  selectedHourWarning: $('#selected-hour-warning'),
  metricMode: $('#metric-mode'),
  metricMeta: $('#metric-meta'),
  chartTitle: $('#chart-title'),
  chartMeta: $('#chart-meta'),
};

/* ── Utilities ─────────────────────────────────────────────────────────────── */

function debounce(fn, ms) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), ms);
  };
}

function formatHour(h) {
  const suffix = h < 12 ? 'AM' : 'PM';
  const display = h === 0 ? 12 : h > 12 ? h - 12 : h;
  return `${display}:00 ${suffix}`;
}

function formatNumber(n) {
  return new Intl.NumberFormat().format(n);
}

function showToast(msg) {
  const el = document.createElement('div');
  el.className = 'toast';
  el.textContent = msg;
  dom.toastContainer.appendChild(el);
  setTimeout(() => el.remove(), 3000);
}

function buildShareUrl() {
  const url = new URL(window.location.href);
  url.search = '';
  if (state.selectedMonths.length) {
    url.searchParams.set('months', state.selectedMonths.join(','));
  }
  if (state.selectedAssignmentGroup) {
    url.searchParams.set('assignmentGroup', state.selectedAssignmentGroup);
  }
  if (state.metricMode && state.metricMode !== DEFAULT_METRIC_MODE) {
    url.searchParams.set('metricMode', state.metricMode);
  }
  return url.toString();
}

/* ── URL query-param persistence ───────────────────────────────────────────── */

function readMonthsFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const raw = params.get('months');
  if (!raw) return [];
  return raw.split(',').filter(m => /^\d{4}-\d{2}$/.test(m.trim())).map(m => m.trim());
}

function readAssignmentGroupFromUrl() {
  const params = new URLSearchParams(window.location.search);
  return (params.get('assignmentGroup') ?? '').trim();
}

function readMetricModeFromUrl() {
  const params = new URLSearchParams(window.location.search);
  return (params.get('metricMode') ?? DEFAULT_METRIC_MODE).trim() || DEFAULT_METRIC_MODE;
}

function normaliseText(input) {
  return (input ?? '').trim().toLowerCase();
}

function sortAssignmentGroups(groups) {
  const sorted = [...groups].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
  sorted.sort((a, b) => {
    const aPriority = normaliseText(a).includes(PRIORITY_ASSIGNMENT_GROUP_KEYWORD) ? 0 : 1;
    const bPriority = normaliseText(b).includes(PRIORITY_ASSIGNMENT_GROUP_KEYWORD) ? 0 : 1;
    if (aPriority !== bPriority) return aPriority - bPriority;
    return a.localeCompare(b, undefined, { sensitivity: 'base' });
  });
  return sorted;
}

function pushStateToUrl() {
  const url = buildShareUrl();
  window.history.replaceState(null, '', url);
  if (dom.shareUrl) dom.shareUrl.textContent = url;
}

/* ── Month selection logic ─────────────────────────────────────────────────── */

function toggleMonth(month) {
  const idx = state.selectedMonths.indexOf(month);
  if (idx === -1) {
    if (state.selectedMonths.length >= MAX_MONTHS) {
      showToast(`Maximum ${MAX_MONTHS} months can be selected.`);
      return;
    }
    state.selectedMonths = [...state.selectedMonths, month];
  } else {
    state.selectedMonths = state.selectedMonths.filter(m => m !== month);
  }
  renderMonthPicker();
  renderSelectedChips();
  pushStateToUrl();
  debouncedFetchData();
}

function getMonthColor(month) {
  const idx = state.selectedMonths.indexOf(month);
  return idx >= 0 ? SERIES_COLORS[idx % SERIES_COLORS.length] : '#ccc';
}

function setAssignmentGroup(group) {
  const value = (group ?? '').trim();
  state.selectedAssignmentGroup = value;
  renderAssignmentGroupList();
  pushStateToUrl();
  debouncedFetchData();
}

function renderAssignmentGroupList() {
  if (!dom.assignmentGroupList) return;

  const search = normaliseText(state.assignmentGroupSearch);
  const groups = sortAssignmentGroups(state.assignmentGroups)
    .filter((g) => normaliseText(g).includes(search));

  dom.assignmentGroupList.innerHTML = '';

  groups.forEach((group) => {
    const selected = state.selectedAssignmentGroup === group;
    const btn = document.createElement('button');
    btn.className = 'assignment-group-item' + (selected ? ' selected' : '');
    btn.setAttribute('type', 'button');
    btn.setAttribute('aria-pressed', String(selected));
    btn.title = group;
    btn.innerHTML = `
      <span>${group}</span>
      <span class="assignment-group-item__check" aria-hidden="true">✓</span>
    `;
    btn.addEventListener('click', () => setAssignmentGroup(group));
    dom.assignmentGroupList.appendChild(btn);
  });

  if (dom.selectedAssignmentGroup) {
    dom.selectedAssignmentGroup.textContent = state.selectedAssignmentGroup
      ? `Selected: ${state.selectedAssignmentGroup}`
      : 'Selected: none';
  }
}

/* ── Render: month picker list ─────────────────────────────────────────────── */

function renderMonthPicker() {
  if (!dom.monthPicker) return;
  dom.monthPicker.innerHTML = '';

  if (state.availableMonths.length === 0) {
    dom.monthPicker.innerHTML = '<p style="padding:8px;color:var(--color-text-muted);font-size:13px;">No months available</p>';
    return;
  }

  state.availableMonths.forEach(month => {
    const isSelected = state.selectedMonths.includes(month);
    const btn = document.createElement('button');
    btn.className = 'month-picker-item' + (isSelected ? ' selected' : '');
    btn.setAttribute('aria-pressed', String(isSelected));
    btn.setAttribute('aria-label', `${isSelected ? 'Deselect' : 'Select'} ${month}`);

    const [year, mon] = month.split('-');
    const label = new Date(+year, +mon - 1, 1).toLocaleString('default', { month: 'long', year: 'numeric' });

    btn.innerHTML = `
      <span>${label}</span>
      <span class="month-picker-item__check" aria-hidden="true">✓</span>
    `;
    btn.addEventListener('click', () => toggleMonth(month));
    dom.monthPicker.appendChild(btn);
  });

  if (dom.selectionCount) {
    dom.selectionCount.textContent = `${state.selectedMonths.length} / ${MAX_MONTHS} selected`;
  }
}

/* ── Render: selected month chips ──────────────────────────────────────────── */

function renderSelectedChips() {
  if (!dom.selectedChips) return;
  dom.selectedChips.innerHTML = '';

  if (state.selectedMonths.length === 0) {
    dom.selectedChips.innerHTML = '<span style="font-size:13px;color:var(--color-text-muted);">None selected — showing last 3 months</span>';
    return;
  }

  state.selectedMonths.forEach((month, i) => {
    const color = SERIES_COLORS[i % SERIES_COLORS.length];
    const [year, mon] = month.split('-');
    const shortLabel = new Date(+year, +mon - 1, 1).toLocaleString('default', { month: 'short' }) + ' ' + year;

    const chip = document.createElement('button');
    chip.className = 'month-chip active';
    chip.setAttribute('aria-label', `Remove ${month}`);
    chip.title = `Click to remove ${month}`;
    chip.innerHTML = `
      <span class="month-chip__dot" style="background:${color}" aria-hidden="true"></span>
      <span>${shortLabel}</span>
      <span aria-hidden="true" style="opacity:.7;font-size:10px;margin-left:2px;">✕</span>
    `;
    chip.addEventListener('click', () => toggleMonth(month));
    dom.selectedChips.appendChild(chip);
  });
}

/* ── Summary metrics ───────────────────────────────────────────────────────── */

function updateMetrics() {
  if (!state.data.length) {
    dom.metricTotal.textContent = '—';
    dom.metricPeak.textContent  = '—';
    dom.metricAvg.textContent   = '—';
    if (dom.metricPeakSub) dom.metricPeakSub.textContent = '';
    if (dom.metricAvgSub)  dom.metricAvgSub.textContent  = '';
    return;
  }

  const total = state.data.reduce((s, r) => s + r.count, 0);
  dom.metricTotal.textContent = formatNumber(total);

  // Aggregate by hour across all selected months
  const byHour = Array.from({ length: 24 }, (_, h) => ({
    hour: h,
    total: state.data.filter(r => r.hour === h).reduce((s, r) => s + r.count, 0),
  }));

  const peak = byHour.reduce((best, cur) => cur.total > best.total ? cur : best, byHour[0]);
  dom.metricPeak.textContent  = formatHour(peak.hour);
  if (dom.metricPeakSub) dom.metricPeakSub.textContent = `${formatNumber(peak.total)} ${state.metricMeta.unit}`;

  const nonZero = byHour.filter(b => b.total > 0);
  const avg = nonZero.length ? Math.round(total / nonZero.length) : 0;
  dom.metricAvg.textContent = formatNumber(avg);
  if (dom.metricAvgSub) dom.metricAvgSub.textContent = 'per active hour';
}

function updateMetricText() {
  const mode = state.metricMeta.metricMode || state.metricMode;
  const unit = state.metricMeta.unit || 'tickets';
  const quality = state.metricMeta.dataQuality || 'legacy';
  const scope = state.metricMeta.scope || 'local_event_time';
  const tzLabel = state.metricMeta.reportingTimezone?.label || 'Madrid/Switzerland';
  const tzIana = state.metricMeta.reportingTimezone?.iana || 'Europe/Madrid';
  if (dom.chartTitle) dom.chartTitle.textContent = METRIC_TITLES[mode] || METRIC_TITLES.open;
  if (dom.chartMeta) {
    dom.chartMeta.textContent = `Grouped by event month and hour (${tzLabel} local time)`;
  }
  if (dom.metricMeta) {
    dom.metricMeta.textContent = `Mode: ${mode} · Unit: ${unit} · Data quality: ${quality} · Scope: ${scope} · TZ: ${tzLabel} (${tzIana})`;
  }
}

/* ── Chart rendering ───────────────────────────────────────────────────────── */

function buildChartDatasets() {
  const months = state.selectedMonths.length
    ? state.selectedMonths
    : [...new Set(state.data.map(r => r.month))].sort().reverse();

  return months.map((month, i) => {
    const color = SERIES_COLORS[i % SERIES_COLORS.length];
    const [year, mon] = month.split('-');
    const label = new Date(+year, +mon - 1, 1)
      .toLocaleString('default', { month: 'short', year: 'numeric' });

    const counts = Array.from({ length: 24 }, (_, h) => {
      const row = state.data.find(r => r.hour === h && r.month === month);
      return row ? row.count : 0;
    });

    return {
      label,
      monthKey: month,
      data: counts,
      backgroundColor: color + 'CC',   // 80% opacity
      borderColor: color,
      borderWidth: 1.5,
      borderRadius: 3,
      borderSkipped: false,
    };
  });
}

function renderChart() {
  const datasets = buildChartDatasets();
  const labels   = Array.from({ length: 24 }, (_, h) => `${String(h).padStart(2, '0')}:00`);

  if (state.chart) {
    state.chart.data.datasets = datasets;
    state.chart.update('active');
    return;
  }

  state.chart = new Chart(dom.chartCanvas, {
    type: 'bar',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      animation: { duration: 300 },
      plugins: {
        legend: {
          position: 'top',
          labels: {
            font: { family: "'Segoe UI', system-ui, sans-serif", size: 13 },
            padding: 16,
            usePointStyle: true,
            pointStyle: 'circle',
          },
        },
        tooltip: {
          backgroundColor: '#1B1B1B',
          titleFont: { size: 13, weight: '600' },
          bodyFont:  { size: 13 },
          padding: 12,
          cornerRadius: 8,
          callbacks: {
            title: (items) => `Hour ${items[0].label} (${formatHour(+items[0].label.split(':')[0])})`,
            label: (ctx) => ` ${ctx.dataset.label}: ${formatNumber(ctx.parsed.y)} tickets`,
          },
        },
      },
      onClick: (_, elements) => {
        handleChartClick(elements);
      },
      scales: {
        x: {
          grid:  { display: false },
          ticks: { font: { size: 11 }, color: '#6B7280', maxRotation: 0 },
          title: {
            display: true,
            text: 'Hour of Day (Madrid/Switzerland)',
            font: { size: 12, weight: '500' },
            color: '#6B7280',
            padding: { top: 8 },
          },
        },
        y: {
          beginAtZero: true,
          grid: { color: '#E1E4E8', lineWidth: 1 },
          ticks: {
            font: { size: 11 },
            color: '#6B7280',
            callback: (v) => formatNumber(v),
          },
          title: {
            display: true,
            text: 'Ticket count',
            font: { size: 12, weight: '500' },
            color: '#6B7280',
            padding: { bottom: 8 },
          },
        },
      },
    },
  });
}

function clearHourDetailPanel() {
  state.selectedHourDetail = null;
  if (dom.selectedHourTicketList) {
    dom.selectedHourTicketList.innerHTML = '';
  }
  if (dom.selectedTicketUrl) {
    dom.selectedTicketUrl.textContent = '—';
    dom.selectedTicketUrl.removeAttribute('href');
  }
  if (dom.selectedHourUniqueCount) dom.selectedHourUniqueCount.textContent = '—';
  if (dom.selectedHourWarning) dom.selectedHourWarning.textContent = '';
  dom.hourDetailPanel?.classList.add('hidden');
}

function renderSelectedTicketLink(ticket) {
  if (!dom.selectedTicketUrl) return;
  if (!ticket?.ticketUrl) {
    dom.selectedTicketUrl.textContent = '—';
    dom.selectedTicketUrl.removeAttribute('href');
    return;
  }
  dom.selectedTicketUrl.textContent = ticket.ticketUrl;
  dom.selectedTicketUrl.href = ticket.ticketUrl;
}

function renderHourTicketList(detail) {
  if (!dom.selectedHourTicketList) return;

  dom.selectedHourTicketList.innerHTML = '';
  const tickets = detail.tickets ?? [];

  if (!tickets.length) {
    const option = document.createElement('option');
    option.value = '';
    option.textContent = 'No tickets found for this hour';
    dom.selectedHourTicketList.appendChild(option);
    renderSelectedTicketLink(null);
    return;
  }

  tickets.forEach((ticket, index) => {
    const option = document.createElement('option');
    option.value = String(index);
    option.textContent = ticket.ticketKey ?? `Ticket ${index + 1}`;
    option.title = ticket.ticketUrl ?? '';
    dom.selectedHourTicketList.appendChild(option);
  });

  dom.selectedHourTicketList.value = '0';
  renderSelectedTicketLink(tickets[0]);
}

function renderHourDetailPanel(detail) {
  if (!dom.hourDetailPanel) return;

  dom.selectedHour.textContent = detail.hourLabel;
  const metricValue = detail.totalTickets ?? detail.count ?? 0;
  dom.selectedHourCount.textContent = formatNumber(metricValue);
  if (dom.selectedHourUniqueCount) {
    dom.selectedHourUniqueCount.textContent = formatNumber(detail.uniqueTickets ?? metricValue);
  }
  if (dom.selectedHourWarning) {
    dom.selectedHourWarning.textContent = detail.warning ?? '';
  }
  renderHourTicketList(detail);

  dom.hourDetailPanel.classList.remove('hidden');
}

async function fetchTicketByHour(hour, month) {
  const params = new URLSearchParams();
  params.set('hour', String(hour));
  if (month) params.set('month', month);
  if (state.selectedAssignmentGroup) {
    params.set('assignmentGroup', state.selectedAssignmentGroup);
  }
  params.set('metricMode', state.metricMode);

  const res = await fetch(`${API_BASE}/api/ticket-by-hour?${params.toString()}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail ?? `HTTP ${res.status}`);
  }
  return res.json();
}

async function handleChartClick(elements) {
  if (!elements?.length || !state.chart) return;

  const first = elements[0];
  const dataset = state.chart.data.datasets[first.datasetIndex];
  const hourLabel = state.chart.data.labels[first.index];
  const hour = Number(String(hourLabel).split(':')[0]);
  const count = Number(dataset.data[first.index] ?? 0);
  const month = dataset.monthKey ?? null;

  try {
    const ticketData = await fetchTicketByHour(hour, month);
    const seen = new Set();
    const tickets = (ticketData.tickets ?? []).map((ticket) => {
      const ticketNumber = ticket.ticketNumber
        ?? (ticket.ticketKey?.replace(JIRA_TICKET_PREFIX, '') || null);
      return {
        ...ticket,
        ticketNumber,
        ticketUrl: ticket.ticketUrl
          ?? (ticketNumber ? `https://axpo.atlassian.net/browse/${JIRA_TICKET_PREFIX}${ticketNumber}` : null),
      };
    }).filter((ticket) => {
      const key = ticket.ticketKey || ticket.ticketUrl || '';
      if (!key) return true;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    const apiTotal = Number(ticketData.totalTickets ?? 0);
    const uniqueCount = tickets.length;
    const warning = apiTotal !== uniqueCount
      ? `Warning: API returned ${apiTotal} records but ${uniqueCount} unique tickets after dedupe.`
      : '';

    state.selectedHourDetail = {
      hourLabel,
      count,
      totalTickets: ticketData.totalTickets ?? tickets.length,
      uniqueTickets: uniqueCount,
      warning,
      tickets,
    };

    renderHourDetailPanel(state.selectedHourDetail);
  } catch (err) {
    showToast(`Could not load ticket for ${hourLabel}: ${err.message}`);
  }
}

/* ── Overlay management ────────────────────────────────────────────────────── */

function showOverlay(which) {
  ['loading', 'empty', 'error'].forEach(name => {
    const el = dom[`${name}Overlay`];
    if (el) el.classList.toggle('hidden', name !== which);
  });
  // Also hide/show the canvas itself
  if (dom.chartCanvas) {
    dom.chartCanvas.style.visibility = (which === 'loading' || which === 'empty') ? 'hidden' : 'visible';
  }
}

function hideAllOverlays() {
  ['loadingOverlay', 'emptyOverlay', 'errorOverlay'].forEach(key => {
    if (dom[key]) dom[key].classList.add('hidden');
  });
  if (dom.chartCanvas) dom.chartCanvas.style.visibility = 'visible';
}

/* ── API calls ─────────────────────────────────────────────────────────────── */

async function fetchAvailableMonths() {
  try {
    const res = await fetch(`${API_BASE}/api/available-months`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.availableMonths = await res.json();
  } catch (err) {
    console.error('Failed to load available months:', err);
    state.availableMonths = [];
  }
  renderMonthPicker();
  renderSelectedChips();
}

async function fetchAssignmentGroups() {
  try {
    const res = await fetch(`${API_BASE}/api/assignment-groups`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.assignmentGroups = await res.json();
  } catch (err) {
    console.error('Failed to load assignment groups:', err);
    state.assignmentGroups = [];
  }

  const sortedGroups = sortAssignmentGroups(state.assignmentGroups);
  const preferredDefault = sortedGroups[0] ?? '';

  // Keep URL-selected value only if available; otherwise fallback to first item.
  if (state.selectedAssignmentGroup && !state.assignmentGroups.includes(state.selectedAssignmentGroup)) {
    state.selectedAssignmentGroup = preferredDefault;
  }

  if (!state.selectedAssignmentGroup) {
    state.selectedAssignmentGroup = preferredDefault;
  }
  renderAssignmentGroupList();
}

async function fetchData() {
  state.loading = true;
  state.error   = null;
  showOverlay('loading');

  try {
    clearHourDetailPanel();
    const searchParams = new URLSearchParams();
    if (state.selectedMonths.length) {
      searchParams.set('months', state.selectedMonths.join(','));
    }
    if (state.selectedAssignmentGroup) {
      searchParams.set('assignmentGroup', state.selectedAssignmentGroup);
    }
    searchParams.set('metricMode', state.metricMode);
    searchParams.set('includeMeta', 'true');

    const query = searchParams.toString();
    const res = await fetch(`${API_BASE}/api/tickets-by-hour${query ? `?${query}` : ''}`);
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body.detail ?? `HTTP ${res.status}`);
    }
    const payload = await res.json();
    if (Array.isArray(payload)) {
      state.data = payload;
      state.metricMeta = {
        metricMode: state.metricMode,
        unit: state.metricMode === 'assignment_transitions' ? 'transitions' : 'tickets',
        dataQuality: 'legacy',
        scope: 'local_event_time',
        reportingTimezone: { label: 'Madrid/Switzerland', iana: 'Europe/Madrid' },
      };
    } else {
      state.data = payload.rows ?? [];
      state.metricMeta = {
        metricMode: payload.meta?.metricMode ?? state.metricMode,
        unit: payload.meta?.unit ?? (state.metricMode === 'assignment_transitions' ? 'transitions' : 'tickets'),
        dataQuality: payload.meta?.dataQuality ?? 'legacy',
        scope: payload.meta?.scope ?? 'local_event_time',
        reportingTimezone: payload.meta?.reportingTimezone ?? { label: 'Madrid/Switzerland', iana: 'Europe/Madrid' },
      };
    }
    state.lastUpdated = new Date();
    updateMetricText();

    if (dom.lastUpdated) {
      dom.lastUpdated.textContent = `Updated ${state.lastUpdated.toLocaleTimeString()}`;
    }

    if (state.data.length === 0) {
      showOverlay('empty');
    } else {
      hideAllOverlays();
      renderChart();
    }
    updateMetrics();
  } catch (err) {
    state.error = err.message;
    if (dom.errorMsg) dom.errorMsg.textContent = err.message;
    showOverlay('error');
    updateMetrics();
  } finally {
    state.loading = false;
    if (dom.btnRefresh) dom.btnRefresh.disabled = false;
  }
}

const debouncedFetchData = debounce(fetchData, DEBOUNCE_MS);

/* ── Export helpers ────────────────────────────────────────────────────────── */

function exportCSV() {
  if (!state.data.length) {
    showToast('No data to export.');
    return;
  }
  const header = 'hour,count,month,year';
  const rows = state.data.map(r => `${r.hour},${r.count},${r.month},${r.year}`);
  const blob = new Blob([[header, ...rows].join('\n')], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `tickets-by-hour-${Date.now()}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

function exportPNG() {
  if (!state.chart) { showToast('Chart not ready.'); return; }
  const a = document.createElement('a');
  a.href = state.chart.toBase64Image();
  a.download = `tickets-by-hour-${Date.now()}.png`;
  a.click();
}

/* ── Event listeners ───────────────────────────────────────────────────────── */

function bindEvents() {
  dom.btnRefresh?.addEventListener('click', () => {
    dom.btnRefresh.disabled = true;
    fetchData();
  });

  dom.btnCopyUrl?.addEventListener('click', () => {
    const url = buildShareUrl();
    navigator.clipboard.writeText(url).then(
      () => showToast('Link copied to clipboard!'),
      () => showToast('Could not copy. Try manually selecting the URL.'),
    );
  });

  $('#btn-export-csv')?.addEventListener('click', exportCSV);
  $('#btn-export-png')?.addEventListener('click', exportPNG);

  dom.btnClearAll?.addEventListener('click', () => {
    state.selectedMonths = [];
    state.selectedAssignmentGroup = '';
    state.metricMode = DEFAULT_METRIC_MODE;
    if (dom.metricMode) dom.metricMode.value = DEFAULT_METRIC_MODE;
    state.assignmentGroupSearch = '';
    if (dom.assignmentGroupSearch) dom.assignmentGroupSearch.value = '';
    clearHourDetailPanel();
    renderMonthPicker();
    renderSelectedChips();
    renderAssignmentGroupList();
    pushStateToUrl();
    fetchData();
  });

  dom.assignmentGroupSearch?.addEventListener('input', (event) => {
    state.assignmentGroupSearch = event.target.value;
    renderAssignmentGroupList();
  });

  dom.selectedHourTicketList?.addEventListener('change', (event) => {
    const selectedIndex = Number(event.target.value);
    const tickets = state.selectedHourDetail?.tickets ?? [];
    const ticket = Number.isNaN(selectedIndex) ? null : tickets[selectedIndex] ?? null;
    renderSelectedTicketLink(ticket);
  });

  dom.metricMode?.addEventListener('change', (event) => {
    state.metricMode = event.target.value || DEFAULT_METRIC_MODE;
    clearHourDetailPanel();
    pushStateToUrl();
    fetchData();
  });
}

/* ── Initialisation ────────────────────────────────────────────────────────── */

async function init() {
  // Restore selections from URL
  const urlMonths = readMonthsFromUrl();
  state.selectedMonths = urlMonths.slice(0, MAX_MONTHS);
  state.selectedAssignmentGroup = readAssignmentGroupFromUrl();
  state.metricMode = readMetricModeFromUrl();
  if (dom.metricMode) dom.metricMode.value = state.metricMode;

  await fetchAvailableMonths();
  await fetchAssignmentGroups();

  // If URL had selections but they're not in available list, keep them anyway
  // (the API will handle the filter correctly)
  renderSelectedChips();
  updateMetricText();
  pushStateToUrl();
  bindEvents();
  await fetchData();
}

document.addEventListener('DOMContentLoaded', init);
