'use strict';

const API_BASE = window.APP_CONFIG?.apiBase ?? 'https://api-srf-axsa.azurewebsites.net';

const state = {
  rows: [],
  filteredRows: [],
  summary: { total_tickets: 0, total_sla_breach: 0, sla_metric_available: false },
  charts: { donut: null, month: null },
  optionLists: { assignmentGroups: [], months: [], assignees: [], statuses: [] },
  searchQueries: { assignmentGroups: '', months: '', assignees: '', statuses: '' },
  filters: {
    assignmentGroups: new Set(),
    months: new Set(),
    assignees: new Set(),
    statuses: new Set(),
  },
};

const dom = {
  state: document.getElementById('state'),
  clearFiltersBtn: document.getElementById('clear-filters-btn'),
  assignmentGroupFilterToggle: document.getElementById('assignment-group-filter-toggle'),
  monthFilterToggle: document.getElementById('month-filter-toggle'),
  assigneeFilterToggle: document.getElementById('assignee-filter-toggle'),
  statusFilterToggle: document.getElementById('status-filter-toggle'),
  assignmentGroupFilterList: document.getElementById('assignment-group-filter-list'),
  monthFilterList: document.getElementById('month-filter-list'),
  assigneeFilterList: document.getElementById('assignee-filter-list'),
  statusFilterList: document.getElementById('status-filter-list'),
  assignmentGroupFilterSummary: document.getElementById('assignment-group-filter-summary'),
  monthFilterSummary: document.getElementById('month-filter-summary'),
  assigneeFilterSummary: document.getElementById('assignee-filter-summary'),
  statusFilterSummary: document.getElementById('status-filter-summary'),
  totalPill: document.getElementById('total-pill'),
  detailBody: document.getElementById('detail-body'),
  assigneeLegend: document.getElementById('assignee-legend'),
  donutCanvas: document.getElementById('donut-chart'),
  monthCanvas: document.getElementById('month-chart'),
};

function aggregateBy(rows, key) {
  const map = new Map();
  rows.forEach((row) => {
    const k = row[key] ?? 'Unknown';
    map.set(k, (map.get(k) ?? 0) + Number(row.ticket_count || 0));
  });
  return [...map.entries()].map(([label, value]) => ({ label, value }));
}

function monthSort(a, b) {
  return a.localeCompare(b);
}

function setStateText(message, isError = false) {
  dom.state.textContent = message;
  dom.state.classList.toggle('error', isError);
}

async function fetchRowsFromApi() {
  const url = `${API_BASE}/api/tickets-per-agent`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 20000);
  let res;
  try {
    res = await fetch(url, { signal: controller.signal });
  } catch (err) {
    if (err?.name === 'AbortError') {
      throw new Error('Request timed out');
    }
    throw err;
  } finally {
    clearTimeout(timeoutId);
  }
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail ?? `HTTP ${res.status}`);
  }
  return res.json();
}

function asLabel(setValues, fallbackLabel) {
  if (setValues.size === 0) {
    return fallbackLabel;
  }
  if (setValues.size === 1) {
    return [...setValues][0];
  }
  return `${setValues.size} selected`;
}

function setDropdownOpen(toggleEl, menuEl, open) {
  toggleEl.setAttribute('aria-expanded', open ? 'true' : 'false');
  menuEl.hidden = !open;
}

function getAssignmentGroupValue(row) {
  return (
    row.assignment_group ??
    row.assignmentGroup ??
    row.assignment_group_name ??
    row.assignmentGroupName ??
    'Unknown'
  );
}

function closeAllDropdowns(exceptKey = '') {
  const dropdowns = [
    {
      key: 'assignment-group',
      toggle: dom.assignmentGroupFilterToggle,
      menu: dom.assignmentGroupFilterList,
    },
    { key: 'month', toggle: dom.monthFilterToggle, menu: dom.monthFilterList },
    { key: 'assignee', toggle: dom.assigneeFilterToggle, menu: dom.assigneeFilterList },
    { key: 'status', toggle: dom.statusFilterToggle, menu: dom.statusFilterList },
  ];

  dropdowns.forEach((item) => {
    if (item.key !== exceptKey) {
      setDropdownOpen(item.toggle, item.menu, false);
    }
  });
}

function setupDropdown(toggleEl, menuEl, key) {
  toggleEl.addEventListener('click', () => {
    const willOpen = toggleEl.getAttribute('aria-expanded') !== 'true';
    closeAllDropdowns(key);
    setDropdownOpen(toggleEl, menuEl, willOpen);
  });
}

function renderMultiList(container, values, selectedSet, fallbackLabel, onChange, searchKey) {
  container.innerHTML = '';

  const searchWrap = document.createElement('div');
  searchWrap.className = 'multi-search-wrap';
  const searchInput = document.createElement('input');
  searchInput.type = 'text';
  searchInput.className = 'multi-search';
  searchInput.placeholder = 'Search...';
  searchInput.value = state.searchQueries[searchKey] ?? '';
  searchInput.addEventListener('input', () => {
    state.searchQueries[searchKey] = searchInput.value;
    renderFilterMenus();
  });
  searchWrap.appendChild(searchInput);
  container.appendChild(searchWrap);

  const normalizedSearch = (state.searchQueries[searchKey] ?? '').trim().toLowerCase();
  const visibleValues = normalizedSearch
    ? values.filter((value) => value.toLowerCase().includes(normalizedSearch))
    : values;

  const allRow = document.createElement('label');
  allRow.className = 'multi-item multi-item--all';
  const allCheck = document.createElement('input');
  allCheck.type = 'checkbox';
  allCheck.checked = selectedSet.size === 0;
  allCheck.addEventListener('change', () => {
    selectedSet.clear();
    onChange();
  });
  const allText = document.createElement('span');
  allText.textContent = fallbackLabel;
  allRow.appendChild(allCheck);
  allRow.appendChild(allText);
  container.appendChild(allRow);

  if (values.length === 0) {
    const empty = document.createElement('div');
    empty.className = 'multi-empty';
    empty.textContent = 'No values found';
    container.appendChild(empty);
    return;
  }

  if (visibleValues.length === 0) {
    const empty = document.createElement('div');
    empty.className = 'multi-empty';
    empty.textContent = 'No matches';
    container.appendChild(empty);
    return;
  }

  visibleValues.forEach((value) => {
    const row = document.createElement('label');
    row.className = 'multi-item';
    const check = document.createElement('input');
    check.type = 'checkbox';
    check.checked = selectedSet.has(value);
    check.addEventListener('change', () => {
      if (check.checked) {
        selectedSet.add(value);
      } else {
        selectedSet.delete(value);
      }
      onChange();
    });

    const text = document.createElement('span');
    text.textContent = value;
    row.appendChild(check);
    row.appendChild(text);
    container.appendChild(row);
  });
}

function pruneFilter(setValues, validValues) {
  const valid = new Set(validValues);
  [...setValues].forEach((value) => {
    if (!valid.has(value)) {
      setValues.delete(value);
    }
  });
}

function applyFilters(rows) {
  return rows.filter((row) => {
    const assignmentGroup = getAssignmentGroupValue(row);
    const assignmentGroupOk =
      state.filters.assignmentGroups.size === 0 ||
      state.filters.assignmentGroups.has(assignmentGroup);
    const monthOk = state.filters.months.size === 0 || state.filters.months.has(row.month);
    const assigneeOk =
      state.filters.assignees.size === 0 || state.filters.assignees.has(row.assignee);
    const statusOk = state.filters.statuses.size === 0 || state.filters.statuses.has(row.status);
    return assignmentGroupOk && monthOk && assigneeOk && statusOk;
  });
}

function renderFilterMenus() {
  renderMultiList(
    dom.assignmentGroupFilterList,
    state.optionLists.assignmentGroups,
    state.filters.assignmentGroups,
    'All assignment groups',
    handleFilterChange,
    'assignmentGroups',
  );
  renderMultiList(
    dom.monthFilterList,
    state.optionLists.months,
    state.filters.months,
    'All months',
    handleFilterChange,
    'months',
  );
  renderMultiList(
    dom.assigneeFilterList,
    state.optionLists.assignees,
    state.filters.assignees,
    'All assignees',
    handleFilterChange,
    'assignees',
  );
  renderMultiList(
    dom.statusFilterList,
    state.optionLists.statuses,
    state.filters.statuses,
    'All statuses',
    handleFilterChange,
    'statuses',
  );
}

function handleFilterChange() {
  renderFilterMenus();
  renderAll();
}

function renderAll() {
  state.filteredRows = applyFilters(state.rows);

  const totalFromFiltered = state.filteredRows.reduce(
    (acc, row) => acc + Number(row.ticket_count || 0),
    0,
  );

  const scope = state.summary?.scope ?? 'unknown_scope';
  dom.totalPill.textContent = `Total tickets: ${totalFromFiltered} (${scope})`;
  renderDonut(state.filteredRows);
  renderMonthBars(state.filteredRows);
  renderTable(state.filteredRows);
  const grain = state.summary?.aggregation_grain ?? 'row_based';
  const unit = state.summary?.unit ?? 'tickets';
  const tzLabel = state.summary?.reportingTimezone?.label ?? 'Madrid/Switzerland';
  const tzIana = state.summary?.reportingTimezone?.iana ?? 'Europe/Madrid';
  setStateText(`Loaded ${state.filteredRows.length} rows · Unit: ${unit} · Grain: ${grain} · TZ: ${tzLabel} (${tzIana})`);

  dom.assignmentGroupFilterSummary.textContent = asLabel(
    state.filters.assignmentGroups,
    'All assignment groups',
  );
  dom.monthFilterSummary.textContent = asLabel(state.filters.months, 'All months');
  dom.assigneeFilterSummary.textContent = asLabel(state.filters.assignees, 'All assignees');
  dom.statusFilterSummary.textContent = asLabel(state.filters.statuses, 'All statuses');
}

function renderFiltersFromRows(rows) {
  const assignmentGroups = [...new Set(rows.map((r) => getAssignmentGroupValue(r)))].sort(
    (a, b) => a.localeCompare(b),
  );
  const months = [...new Set(rows.map((r) => r.month))].sort(monthSort);
  const assignees = [...new Set(rows.map((r) => r.assignee))].sort((a, b) => a.localeCompare(b));
  const statuses = [...new Set(rows.map((r) => r.status))].sort((a, b) => a.localeCompare(b));

  pruneFilter(state.filters.assignmentGroups, assignmentGroups);
  pruneFilter(state.filters.months, months);
  pruneFilter(state.filters.assignees, assignees);
  pruneFilter(state.filters.statuses, statuses);

  state.optionLists.assignmentGroups = assignmentGroups;
  state.optionLists.months = months;
  state.optionLists.assignees = assignees;
  state.optionLists.statuses = statuses;
  renderFilterMenus();
}

function renderLegend(items) {
  dom.assigneeLegend.innerHTML = '';
  items.sort((a, b) => b.value - a.value).forEach((item) => {
    const li = document.createElement('li');
    li.innerHTML = `<span>${item.label}</span><strong>${item.value}</strong>`;
    dom.assigneeLegend.appendChild(li);
  });
}

function renderDonut(rows) {
  const assigneeData = aggregateBy(rows, 'assignee').sort((a, b) => b.value - a.value);
  const labels = assigneeData.map((d) => d.label);
  const values = assigneeData.map((d) => d.value);

  const colors = ['#0B6FBF', '#2F8DE0', '#5EA9EB', '#7CC3F1', '#9FD2F5', '#BEDFF7', '#D9ECFB'];

  if (state.charts.donut) state.charts.donut.destroy();

  state.charts.donut = new Chart(dom.donutCanvas, {
    type: 'doughnut',
    data: {
      labels,
      datasets: [{ data: values, backgroundColor: labels.map((_, i) => colors[i % colors.length]), borderWidth: 1 }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '62%',
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: (ctx) => `${ctx.label}: ${ctx.parsed}` } },
      },
    },
    plugins: [{
      id: 'centerText',
      afterDraw(chart) {
        const { ctx } = chart;
        const total = values.reduce((acc, v) => acc + v, 0);
        const x = chart.getDatasetMeta(0).data[0]?.x;
        const y = chart.getDatasetMeta(0).data[0]?.y;
        if (!x || !y) return;
        ctx.save();
        ctx.fillStyle = '#1a2433';
        ctx.font = '700 24px Segoe UI';
        ctx.textAlign = 'center';
        ctx.fillText(String(total), x, y - 2);
        ctx.fillStyle = '#5e6d7e';
        ctx.font = '12px Segoe UI';
        ctx.fillText('Total issues', x, y + 18);
        ctx.restore();
      },
    }],
  });

  renderLegend(assigneeData);
}

function renderMonthBars(rows) {
  const monthData = aggregateBy(rows, 'month').sort((a, b) => a.label.localeCompare(b.label));

  if (state.charts.month) state.charts.month.destroy();

  state.charts.month = new Chart(dom.monthCanvas, {
    type: 'bar',
    data: {
      labels: monthData.map((d) => d.label),
      datasets: [{
        label: 'Tickets',
        data: monthData.map((d) => d.value),
        backgroundColor: '#0B6FBFCC',
        borderColor: '#0B6FBF',
        borderWidth: 1,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true } },
    },
  });
}

function renderTable(rows) {
  dom.detailBody.innerHTML = '';
  rows
    .slice()
    .sort((a, b) => a.month.localeCompare(b.month) || b.ticket_count - a.ticket_count)
    .forEach((row) => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${row.month}</td>
        <td>${row.assignee}</td>
        <td>${row.status}</td>
        <td>${row.priority}</td>
        <td><span class="badge">${row.ticket_count}</span></td>
        <td><span class="badge sla">${row.sla_breach_count}</span></td>
      `;
      dom.detailBody.appendChild(tr);
    });
}

async function refresh() {
  setStateText('Loading data...');
  try {
    const payload = await fetchRowsFromApi();
    state.rows = payload.rows ?? [];
    state.summary = payload.summary ?? state.summary;
    renderFiltersFromRows(state.rows);
    renderAll();
  } catch (err) {
    setStateText(`Failed to load data: ${err.message}`, true);
  }
}

async function init() {
  setupDropdown(
    dom.assignmentGroupFilterToggle,
    dom.assignmentGroupFilterList,
    'assignment-group',
  );
  setupDropdown(dom.monthFilterToggle, dom.monthFilterList, 'month');
  setupDropdown(dom.assigneeFilterToggle, dom.assigneeFilterList, 'assignee');
  setupDropdown(dom.statusFilterToggle, dom.statusFilterList, 'status');

  dom.clearFiltersBtn.addEventListener('click', () => {
    state.filters.assignmentGroups.clear();
    state.filters.months.clear();
    state.filters.assignees.clear();
    state.filters.statuses.clear();
    state.searchQueries.assignmentGroups = '';
    state.searchQueries.months = '';
    state.searchQueries.assignees = '';
    state.searchQueries.statuses = '';
    renderFilterMenus();
    renderAll();
  });

  document.addEventListener('click', (event) => {
    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }
    if (!target.closest('[data-filter-dropdown]')) {
      closeAllDropdowns();
    }
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      closeAllDropdowns();
    }
  });

  await refresh();
}

document.addEventListener('DOMContentLoaded', init);

