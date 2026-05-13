'use strict';

const configuredApiBase = (window.APP_CONFIG?.apiBase || '').trim();
const API_BASE = configuredApiBase && !/^%%.+%%$/.test(configuredApiBase)
  ? configuredApiBase.replace(/\/$/, '')
  : window.location.origin;

const state = {
  availableMonths: [],
  assignmentGroups: [],
  selectedMonths: new Set(),
  selectedGroup: '',
  groupSearchQuery: '',
  payload: { monthly: [], summary: {} },
  charts: { gauge: null, monthly: null },
  autoRefreshTimer: null,
};

const dom = {
  state: document.getElementById('state'),
  bringDataBtn: document.getElementById('bring-data-btn'),
  clearFiltersBtn: document.getElementById('clear-filters-btn'),
  exportPdfBtn: document.getElementById('export-pdf-btn'),
  monthToggle: document.getElementById('month-filter-toggle'),
  monthSummary: document.getElementById('month-filter-summary'),
  monthList: document.getElementById('month-filter-list'),
  groupToggle: document.getElementById('group-filter-toggle'),
  groupSummary: document.getElementById('group-filter-summary'),
  groupList: document.getElementById('group-filter-list'),
  scopePill: document.getElementById('scope-pill'),
  groupPill: document.getElementById('group-pill'),
  gt3Value: document.getElementById('gt3-value'),
  overallValue: document.getElementById('overall-value'),
  selectedRatio: document.getElementById('selected-ratio'),
  overallRatio: document.getElementById('overall-ratio'),
  deltaRatio: document.getElementById('delta-ratio'),
  detailBody: document.getElementById('detail-body'),
  breakdownSummary: document.getElementById('breakdown-summary'),
  breakdownBody: document.getElementById('breakdown-body'),
  gaugeCanvas: document.getElementById('ratio-gauge'),
  monthlyCanvas: document.getElementById('monthly-chart'),
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

function destroyChart(chart) {
  if (chart) {
    chart.destroy();
  }
}

function parseNumber(value) {
  const num = Number(value || 0);
  return Number.isFinite(num) ? num : 0;
}

function formatPct(value) {
  return `${parseNumber(value).toFixed(2)}%`;
}

function formatPp(value) {
  const n = parseNumber(value);
  const sign = n > 0 ? '+' : '';
  return `${sign}${n.toFixed(2)} pp`;
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
    dom.monthSummary.textContent = asMonthLabel(state.selectedMonths);
    scheduleAutoRefresh();
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
      dom.monthSummary.textContent = asMonthLabel(state.selectedMonths);
      scheduleAutoRefresh();
    });

    const text = document.createElement('span');
    text.textContent = month;
    row.appendChild(check);
    row.appendChild(text);
    dom.monthList.appendChild(row);
  });

  dom.monthSummary.textContent = asMonthLabel(state.selectedMonths);
}

function renderGroupFilter() {
  dom.groupList.innerHTML = '';

  const searchWrap = document.createElement('div');
  searchWrap.className = 'single-search-wrap';
  const searchInput = document.createElement('input');
  searchInput.type = 'text';
  searchInput.className = 'single-search';
  searchInput.placeholder = 'Search assignment group...';
  searchInput.value = state.groupSearchQuery;
  searchInput.addEventListener('input', () => {
    state.groupSearchQuery = searchInput.value;
    renderGroupOptions();
  });
  searchWrap.appendChild(searchInput);
  dom.groupList.appendChild(searchWrap);

  const optionsWrap = document.createElement('div');
  optionsWrap.id = 'group-filter-options';
  dom.groupList.appendChild(optionsWrap);

  renderGroupOptions();
}

function renderGroupOptions() {
  const optionsWrap = document.getElementById('group-filter-options');
  if (!optionsWrap) {
    return;
  }

  optionsWrap.innerHTML = '';

  const normalizedSearch = state.groupSearchQuery.trim().toLowerCase();
  const visibleGroups = normalizedSearch
    ? state.assignmentGroups.filter((group) => group.toLowerCase().includes(normalizedSearch))
    : state.assignmentGroups;

  const fragment = document.createDocumentFragment();

  const allButton = document.createElement('button');
  allButton.type = 'button';
  allButton.className = 'single-item';
  allButton.textContent = 'All assignment groups';
  allButton.classList.toggle('is-selected', !state.selectedGroup);
  allButton.addEventListener('click', () => {
    state.selectedGroup = '';
    dom.groupSummary.textContent = 'All assignment groups';
    renderGroupOptions();
    dom.groupToggle.setAttribute('aria-expanded', 'false');
    dom.groupList.hidden = true;
    scheduleAutoRefresh();
  });
  fragment.appendChild(allButton);

  visibleGroups.forEach((group) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'single-item';
    button.textContent = group;
    button.classList.toggle('is-selected', state.selectedGroup === group);
    button.addEventListener('click', () => {
      state.selectedGroup = group;
      dom.groupSummary.textContent = group;
      renderGroupOptions();
      dom.groupToggle.setAttribute('aria-expanded', 'false');
      dom.groupList.hidden = true;
      scheduleAutoRefresh();
    });
    fragment.appendChild(button);
  });

  if (visibleGroups.length === 0) {
    const empty = document.createElement('div');
    empty.className = 'single-empty';
    empty.textContent = 'No assignment groups found';
    fragment.appendChild(empty);
  }

  optionsWrap.appendChild(fragment);
}

function renderGauge(selectedRatioPct) {
  const ratio = Math.max(0, Math.min(100, parseNumber(selectedRatioPct)));
  destroyChart(state.charts.gauge);

  state.charts.gauge = new Chart(dom.gaugeCanvas, {
    type: 'doughnut',
    data: {
      labels: ['Ratio >3', 'Remaining'],
      datasets: [{
        data: [ratio, Math.max(0, 100 - ratio)],
        backgroundColor: ['#1c7ed6', '#cfd4da'],
        borderWidth: 0,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '82%',
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.label}: ${ctx.parsed.toFixed(2)}%`,
          },
        },
      },
    },
    plugins: [{
      id: 'gauge-center',
      afterDraw(chart) {
        const arc = chart.getDatasetMeta(0)?.data?.[0];
        if (!arc) {
          return;
        }
        const { ctx } = chart;
        ctx.save();
        ctx.fillStyle = '#1d232b';
        ctx.font = '700 64px Segoe UI';
        ctx.textAlign = 'center';
        ctx.fillText(`${ratio.toFixed(0)}%`, arc.x, arc.y + 12);
        ctx.restore();
      },
    }],
  });
}

function renderMonthlyChart(monthlyRows) {
  const sortedRows = [...monthlyRows].sort((a, b) => String(a.month || '').localeCompare(String(b.month || '')));
  const labels = sortedRows.map((row) => String(row.month || ''));
  const selectedData = sortedRows.map((row) => parseNumber(row.selected_ratio_pct));
  const overallData = sortedRows.map((row) => parseNumber(row.overall_ratio_pct));

  destroyChart(state.charts.monthly);
  state.charts.monthly = new Chart(dom.monthlyCanvas, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Selected group',
          data: selectedData,
          borderColor: '#ff5a2f',
          backgroundColor: 'rgba(255, 90, 47, 0.2)',
          pointRadius: 3,
          pointHoverRadius: 4,
          tension: 0.28,
        },
        {
          label: 'General',
          data: overallData,
          borderColor: '#1c7ed6',
          backgroundColor: 'rgba(28, 126, 214, 0.2)',
          pointRadius: 3,
          pointHoverRadius: 4,
          tension: 0.28,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom' },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${parseNumber(ctx.parsed.y).toFixed(2)}%`,
          },
        },
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            callback: (value) => `${value}%`,
          },
        },
      },
    },
  });
}

function renderTable(monthlyRows) {
  dom.detailBody.innerHTML = '';

  if (!monthlyRows.length) {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td colspan="7">No rows for selected filters.</td>';
    dom.detailBody.appendChild(tr);
    return;
  }

  monthlyRows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${String(row.month || '')}</td>
      <td>${formatPct(row.selected_ratio_pct)}</td>
      <td>${formatPct(row.overall_ratio_pct)}</td>
      <td>${parseNumber(row.selected_gt3).toLocaleString()}</td>
      <td>${parseNumber(row.selected_total).toLocaleString()}</td>
      <td>${parseNumber(row.overall_gt3).toLocaleString()}</td>
      <td>${parseNumber(row.overall_total).toLocaleString()}</td>
    `;
    dom.detailBody.appendChild(tr);
  });
}

function renderBreakdown(rows, selectedGt3) {
  const breakdownRows = Array.isArray(rows) ? rows : [];
  const totalGt3 = parseNumber(selectedGt3);
  dom.breakdownBody.innerHTML = '';

  if (!breakdownRows.length) {
    dom.breakdownSummary.textContent = `Reassignment breakdown (>3): ${totalGt3.toLocaleString()} tickets, no detailed buckets.`;
    const tr = document.createElement('tr');
    tr.innerHTML = '<td colspan="2">No tickets above threshold for selected filters.</td>';
    dom.breakdownBody.appendChild(tr);
    return;
  }

  dom.breakdownSummary.textContent = `From ${totalGt3.toLocaleString()} tickets with more than 3 reassignments:`;
  breakdownRows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${parseNumber(row.reassignment_count).toLocaleString()}</td>
      <td>${parseNumber(row.ticket_count).toLocaleString()}</td>
    `;
    dom.breakdownBody.appendChild(tr);
  });
}

function renderSummary(summary) {
  const selected = summary?.selected_metrics || {};
  const overall = summary?.overall_metrics || {};
  const delta = parseNumber(summary?.delta_vs_overall_pct_points);

  dom.gt3Value.textContent = parseNumber(selected.reassignments_gt_3).toLocaleString();
  dom.overallValue.textContent = parseNumber(selected.tickets_overall).toLocaleString();

  dom.selectedRatio.textContent = `Selected: ${formatPct(selected.reassignment_ratio_pct)}`;
  dom.overallRatio.textContent = `General: ${formatPct(overall.reassignment_ratio_pct)}`;
  dom.deltaRatio.textContent = `Delta: ${formatPp(delta)}`;
  dom.deltaRatio.classList.toggle('good', delta <= 0);
  dom.deltaRatio.classList.toggle('bad', delta > 0);

  dom.scopePill.textContent = `Scope: ${summary?.scope || '--'}`;
  dom.groupPill.textContent = `Group: ${summary?.selected_group || 'All assignment groups'}`;

  renderGauge(selected.reassignment_ratio_pct);
}

function renderAll() {
  const payload = state.payload || { monthly: [], summary: {} };
  renderSummary(payload.summary || {});
  renderMonthlyChart(Array.isArray(payload.monthly) ? payload.monthly : []);
  renderTable(Array.isArray(payload.monthly) ? payload.monthly : []);
  renderBreakdown(payload.reassignment_breakdown, payload.summary?.selected_metrics?.reassignments_gt_3);

  const monthlyCount = Array.isArray(payload.monthly) ? payload.monthly.length : 0;
  setStateText(`Loaded ${monthlyCount} month rows. Formula uses threshold >3.`);
}

function buildDataUrl() {
  const params = new URLSearchParams();
  const months = [...state.selectedMonths].sort();
  if (months.length > 0) {
    params.set('months', months.join(','));
  }
  if (state.selectedGroup) {
    params.set('assignmentGroup', state.selectedGroup);
  }
  return `${API_BASE}/api/reassignment-ratio?${params.toString()}`;
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

async function fetchMonths() {
  return fetchJson(`${API_BASE}/api/reassignment-ratio/months`);
}

async function fetchAssignmentGroups() {
  return fetchJson(`${API_BASE}/api/reassignment-ratio/assignment-groups`);
}

async function bringData() {
  dom.bringDataBtn.disabled = true;
  setStateText('Loading data...');
  try {
    state.payload = await fetchJson(buildDataUrl());
    renderAll();
  } catch (err) {
    setStateText(`Failed to load data: ${err.message || String(err)}`, true);
  } finally {
    dom.bringDataBtn.disabled = false;
  }
}

function scheduleAutoRefresh() {
  if (state.autoRefreshTimer) {
    window.clearTimeout(state.autoRefreshTimer);
  }
  state.autoRefreshTimer = window.setTimeout(() => {
    void bringData();
  }, 250);
}

function clearFilters() {
  state.selectedMonths.clear();
  state.selectedGroup = '';
  dom.groupSummary.textContent = 'All assignment groups';
  renderMonthFilter();
  renderGroupFilter();
  scheduleAutoRefresh();
}

function buildPdfFileName() {
  const months = [...state.selectedMonths].sort();
  const monthPart = months.length > 0 ? months.join('_') : 'all-months';
  const groupRaw = (state.selectedGroup || 'all-groups').toLowerCase();
  const groupPart = groupRaw.replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '') || 'all-groups';
  return `reassignment-ratio_${monthPart}_${groupPart}.pdf`;
}

function exportPdf() {
  const jsPdfApi = window.jspdf?.jsPDF;
  if (!jsPdfApi) {
    setStateText('PDF export unavailable: jsPDF library not loaded.', true);
    return;
  }

  const summary = state.payload?.summary || {};
  const monthlyRows = Array.isArray(state.payload?.monthly) ? state.payload.monthly : [];
  const breakdownRows = Array.isArray(state.payload?.reassignment_breakdown) ? state.payload.reassignment_breakdown : [];
  const selected = summary.selected_metrics || {};
  const overall = summary.overall_metrics || {};

  const pdf = new jsPdfApi({ orientation: 'portrait', unit: 'pt', format: 'a4' });
  const pageWidth = pdf.internal.pageSize.getWidth();
  const pageHeight = pdf.internal.pageSize.getHeight();
  const margin = 40;
  const contentWidth = pageWidth - margin * 2;
  let y = margin;

  const ensureSpace = (requiredHeight) => {
    if (y + requiredHeight <= pageHeight - margin) {
      return;
    }
    pdf.addPage();
    y = margin;
  };

  const drawLine = (text, fontSize = 10, color = [29, 35, 43]) => {
    ensureSpace(fontSize + 8);
    pdf.setFontSize(fontSize);
    pdf.setTextColor(color[0], color[1], color[2]);
    pdf.text(text, margin, y);
    y += fontSize + 6;
  };

  pdf.setFont('helvetica', 'bold');
  drawLine('Reassignment Ratio > 3', 18);
  pdf.setFont('helvetica', 'normal');
  drawLine('Snapshot exported from current dashboard selection', 11, [105, 117, 130]);
  drawLine(`Generated at: ${new Date().toLocaleString()}`, 10, [105, 117, 130]);
  y += 8;

  pdf.setFont('helvetica', 'bold');
  drawLine('Filters', 12);
  pdf.setFont('helvetica', 'normal');
  drawLine(`Month: ${dom.monthSummary.textContent || 'All months'}`);
  drawLine(`Assignment Group: ${summary.selected_group || 'All assignment groups'}`);
  drawLine(`Scope: ${summary.scope || '--'}`);
  y += 6;

  pdf.setFont('helvetica', 'bold');
  drawLine('KPIs', 12);
  pdf.setFont('helvetica', 'normal');
  drawLine(`Reassignments > 3 (selected): ${parseNumber(selected.reassignments_gt_3).toLocaleString()}`);
  drawLine(`Tickets overall (selected): ${parseNumber(selected.tickets_overall).toLocaleString()}`);
  drawLine(`Selected ratio: ${formatPct(selected.reassignment_ratio_pct)}`);
  drawLine(`General ratio: ${formatPct(overall.reassignment_ratio_pct)}`);
  drawLine(`Delta: ${formatPp(summary.delta_vs_overall_pct_points)}`);
  y += 10;

  const gaugeImage = dom.gaugeCanvas?.toDataURL('image/png');
  const monthlyImage = dom.monthlyCanvas?.toDataURL('image/png');
  if (gaugeImage) {
    ensureSpace(200);
    pdf.setFont('helvetica', 'bold');
    drawLine('Gauge', 12);
    pdf.addImage(gaugeImage, 'PNG', margin, y, 180, 180);
    y += 188;
  }

  if (monthlyImage) {
    ensureSpace(260);
    pdf.setFont('helvetica', 'bold');
    drawLine('Monthly trend', 12);
    pdf.addImage(monthlyImage, 'PNG', margin, y, contentWidth, 220);
    y += 228;
  }

  pdf.setFont('helvetica', 'bold');
  drawLine('Monthly detail', 12);
  pdf.setFont('helvetica', 'normal');
  if (!monthlyRows.length) {
    drawLine('No rows for selected filters.');
  } else {
    drawLine('Month | Sel% | Gen% | Sel>3 | SelTotal | Gen>3 | GenTotal', 9, [105, 117, 130]);
    monthlyRows.forEach((row) => {
      const line = [
        String(row.month || ''),
        formatPct(row.selected_ratio_pct),
        formatPct(row.overall_ratio_pct),
        parseNumber(row.selected_gt3).toLocaleString(),
        parseNumber(row.selected_total).toLocaleString(),
        parseNumber(row.overall_gt3).toLocaleString(),
        parseNumber(row.overall_total).toLocaleString(),
      ].join(' | ');
      drawLine(line, 9);
    });
  }

  y += 8;
  pdf.setFont('helvetica', 'bold');
  drawLine('Reassignment breakdown (>3)', 12);
  pdf.setFont('helvetica', 'normal');
  drawLine(`Total tickets >3: ${parseNumber(selected.reassignments_gt_3).toLocaleString()}`);
  if (!breakdownRows.length) {
    drawLine('No detailed buckets for current selection.');
  } else {
    breakdownRows.forEach((row) => {
      drawLine(`Reassignments ${parseNumber(row.reassignment_count)}: ${parseNumber(row.ticket_count).toLocaleString()} tickets`, 10);
    });
  }

  pdf.save(buildPdfFileName());
  setStateText('PDF generated from current selection.');
}

function wireEvents() {
  dom.bringDataBtn.addEventListener('click', bringData);
  dom.clearFiltersBtn.addEventListener('click', clearFilters);
  dom.exportPdfBtn.addEventListener('click', exportPdf);

  dom.monthToggle.addEventListener('click', () => {
    const open = dom.monthToggle.getAttribute('aria-expanded') !== 'true';
    dom.monthToggle.setAttribute('aria-expanded', open ? 'true' : 'false');
    dom.monthList.hidden = !open;
    if (open) {
      dom.groupToggle.setAttribute('aria-expanded', 'false');
      dom.groupList.hidden = true;
    }
  });

  dom.groupToggle.addEventListener('click', () => {
    const open = dom.groupToggle.getAttribute('aria-expanded') !== 'true';
    dom.groupToggle.setAttribute('aria-expanded', open ? 'true' : 'false');
    dom.groupList.hidden = !open;
    if (open) {
      dom.monthToggle.setAttribute('aria-expanded', 'false');
      dom.monthList.hidden = true;
    }
  });

  document.addEventListener('click', (event) => {
    if (!dom.monthToggle.contains(event.target) && !dom.monthList.contains(event.target)) {
      dom.monthToggle.setAttribute('aria-expanded', 'false');
      dom.monthList.hidden = true;
    }
    if (!dom.groupToggle.contains(event.target) && !dom.groupList.contains(event.target)) {
      dom.groupToggle.setAttribute('aria-expanded', 'false');
      dom.groupList.hidden = true;
    }
  });
}

async function init() {
  wireEvents();
  try {
    const [months, groups] = await Promise.all([fetchMonths(), fetchAssignmentGroups()]);
    state.availableMonths = Array.isArray(months) ? months.sort(monthSortDesc) : [];
    state.assignmentGroups = Array.isArray(groups) ? groups : [];

    if (state.availableMonths.length > 0) {
      state.selectedMonths.add(state.availableMonths[0]);
    }

    renderMonthFilter();
    renderGroupFilter();
    await bringData();
  } catch (err) {
    setStateText(`Failed to initialize: ${err.message || String(err)}`, true);
  }
}

init();
