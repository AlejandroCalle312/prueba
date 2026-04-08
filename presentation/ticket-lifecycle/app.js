'use strict';

const API_BASE = window.APP_CONFIG?.apiBase ?? 'https://api-srf-axsa.azurewebsites.net';
const PRIORITY_GROUP_KEYWORD = 'service management center';
const JIRA_BASE_URL = 'https://axpo.atlassian.net/browse';

const state = {
  months: [],
  selectedMonths: [],
  groups: [],
  selectedGroup: '',
  groupSearch: '',
  tickets: [],
  ticketSearch: '',
  selectedTicket: null,
  details: null,
};

const $ = (selector) => document.querySelector(selector);

const dom = {
  monthsList: $('#months-list'),
  groupsList: $('#groups-list'),
  groupSearch: $('#group-search'),
  groupSelected: $('#group-selected'),
  ticketSearch: $('#ticket-search'),
  ticketsMeta: $('#tickets-meta'),
  ticketsList: $('#tickets-list'),
  btnClear: $('#btn-clear'),
  metricTotal: $('#metric-total'),
  metricStatus: $('#metric-status'),
  metricSource: $('#metric-source'),
  groupsDurationTable: $('#groups-duration-table'),
  groupsDurationBody: $('#groups-duration-body'),
  groupsDurationEmpty: $('#groups-duration-empty'),
  groupsTotal: $('#groups-total'),
  groupsTotalLabel: $('#groups-total-label'),
  groupsTotalFill: $('#groups-total-fill'),
  timelineList: $('#timeline-list'),
  timelineEmpty: $('#timeline-empty'),
  slaContent: $('#sla-content'),
  slaEmpty: $('#sla-empty'),
  slaTarget: $('#sla-target'),
  slaPriority: $('#sla-priority'),
  slaBalance: $('#sla-balance'),
  slaTicketLink: $('#sla-ticket-link'),
};

function normalise(value) {
  return (value ?? '').toString().trim().toLowerCase();
}

function formatDateLabel(isoDate) {
  if (!isoDate) return '-';
  const dt = new Date(isoDate);
  if (Number.isNaN(dt.getTime())) return isoDate;
  return dt.toLocaleString('en-GB', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
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

function formatSignedDuration(totalSeconds) {
  const value = Number(totalSeconds) || 0;
  const prefix = value < 0 ? '-' : '+';
  return `${prefix}${formatDuration(Math.abs(value))}`;
}

function buildJiraUrl(ticketKey, ticketId) {
  const key = (ticketKey || '').trim();
  if (key) return `${JIRA_BASE_URL}/${key}`;
  const idValue = (ticketId || '').trim();
  if (idValue && /^\d+$/.test(idValue)) return `${JIRA_BASE_URL}/ITHUB-${idValue}`;
  return '';
}

function monthToLabel(month) {
  const [year, monthNumber] = String(month).split('-');
  const dt = new Date(Number(year), Number(monthNumber) - 1, 1);
  if (Number.isNaN(dt.getTime())) return month;
  return dt.toLocaleDateString('en-GB', { month: 'long', year: 'numeric' });
}

function sortGroups(groups) {
  return [...groups].sort((a, b) => {
    const aPriority = normalise(a).includes(PRIORITY_GROUP_KEYWORD) ? 0 : 1;
    const bPriority = normalise(b).includes(PRIORITY_GROUP_KEYWORD) ? 0 : 1;
    if (aPriority !== bPriority) return aPriority - bPriority;
    return a.localeCompare(b, undefined, { sensitivity: 'base' });
  });
}

function getFilteredGroups() {
  const search = normalise(state.groupSearch);
  return sortGroups(state.groups).filter((group) => normalise(group).includes(search));
}

function getFilteredTickets() {
  const search = normalise(state.ticketSearch);
  if (!search) return state.tickets;
  return state.tickets.filter((ticket) => {
    const key = normalise(ticket.ticketKey);
    const id = normalise(ticket.ticketId);
    const number = key.includes('-') ? key.split('-').pop() : key;
    return key.includes(search) || id.includes(search) || number.includes(search);
  });
}

function renderMonths() {
  dom.monthsList.innerHTML = '';
  if (!state.months.length) {
    dom.monthsList.innerHTML = '<div class="empty">No months available.</div>';
    return;
  }

  state.months.forEach((month) => {
    const selected = state.selectedMonths.includes(month);
    const item = document.createElement('button');
    item.type = 'button';
    item.className = `menu-item${selected ? ' selected' : ''}`;
    item.setAttribute('aria-pressed', String(selected));
    item.textContent = monthToLabel(month);
    item.addEventListener('click', async () => {
      if (selected) {
        state.selectedMonths = state.selectedMonths.filter((m) => m !== month);
      } else {
        state.selectedMonths = [...state.selectedMonths, month];
      }
      renderMonths();
      await refreshTickets();
    });
    dom.monthsList.appendChild(item);
  });
}

function renderGroups() {
  dom.groupsList.innerHTML = '';
  const groups = getFilteredGroups();

  if (!groups.length) {
    dom.groupsList.innerHTML = '<div class="empty">No assignment groups found.</div>';
  }

  groups.forEach((group) => {
    const selected = state.selectedGroup === group;
    const item = document.createElement('button');
    item.type = 'button';
    item.className = `menu-item${selected ? ' selected' : ''}`;
    item.setAttribute('aria-pressed', String(selected));
    item.textContent = group;
    item.addEventListener('click', async () => {
      state.selectedGroup = group;
      renderGroups();
      renderGroupSummary();
      await refreshTickets();
    });
    dom.groupsList.appendChild(item);
  });

  renderGroupSummary();
}

function renderGroupSummary() {
  dom.groupSelected.textContent = state.selectedGroup
    ? `Selected: ${state.selectedGroup}`
    : 'Selected: none';
}

function renderTickets() {
  const filtered = getFilteredTickets();
  dom.ticketsList.innerHTML = '';

  if (!state.selectedMonths.length || !state.selectedGroup) {
    dom.ticketsMeta.textContent = 'Select month(s) and assignment group to load tickets.';
    dom.ticketsList.innerHTML = '<div class="empty">Waiting for filters.</div>';
    return;
  }

  dom.ticketsMeta.textContent = `${filtered.length} ticket(s) visible`;

  if (!filtered.length) {
    dom.ticketsList.innerHTML = '<div class="empty">No tickets match current filters.</div>';
    return;
  }

  filtered.forEach((ticket) => {
    const selected = state.selectedTicket?.ticketKey === ticket.ticketKey;
    const item = document.createElement('button');
    item.type = 'button';
    item.className = `menu-item${selected ? ' selected' : ''}`;
    item.innerHTML = `
      <span>${ticket.ticketKey || ticket.ticketId || 'Ticket'}</span>
      <small>${ticket.month || '-'} | ${ticket.status || '-'}</small>
    `;
    item.addEventListener('click', async () => {
      state.selectedTicket = ticket;
      renderTickets();
      await loadTicketDetails(ticket);
    });
    dom.ticketsList.appendChild(item);
  });
}

function clearDetails() {
  state.details = null;
  dom.metricTotal.textContent = '-';
  dom.metricStatus.textContent = '-';
  dom.metricSource.textContent = '-';
  dom.groupsDurationBody.innerHTML = '';
  dom.groupsDurationTable.classList.add('hidden');
  dom.groupsDurationEmpty.classList.remove('hidden');
  if (dom.groupsTotal) dom.groupsTotal.classList.add('hidden');
  if (dom.groupsTotalLabel) dom.groupsTotalLabel.textContent = '-';
  if (dom.groupsTotalFill) dom.groupsTotalFill.style.width = '100%';
  dom.timelineList.innerHTML = '';
  dom.timelineList.classList.add('hidden');
  dom.timelineEmpty.classList.remove('hidden');
  dom.timelineEmpty.textContent = 'No ticket selected.';
  if (dom.slaContent) dom.slaContent.classList.add('hidden');
  if (dom.slaEmpty) {
    dom.slaEmpty.classList.remove('hidden');
    dom.slaEmpty.textContent = 'Select a ticket to view SLA target and balance.';
  }
  if (dom.slaTarget) dom.slaTarget.textContent = '-';
  if (dom.slaPriority) dom.slaPriority.textContent = '-';
  if (dom.slaBalance) {
    dom.slaBalance.textContent = '-';
    dom.slaBalance.classList.remove('positive', 'breached');
    dom.slaBalance.classList.add('neutral');
  }
  if (dom.slaTicketLink) {
    dom.slaTicketLink.removeAttribute('href');
    dom.slaTicketLink.classList.add('hidden');
  }
}

function showDetailsError(message) {
  clearDetails();
  dom.metricStatus.textContent = 'Error';
  dom.metricSource.textContent = 'API';
  dom.timelineEmpty.textContent = `Unable to load ticket details: ${message}`;
  if (dom.slaEmpty) {
    dom.slaEmpty.textContent = 'SLA unavailable until API error is resolved.';
  }
}

function renderDetails() {
  const details = state.details;
  if (!details) {
    clearDetails();
    return;
  }

  dom.metricTotal.textContent = formatDuration(details.totalDurationSeconds);
  dom.metricStatus.textContent = details.status || '-';
  dom.metricSource.textContent = details.meta?.transitionSource || '-';

  const sla = details.sla || null;
  if (sla && dom.slaContent && dom.slaTarget && dom.slaPriority && dom.slaBalance) {
    dom.slaContent.classList.remove('hidden');
    if (dom.slaEmpty) dom.slaEmpty.classList.add('hidden');
    const jiraUrl = buildJiraUrl(details.ticketKey || state.selectedTicket?.ticketKey, details.ticketId || state.selectedTicket?.ticketId);
    if (dom.slaTicketLink) {
      if (jiraUrl) {
        dom.slaTicketLink.href = jiraUrl;
        dom.slaTicketLink.classList.remove('hidden');
      } else {
        dom.slaTicketLink.removeAttribute('href');
        dom.slaTicketLink.classList.add('hidden');
      }
    }
    dom.slaTarget.textContent = sla.targetSeconds != null ? formatDuration(sla.targetSeconds) : '-';
    dom.slaPriority.textContent = sla.priority || details.priority || '-';

    const balance = Number(sla.balanceSeconds);
    dom.slaBalance.classList.remove('positive', 'breached', 'neutral');
    if (Number.isNaN(balance)) {
      dom.slaBalance.textContent = '-';
      dom.slaBalance.classList.add('neutral');
    } else if (balance < 0) {
      dom.slaBalance.textContent = formatSignedDuration(balance);
      dom.slaBalance.classList.add('breached');
    } else {
      dom.slaBalance.textContent = formatSignedDuration(balance);
      dom.slaBalance.classList.add('positive');
    }
  }

  const groupDurations = details.groupDurations || [];
  const total = Math.max(1, Number(details.totalDurationSeconds) || 0);
  dom.groupsDurationBody.innerHTML = '';

  if (!groupDurations.length) {
    dom.groupsDurationTable.classList.add('hidden');
    dom.groupsDurationEmpty.classList.remove('hidden');
    if (dom.groupsTotal) dom.groupsTotal.classList.add('hidden');
  } else {
    groupDurations.forEach((row) => {
      const tr = document.createElement('tr');
      const pct = ((Number(row.durationSeconds) || 0) * 100) / total;
      tr.innerHTML = `
        <td>${row.assignmentGroup}</td>
        <td>${formatDuration(row.durationSeconds)}</td>
        <td>${pct.toFixed(1)}%</td>
      `;
      dom.groupsDurationBody.appendChild(tr);
    });
    dom.groupsDurationTable.classList.remove('hidden');
    dom.groupsDurationEmpty.classList.add('hidden');
    if (dom.groupsTotal) dom.groupsTotal.classList.remove('hidden');
    if (dom.groupsTotalLabel) dom.groupsTotalLabel.textContent = formatDuration(details.totalDurationSeconds);
    if (dom.groupsTotalFill) dom.groupsTotalFill.style.width = '100%';
  }

  const transitions = details.transitions || [];
  dom.timelineList.innerHTML = '';
  if (!transitions.length) {
    dom.timelineList.classList.add('hidden');
    dom.timelineEmpty.classList.remove('hidden');
    dom.timelineEmpty.textContent = 'No assignment-group transitions found for selected ticket.';
  } else {
    transitions.forEach((transition) => {
      const li = document.createElement('li');
      li.innerHTML = `
        <div><strong>${transition.fromGroup || 'Unknown'}</strong> -> <strong>${transition.toGroup || 'Unknown'}</strong></div>
        <div class="time">${formatDateLabel(transition.timestamp)} (${transition.source || '-'})</div>
      `;
      dom.timelineList.appendChild(li);
    });
    dom.timelineList.classList.remove('hidden');
    dom.timelineEmpty.classList.add('hidden');
    dom.timelineEmpty.textContent = '';
  }
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || `HTTP ${response.status}`);
  }
  return response.json();
}

async function loadMonths() {
  state.months = await fetchJson(`${API_BASE}/api/ticket-lifecycle/available-months`);
  if (state.months.length && !state.selectedMonths.length) {
    state.selectedMonths = [state.months[0]];
  }
  renderMonths();
}

async function loadGroups() {
  state.groups = await fetchJson(`${API_BASE}/api/ticket-lifecycle/assignment-groups`);
  if (!state.selectedGroup && state.groups.length) {
    state.selectedGroup = sortGroups(state.groups)[0] || '';
  }
  renderGroups();
}

async function refreshTickets() {
  clearDetails();
  state.selectedTicket = null;

  if (!state.selectedMonths.length || !state.selectedGroup) {
    state.tickets = [];
    renderTickets();
    return;
  }

  const query = new URLSearchParams();
  query.set('months', state.selectedMonths.join(','));
  query.set('assignmentGroup', state.selectedGroup);
  query.set('limit', '2000');

  const payload = await fetchJson(`${API_BASE}/api/ticket-lifecycle/tickets?${query.toString()}`);
  state.tickets = payload.rows || [];
  renderTickets();
}

async function loadTicketDetails(ticket) {
  if (!ticket) {
    clearDetails();
    return;
  }

  const query = new URLSearchParams();
  if (ticket.ticketKey) query.set('ticketKey', ticket.ticketKey);
  if (!ticket.ticketKey && ticket.ticketId) query.set('ticketId', ticket.ticketId);

  try {
    state.details = await fetchJson(`${API_BASE}/api/ticket-lifecycle/details?${query.toString()}`);
    renderDetails();
  } catch (error) {
    state.details = null;
    showDetailsError(error.message || 'Unknown error');
    console.error(error);
  }
}

function bindEvents() {
  dom.groupSearch.addEventListener('input', () => {
    state.groupSearch = dom.groupSearch.value;
    renderGroups();
  });

  dom.ticketSearch.addEventListener('input', () => {
    state.ticketSearch = dom.ticketSearch.value;
    renderTickets();
  });

  dom.btnClear.addEventListener('click', () => {
    state.selectedMonths = [];
    state.selectedGroup = '';
    state.groupSearch = '';
    state.ticketSearch = '';
    state.tickets = [];
    state.selectedTicket = null;
    dom.groupSearch.value = '';
    dom.ticketSearch.value = '';
    renderMonths();
    renderGroups();
    renderTickets();
    clearDetails();
  });
}

async function init() {
  bindEvents();
  clearDetails();

  try {
    await loadMonths();
    await loadGroups();
    await refreshTickets();
  } catch (error) {
    dom.ticketsMeta.textContent = `Error: ${error.message}`;
    console.error(error);
  }
}

document.addEventListener('DOMContentLoaded', init);
