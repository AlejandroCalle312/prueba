# Ticket Lifecycle Quick Start

Use this folder as the reference implementation for timeline + SLA analytics pages.

## Start Here

- Full implementation guide: `TICKET_LIFECYCLE_LOGIC.md`
- Frontend entrypoint: `index.html`
- Frontend logic: `app.js`
- Styling: `styles.css`

## What This Page Already Solves

- Month/group/ticket filtering UI
- Assignment-group transition timeline
- Lifecycle duration from first activity to close/resolved
- Time per assignment group with total bar
- SLA target and SLA balance with visual breach state
- Quick Jira link for selected ticket

## Reuse In Another Page

1. Copy UI structure from `index.html`.
2. Reuse data fetch + rendering flow from `app.js`.
3. Keep business rules in backend (`databricks_client.py`) and keep frontend presentational.
4. Preserve API contract of `/api/ticket-lifecycle/details` unless you version it.
5. Keep `dev/presentation` and `presentation` synchronized.

## Core Endpoint

- `GET /api/ticket-lifecycle/details?ticketKey=ITHUB-123`

Main fields used by frontend:

- `totalDurationSeconds`
- `groupDurations[]`
- `transitions[]`
- `sla{ targetSeconds, balanceSeconds, isBreached, priority }`
