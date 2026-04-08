# Ticket Lifecycle Logic Reference

This document explains the full logic behind the `Ticket Lifecycle` page so it can be reused as a reference for building similar pages.

## 1. Purpose

The page analyzes one closed/resolved Jira ticket and shows:

- lifecycle total duration
- time spent per assignment group
- transition timeline between groups
- SLA target and SLA balance (breach or remaining)
- quick link to open the ticket in Jira

## 2. Main Files

- Frontend HTML: `presentation/ticket-lifecycle/index.html`
- Frontend JS: `presentation/ticket-lifecycle/app.js`
- Frontend CSS: `presentation/ticket-lifecycle/styles.css`
- API routes: `serving/srf-axsa-api/app.py`
- Data logic: `serving/srf-axsa-api/databricks_client.py`

## 3. API Endpoints Used By The Page

- `GET /api/ticket-lifecycle/available-months`
  - returns months in `YYYY-MM`, newest first
- `GET /api/ticket-lifecycle/assignment-groups`
  - returns assignment groups sorted with SMC-first preference
- `GET /api/ticket-lifecycle/tickets?months=...&assignmentGroup=...&limit=...`
  - returns ticket list filtered by selected months and assignment group
- `GET /api/ticket-lifecycle/details?ticketKey=...` (or `ticketId=...`)
  - returns full ticket lifecycle details, transitions, segments, group durations, and SLA

## 4. Ticket Scope Rules

- only tickets in `Closed` or `Resolved` scope are included
- assignment group filtering is case-insensitive trim-based matching
- month filter is applied on local reporting timezone month (`Europe/Madrid`)

## 5. Lifecycle Timeline Logic

### 5.1 Source Priority

Transitions are generated from activity table first. If activity parsing fails or produces no transitions, fallback is inferred from ticket history snapshots.

### 5.2 Transition Parsing

Activity content is parsed for assignment-group movement patterns (including Jira-style arrow formats like `A --> B`).

Non-assignment noise (for example generic workflow/status text) is filtered to avoid false transitions.

### 5.3 Lifecycle Start And End

- `createdIn`: original ticket created timestamp
- `lifecycleStartIn`: first activity timestamp if available and before close; otherwise ticket creation
- `closedOrResolvedIn`: close/resolved timestamp

`totalDurationSeconds` is computed from `lifecycleStartIn` to `closedOrResolvedIn`.

### 5.4 Closure Event

A final synthetic timeline event is appended at close time:

- `fromGroup`: last active assignment group
- `toGroup`: final status (`Resolved` or `Closed`)
- `source`: `closure`

This guarantees a visible terminal point in the timeline.

### 5.5 Tiny Initial None Segment

If the first segment is `None` and very short (configurable threshold), it is ignored in `groupDurations` totals to avoid polluting the table.

## 6. SLA Logic

SLA is calculated in backend and returned as `details.sla`.

### 6.1 Priority Mapping

- P1 -> 6h
- P2 -> 12h
- P3 -> 24h (2 days x 12h)
- P4 -> 60h (5 days x 12h)
- P5 -> 120h (10 days x 12h)

Priority used is the final priority at closure.

### 6.2 Clock Rules

- P1/P2: 24x7 elapsed time
- P3/P4/P5: business-time only
  - Monday to Friday
  - 07:00 to 19:00
  - timezone: `Europe/Madrid`

### 6.3 Excluded Status Time

Any status variant matching `await*` + `customer` (including typo variants like `costumer`) is excluded from SLA effective time.

### 6.4 SLA Fields Returned

`sla` object contains:

- `priority` (normalized, for example `P4`)
- `targetSeconds`
- `effectiveSeconds`
- `balanceSeconds` (`target - effective`)
- `isBreached` (`true` when balance is negative)
- `clock` (`24x7` or weekday business clock marker)

## 7. Frontend Rendering Logic

## 7.1 Filters

- multi-select months
- searchable assignment group selector
- searchable ticket list
- clear button resets all filters and detail cards

## 7.2 Cards

- Metrics card: total lifecycle, ticket status, transition source
- SLA card:
  - ticket quick link (`Open in Jira`)
  - SLA target
  - priority at closure
  - SLA balance
  - balance style:
    - red + white text when breached
    - soft blue when positive
- Time per Assignment Group card:
  - table + share percentage
  - extra `Total lifecycle` bar summary below table
- Transition Timeline card:
  - ordered transition events including final `closure` event

## 7.3 Error Handling

- API errors are shown in details area rather than silent failure
- backend maps upstream failures to `502/503` with explicit details
- Databricks `401` handling includes reconnect + one retry in client execute logic

## 8. Data Contract Example (`/details`)

Main response fields:

- `ticketId`, `ticketKey`
- `status`, `priority`
- `createdIn`, `lifecycleStartIn`, `closedOrResolvedIn`
- `totalDurationSeconds`
- `groupDurations[]`
- `segments[]`
- `transitions[]`
- `sla{...}`
- `meta{ transitionSource, lifecycleStartSource, reportingTimezone, ... }`

## 9. Reuse Checklist For New Pages

When creating a new analytics page based on this one:

1. Define strict scope and status filters first.
2. Decide source-of-truth precedence (activity vs inferred history).
3. Keep a stable details contract from backend to frontend.
4. Include explicit error states in UI.
5. Keep timezone and business-time rules centralized in backend.
6. Add synthetic closure/end events when timeline completeness is required.
7. Keep both trees synchronized:
   - `dev/presentation/...`
   - `presentation/...`

## 10. Notes

- All UI text in `presentation` should remain in English.
- This page intentionally favors deterministic backend logic over frontend-only calculations.
- If SLA rules evolve (holidays, region calendars, custom work shifts), update backend SLA helpers first and keep the frontend purely presentational.

