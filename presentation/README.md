# Presentation (Consolidated)

This folder contains the synchronized presentation output.

## Purpose

- Keep local presentation development aligned with production output.
- Work with the same `tickets-by-hour` static assets used by production.
- Avoid presentation forks that would diverge from prod behavior.

## Scope rules

- Contains HTML templates, static assets, and client-side rendering logic.
- Must not contain SQL, Spark, Databricks SDK code, secrets, or tokens.
- Must consume governed serving contracts only.

## Current structure

- `tickets-by-hour/`: Tickets by hour dashboard page.
- `tickets-per-agent/`: Tickets per agent dashboard page.
- `ticket-lifecycle/`: Ticket lifecycle page.
- `shared/`: Shared UI assets used across pages.
- `start-local.ps1`: Starts a local static server from `presentation`.

## Run presentation only

```bash
cd presentation
python -m http.server 3000
```

Open `http://localhost:3000`.

## Run full local stack (recommended)

```bash
cd dev/infra-local
docker compose up --build
```

- Frontend: `http://localhost:3000`
- Mock API: `http://localhost:8000`

## Run with real local serving API

1. Start API from `serving/srf-axsa-api` on port `8000`.
2. Start presentation static server on port `3000`.
3. Open the menu on `http://localhost:3000`.

The dashboard uses localhost API when no deployment substitution is present.

