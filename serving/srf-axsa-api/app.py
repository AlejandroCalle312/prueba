"""
SRF-AXSA Serving API — FastAPI application entry point.

Endpoints
---------
GET /health
    Liveness probe.

GET /api/available-months
    Returns the list of YYYY-MM months available in the last 24 months.

GET /api/assignment-groups
    Returns distinct assignment groups.

GET /api/tickets-by-hour?months=YYYY-MM,YYYY-MM
    Returns hourly ticket counts for the requested month(s).
    Defaults to the rolling last 3 months when *months* is omitted.

GET /api/ticket-by-hour?hour=9&month=2026-01
    Returns all tickets for the selected hour filter.

GET /api/tickets-per-agent?month=2026-02&assignee=...&status=...
    Returns monthly segmented analysis for Baden onsite support tickets.

GET /api/ticket-lifecycle/available-months
    Returns month options for lifecycle view.

GET /api/ticket-lifecycle/assignment-groups
    Returns assignment groups, prioritizing Service Management Center matches.

GET /api/ticket-lifecycle/tickets?months=YYYY-MM&assignmentGroup=...
    Returns closed/resolved tickets for the selected scope.

GET /api/ticket-lifecycle/details?ticketKey=ITHUB-123
    Returns total lifecycle duration and per-assignment-group segmentation.

POST /api/cache/invalidate
    Clears the in-process response cache. Protected by a shared secret
    passed in the X-Cache-Token header.
"""
from __future__ import annotations

import logging
import os
import re
import sys
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from databricks.sql.exc import RequestError

# Load .env for local runs from this folder regardless of process CWD.
# This must run before importing databricks_client because it reads env at import-time.
_DOTENV_PATH = Path(__file__).resolve().with_name(".env")
load_dotenv(dotenv_path=_DOTENV_PATH)

# Add serving/ to import path so this app can include sibling API routers.
_SERVING_ROOT = Path(__file__).resolve().parents[1]
if str(_SERVING_ROOT) not in sys.path:
    sys.path.append(str(_SERVING_ROOT))

from databricks_client import (
    DatabricksClient,
    get_client,
    _TICKETS_TABLE,
    _ACTIVITY_TABLE,
)
from api.tickets_per_agent import router as tickets_per_agent_router

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── App bootstrap ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="SRF-AXSA Serving API",
    description="REST API for Jira ticket analytics dashboards.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url=None,
)

# CORS — allow the Static Web App origin and localhost for development.
_allowed_origins: list[str] = [
    o.strip()
    for o in os.getenv(
        "CORS_ALLOWED_ORIGINS",
        "http://localhost:3000,http://localhost:3001",
    ).split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(tickets_per_agent_router)


@app.on_event("startup")
async def _log_active_databricks_source() -> None:
    logger.info(
        "Active Databricks source | workspace=%s | http_path=%s | tickets_table=%s | activity_table=%s",
        os.getenv("DATABRICKS_WORKSPACE_URL", "<unset>"),
        os.getenv("DATABRICKS_HTTP_PATH", "<unset>"),
        _TICKETS_TABLE,
        _ACTIVITY_TABLE,
    )

_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
_MAX_MONTHS = 24
_VALID_METRIC_MODES = {
    "open",
    "entry_smc_first",
    "closed",
    "assignment_transitions",
}


# ── Helpers ───────────────────────────────────────────────────────────────────


def _parse_months(months: str | None) -> list[str] | None:
    if not months:
        return None
    parsed = [m.strip() for m in months.split(",") if m.strip()]
    for m in parsed:
        if not _MONTH_RE.match(m):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid month format '{m}'. Expected YYYY-MM.",
            )
    if len(parsed) > _MAX_MONTHS:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum {_MAX_MONTHS} months per request.",
        )
    return parsed or None


def _parse_metric_mode(metric_mode: str | None) -> str:
    mode = (metric_mode or "open").strip().lower()
    if mode not in _VALID_METRIC_MODES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid metricMode '{metric_mode}'. "
                f"Allowed values: {', '.join(sorted(_VALID_METRIC_MODES))}."
            ),
        )
    return mode


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/health", tags=["ops"])
async def health_check() -> dict:
    """Liveness/readiness probe."""
    return {"status": "ok", "service": "srf-axsa-serving-api", "version": "1.0.0"}


@app.get("/api/available-months", tags=["tickets"])
async def available_months() -> JSONResponse:
    """
    Return distinct YYYY-MM months available in the dataset (last 24 months).

    Response: ``["2025-03", "2025-02", ...]``
    """
    client: DatabricksClient = get_client()
    try:
        data = client.get_available_months()
    except Exception as exc:
        logger.error("available-months query failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=502, detail="Upstream data source error."
        ) from exc
    return JSONResponse(content=data)


@app.get("/api/assignment-groups", tags=["tickets"])
async def assignment_groups() -> JSONResponse:
    """Return distinct assignment groups available in the dataset."""
    client: DatabricksClient = get_client()
    try:
        data = client.get_assignment_groups()
    except Exception as exc:
        logger.error("assignment-groups query failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=502, detail="Upstream data source error."
        ) from exc
    return JSONResponse(content=data)


@app.get("/api/tickets-by-hour", tags=["tickets"])
async def tickets_by_hour(
    months: Annotated[
        str | None,
        Query(
            description=(
                "Comma-separated YYYY-MM values, e.g. 2025-01,2025-02. "
                "Omit to default to the rolling last 3 months."
            ),
            example="2025-01,2025-02",
        ),
    ] = None,
    assignment_group: Annotated[
        str | None,
        Query(
            alias="assignmentGroup",
            description="Optional assignment group exact match filter.",
            example="AXPO Service Management Center",
        ),
    ] = None,
    metric_mode: Annotated[
        str | None,
        Query(
            alias="metricMode",
            description=(
                "Metric semantics: open, entry_smc_first, closed, "
                "assignment_transitions."
            ),
            example="open",
        ),
    ] = "open",
    include_meta: Annotated[
        bool,
        Query(
            alias="includeMeta",
            description=(
                "When true, return `{rows, meta}` payload instead of legacy row array."
            ),
        ),
    ] = False,
) -> JSONResponse:
    """
    Return hourly ticket-creation counts for the selected month(s).

    Response schema::

        [
          { "hour": 0, "count": 12, "month": "2025-01", "year": 2025 },
          ...
        ]

    Hours are in the range 0–23 (UTC). Missing hours have no entry
    (count zero is omitted at the database level).
    """
    parsed = _parse_months(months)
    parsed_metric_mode = _parse_metric_mode(metric_mode)
    client: DatabricksClient = get_client()
    try:
        data = client.get_tickets_by_hour(parsed, assignment_group, parsed_metric_mode)
    except Exception as exc:
        logger.error("tickets-by-hour query failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=502, detail="Upstream data source error."
        ) from exc
    if include_meta:
        return JSONResponse(content=data)
    if isinstance(data, list):
        return JSONResponse(content=data)
    return JSONResponse(content=data.get("rows", []))


@app.get("/api/ticket-by-hour", tags=["tickets"])
async def ticket_by_hour(
    hour: Annotated[
        int,
        Query(
            ge=0,
            le=23,
            description="Hour of day in UTC (0-23).",
            example=9,
        ),
    ],
    month: Annotated[
        str | None,
        Query(
            description="Optional month filter YYYY-MM.",
            example="2026-01",
        ),
    ] = None,
    assignment_group: Annotated[
        str | None,
        Query(
            alias="assignmentGroup",
            description="Optional assignment group exact match filter.",
            example="AXPO Service Management Center",
        ),
    ] = None,
    metric_mode: Annotated[
        str | None,
        Query(
            alias="metricMode",
            description=(
                "Metric semantics: open, entry_smc_first, closed, "
                "assignment_transitions."
            ),
            example="open",
        ),
    ] = "open",
) -> JSONResponse:
    """Return all tickets for the selected hour and filters."""
    if month and not _MONTH_RE.match(month):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid month format '{month}'. Expected YYYY-MM.",
        )


    parsed_metric_mode = _parse_metric_mode(metric_mode)
    client: DatabricksClient = get_client()
    try:
        data = client.get_ticket_by_hour(hour, month, assignment_group, parsed_metric_mode)
    except Exception as exc:
        logger.error("ticket-by-hour query failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=502, detail="Upstream data source error."
        ) from exc
    return JSONResponse(content=data)


@app.get("/api/ticket-lifecycle/available-months", tags=["ticket-lifecycle"])
async def ticket_lifecycle_available_months() -> JSONResponse:
    """Return distinct YYYY-MM values for lifecycle filter (newest first)."""
    client: DatabricksClient = get_client()
    try:
        data = client.get_available_months()
    except Exception as exc:
        logger.error("ticket-lifecycle available-months query failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream data source error.") from exc
    return JSONResponse(content=data)


@app.get("/api/ticket-lifecycle/assignment-groups", tags=["ticket-lifecycle"])
async def ticket_lifecycle_assignment_groups() -> JSONResponse:
    """Return assignment groups for lifecycle view, SMC-prioritized."""
    client: DatabricksClient = get_client()
    try:
        data = client.get_ticket_lifecycle_assignment_groups()
    except Exception as exc:
        logger.error(
            "ticket-lifecycle assignment-groups query failed: %s",
            exc,
            exc_info=True,
        )
        raise HTTPException(status_code=502, detail="Upstream data source error.") from exc
    return JSONResponse(content=data)


@app.get("/api/ticket-lifecycle/tickets", tags=["ticket-lifecycle"])
async def ticket_lifecycle_tickets(
    months: Annotated[
        str | None,
        Query(
            description="Comma-separated YYYY-MM values for month filter.",
            example="2026-03,2026-02",
        ),
    ] = None,
    assignment_group: Annotated[
        str,
        Query(
            alias="assignmentGroup",
            description="Assignment group exact match filter.",
            example="AXPO Service Management Center",
            min_length=1,
        ),
    ] = "",
    ticket_search: Annotated[
        str | None,
        Query(
            alias="ticketSearch",
            description="Optional ticket key/id contains search.",
            example="ITHUB-123",
        ),
    ] = None,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=2000,
            description="Maximum rows to return.",
        ),
    ] = 500,
) -> JSONResponse:
    """Return closed/resolved tickets for selected months + assignment group."""
    parsed_months = _parse_months(months)
    if not assignment_group.strip():
        raise HTTPException(status_code=400, detail="assignmentGroup is required.")

    client: DatabricksClient = get_client()
    try:
        data = client.get_ticket_lifecycle_tickets(
            parsed_months,
            assignment_group,
            ticket_search,
            limit,
        )
    except Exception as exc:
        logger.error("ticket-lifecycle tickets query failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream data source error.") from exc
    return JSONResponse(content=data)


@app.get("/api/ticket-lifecycle/details", tags=["ticket-lifecycle"])
async def ticket_lifecycle_details(
    ticket_key: Annotated[
        str | None,
        Query(
            alias="ticketKey",
            description="Ticket key, e.g. ITHUB-123.",
            example="ITHUB-123",
        ),
    ] = None,
    ticket_id: Annotated[
        str | None,
        Query(
            alias="ticketId",
            description="Ticket ID as stored in source table.",
        ),
    ] = None,
) -> JSONResponse:
    """Return lifecycle details for one closed/resolved ticket."""
    if not (ticket_key and ticket_key.strip()) and not (ticket_id and ticket_id.strip()):
        raise HTTPException(status_code=400, detail="ticketKey or ticketId is required.")

    client: DatabricksClient = get_client()
    try:
        data = client.get_ticket_lifecycle_details(ticket_key, ticket_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RequestError as exc:
        message = str(exc)
        if "401" in message or "UNAUTHORIZED" in message.upper():
            raise HTTPException(
                status_code=503,
                detail="Databricks authentication failed (401). Refresh credentials and restart API.",
            ) from exc
        logger.error("ticket-lifecycle details upstream request failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream data source request failed.") from exc
    except Exception as exc:
        logger.error("ticket-lifecycle details query failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream data source error.") from exc
    return JSONResponse(content=data)


@app.get("/api/ticket-lifecycle/score-engine", tags=["ticket-lifecycle"])
async def ticket_lifecycle_score_engine(
    months: Annotated[
        str | None,
        Query(
            description="Comma-separated YYYY-MM values for month filter.",
            example="2026-03,2026-02",
        ),
    ] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=5000, description="Maximum groups to return."),
    ] = 2000,
) -> JSONResponse:
    """Return resolution score engine with per-group scoring and forecast."""
    parsed_months = _parse_months(months)
    client: DatabricksClient = get_client()
    try:
        data = client.get_ticket_lifecycle_score_engine(parsed_months, limit)
    except Exception as exc:
        logger.error("ticket-lifecycle score-engine query failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream data source error.") from exc
    return JSONResponse(content=data)


@app.post("/api/cache/invalidate", tags=["ops"])
async def invalidate_cache(
    x_cache_token: Annotated[
        str | None,
        Header(description="Shared secret to authorise cache invalidation."),
    ] = None,
) -> dict:
    """
    Clear the in-process response cache.

    Requires the ``X-Cache-Token`` header to match the
    ``CACHE_INVALIDATION_TOKEN`` environment variable.
    """
    expected = os.getenv("CACHE_INVALIDATION_TOKEN", "")
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="Cache invalidation is not configured on this instance.",
        )
    if x_cache_token != expected:
        raise HTTPException(status_code=403, detail="Invalid cache token.")

    get_client().invalidate_cache()
    return {"status": "cache cleared"}


# Prevent browsers/proxies from caching HTML pages.
@app.middleware("http")
async def no_cache_html(request, call_next):
    response = await call_next(request)
    ct = response.headers.get("content-type", "")
    if "text/html" in ct:
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


# Serve the presentation assets from the same domain in production.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_PRESENTATION_ROOT = _REPO_ROOT / "presentation"
if _PRESENTATION_ROOT.exists():
    app.mount("/", StaticFiles(directory=str(_PRESENTATION_ROOT), html=True), name="presentation")

