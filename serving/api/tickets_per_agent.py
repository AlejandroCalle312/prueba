# pyright: reportMissingImports=false

from __future__ import annotations

import re
import sys
import logging
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

# Add serving/srf-axsa-api to import path for shared databricks client.
_SRF_AXSA_API_DIR = Path(__file__).resolve().parents[1] / "srf-axsa-api"
if str(_SRF_AXSA_API_DIR) not in sys.path:
    sys.path.append(str(_SRF_AXSA_API_DIR))

import databricks_client

router = APIRouter(tags=["tickets-per-agent"])
logger = logging.getLogger(__name__)

_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


@router.get("/api/tickets-per-agent")
async def tickets_per_agent(
    month: Annotated[
        str | None,
        Query(
            description="Optional month filter in YYYY-MM format.",
            example="2026-02",
        ),
    ] = None,
    assignee: Annotated[
        str | None,
        Query(
            description="Optional assignee exact match filter.",
            example="John Doe",
        ),
    ] = None,
    status: Annotated[
        str | None,
        Query(
            description="Optional status exact match filter.",
            example="Open",
        ),
    ] = None,
    assignment_group: Annotated[
        str | None,
        Query(
            alias="assignmentGroup",
            description="Optional assignment group exact match filter.",
            example="Axpo Onsite Support CH - Baden",
        ),
    ] = None,
) -> JSONResponse:
    """Return monthly segmented ticket analysis for onsite Baden support."""
    if month and not _MONTH_RE.match(month):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid month format '{month}'. Expected YYYY-MM.",
        )

    client = databricks_client.get_client()
    try:
        data = client.get_tickets_per_agent(
            month=month,
            assignee=assignee,
            status=status,
            assignment_group=assignment_group,
        )
    except Exception as exc:
        logger.error("tickets-per-agent query failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream data source error.") from exc
    return JSONResponse(content=data)

