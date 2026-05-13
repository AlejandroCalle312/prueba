"""
Databricks SQL Warehouse client.

Authentication priority:
    1. Entra client credentials  — DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET
    2. DATABRICKS_TOKEN env var  — PAT token injected from Key Vault at runtime
    3. Azure Managed Identity    — via azure-identity when running on Azure compute

Caching:
  Responses are cached in-process with a configurable TTL (default 3 600 s).
  The cache is keyed on the sorted month list so that equivalent queries share
  the same entry regardless of order.
"""
from __future__ import annotations

import logging
import os
import re
import time
import json
from collections import Counter
from html import unescape
from datetime import datetime, timezone, timedelta, time as dt_time
from functools import lru_cache
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_CACHE_TTL_SECONDS: int = int(os.getenv("CACHE_TTL_SECONDS", "3600"))
_TICKETS_TABLE: str = os.getenv(
    "DATABRICKS_TICKETS_TABLE", "axsa_dev_bronze.jira_tickets_mtb.tickets"
)
_ACTIVITY_TABLE: str = os.getenv(
    "DATABRICKS_ACTIVITY_TABLE",
    _TICKETS_TABLE.rsplit(".", 1)[0] + ".activity" if "." in _TICKETS_TABLE else "axsa_dev_bronze.jira_tickets_mtb.activity",
)
_TICKET_ID_COLUMN: str = os.getenv("DATABRICKS_TICKET_ID_COLUMN", "id")
_TICKET_KEY_COLUMN: str = os.getenv("DATABRICKS_TICKET_KEY_COLUMN", "key")
_ASSIGNED_GROUP_COLUMN: str = os.getenv("DATABRICKS_ASSIGNED_GROUP_COLUMN", "").strip()
_ISSUE_TYPE_COLUMN: str = os.getenv("DATABRICKS_ISSUE_TYPE_COLUMN", "").strip()
_PROJECT_COLUMN: str = os.getenv("DATABRICKS_PROJECT_COLUMN", "").strip()
_ASSIGNEE_COLUMN: str = os.getenv("DATABRICKS_ASSIGNEE_COLUMN", "").strip()
_STATUS_COLUMN: str = os.getenv("DATABRICKS_STATUS_COLUMN", "").strip()
_PRIORITY_COLUMN: str = os.getenv("DATABRICKS_PRIORITY_COLUMN", "").strip()
_SLA_BREACH_COLUMN: str = os.getenv("DATABRICKS_SLA_BREACH_COLUMN", "").strip()
_TIMELINE_EVENT_COLUMN: str = os.getenv("DATABRICKS_TIMELINE_EVENT_COLUMN", "").strip()
_CLOSED_TIME_COLUMN: str = os.getenv("DATABRICKS_CLOSED_TIME_COLUMN", "").strip()
_RESOLVED_TIME_COLUMN: str = os.getenv("DATABRICKS_RESOLVED_TIME_COLUMN", "").strip()
_JIRA_BASE_URL: str = os.getenv("JIRA_BASE_URL", "https://axpo.atlassian.net/browse")
_ONSITE_PROJECT_NAME: str = os.getenv("ONSITE_PROJECT_NAME", "IT Hub")
_ONSITE_PROJECT_KEY_PREFIX: str = os.getenv("ONSITE_PROJECT_KEY_PREFIX", "ITHUB-")
_ONSITE_ASSIGNMENT_GROUP: str = os.getenv(
    "ONSITE_ASSIGNMENT_GROUP", "Axpo Onsite Support CH - Baden"
)
_ONSITE_ISSUE_TYPES: list[str] = [
    item.strip().lower()
    for item in os.getenv(
        "ONSITE_ISSUE_TYPES",
        "System Incident,System Service request,System Service request with approvals",
    ).split(",")
    if item.strip()
]
_ENTRY_SMC_GROUP: str = os.getenv(
    "ENTRY_SMC_GROUP", "AXPO Service Management Center"
)
_REPORTING_TIMEZONE: str = os.getenv("REPORTING_TIMEZONE", "Europe/Madrid")
_REPORTING_TIMEZONE_LABEL: str = os.getenv(
    "REPORTING_TIMEZONE_LABEL", "Madrid/Switzerland"
)
_VALID_METRIC_MODES: set[str] = {
    "open",
    "entry_smc_first",
    "closed",
    "assignment_transitions",
}
_LIFECYCLE_CLOSED_STATUSES: set[str] = {"closed", "resolved"}
_NEGLIGIBLE_UNASSIGNED_SECONDS: int = int(os.getenv("LIFECYCLE_NEGLIGIBLE_UNASSIGNED_SECONDS", "60"))
_SLA_TARGET_HOURS: dict[int, int] = {1: 6, 2: 12, 3: 24, 4: 60, 5: 120}
_SLA_WORKDAY_START_HOUR: int = int(os.getenv("SLA_WORKDAY_START_HOUR", "7"))
_SLA_WORKDAY_END_HOUR: int = int(os.getenv("SLA_WORKDAY_END_HOUR", "19"))
_SCORE_ENGINE_WORKDAY_START_HOUR: int = int(os.getenv("SCORE_ENGINE_WORKDAY_START_HOUR", "7"))
_SCORE_ENGINE_WORKDAY_END_HOUR: int = int(os.getenv("SCORE_ENGINE_WORKDAY_END_HOUR", "18"))

# Databricks resource ID used when requesting an AAD token via Managed Identity
_DATABRICKS_ARM_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"


class _CacheEntry:
    __slots__ = ("value", "expires_at")

    def __init__(self, value: Any, ttl: int) -> None:
        self.value = value
        self.expires_at = time.monotonic() + ttl


class DatabricksClient:
    """Thread-safe Databricks SQL Warehouse client with TTL response cache."""

    def __init__(self) -> None:
        workspace_url = (
            os.environ.get("DATABRICKS_WORKSPACE_URL", "").strip()
            or os.environ.get("DATABRICKS_HOST", "").strip()
        ).rstrip("/")
        if workspace_url and not workspace_url.startswith("http://") and not workspace_url.startswith("https://"):
            workspace_url = f"https://{workspace_url}"

        http_path = os.environ.get("DATABRICKS_HTTP_PATH", "").strip()
        if not http_path:
            warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "").strip()
            if warehouse_id:
                http_path = f"/sql/1.0/warehouses/{warehouse_id}"

        if not workspace_url or not http_path:
            raise RuntimeError(
                "Databricks settings missing. Set DATABRICKS_WORKSPACE_URL (or DATABRICKS_HOST) and DATABRICKS_HTTP_PATH (or DATABRICKS_SQL_WAREHOUSE_ID)."
            )
        self._server_hostname = workspace_url.replace("https://", "").replace(
            "http://", ""
        )
        self._http_path = http_path
        self._cache: dict[str, _CacheEntry] = {}
        self._connection: Any = None
        self._resolved_assignment_group_column: str | None = None
        self._resolved_issue_type_column: str | None = None
        self._resolved_project_column: str | None = None
        self._resolved_assignee_column: str | None = None
        self._resolved_status_column: str | None = None
        self._resolved_priority_column: str | None = None
        self._resolved_sla_breach_column: str | None = None
        self._resolved_timeline_event_column: str | None = None
        self._resolved_closed_time_column: str | None = None
        self._resolved_resolved_time_column: str | None = None
        self._resolved_activity_ticket_id_column: str | None = None
        self._resolved_activity_date_column: str | None = None
        self._resolved_activity_content_column: str | None = None
        self._resolved_internal_ticket_id_column: str | None = None
        self._resolved_activity_author_column: str | None = None
        self._resolved_activity_visibility_column: str | None = None
        self._resolved_activity_updated_column: str | None = None
        self._resolved_description_column: str | None = None
        self._resolved_smc_assignments_column: str | None = None
        self._resolved_smc_reassignment_column: str | None = None

    @staticmethod
    def _as_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if not isinstance(value, str):
            return None
        candidate = value.strip()
        if not candidate:
            return None
        try:
            normalized = candidate.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _to_utc_iso(value: datetime | None) -> str | None:
        if value is None:
            return None
        utc_value = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return utc_value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    # ── Authentication ────────────────────────────────────────────────────────

    def _resolve_token(self) -> str:
        """Return a valid Databricks access token."""
        # Prefer service principal flow for production reliability.
        # PAT values can expire/revoke and cause recurrent 401 errors.
        client_id = os.getenv("DATABRICKS_CLIENT_ID", "").strip()
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "").strip()
        tenant_id = (os.getenv("AZURE_TENANT_ID", "").strip() or os.getenv("DATABRICKS_TENANT_ID", "").strip())
        if client_id and client_secret and tenant_id:
            token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            token_body = urlencode(
                {
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": f"{_DATABRICKS_ARM_RESOURCE}/.default",
                }
            ).encode("utf-8")
            req = Request(
                token_url,
                data=token_body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            try:
                with urlopen(req, timeout=20) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                access_token = (payload.get("access_token") or "").strip()
                if access_token:
                    logger.info("Using Entra client credentials authentication for Databricks.")
                    return access_token
            except Exception:
                logger.info("Entra client credentials flow failed; trying Azure identity fallbacks.")

        pat = os.getenv("DATABRICKS_TOKEN", "").strip()
        if pat:
            logger.info("Using DATABRICKS_TOKEN authentication.")
            return pat

        # Fallback 1 (local): Azure CLI identity
        try:
            from azure.identity import AzureCliCredential

            credential = AzureCliCredential()
            token = credential.get_token(f"{_DATABRICKS_ARM_RESOURCE}/.default")
            logger.info("Using Azure CLI authentication for Databricks.")
            return token.token
        except Exception:
            logger.info("Azure CLI credential not available; trying Managed Identity.")

        # Fallback 2 (Azure runtime): Managed Identity
        try:
            from azure.identity import ManagedIdentityCredential

            client_id = os.getenv("AZURE_CLIENT_ID")
            credential = (
                ManagedIdentityCredential(client_id=client_id)
                if client_id
                else ManagedIdentityCredential()
            )
            token = credential.get_token(f"{_DATABRICKS_ARM_RESOURCE}/.default")
            logger.info("Using Managed Identity authentication for Databricks.")
            return token.token
        except Exception as exc:
            raise RuntimeError(
                "Cannot resolve a Databricks token. "
                "Set DATABRICKS_TOKEN or authenticate via Azure CLI (az login)."
            ) from exc

    # ── Connection ────────────────────────────────────────────────────────────

    def _get_connection(self) -> Any:
        """Lazy-initialise and return the Databricks SQL connection."""
        if self._connection is None:
            from databricks import sql as dbsql  # type: ignore[import-untyped]

            self._connection = dbsql.connect(
                server_hostname=self._server_hostname,
                http_path=self._http_path,
                access_token=self._resolve_token(),
            )
            logger.info("Databricks SQL connection established to %s", self._server_hostname)
        return self._connection

    def _reset_connection(self) -> None:
        """Dispose current connection so next query can re-authenticate."""
        conn = self._connection
        self._connection = None
        if conn is None:
            return
        try:
            conn.close()
        except Exception:
            # Best effort close; a new connection will still be created on next use.
            pass

    # ── Query helpers ─────────────────────────────────────────────────────────

    def _execute(self, sql: str, params: list | None = None) -> list[dict]:
        """Execute *sql* with positional *params* and return rows as dicts."""
        from databricks.sql.exc import RequestError  # type: ignore[import-untyped]

        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(sql, params or [])
                cols = [desc[0] for desc in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
        except RequestError as exc:
            # Databricks can surface auth/session expiry as a generic RequestError message.
            # Always reconnect and retry once to recover from idle-session/token expiry.
            logger.warning(
                "Databricks RequestError received (%s); resetting connection and retrying query once.",
                exc,
            )
            self._reset_connection()
            try:
                conn = self._get_connection()
                with conn.cursor() as cur:
                    cur.execute(sql, params or [])
                    cols = [desc[0] for desc in cur.description]
                    return [dict(zip(cols, row)) for row in cur.fetchall()]
            except RequestError:
                # Bubble up the original failure shape to preserve API error handling.
                raise

    def _get_assignment_group_column(self) -> str:
        """Resolve assignment-group column name across schema variants."""
        if self._resolved_assignment_group_column:
            return self._resolved_assignment_group_column

        candidates: list[str] = []
        if _ASSIGNED_GROUP_COLUMN:
            candidates.append(_ASSIGNED_GROUP_COLUMN)
        candidates.extend(["assigned_group", "assignment_group", "smc_assignments"])

        # Keep order but remove duplicates/casing variants.
        seen: set[str] = set()
        unique_candidates: list[str] = []
        for candidate in candidates:
            c = candidate.strip().lower()
            if c and c not in seen:
                seen.add(c)
                unique_candidates.append(c)

        last_exc: Exception | None = None
        for c in unique_candidates:
            try:
                probe_sql = f"""
                    SELECT {c} AS assignment_group
                    FROM {_TICKETS_TABLE}
                    WHERE {c} IS NOT NULL
                    LIMIT 1
                """
                self._execute(probe_sql)
                self._resolved_assignment_group_column = c
                logger.info("Using assignment group column: %s", c)
                return c
            except Exception as exc:  # pragma: no cover - environment dependent
                last_exc = exc
                continue

        raise RuntimeError(
            "Could not resolve assignment group column in source table. "
            "Set DATABRICKS_ASSIGNED_GROUP_COLUMN to a valid column name."
        ) from last_exc

    def _resolve_first_existing_column(
        self,
        candidates: list[str],
        label: str,
        required: bool = True,
    ) -> str | None:
        seen: set[str] = set()
        unique_candidates: list[str] = []
        for candidate in candidates:
            c = candidate.strip().lower()
            if c and c not in seen:
                seen.add(c)
                unique_candidates.append(c)

        last_exc: Exception | None = None
        for c in unique_candidates:
            try:
                probe_sql = f"""
                    SELECT {c}
                    FROM {_TICKETS_TABLE}
                    WHERE {c} IS NOT NULL
                    LIMIT 1
                """
                self._execute(probe_sql)
                logger.info("Using %s column: %s", label, c)
                return c
            except Exception as exc:  # pragma: no cover - environment dependent
                last_exc = exc
                continue

        if required:
            raise RuntimeError(
                f"Could not resolve {label} column in source table."
            ) from last_exc
        return None

    def _get_project_column(self) -> str | None:
        if self._resolved_project_column is not None:
            return self._resolved_project_column
        self._resolved_project_column = self._resolve_first_existing_column(
            [_PROJECT_COLUMN, "project", "project_name", "project_key"],
            "project",
            required=False,
        )
        return self._resolved_project_column

    def _get_assignee_column(self) -> str:
        if self._resolved_assignee_column:
            return self._resolved_assignee_column
        self._resolved_assignee_column = self._resolve_first_existing_column(
            [_ASSIGNEE_COLUMN, "assignee", "assignee_name", "assigned_to", "reporter"],
            "assignee",
            required=True,
        )
        return self._resolved_assignee_column

    def _get_status_column(self) -> str | None:
        if self._resolved_status_column is not None:
            return self._resolved_status_column
        self._resolved_status_column = self._resolve_first_existing_column(
            [_STATUS_COLUMN, "status", "ticket_status", "state"],
            "status",
            required=False,
        )
        return self._resolved_status_column

    def _get_priority_column(self) -> str | None:
        if self._resolved_priority_column is not None:
            return self._resolved_priority_column
        self._resolved_priority_column = self._resolve_first_existing_column(
            [_PRIORITY_COLUMN, "priority", "ticket_priority", "severity"],
            "priority",
            required=False,
        )
        return self._resolved_priority_column

    def _get_sla_breach_column(self) -> str | None:
        if self._resolved_sla_breach_column is not None:
            return self._resolved_sla_breach_column
        self._resolved_sla_breach_column = self._resolve_first_existing_column(
            [_SLA_BREACH_COLUMN, "sla_breached", "is_sla_breached", "sla_breach"],
            "sla breach",
            required=False,
        )
        return self._resolved_sla_breach_column

    def _get_timeline_event_column(self) -> str:
        if self._resolved_timeline_event_column:
            return self._resolved_timeline_event_column

        resolved = self._resolve_first_existing_column(
            [
                _TIMELINE_EVENT_COLUMN,
                "updated_in",
                "updated_at",
                "last_updated",
                "modified_in",
                "modified_at",
                "ingested_at",
                "created_in",
            ],
            "timeline event timestamp",
            required=True,
        )
        self._resolved_timeline_event_column = resolved
        return resolved

    def _get_closed_time_column(self) -> str | None:
        if self._resolved_closed_time_column is not None:
            return self._resolved_closed_time_column
        self._resolved_closed_time_column = self._resolve_first_existing_column(
            [_CLOSED_TIME_COLUMN, "closed_in", "closed_at", "closed"],
            "closed time",
            required=False,
        )
        return self._resolved_closed_time_column

    def _get_resolved_time_column(self) -> str | None:
        if self._resolved_resolved_time_column is not None:
            return self._resolved_resolved_time_column
        self._resolved_resolved_time_column = self._resolve_first_existing_column(
            [_RESOLVED_TIME_COLUMN, "resolved_in", "resolved_at", "resolved"],
            "resolved time",
            required=False,
        )
        return self._resolved_resolved_time_column

    @staticmethod
    def _validate_metric_mode(metric_mode: str) -> str:
        mode = (metric_mode or "open").strip().lower()
        if mode not in _VALID_METRIC_MODES:
            raise ValueError(
                f"Unsupported metric mode '{metric_mode}'. "
                f"Allowed values: {', '.join(sorted(_VALID_METRIC_MODES))}."
            )
        return mode

    @staticmethod
    def _metric_unit(metric_mode: str) -> str:
        return "transitions" if metric_mode == "assignment_transitions" else "tickets"

    @staticmethod
    def _reporting_timezone() -> dict[str, str]:
        return {
            "iana": _REPORTING_TIMEZONE,
            "label": _REPORTING_TIMEZONE_LABEL,
        }

    def _cached(self, key: str, fetch_fn) -> Any:  # type: ignore[type-arg]
        """Return cached value or call *fetch_fn* and cache the result."""
        entry = self._cache.get(key)
        if entry and time.monotonic() < entry.expires_at:
            logger.debug("Cache hit: %s", key)
            return entry.value
        value = fetch_fn()
        self._cache[key] = _CacheEntry(value, _CACHE_TTL_SECONDS)
        logger.debug("Cache miss — stored: %s (TTL %ss)", key, _CACHE_TTL_SECONDS)
        return value

    def _get_issue_type_column(self) -> str:
        """Resolve issue-type column name across schema variants."""
        if self._resolved_issue_type_column:
            return self._resolved_issue_type_column

        candidates: list[str] = []
        if _ISSUE_TYPE_COLUMN:
            candidates.append(_ISSUE_TYPE_COLUMN)
        candidates.extend(["issue_type", "issuetype", "type", "issueType"])

        seen: set[str] = set()
        unique_candidates: list[str] = []
        for candidate in candidates:
            c = candidate.strip().lower()
            if c and c not in seen:
                seen.add(c)
                unique_candidates.append(c)

        last_exc: Exception | None = None
        for c in unique_candidates:
            try:
                probe_sql = f"""
                    SELECT {c}
                    FROM {_TICKETS_TABLE}
                    WHERE {c} IS NOT NULL
                    LIMIT 1
                """
                self._execute(probe_sql)
                self._resolved_issue_type_column = c
                logger.info("Using issue type column: %s", c)
                return c
            except Exception as exc:  # pragma: no cover - environment dependent
                last_exc = exc
                continue

        raise RuntimeError(
            "Could not resolve issue type column in source table. "
            "Set DATABRICKS_ISSUE_TYPE_COLUMN to a valid column name."
        ) from last_exc

    def _get_description_column(self) -> str:
        if self._resolved_description_column:
            return self._resolved_description_column
        self._resolved_description_column = self._resolve_first_existing_column(
            ["description", "translated_description", "title", "translated_title"],
            "description",
            required=True,
        )
        return self._resolved_description_column

    def _get_smc_assignments_column(self) -> str:
        if self._resolved_smc_assignments_column:
            return self._resolved_smc_assignments_column
        self._resolved_smc_assignments_column = self._resolve_first_existing_column(
            ["smc_assignments"],
            "smc assignments",
            required=True,
        )
        return self._resolved_smc_assignments_column

    def _get_smc_reassignment_column(self) -> str:
        if self._resolved_smc_reassignment_column:
            return self._resolved_smc_reassignment_column
        self._resolved_smc_reassignment_column = self._resolve_first_existing_column(
            ["smc_reassignment", "smc_reassignments", "smc_assignments"],
            "smc reassignment",
            required=True,
        )
        return self._resolved_smc_reassignment_column

    def _append_it_hub_filter(self, where_parts: list[str], params: list[Any]) -> None:
        project_column = self._get_project_column()
        project_key_like = f"{_ONSITE_PROJECT_KEY_PREFIX.upper()}%"
        if project_column:
            where_parts.append(
                "(LOWER(TRIM({project_col})) = LOWER(TRIM(?)) OR UPPER({key_col}) LIKE ?)".format(
                    project_col=project_column,
                    key_col=_TICKET_KEY_COLUMN,
                )
            )
            params.extend([_ONSITE_PROJECT_NAME, project_key_like])
            return
        where_parts.append(f"UPPER({_TICKET_KEY_COLUMN}) LIKE ?")
        params.append(project_key_like)

    def _append_incident_filter(self, where_parts: list[str], params: list[Any]) -> None:
        issue_type_col = self._get_issue_type_column()
        where_parts.append(f"LOWER({issue_type_col}) LIKE ?")
        params.append("%incident%")

    def _resolve_first_existing_column_in_table(
        self,
        table: str,
        candidates: list[str],
        label: str,
        required: bool = True,
    ) -> str | None:
        seen: set[str] = set()
        unique_candidates: list[str] = []
        for candidate in candidates:
            c = candidate.strip().lower()
            if c and c not in seen:
                seen.add(c)
                unique_candidates.append(c)

        last_exc: Exception | None = None
        for c in unique_candidates:
            try:
                probe_sql = f"""
                    SELECT {c}
                    FROM {table}
                    WHERE {c} IS NOT NULL
                    LIMIT 1
                """
                self._execute(probe_sql)
                logger.info("Using %s column on %s: %s", label, table, c)
                return c
            except Exception as exc:  # pragma: no cover - environment dependent
                last_exc = exc
                continue

        if required:
            raise RuntimeError(
                f"Could not resolve {label} column in source table '{table}'."
            ) from last_exc
        return None

    def _get_activity_ticket_id_column(self) -> str:
        if self._resolved_activity_ticket_id_column:
            return self._resolved_activity_ticket_id_column
        resolved = self._resolve_first_existing_column_in_table(
            _ACTIVITY_TABLE,
            ["ticket_id", "ticketid", "issue_id", "parent_ticket_id"],
            "activity ticket id",
            required=True,
        )
        self._resolved_activity_ticket_id_column = resolved
        return resolved

    def _get_internal_ticket_id_column(self) -> str | None:
        if self._resolved_internal_ticket_id_column:
            return self._resolved_internal_ticket_id_column
        resolved = self._resolve_first_existing_column_in_table(
            _TICKETS_TABLE,
            ["id", "ticket_id", "issue_id"],
            "internal ticket id",
            required=False,
        )
        self._resolved_internal_ticket_id_column = resolved
        return resolved

    def _get_activity_date_column(self) -> str:
        if self._resolved_activity_date_column:
            return self._resolved_activity_date_column
        resolved = self._resolve_first_existing_column_in_table(
            _ACTIVITY_TABLE,
            ["date", "activity_date", "created_in", "created_at", "updated_in", "updated_at"],
            "activity date",
            required=True,
        )
        self._resolved_activity_date_column = resolved
        return resolved

    def _get_activity_content_column(self) -> str:
        if self._resolved_activity_content_column:
            return self._resolved_activity_content_column
        resolved = self._resolve_first_existing_column_in_table(
            _ACTIVITY_TABLE,
            ["content", "body", "description", "message"],
            "activity content",
            required=True,
        )
        self._resolved_activity_content_column = resolved
        return resolved

    def _get_activity_author_column(self) -> str | None:
        if self._resolved_activity_author_column:
            return self._resolved_activity_author_column
        resolved = self._resolve_first_existing_column_in_table(
            _ACTIVITY_TABLE,
            ["author", "created_by", "user", "username"],
            "activity author",
            required=False,
        )
        self._resolved_activity_author_column = resolved
        return resolved

    def _get_activity_visibility_column(self) -> str | None:
        if self._resolved_activity_visibility_column:
            return self._resolved_activity_visibility_column
        resolved = self._resolve_first_existing_column_in_table(
            _ACTIVITY_TABLE,
            ["visibility", "access", "scope", "security_level"],
            "activity visibility",
            required=False,
        )
        self._resolved_activity_visibility_column = resolved
        return resolved

    def _get_activity_updated_column(self) -> str | None:
        if self._resolved_activity_updated_column:
            return self._resolved_activity_updated_column
        resolved = self._resolve_first_existing_column_in_table(
            _ACTIVITY_TABLE,
            ["updated", "updated_at", "last_updated", "modified_at"],
            "activity updated",
            required=False,
        )
        self._resolved_activity_updated_column = resolved
        return resolved

    @staticmethod
    def _duration_seconds(start: datetime | None, end: datetime | None) -> int:
        if not start or not end:
            return 0
        start_utc = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
        end_utc = end if end.tzinfo else end.replace(tzinfo=timezone.utc)
        delta = int((end_utc - start_utc).total_seconds())
        return max(delta, 0)

    @staticmethod
    def _median_seconds(values: list[int]) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        size = len(ordered)
        middle = size // 2
        if size % 2 == 1:
            return float(ordered[middle])
        return (ordered[middle - 1] + ordered[middle]) / 2.0

    @staticmethod
    def _normalise_group(value: Any) -> str:
        text = str(value or "").strip()
        return text if text else "Unknown"

    @staticmethod
    def _parse_priority_level(priority_value: Any) -> int | None:
        text = str(priority_value or "").strip()
        if not text:
            return None
        match = re.search(r"\bP\s*([1-5])\b", text, flags=re.IGNORECASE)
        if not match:
            return None
        try:
            return int(match.group(1))
        except Exception:
            return None

    @staticmethod
    def _is_awaiting_customer_status(status_value: Any) -> bool:
        text = re.sub(r"\s+", " ", str(status_value or "").strip().lower())
        return "await" in text and ("customer" in text or "costumer" in text)

    @staticmethod
    def _business_seconds_weekdays(
        start: datetime,
        end: datetime,
        tz_name: str,
        start_hour: int | None = None,
        end_hour: int | None = None,
    ) -> int:
        if end <= start:
            return 0

        tz = ZoneInfo(tz_name)
        local_start = start.astimezone(tz)
        local_end = end.astimezone(tz)
        day_cursor = local_start.date()
        last_day = local_end.date()
        total_seconds = 0

        workday_start = _SLA_WORKDAY_START_HOUR if start_hour is None else int(start_hour)
        workday_end = _SLA_WORKDAY_END_HOUR if end_hour is None else int(end_hour)
        if workday_end <= workday_start:
            return 0

        while day_cursor <= last_day:
            if day_cursor.weekday() < 5:
                window_start = datetime.combine(day_cursor, dt_time(workday_start, 0), tzinfo=tz)
                window_end = datetime.combine(day_cursor, dt_time(workday_end, 0), tzinfo=tz)
                overlap_start = max(local_start, window_start)
                overlap_end = min(local_end, window_end)
                if overlap_end > overlap_start:
                    total_seconds += int((overlap_end - overlap_start).total_seconds())
            day_cursor += timedelta(days=1)

        return max(total_seconds, 0)

    def _calculate_sla_metrics(
        self,
        start_dt: datetime,
        end_dt: datetime,
        ticket_history: list[dict[str, Any]],
        final_priority: Any,
    ) -> dict[str, Any]:
        priority_level = self._parse_priority_level(final_priority)
        if priority_level is None:
            return {
                "priority": str(final_priority or "Unknown"),
                "targetSeconds": None,
                "effectiveSeconds": None,
                "balanceSeconds": None,
                "isBreached": None,
                "clock": "unknown",
            }

        target_seconds = _SLA_TARGET_HOURS[priority_level] * 3600
        always_on = priority_level in {1, 2}

        status_events: list[tuple[datetime, str]] = []
        for row in ticket_history:
            ts = self._as_datetime(row.get("event_time"))
            if ts is None:
                continue
            status_events.append((ts, str(row.get("status") or "")))

        status_events.sort(key=lambda item: item[0])
        current_status = str(ticket_history[0].get("status") or "") if ticket_history else ""
        for ts, status in status_events:
            if ts <= start_dt:
                current_status = status
            else:
                break

        effective_seconds = 0
        cursor = start_dt
        for ts, status in status_events:
            if ts <= cursor:
                current_status = status
                continue
            if ts > end_dt:
                break
            if not self._is_awaiting_customer_status(current_status):
                if always_on:
                    effective_seconds += self._duration_seconds(cursor, ts)
                else:
                    effective_seconds += self._business_seconds_weekdays(cursor, ts, _REPORTING_TIMEZONE)
            current_status = status
            cursor = ts

        if end_dt > cursor and not self._is_awaiting_customer_status(current_status):
            if always_on:
                effective_seconds += self._duration_seconds(cursor, end_dt)
            else:
                effective_seconds += self._business_seconds_weekdays(cursor, end_dt, _REPORTING_TIMEZONE)

        balance_seconds = target_seconds - effective_seconds
        return {
            "priority": f"P{priority_level}",
            "targetSeconds": target_seconds,
            "effectiveSeconds": effective_seconds,
            "balanceSeconds": balance_seconds,
            "isBreached": balance_seconds < 0,
            "clock": "24x7" if always_on else f"weekday_{_SLA_WORKDAY_START_HOUR:02d}_{_SLA_WORKDAY_END_HOUR:02d}",
        }

    @staticmethod
    def _parse_assignment_group_transitions_from_activity(content: str) -> tuple[str | None, str | None]:
        raw_text = (content or "").strip()
        if not raw_text:
            return (None, None)

        # Activity payloads may include HTML, entities and escaped JSON-like fragments.
        text = unescape(raw_text)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            return (None, None)

        def _clean_group(value: str | None) -> str | None:
            if value is None:
                return None
            cleaned = unescape(str(value)).strip()
            # Many activity rows append " - ..." metadata after values.
            cleaned = re.split(r"\s+-\s+", cleaned, maxsplit=1)[0]
            cleaned = re.sub(r"^['\"`\[\{\s]+", "", cleaned)
            cleaned = re.sub(r"['\"`\]\}\s]+$", "", cleaned)
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            if not cleaned:
                return None
            lowered = cleaned.lower()
            if lowered in {"null", "n/a", "na", "unknown", "unassigned", "empty", "none", "-"}:
                return "None"
            if len(cleaned) <= 2:
                return None
            return cleaned

        def _looks_like_assignment_group(value: str | None) -> bool:
            if not value:
                return False
            lowered = value.lower().strip()
            # Jira identity payloads (ari:cloud:identity::user/...) are user IDs, not assignment groups.
            if "ari:cloud:identity::user/" in lowered:
                return False
            if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", lowered):
                return False
            if re.fullmatch(
                r"(?:ari:cloud:identity::user/[0-9]+:[0-9a-f-]{36})(?:\s*,\s*ari:cloud:identity::user/[0-9]+:[0-9a-f-]{36})+",
                lowered,
            ):
                return False
            if lowered in {
                "assigned",
                "work in progress",
                "awaiting caller",
                "pending on credentials",
                "resolved",
                "closed",
                "open",
                "in progress",
            }:
                return False
            keywords = [
                "axpo",
                "service management",
                "digital workplace",
                "identity",
                "access management",
                "onsite",
                "support",
            ]
            return any(k in lowered for k in keywords)

        patterns = [
            re.compile(
                r"assignment\s*group[^\n]*?from\s+['\"]?(?P<from>[^'\"\n]+)['\"]?\s+to\s+['\"]?(?P<to>[^'\"\n]+)['\"]?(?:$|[\.;,]|\s{2,}|\|)",
                flags=re.IGNORECASE,
            ),
            re.compile(
                r"assigned\s*group[^\n]*?from\s+['\"]?(?P<from>[^'\"\n]+)['\"]?\s+to\s+['\"]?(?P<to>[^'\"\n]+)['\"]?(?:$|[\.;,]|\s{2,}|\|)",
                flags=re.IGNORECASE,
            ),
            re.compile(
                r"assignment\s*group\s*[:=]\s*['\"]?(?P<from>[^'\"\n]+)['\"]?\s*(?:->|=>|→|&rarr;)\s*['\"]?(?P<to>[^'\"\n]+)['\"]?",
                flags=re.IGNORECASE,
            ),
            re.compile(
                r"assignment\s*group[^\n]*?changed\s*from\s+['\"]?(?P<from>[^'\"\n]+)['\"]?\s+to\s+['\"]?(?P<to>[^'\"\n]+)['\"]?",
                flags=re.IGNORECASE,
            ),
            re.compile(
                r"(?:field|name)\s*[:=]\s*['\"]?assignment\s*group['\"]?[^\n]*?from(?:string)?\s*[:=]\s*['\"]?(?P<from>[^'\"\n,}]+)['\"]?[^\n]*?to(?:string)?\s*[:=]\s*['\"]?(?P<to>[^'\"\n,}]+)['\"]?",
                flags=re.IGNORECASE,
            ),
            re.compile(
                r"assignment\s*group[^\n]*?to\s+['\"]?(?P<to>[^'\"\n]+)['\"]?(?:$|[\.;,]|\s{2,}|\|)",
                flags=re.IGNORECASE,
            ),
            re.compile(
                r"(?P<from>[^\n]+?)\s*--?>\s*(?P<to>[^\n]+)",
                flags=re.IGNORECASE,
            ),
        ]

        for pattern in patterns:
            match = pattern.search(text)
            if not match:
                continue
            from_group = _clean_group(match.groupdict().get("from"))
            to_group = _clean_group(match.groupdict().get("to"))
            if not to_group:
                continue
            if not (
                _looks_like_assignment_group(from_group)
                or _looks_like_assignment_group(to_group)
                or (from_group == "None" and _looks_like_assignment_group(to_group))
            ):
                continue
            return (
                DatabricksClient._normalise_group(from_group) if from_group else None,
                DatabricksClient._normalise_group(to_group),
            )
        return (None, None)

    @staticmethod
    def _select_lifecycle_tickets_where_status(status_column: str | None) -> str:
        if not status_column:
            return "1 = 1"
        statuses = "', '".join(sorted(_LIFECYCLE_CLOSED_STATUSES))
        return f"LOWER(TRIM({status_column})) IN ('{statuses}')"

    @staticmethod
    def _sort_groups_with_smc_first(groups: list[str]) -> list[str]:
        keyword = "service management center"
        deduped = sorted({(g or "").strip() for g in groups if (g or "").strip()}, key=lambda g: g.lower())
        return sorted(
            deduped,
            key=lambda g: (0 if keyword in g.lower() else 1, g.lower()),
        )

    def get_ticket_lifecycle_assignment_groups(self) -> list[str]:
        cache_key = "__ticket_lifecycle_assignment_groups__"
        return self._cached(cache_key, lambda: self._sort_groups_with_smc_first(self.get_assignment_groups()))

    def get_ticket_lifecycle_tickets(
        self,
        months: list[str] | None,
        assignment_group: str,
        ticket_search: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        if not assignment_group or not assignment_group.strip():
            return {
                "rows": [],
                "meta": {
                    "scope": "closed_or_resolved",
                    "reportingTimezone": self._reporting_timezone(),
                },
            }

        cleaned_months = sorted(months or [])
        cache_key = "ticket_lifecycle_tickets::months={months}::group={group}::search={search}::limit={limit}".format(
            months=",".join(cleaned_months) if cleaned_months else "__none__",
            group=assignment_group.strip().lower(),
            search=(ticket_search or "").strip().lower() or "__none__",
            limit=max(1, min(int(limit or 500), 2000)),
        )
        return self._cached(
            cache_key,
            lambda: self._fetch_ticket_lifecycle_tickets(cleaned_months, assignment_group, ticket_search, limit),
        )

    def _fetch_ticket_lifecycle_tickets(
        self,
        months: list[str],
        assignment_group: str,
        ticket_search: str | None,
        limit: int,
    ) -> dict[str, Any]:
        assignment_group_column = self._get_assignment_group_column()
        timeline_column = self._get_timeline_event_column()
        status_column = self._get_status_column()
        closed_col = self._get_closed_time_column()
        resolved_col = self._get_resolved_time_column()
        local_created_expr = f"FROM_UTC_TIMESTAMP(created_in, '{_REPORTING_TIMEZONE}')"
        status_filter_sql = self._select_lifecycle_tickets_where_status(status_column)

        close_candidates = [c for c in [closed_col, resolved_col, timeline_column] if c]
        close_expr = "COALESCE(" + ", ".join(close_candidates) + ")"

        params: list[Any] = [assignment_group.strip()]
        where_parts = [
            "created_in IS NOT NULL",
            f"LOWER(TRIM({assignment_group_column})) = LOWER(TRIM(?))",
            f"{close_expr} IS NOT NULL",
            status_filter_sql,
        ]
        self._append_incident_filter(where_parts, params)

        if months:
            placeholders = ", ".join(["?" for _ in months])
            where_parts.append(f"DATE_FORMAT({local_created_expr}, 'yyyy-MM') IN ({placeholders})")
            params.extend(months)

        if ticket_search and ticket_search.strip():
            pattern = f"%{ticket_search.strip().lower()}%"
            where_parts.append(
                f"(LOWER({_TICKET_KEY_COLUMN}) LIKE ? OR LOWER(CAST({_TICKET_ID_COLUMN} AS STRING)) LIKE ?)"
            )
            params.extend([pattern, pattern])

        limit_value = max(1, min(int(limit or 500), 2000))
        where_sql = "\n                AND ".join(where_parts)

        sql = f"""
            WITH scoped AS (
                SELECT
                    {_TICKET_ID_COLUMN} AS ticket_id,
                    {_TICKET_KEY_COLUMN} AS ticket_key,
                    created_in,
                    {close_expr} AS closed_or_resolved_in,
                    COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS assignment_group,
                    {status_column if status_column else "'Unknown'"} AS status,
                    ROW_NUMBER() OVER (
                        PARTITION BY {_TICKET_KEY_COLUMN}
                        ORDER BY {timeline_column} DESC
                    ) AS rn
                FROM {_TICKETS_TABLE}
                WHERE
                    {where_sql}
            )
            SELECT
                ticket_id,
                ticket_key,
                created_in,
                closed_or_resolved_in,
                assignment_group,
                status,
                DATE_FORMAT(FROM_UTC_TIMESTAMP(created_in, '{_REPORTING_TIMEZONE}'), 'yyyy-MM') AS month
            FROM scoped
            WHERE rn = 1
            ORDER BY closed_or_resolved_in DESC
            LIMIT {limit_value}
        """

        rows = self._execute(sql, params)
        payload_rows: list[dict[str, Any]] = []
        for row in rows:
            created = self._as_datetime(row.get("created_in"))
            closed = self._as_datetime(row.get("closed_or_resolved_in"))
            payload_rows.append(
                {
                    "ticketId": str(row.get("ticket_id") or "") or None,
                    "ticketKey": str(row.get("ticket_key") or "") or None,
                    "month": str(row.get("month") or ""),
                    "assignmentGroup": self._normalise_group(row.get("assignment_group")),
                    "status": str(row.get("status") or "Unknown"),
                    "createdIn": self._to_utc_iso(created),
                    "closedOrResolvedIn": self._to_utc_iso(closed),
                }
            )

        return {
            "rows": payload_rows,
            "meta": {
                "scope": "closed_or_resolved",
                "reportingTimezone": self._reporting_timezone(),
            },
        }

    def get_ticket_lifecycle_details(
        self,
        ticket_key: str | None = None,
        ticket_id: str | None = None,
    ) -> dict[str, Any]:
        key = (ticket_key or "").strip()
        identifier = (ticket_id or "").strip()
        if not key and not identifier:
            raise ValueError("ticketKey or ticketId is required.")

        cache_key = f"ticket_lifecycle_details::key={key or '__none__'}::id={identifier or '__none__'}"
        return self._cached(cache_key, lambda: self._fetch_ticket_lifecycle_details(key, identifier))

    def _fetch_ticket_lifecycle_details(self, ticket_key: str, ticket_id: str) -> dict[str, Any]:
        assignment_group_column = self._get_assignment_group_column()
        timeline_column = self._get_timeline_event_column()
        status_column = self._get_status_column()
        priority_column = self._get_priority_column()
        internal_ticket_id_column = self._get_internal_ticket_id_column()
        closed_col = self._get_closed_time_column()
        resolved_col = self._get_resolved_time_column()

        close_candidates = [c for c in [closed_col, resolved_col, timeline_column] if c]
        close_expr = "COALESCE(" + ", ".join(close_candidates) + ")"
        status_filter_sql = self._select_lifecycle_tickets_where_status(status_column)

        ticket_filters: list[str] = ["1 = 1"]
        params: list[Any] = []
        if ticket_key:
            ticket_filters.append(f"{_TICKET_KEY_COLUMN} = ?")
            params.append(ticket_key)
        if ticket_id:
            ticket_filters.append(f"CAST({_TICKET_ID_COLUMN} AS STRING) = ?")
            params.append(ticket_id)

        where_sql = " AND ".join(ticket_filters)
        ticket_sql = f"""
            SELECT
                {_TICKET_ID_COLUMN} AS ticket_id,
                {_TICKET_KEY_COLUMN} AS ticket_key,
                {internal_ticket_id_column if internal_ticket_id_column else 'NULL'} AS internal_ticket_id,
                created_in,
                {close_expr} AS closed_or_resolved_in,
                COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS assignment_group,
                {status_column if status_column else "'Unknown'"} AS status,
                {priority_column if priority_column else "'Unknown'"} AS priority,
                {timeline_column} AS event_time
            FROM {_TICKETS_TABLE}
            WHERE {where_sql}
              AND created_in IS NOT NULL
              AND {close_expr} IS NOT NULL
              AND {status_filter_sql}
            ORDER BY {timeline_column} ASC
        """
        ticket_history = self._execute(ticket_sql, params)
        if not ticket_history:
            raise ValueError("Ticket not found in closed/resolved scope.")

        first_row = ticket_history[0]
        last_row = ticket_history[-1]
        baseline_ticket_id = str(last_row.get("ticket_id") or first_row.get("ticket_id") or "").strip()
        internal_ticket_id = str(last_row.get("internal_ticket_id") or first_row.get("internal_ticket_id") or "").strip()
        baseline_ticket_key = str(last_row.get("ticket_key") or first_row.get("ticket_key") or "").strip()
        created_dt = self._as_datetime(first_row.get("created_in"))
        closed_dt = self._as_datetime(last_row.get("closed_or_resolved_in"))

        if not created_dt or not closed_dt:
            raise ValueError("Ticket has incomplete timestamps for lifecycle calculation.")

        inferred_changes: list[dict[str, Any]] = []
        previous_group: str | None = None
        inferred_initial_group: str | None = None
        for row in ticket_history:
            event_time = self._as_datetime(row.get("event_time"))
            group = self._normalise_group(row.get("assignment_group"))
            if inferred_initial_group is None:
                inferred_initial_group = group
            if previous_group is not None and group != previous_group and event_time is not None:
                inferred_changes.append(
                    {
                        "timestamp": event_time,
                        "fromGroup": previous_group,
                        "toGroup": group,
                        "source": "inferred",
                    }
                )
            previous_group = group

        activity_changes: list[dict[str, Any]] = []
        activity_events: list[dict[str, Any]] = []
        activity_error: str | None = None
        first_activity_ts: datetime | None = None
        try:
            activity_ticket_col = self._get_activity_ticket_id_column()
            activity_date_col = self._get_activity_date_column()
            activity_content_col = self._get_activity_content_column()
            activity_author_col = self._get_activity_author_column()
            activity_visibility_col = self._get_activity_visibility_column()
            activity_updated_col = self._get_activity_updated_column()
            activity_id_candidates = []
            for candidate in [baseline_ticket_id, internal_ticket_id, ticket_id]:
                value = (candidate or "").strip()
                if value and value not in activity_id_candidates:
                    activity_id_candidates.append(value)
            if not activity_id_candidates:
                activity_id_candidates.append(baseline_ticket_id)

            activity_select_parts = [
                f"{activity_date_col} AS activity_time",
                f"{activity_content_col} AS content",
            ]
            if activity_author_col:
                activity_select_parts.append(f"{activity_author_col} AS activity_author")
            if activity_visibility_col:
                activity_select_parts.append(f"{activity_visibility_col} AS activity_visibility")
            if activity_updated_col:
                activity_select_parts.append(f"{activity_updated_col} AS activity_updated")
            id_filter_sql = " OR ".join([f"CAST({activity_ticket_col} AS STRING) = ?" for _ in activity_id_candidates])
            activity_sql = f"""
                SELECT
                    {", ".join(activity_select_parts)}
                FROM {_ACTIVITY_TABLE}
                WHERE ({id_filter_sql})
                  AND {activity_date_col} IS NOT NULL
                  AND {activity_content_col} IS NOT NULL
                ORDER BY {activity_date_col} ASC
            """
            activity_rows = self._execute(activity_sql, activity_id_candidates)
            for row in activity_rows:
                ts = self._as_datetime(row.get("activity_time"))
                content_value = str(row.get("content") or "").strip()
                if not content_value:
                    continue
                if ts is not None and (first_activity_ts is None or ts < first_activity_ts):
                    first_activity_ts = ts
                activity_events.append(
                    {
                        "timestamp": self._to_utc_iso(ts) if ts else str(row.get("activity_time") or "").strip(),
                        "updated": self._to_utc_iso(self._as_datetime(row.get("activity_updated")))
                        if row.get("activity_updated") is not None
                        else None,
                        "author": str(row.get("activity_author") or "").strip() or None,
                        "visibility": str(row.get("activity_visibility") or "").strip() or None,
                        "content": content_value,
                    }
                )
                if ts is None:
                    continue
                from_group, to_group = self._parse_assignment_group_transitions_from_activity(
                    content_value
                )
                if not to_group:
                    continue
                activity_changes.append(
                    {
                        "timestamp": ts,
                        "fromGroup": from_group,
                        "toGroup": to_group,
                        "source": "activity",
                    }
                )
        except Exception as exc:  # pragma: no cover - depends on activity schema
            activity_error = str(exc)
            logger.warning("Activity transition parsing failed, using inferred fallback: %s", exc)

        transitions = activity_changes if activity_changes else inferred_changes
        transition_source = "activity" if activity_changes else "inferred"

        lifecycle_start_dt = created_dt
        lifecycle_start_source = "ticket_created"
        if first_activity_ts and first_activity_ts <= closed_dt:
            lifecycle_start_dt = first_activity_ts
            lifecycle_start_source = "first_activity"

        initial_group = None
        if transition_source == "activity" and transitions:
            first_from = self._normalise_group(transitions[0].get("fromGroup")) if transitions[0].get("fromGroup") else None
            if first_from:
                initial_group = first_from
            elif inferred_initial_group and inferred_initial_group != self._normalise_group(transitions[0].get("toGroup")):
                initial_group = inferred_initial_group
            else:
                # Jira changelog often stores only "to" values for the first assignment event.
                initial_group = "None"
        if not initial_group:
            initial_group = inferred_initial_group or self._normalise_group(last_row.get("assignment_group"))

        transitions = [
            t for t in transitions
            if isinstance(t.get("timestamp"), datetime)
            and lifecycle_start_dt <= t["timestamp"] <= closed_dt
        ]
        transitions.sort(key=lambda t: t["timestamp"])

        segments: list[dict[str, Any]] = []
        cursor = lifecycle_start_dt
        current_group = self._normalise_group(initial_group)

        for event in transitions:
            ts = event["timestamp"]
            if ts <= cursor:
                current_group = self._normalise_group(event.get("toGroup") or current_group)
                continue
            duration_seconds = self._duration_seconds(cursor, ts)
            segments.append(
                {
                    "assignmentGroup": current_group,
                    "start": self._to_utc_iso(cursor),
                    "end": self._to_utc_iso(ts),
                    "durationSeconds": duration_seconds,
                }
            )
            current_group = self._normalise_group(event.get("toGroup") or current_group)
            cursor = ts

        if closed_dt > cursor:
            segments.append(
                {
                    "assignmentGroup": current_group,
                    "start": self._to_utc_iso(cursor),
                    "end": self._to_utc_iso(closed_dt),
                    "durationSeconds": self._duration_seconds(cursor, closed_dt),
                }
            )

        closure_status = str(last_row.get("status") or "Closed")
        closure_transition = {
            "timestamp": closed_dt,
            "fromGroup": current_group,
            "toGroup": closure_status,
            "source": "closure",
        }
        if not transitions or transitions[-1].get("timestamp") != closed_dt:
            transitions.append(closure_transition)

        totals_by_group: dict[str, int] = {}
        for idx, segment in enumerate(segments):
            group = self._normalise_group(segment.get("assignmentGroup"))
            seconds = int(segment.get("durationSeconds") or 0)
            # Ignore tiny initial unassigned gap inferred from Jira "empty --> ..." events.
            if (
                idx == 0
                and group == "None"
                and seconds <= _NEGLIGIBLE_UNASSIGNED_SECONDS
            ):
                continue
            totals_by_group[group] = totals_by_group.get(group, 0) + seconds

        group_durations = [
            {"assignmentGroup": group, "durationSeconds": seconds}
            for group, seconds in sorted(totals_by_group.items(), key=lambda kv: kv[1], reverse=True)
        ]

        response_transitions = [
            {
                "timestamp": self._to_utc_iso(t["timestamp"]),
                "fromGroup": self._normalise_group(t.get("fromGroup")),
                "toGroup": self._normalise_group(t.get("toGroup")),
                "source": t.get("source") or transition_source,
            }
            for t in transitions
        ]

        sla = self._calculate_sla_metrics(
            lifecycle_start_dt,
            closed_dt,
            ticket_history,
            last_row.get("priority"),
        )

        return {
            "ticketId": baseline_ticket_id or None,
            "ticketKey": baseline_ticket_key or None,
            "status": str(last_row.get("status") or "Unknown"),
            "priority": str(last_row.get("priority") or "Unknown"),
            "createdIn": self._to_utc_iso(created_dt),
            "lifecycleStartIn": self._to_utc_iso(lifecycle_start_dt),
            "closedOrResolvedIn": self._to_utc_iso(closed_dt),
            "totalDurationSeconds": self._duration_seconds(lifecycle_start_dt, closed_dt),
            "groupDurations": group_durations,
            "segments": segments,
            "transitions": response_transitions,
            "activityEvents": activity_events,
            "sla": sla,
            "meta": {
                "transitionSource": transition_source,
                "activityFallbackUsed": transition_source != "activity",
                "activityError": activity_error,
                "activityEventCount": len(activity_events),
                "lifecycleStartSource": lifecycle_start_source,
                "reportingTimezone": self._reporting_timezone(),
                "scope": "closed_or_resolved",
            },
        }

    # ── Score Engine ──────────────────────────────────────────────────────────

    def get_ticket_lifecycle_score_engine(
        self,
        months: list[str] | None,
        limit: int = 2000,
    ) -> dict[str, Any]:
        """Aggregate resolution statistics per assignment group for scoring/forecast."""
        cleaned_months = sorted(months or [])
        cache_key = "score_engine::months={months}::limit={limit}".format(
            months=",".join(cleaned_months) if cleaned_months else "__all__",
            limit=max(1, min(int(limit or 2000), 5000)),
        )
        return self._cached(cache_key, lambda: self._fetch_score_engine(cleaned_months, limit))

    def _fetch_score_engine(self, months: list[str], limit: int) -> dict[str, Any]:
        assignment_group_column = self._get_assignment_group_column()
        timeline_column = self._get_timeline_event_column()
        priority_column = self._get_priority_column()
        issue_type_column = self._get_issue_type_column()
        local_created_expr = f"FROM_UTC_TIMESTAMP(created_in, '{_REPORTING_TIMEZONE}')"
        score_type_filter = f"LOWER(TRIM({issue_type_column})) LIKE '%incident%'"
        smc_filter = "AND smc_assignments > 0"

        limit_value = max(1, min(int(limit or 2000), 5000))

        # Month filter aligned with Jira dashboard X Axis: Created.
        month_filter = ""
        if months:
            month_list = ", ".join([f"'{m}'" for m in months])
            month_filter = f"AND DATE_FORMAT({local_created_expr}, 'yyyy-MM') IN ({month_list})"

        sql = f"""
            WITH ticket_pool AS (
                SELECT
                    CAST({_TICKET_ID_COLUMN} AS STRING) AS ticket_id,
                    COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS assignment_group
                FROM {_TICKETS_TABLE}
                WHERE created_in IS NOT NULL
                  AND {score_type_filter}
                  {smc_filter}
                  {month_filter}
            ),
            group_assignments AS (
                SELECT a.ticket_id,
                       TRIM(ELEMENT_AT(SPLIT(a.content, ' --> '), 2)) AS to_group
                FROM {_ACTIVITY_TABLE} a
                INNER JOIN ticket_pool tp ON a.ticket_id = tp.ticket_id
                WHERE a.updated = 'Assignment group'
                  AND a.content LIKE '%-->%'
                UNION
                SELECT ticket_id, assignment_group AS to_group
                FROM ticket_pool
            ),
            received_per_group AS (
                SELECT to_group AS assignment_group,
                       COUNT(DISTINCT ticket_id) AS tickets_received
                FROM group_assignments
                GROUP BY to_group
            ),
            resolved_tickets AS (
                SELECT
                    {_TICKET_KEY_COLUMN} AS ticket_key,
                    {_TICKET_ID_COLUMN} AS ticket_id,
                    created_in,
                    {timeline_column} AS updated_in,
                    COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS assignment_group,
                    {priority_column if priority_column else "'Unknown'"} AS priority,
                    UNIX_TIMESTAMP({timeline_column}) - UNIX_TIMESTAMP(created_in) AS duration_seconds
                FROM {_TICKETS_TABLE}
                WHERE created_in IS NOT NULL
                  AND {score_type_filter}
                  {smc_filter}
                  {month_filter}
            )
            SELECT
                r.assignment_group,
                COUNT(*) AS tickets_resolved,
                COALESCE(rpg.tickets_received, COUNT(*)) AS tickets_received,
                AVG(r.duration_seconds) AS avg_resolution_seconds,
                MIN(r.duration_seconds) AS min_resolution_seconds,
                MAX(r.duration_seconds) AS max_resolution_seconds,
                PERCENTILE_APPROX(r.duration_seconds, 0.5) AS median_resolution_seconds,
                SUM(r.duration_seconds) AS total_time_held_seconds,
                COUNT(CASE WHEN UPPER(r.priority) LIKE '%P1%' THEN 1 END) AS p1_count,
                COUNT(CASE WHEN UPPER(r.priority) LIKE '%P2%' THEN 1 END) AS p2_count,
                COUNT(CASE WHEN UPPER(r.priority) LIKE '%P3%' THEN 1 END) AS p3_count,
                COUNT(CASE WHEN UPPER(r.priority) LIKE '%P4%' THEN 1 END) AS p4_count,
                COUNT(CASE WHEN UPPER(r.priority) LIKE '%P5%' THEN 1 END) AS p5_count
            FROM resolved_tickets r
            LEFT JOIN received_per_group rpg ON r.assignment_group = rpg.assignment_group
            GROUP BY r.assignment_group, rpg.tickets_received
            ORDER BY tickets_resolved DESC
            LIMIT {limit_value}
        """

        rows = self._execute(sql)

        business_duration_sql = f"""
            SELECT
                COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS assignment_group,
                created_in,
                {timeline_column} AS updated_in
            FROM {_TICKETS_TABLE}
            WHERE created_in IS NOT NULL
              AND {score_type_filter}
              {smc_filter}
              {month_filter}
        """
        business_rows = self._execute(business_duration_sql)
        business_seconds_by_group: dict[str, list[int]] = {}
        for detail in business_rows:
            created_dt = self._as_datetime(detail.get("created_in"))
            updated_dt = self._as_datetime(detail.get("updated_in"))
            if not created_dt or not updated_dt:
                continue
            duration_seconds = self._business_seconds_weekdays(
                created_dt,
                updated_dt,
                _REPORTING_TIMEZONE,
                start_hour=_SCORE_ENGINE_WORKDAY_START_HOUR,
                end_hour=_SCORE_ENGINE_WORKDAY_END_HOUR,
            )
            group_name = self._normalise_group(detail.get("assignment_group"))
            business_seconds_by_group.setdefault(group_name, []).append(duration_seconds)

        grand_total_tickets = sum(int(r.get("tickets_resolved") or 0) for r in rows)
        grand_total_tickets = max(grand_total_tickets, 1)

        scored_groups: list[dict[str, Any]] = []
        for row in rows:
            resolved = int(row.get("tickets_resolved") or 0)
            received = int(row.get("tickets_received") or resolved)
            group_name = self._normalise_group(row.get("assignment_group"))
            group_business_seconds = business_seconds_by_group.get(group_name, [])

            if group_business_seconds:
                avg_secs = float(sum(group_business_seconds) / len(group_business_seconds))
                median_secs = self._median_seconds(group_business_seconds)
                min_secs = float(min(group_business_seconds))
                max_secs = float(max(group_business_seconds))
                total_held = float(sum(group_business_seconds))
            else:
                avg_secs = float(row.get("avg_resolution_seconds") or 0)
                median_secs = float(row.get("median_resolution_seconds") or 0)
                min_secs = float(row.get("min_resolution_seconds") or 0)
                max_secs = float(row.get("max_resolution_seconds") or 0)
                total_held = float(row.get("total_time_held_seconds") or 0)

            resolution_share_pct = round((resolved / grand_total_tickets) * 100, 1)
            resolution_rate_pct = round((resolved / max(received, 1)) * 100, 1)

            # Composite score: volume-weighted (higher = more dominant resolver)
            # 60% resolution share + 40% inverse of avg resolution time (faster = better)
            max_avg = max(float(r.get("avg_resolution_seconds") or 1) for r in rows)
            speed_score = round((1 - (avg_secs / max(max_avg, 1))) * 100, 1) if max_avg > 0 else 50.0
            composite_score = round(resolution_share_pct * 0.6 + speed_score * 0.4, 1)

            scored_groups.append({
                "assignmentGroup": group_name,
                "ticketsReceived": received,
                "ticketsResolved": resolved,
                "resolutionRatePct": resolution_rate_pct,
                "resolutionSharePct": resolution_share_pct,
                "avgResolutionSeconds": round(avg_secs),
                "medianResolutionSeconds": round(median_secs),
                "minResolutionSeconds": round(min_secs),
                "maxResolutionSeconds": round(max_secs),
                "totalTimeHeldSeconds": round(total_held),
                "priorityBreakdown": {
                    "P1": int(row.get("p1_count") or 0),
                    "P2": int(row.get("p2_count") or 0),
                    "P3": int(row.get("p3_count") or 0),
                    "P4": int(row.get("p4_count") or 0),
                    "P5": int(row.get("p5_count") or 0),
                },
                "speedScore": speed_score,
                "compositeScore": composite_score,
            })

        # Sort by tickets resolved descending
        scored_groups.sort(key=lambda g: g["ticketsResolved"], reverse=True)

        # Assign ranks
        for idx, group in enumerate(scored_groups):
            group["rank"] = idx + 1

        # Forecast: regression-based prediction using all historical months available.
        # This avoids simply mirroring the current selected-month share when a single month is queried.
        history_sql = f"""
            SELECT
                DATE_FORMAT({local_created_expr}, 'yyyy-MM') AS month,
                COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS assignment_group,
                COUNT(*) AS resolved
            FROM {_TICKETS_TABLE}
            WHERE created_in IS NOT NULL
              AND {score_type_filter}
              {smc_filter}
            GROUP BY DATE_FORMAT({local_created_expr}, 'yyyy-MM'),
                     COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown')
        """
        history_rows = self._execute(history_sql)

        month_totals_history: dict[str, int] = {}
        group_month_history: dict[str, dict[str, int]] = {}
        for tr in history_rows:
            month_key = str(tr.get("month") or "")
            group_key = str(tr.get("assignment_group") or "Unknown")
            resolved_count = int(tr.get("resolved") or 0)
            month_totals_history[month_key] = month_totals_history.get(month_key, 0) + resolved_count
            group_month_history.setdefault(group_key, {})[month_key] = resolved_count

        sorted_history_months = sorted(month_totals_history.keys())

        # Anchor forecast to the selected month context so the prediction changes by month.
        # If months are selected, use the latest selected month as the cutoff and forecast next month.
        # Otherwise, use the latest month available in history.
        selected_months = sorted([m for m in (months or []) if m in month_totals_history])
        forecast_base_month = selected_months[-1] if selected_months else (sorted_history_months[-1] if sorted_history_months else "")
        history_months_for_regression = [m for m in sorted_history_months if m <= forecast_base_month] if forecast_base_month else sorted_history_months

        # Use only the last 3 months for regression so predictions stay close
        # to recent reality. A 6-month window still produced ~10pt errors for
        # months before Dec 2025 due to structural shifts in share distribution.
        _FORECAST_WINDOW = 3
        regression_months = history_months_for_regression[-_FORECAST_WINDOW:]

        forecast = []
        for group in scored_groups:
            group_name = group["assignmentGroup"]
            group_history = group_month_history.get(group_name, {})
            shares_history = [
                round(group_history.get(m, 0) / max(month_totals_history.get(m, 1), 1) * 100, 1)
                for m in regression_months
            ]

            sample_size = len(shares_history)
            slope = 0.0
            if sample_size >= 2:
                x_values = list(range(sample_size))
                x_mean = sum(x_values) / sample_size
                y_mean = sum(shares_history) / sample_size
                numerator = sum((x_values[i] - x_mean) * (shares_history[i] - y_mean) for i in range(sample_size))
                denominator = sum((x_values[i] - x_mean) ** 2 for i in range(sample_size))
                slope = numerator / denominator if denominator != 0 else 0.0
                intercept = y_mean - slope * x_mean
                predicted_share = intercept + slope * sample_size
            elif sample_size == 1:
                predicted_share = shares_history[0]
            else:
                predicted_share = float(group["resolutionSharePct"])

            predicted_share = round(min(max(predicted_share, 0.0), 100.0), 1)

            trend_dir = "stable"
            if sample_size >= 2:
                if slope > 0.5:
                    trend_dir = "up"
                elif slope < -0.5:
                    trend_dir = "down"

            if sample_size >= 3:
                confidence = "high"
            elif sample_size >= 2:
                confidence = "medium"
            else:
                confidence = "low"

            recent_months = history_months_for_regression[-12:]
            forecast.append({
                "assignmentGroup": group_name,
                "forecastSharePct": predicted_share,
                "trend": trend_dir,
                "confidence": confidence,
                "historyMonthsConsidered": sample_size,
                "forecastBaseMonth": forecast_base_month,
                "monthlyShares": {
                    m: round(group_history.get(m, 0) / max(month_totals_history.get(m, 1), 1) * 100, 1)
                    for m in recent_months
                },
            })

        return {
            "groups": scored_groups,
            "forecast": forecast,
            "summary": {
                "totalTicketsAnalyzed": grand_total_tickets,
                "groupCount": len(scored_groups),
                "monthsAnalyzed": months if months else "all",
            },
            "meta": {
                "reportingTimezone": self._reporting_timezone(),
                "scope": "closed_or_resolved",
                "scoringWeights": {
                    "resolutionShare": 0.6,
                    "speedScore": 0.4,
                },
            },
        }

    # ── Public API ────────────────────────────────────────────────────────────

    def _build_event_cte(self, metric_mode: str) -> tuple[str, str]:
        mode = self._validate_metric_mode(metric_mode)
        assignment_group_column = self._get_assignment_group_column()
        issue_type_column = self._get_issue_type_column()
        event_time_column = self._get_timeline_event_column()
        status_column = self._get_status_column()
        closed_time_column = self._get_closed_time_column()
        resolved_time_column = self._get_resolved_time_column()

        incident_filter = f"LOWER({issue_type_column}) LIKE '%incident%'"

        if mode == "open":
            return (
                f"""
                WITH ticket_events AS (
                    SELECT
                        {_TICKET_KEY_COLUMN} AS ticket_key,
                        created_in AS event_time,
                        COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS effective_group,
                        'OPEN' AS event_type,
                        'authoritative' AS data_quality
                    FROM {_TICKETS_TABLE}
                    WHERE created_in IS NOT NULL
                      AND {_TICKET_KEY_COLUMN} IS NOT NULL
                      AND TRIM({_TICKET_KEY_COLUMN}) <> ''
                      AND {incident_filter}
                )
                """,
                "ticket_events",
            )

        if mode == "entry_smc_first":
            return (
                f"""
                WITH ranked_events AS (
                    SELECT
                        {_TICKET_KEY_COLUMN} AS ticket_key,
                        {event_time_column} AS event_time,
                        COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS effective_group,
                        ROW_NUMBER() OVER (
                            PARTITION BY {_TICKET_KEY_COLUMN}
                            ORDER BY {event_time_column} ASC
                        ) AS rn
                    FROM {_TICKETS_TABLE}
                    WHERE {event_time_column} IS NOT NULL
                      AND {_TICKET_KEY_COLUMN} IS NOT NULL
                      AND TRIM({_TICKET_KEY_COLUMN}) <> ''
                      AND LOWER(TRIM({assignment_group_column})) = LOWER(TRIM(?))
                      AND {incident_filter}
                ),
                ticket_events AS (
                    SELECT
                        ticket_key,
                        event_time,
                        effective_group,
                        'ENTRY_SMC_FIRST' AS event_type,
                        'inferred' AS data_quality
                    FROM ranked_events
                    WHERE rn = 1
                )
                """,
                "ticket_events",
            )

        if mode == "closed":
            if not status_column:
                raise RuntimeError("Could not resolve status column for closed metric mode.")

            close_expr_parts = [
                col
                for col in [closed_time_column, resolved_time_column, event_time_column]
                if col
            ]
            close_expr = "COALESCE(" + ", ".join(close_expr_parts) + ")"

            return (
                f"""
                WITH ticket_events AS (
                    SELECT
                        {_TICKET_KEY_COLUMN} AS ticket_key,
                        {close_expr} AS event_time,
                        COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS effective_group,
                        'CLOSE' AS event_type,
                        'inferred' AS data_quality
                    FROM {_TICKETS_TABLE}
                    WHERE {_TICKET_KEY_COLUMN} IS NOT NULL
                      AND TRIM({_TICKET_KEY_COLUMN}) <> ''
                      AND {close_expr} IS NOT NULL
                      AND LOWER(TRIM({status_column})) = 'closed'
                      AND {incident_filter}
                )
                """,
                "ticket_events",
            )

        return (
            f"""
            WITH ordered_rows AS (
                SELECT
                    {_TICKET_KEY_COLUMN} AS ticket_key,
                    {event_time_column} AS event_time,
                    COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS effective_group,
                    LAG(COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown')) OVER (
                        PARTITION BY {_TICKET_KEY_COLUMN}
                        ORDER BY {event_time_column} ASC
                    ) AS previous_group
                FROM {_TICKETS_TABLE}
                WHERE {_TICKET_KEY_COLUMN} IS NOT NULL
                  AND TRIM({_TICKET_KEY_COLUMN}) <> ''
                  AND {event_time_column} IS NOT NULL
                  AND {assignment_group_column} IS NOT NULL
                  AND TRIM({assignment_group_column}) <> ''
                  AND {incident_filter}
            ),
            ticket_events AS (
                SELECT
                    ticket_key,
                    event_time,
                    effective_group,
                    'ASSIGNMENT_GROUP_CHANGE' AS event_type,
                    'inferred' AS data_quality
                FROM ordered_rows
                WHERE previous_group IS NOT NULL
                  AND previous_group <> effective_group
            )
            """,
            "ticket_events",
        )

    def get_tickets_by_hour(
        self,
        months: list[str] | None,
        assignment_group: str | None = None,
        metric_mode: str = "open",
    ) -> dict:
        mode = self._validate_metric_mode(metric_mode)
        months_key = "__all__" if not months else ",".join(sorted(months))
        group_key = (assignment_group or "__all_groups__").strip().lower()
        cache_key = f"tickets_by_hour::{mode}::{months_key}::group={group_key}"
        return self._cached(
            cache_key,
            lambda: self._fetch_by_hour(months, assignment_group, mode),
        )

    def _fetch_by_hour(
        self,
        months: list[str] | None,
        assignment_group: str | None,
        metric_mode: str,
    ) -> dict:
        cte_sql, event_table = self._build_event_cte(metric_mode)
        params: list[Any] = []
        local_event_expr = f"FROM_UTC_TIMESTAMP(event_time, '{_REPORTING_TIMEZONE}')"

        if metric_mode == "entry_smc_first":
            params.append(_ENTRY_SMC_GROUP)

        where_parts = ["event_time IS NOT NULL"]
        if months:
            placeholders = ", ".join(["?" for _ in months])
            where_parts.append(
                f"DATE_FORMAT({local_event_expr}, 'yyyy-MM') IN ({placeholders})"
            )
            params.extend(months)
        else:
            where_parts.append(
                f"{local_event_expr} >= ADD_MONTHS(CURRENT_DATE(), -3)"
            )

        if assignment_group and assignment_group.strip():
            where_parts.append("LOWER(TRIM(effective_group)) = LOWER(TRIM(?))")
            params.append(assignment_group.strip())

        measure_sql = (
            "COUNT(*)" if metric_mode == "assignment_transitions" else "COUNT(DISTINCT ticket_key)"
        )
        where_sql = "\n                    AND ".join(where_parts)
        sql = f"""
            {cte_sql}
            SELECT
                HOUR({local_event_expr}) AS hour,
                DATE_FORMAT({local_event_expr}, 'yyyy-MM') AS month,
                YEAR({local_event_expr}) AS year,
                {measure_sql} AS metric_count,
                MIN(data_quality) AS data_quality
            FROM {event_table}
            WHERE
                {where_sql}
            GROUP BY
                HOUR({local_event_expr}),
                DATE_FORMAT({local_event_expr}, 'yyyy-MM'),
                YEAR({local_event_expr})
            ORDER BY year DESC, month DESC, hour ASC
        """
        rows = self._execute(sql, params)
        payload_rows = [
            {
                "hour": int(r["hour"]),
                "count": int(r["metric_count"]),
                "month": str(r["month"]),
                "year": int(r["year"]),
            }
            for r in rows
        ]
        data_quality = "authoritative" if metric_mode == "open" else "inferred"
        return {
            "rows": payload_rows,
            "meta": {
                "metricMode": metric_mode,
                "unit": self._metric_unit(metric_mode),
                "dataQuality": data_quality,
                "scope": "event_time",
                "reportingTimezone": self._reporting_timezone(),
            },
        }

    def get_available_months(self) -> list[str]:
        """Return distinct YYYY-MM values available in the last 24 months."""
        return self._cached(
            "__available_months__", self._fetch_available_months
        )

    def _fetch_available_months(self) -> list[str]:
        local_created_expr = f"FROM_UTC_TIMESTAMP(created_in, '{_REPORTING_TIMEZONE}')"
        where_parts = [f"{local_created_expr} >= ADD_MONTHS(CURRENT_DATE(), -24)"]
        params: list[Any] = []
        self._append_incident_filter(where_parts, params)
        where_sql = "\n                AND ".join(where_parts)

        sql = f"""
            SELECT DISTINCT DATE_FORMAT({local_created_expr}, 'yyyy-MM') AS month
            FROM {_TICKETS_TABLE}
            WHERE
                {where_sql}
            ORDER BY month DESC
        """
        rows = self._execute(sql, params)
        return [r["month"] for r in rows]

    def get_assignment_groups(self) -> list[str]:
        """Return distinct non-empty assignment group values."""
        return self._cached("__assignment_groups__", self._fetch_assignment_groups)

    def get_tickets_per_agent(
        self,
        month: str | None = None,
        assignee: str | None = None,
        status: str | None = None,
        assignment_group: str | None = None,
    ) -> dict:
        cache_key = (
            "tickets_per_agent::month={month}::assignee={assignee}::status={status}::group={group}"
        ).format(
            month=(month or "__all__").strip().lower(),
            assignee=(assignee or "__all__").strip().lower(),
            status=(status or "__all__").strip().lower(),
            group=(assignment_group or "__all__").strip().lower(),
        )
        return self._cached(
            cache_key,
            lambda: self._fetch_tickets_per_agent(
                month,
                assignee,
                status,
                assignment_group,
            ),
        )

    def _fetch_tickets_per_agent(
        self,
        month: str | None = None,
        assignee: str | None = None,
        status: str | None = None,
        assignment_group: str | None = None,
    ) -> dict:
        assignment_group_column = self._get_assignment_group_column()
        assignee_column = self._get_assignee_column()
        status_column = self._get_status_column()
        priority_column = self._get_priority_column()
        sla_breach_column = self._get_sla_breach_column()
        local_created_expr = f"FROM_UTC_TIMESTAMP(created_in, '{_REPORTING_TIMEZONE}')"

        where_parts = [
            "created_in IS NOT NULL",
        ]
        params: list[Any] = []

        self._append_incident_filter(where_parts, params)

        if assignment_group and assignment_group.strip():
            where_parts.append(f"LOWER(TRIM({assignment_group_column})) = LOWER(TRIM(?))")
            params.append(assignment_group.strip())
        if month:
            where_parts.append(f"DATE_FORMAT({local_created_expr}, 'yyyy-MM') = ?")
            params.append(month.strip())

        if assignee and assignee.strip():
            where_parts.append(
                f"COALESCE(NULLIF(TRIM({assignee_column}), ''), 'Unassigned') = ?"
            )
            params.append(assignee.strip())

        if status and status.strip() and status_column:
            where_parts.append(f"{status_column} = ?")
            params.append(status.strip())

        where_sql = "\n                AND ".join(where_parts)

        if sla_breach_column:
            sla_select = (
                "SUM(CASE WHEN LOWER(CAST({col} AS STRING)) IN "
                "('1', 'true', 'yes', 'y', 'breached', 'breach') THEN 1 ELSE 0 END) "
                "AS sla_breach_count"
            ).format(col=sla_breach_column)
        else:
            sla_select = "CAST(0 AS BIGINT) AS sla_breach_count"

        status_select = f"{status_column} AS status" if status_column else "'Unknown' AS status"
        status_group = status_column if status_column else "'Unknown'"
        priority_select = (
            f"{priority_column} AS priority" if priority_column else "'Unknown' AS priority"
        )
        priority_group = priority_column if priority_column else "'Unknown'"

        sql = f"""
            SELECT
                DATE_FORMAT({local_created_expr}, 'yyyy-MM') AS month,
                COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS assignment_group,
                COALESCE(NULLIF(TRIM({assignee_column}), ''), 'Unassigned') AS assignee,
                {status_select},
                {priority_select},
                COUNT(DISTINCT {_TICKET_KEY_COLUMN}) AS ticket_count,
                {sla_select}
            FROM {_TICKETS_TABLE}
            WHERE
                {where_sql}
            GROUP BY
                DATE_FORMAT({local_created_expr}, 'yyyy-MM'),
                COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown'),
                COALESCE(NULLIF(TRIM({assignee_column}), ''), 'Unassigned'),
                {status_group},
                {priority_group}
            ORDER BY month DESC, assignment_group ASC, assignee ASC, status ASC, priority ASC
        """

        rows = self._execute(sql, params)
        payload_rows = [
            {
                "month": str(row["month"]),
                "assignment_group": str(row["assignment_group"]),
                "assignee": str(row["assignee"]),
                "status": str(row["status"]),
                "priority": str(row["priority"]),
                "ticket_count": int(row["ticket_count"]),
                "sla_breach_count": int(row["sla_breach_count"]),
            }
            for row in rows
        ]

        total_tickets = sum(item["ticket_count"] for item in payload_rows)
        total_sla_breach = sum(item["sla_breach_count"] for item in payload_rows)

        return {
            "rows": payload_rows,
            "summary": {
                "total_tickets": total_tickets,
                "total_sla_breach": total_sla_breach,
                "sla_metric_available": sla_breach_column is not None,
                "scope": "incidents",
                "unit": "tickets",
                "aggregation_grain": "unique_ticket_key",
                "reportingTimezone": self._reporting_timezone(),
            },
        }

    def get_ticket_by_hour(
        self,
        hour: int,
        month: str | None = None,
        assignment_group: str | None = None,
        metric_mode: str = "open",
    ) -> dict:
        """Return all matching tickets for a selected hour and filters."""
        month_key = month or "__latest3__"
        group_key = (assignment_group or "__all_groups__").strip().lower()
        mode = self._validate_metric_mode(metric_mode)
        cache_key = f"ticket_by_hour::{mode}::{hour}::{month_key}::{group_key}"
        return self._cached(
            cache_key,
            lambda: self._fetch_ticket_by_hour(hour, month, assignment_group, mode),
        )

    def _fetch_ticket_by_hour(
        self,
        hour: int,
        month: str | None = None,
        assignment_group: str | None = None,
        metric_mode: str = "open",
    ) -> dict:
        cte_sql, event_table = self._build_event_cte(metric_mode)
        params: list[Any] = []
        local_event_expr = f"FROM_UTC_TIMESTAMP(event_time, '{_REPORTING_TIMEZONE}')"
        if metric_mode == "entry_smc_first":
            params.append(_ENTRY_SMC_GROUP)

        where_parts = ["event_time IS NOT NULL", f"HOUR({local_event_expr}) = ?"]
        params.append(hour)

        if month:
            where_parts.append(f"DATE_FORMAT({local_event_expr}, 'yyyy-MM') = ?")
            params.append(month)
        else:
            where_parts.append(f"{local_event_expr} >= ADD_MONTHS(CURRENT_DATE(), -3)")

        if assignment_group and assignment_group.strip():
            where_parts.append("LOWER(TRIM(effective_group)) = LOWER(TRIM(?))")
            params.append(assignment_group.strip())

        where_sql = "\n                AND ".join(where_parts)
        sql = f"""
            {cte_sql}
            SELECT
                ticket_key,
                event_time,
                effective_group,
                event_type,
                data_quality
            FROM {event_table}
            WHERE
                {where_sql}
            ORDER BY event_time DESC
        """
        rows = self._execute(sql, params)

        seen_keys: set[str] = set()
        tickets: list[dict[str, Any]] = []
        for row in rows:
            ticket_key = str(row["ticket_key"]) if row.get("ticket_key") is not None else ""
            if not ticket_key or ticket_key in seen_keys:
                continue
            seen_keys.add(ticket_key)
            match = re.search(r"(\d+)$", ticket_key)
            ticket_number = match.group(1) if match else None
            ticket_url = f"{_JIRA_BASE_URL}/{ticket_key}" if ticket_key else None
            event_time = self._as_datetime(row.get("event_time"))
            event_value = self._to_utc_iso(event_time) if event_time else row.get("event_time")
            event_local_value = event_value
            tickets.append(
                {
                    "ticketKey": ticket_key or None,
                    "ticketNumber": ticket_number,
                    "ticketUrl": ticket_url,
                    "createdIn": event_value,
                    "createdInLocal": event_local_value,
                    "eventType": str(row.get("event_type") or ""),
                    "eventTime": event_value,
                    "eventTimeLocal": event_local_value,
                    "effectiveGroup": str(row.get("effective_group") or "Unknown"),
                    "dataQuality": str(row.get("data_quality") or "inferred"),
                }
            )

        data_quality = "authoritative" if metric_mode == "open" else "inferred"
        return {
            "hour": hour,
            "month": month,
            "totalTickets": len(tickets),
            "metricMode": metric_mode,
            "unit": self._metric_unit(metric_mode),
            "dataQuality": data_quality,
            "reportingTimezone": self._reporting_timezone(),
            "tickets": tickets,
        }

    def _fetch_assignment_groups(self) -> list[str]:
        candidates: list[str] = []
        if _ASSIGNED_GROUP_COLUMN:
            candidates.append(_ASSIGNED_GROUP_COLUMN)
        candidates.extend(["assigned_group", "assignment_group", "smc_assignments"])

        seen: set[str] = set()
        unique_candidates: list[str] = []
        for candidate in candidates:
            c = candidate.strip().lower()
            if not c or c in seen:
                continue
            seen.add(c)
            unique_candidates.append(c)

        last_exc: Exception | None = None
        for assigned_group_col in unique_candidates:
            try:
                issue_type_col = self._get_issue_type_column()
                sql = f"""
                    SELECT DISTINCT {assigned_group_col} AS assignment_group
                    FROM {_TICKETS_TABLE}
                    WHERE {assigned_group_col} IS NOT NULL
                      AND TRIM({assigned_group_col}) <> ''
                      AND LOWER({issue_type_col}) LIKE ?
                    ORDER BY LOWER(assignment_group) ASC
                """
                rows = self._execute(sql, ["%incident%"])
                self._resolved_assignment_group_column = assigned_group_col
                logger.info("Using assignment group column: %s", assigned_group_col)
                return [str(r["assignment_group"]) for r in rows]
            except Exception as exc:  # pragma: no cover - depends on schema
                last_exc = exc
                continue

        raise RuntimeError(
            "Could not query assignment groups with known column names."
        ) from last_exc

    # ── Ticket Routing Analysis ──────────────────────────────────────────────

    _FRONT_LINE_GROUPS: list[str] = [
        "axpo service management center",
        "axpo onsite support ch - baden",
        "axpo onsite support ch - beznau",
        "axpo onsite support es",
    ]

    def get_ticket_routing_analysis(
        self, months: list[str] | None = None, limit: int = 2000
    ) -> dict:
        """Analyse tickets resolved by front-line groups and map to natural owners via it_service."""
        cache_key = f"routing:{'|'.join(sorted(months)) if months else 'all'}:{limit}"
        return self._cached(cache_key, lambda: self._fetch_ticket_routing(months, limit))

    def _fetch_ticket_routing(self, months: list[str] | None, limit: int) -> dict:
        assignment_group_column = self._get_assignment_group_column()
        issue_type_column = self._get_issue_type_column()
        local_created_expr = f"FROM_UTC_TIMESTAMP(created_in, '{_REPORTING_TIMEZONE}')"
        score_type_filter = f"LOWER(TRIM({issue_type_column})) LIKE '%incident%'"

        front_filter = ", ".join(f"'{g}'" for g in self._FRONT_LINE_GROUPS)
        front_line_where = f"LOWER(TRIM({assignment_group_column})) IN ({front_filter})"

        month_filter = ""
        if months:
            month_list = ", ".join(f"'{m}'" for m in months)
            month_filter = f"AND DATE_FORMAT({local_created_expr}, 'yyyy-MM') IN ({month_list})"

        # ── 1) Tickets resolved by front-line, with per-ticket detail ─────────
        front_line_sql = f"""
            SELECT
                CAST({_TICKET_ID_COLUMN} AS STRING) AS ticket_id,
                COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS resolver,
                COALESCE(NULLIF(TRIM(it_service), ''), '(Unclassified)') AS it_service,
                SUBSTR(COALESCE(NULLIF(TRIM(translated_description), ''),
                                NULLIF(TRIM(description), ''), ''), 1, 500) AS ticket_desc
            FROM {_TICKETS_TABLE}
            WHERE created_in IS NOT NULL
              AND {score_type_filter}
              AND smc_assignments > 0
              AND {front_line_where}
              {month_filter}
        """
        front_rows = self._execute(front_line_sql)
        if not front_rows:
            return {
                "summary": {"totalFrontLineResolved": 0, "classifiedTickets": 0,
                            "unclassifiedTickets": 0, "reroutableTickets": 0,
                            "reroutePct": 0, "monthsAnalyzed": months or "all"},
                "resolvers": [], "resolverRouting": [], "ownerRanking": [],
                "routingDetails": [],
            }

        ticket_ids = [str(r["ticket_id"]) for r in front_rows]

        # Build exact IN-list of front-line groups found in data
        _front_groups_found: set[str] = set()
        for r in front_rows:
            _front_groups_found.add(str(r["resolver"]).lower().strip())
        front_filter = ", ".join(f"'{g}'" for g in _front_groups_found)

        # ── 1.5) Whitelist: only real assignment groups from the tickets table ─
        _valid_groups: set[str] = set()
        try:
            grp_rows = self._execute(f"""
                SELECT DISTINCT LOWER(TRIM({assignment_group_column})) AS g
                FROM {_TICKETS_TABLE}
                WHERE {assignment_group_column} IS NOT NULL
                  AND TRIM({assignment_group_column}) != ''
            """)
            _valid_groups = {str(r["g"]).strip() for r in grp_rows if r.get("g")}
            logger.info("Loaded %d valid assignment groups", len(_valid_groups))
        except Exception:
            logger.warning("Could not load assignment groups whitelist")

        def _is_group(name: str) -> bool:
            """Return True only if name is a known assignment group."""
            if not name or name in ("Unknown", "(Unclassified)"):
                return False
            return name.lower().strip() in _valid_groups

        # ── 2) Escalation history: for each ticket, find the LAST specialist
        #        group it was assigned to (via activity table) ──────────────────
        # Process in batches to avoid SQL size limits
        escalation_map: dict[str, str] = {}  # ticket_id → last specialist group
        batch_size = 500
        for i in range(0, len(ticket_ids), batch_size):
            batch = ticket_ids[i:i + batch_size]
            id_list = ", ".join(f"'{tid}'" for tid in batch)
            esc_sql = f"""
                WITH assignments AS (
                    SELECT
                        CAST(a.ticket_id AS STRING) AS ticket_id,
                        TRIM(ELEMENT_AT(SPLIT(a.content, ' --> '), 2)) AS to_group,
                        a.date AS change_date
                    FROM {_ACTIVITY_TABLE} a
                    WHERE CAST(a.ticket_id AS STRING) IN ({id_list})
                      AND a.updated = 'Assignment group'
                      AND a.content LIKE '%-->%'
                ),
                specialist_assignments AS (
                    SELECT ticket_id, to_group, change_date,
                           ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY change_date DESC) AS rn
                    FROM assignments
                    WHERE LOWER(TRIM(to_group)) NOT IN ({front_filter})
                      AND TRIM(to_group) != ''
                )
                SELECT ticket_id, to_group
                FROM specialist_assignments
                WHERE rn = 1
            """
            esc_rows = self._execute(esc_sql)
            for row in esc_rows:
                grp = str(row["to_group"]).strip()
                if _is_group(grp):
                    escalation_map[str(row["ticket_id"])] = grp

        logger.info(
            "Routing: %d front-line tickets, %d with real escalation history",
            len(front_rows), len(escalation_map),
        )

        # ── 2.5) Description keyword model ───────────────────────────────────
        # Build word-frequency profiles per specialist group from ALL historical
        # escalation data (not limited to the selected months).
        _DESC_STOP = {
            "the","and","for","with","that","this","from","has","was","are",
            "not","but","can","will","been","have","had","does","did","get",
            "got","just","its","our","one","all","also","than","other","any",
            "into","more","some","out","over","such","after","before","about",
            "please","hello","could","would","should","thanks","thank","thanks",
            "need","want","help","like","new","use","using","used","may",
            "user","ticket","issue","error","work","working","request",
            "dear","team","support","service","good","morning","afternoon",
            "der","die","das","und","ist","ein","eine","auf","mit","dem",
            "den","des","von","für","nicht","sich","auch","als","noch",
            "wie","bei","nach","wird","aus","oder","hat","kann","sind",
            "nur","wenn","schon","wir","uns","ich","bitte","hallo","guten",
            "werden","wurde","haben","diese","dieser","dieses","einem",
            "einer","mein","meine","sein","ihr","ihre","morgen","tag",
        }

        def _tokenize(text: str) -> set[str]:
            return set(re.findall(r'\b[a-zäöüàéèáíóúñ]{3,}\b', text.lower())) - _DESC_STOP

        desc_model_sql = f"""
            SELECT
                SUBSTR(COALESCE(NULLIF(TRIM(translated_description), ''),
                                NULLIF(TRIM(description), ''), ''), 1, 500) AS desc_text,
                TRIM({assignment_group_column}) AS resolver_group
            FROM {_TICKETS_TABLE}
            WHERE created_in IS NOT NULL
              AND {score_type_filter}
              AND smc_assignments > 0
              AND LOWER(TRIM({assignment_group_column})) NOT IN ({front_filter})
              AND TRIM({assignment_group_column}) != ''
              AND LENGTH(COALESCE(translated_description, description, '')) > 10
            LIMIT 8000
        """
        group_words: dict[str, Counter] = {}
        group_docs: dict[str, int] = {}
        try:
            model_rows = self._execute(desc_model_sql)
            for mr in model_rows:
                grp = str(mr.get("resolver_group", "")).strip()
                txt = str(mr.get("desc_text", ""))
                if not grp or not _is_group(grp) or len(txt) < 10:
                    continue
                tokens = _tokenize(txt)
                if not tokens:
                    continue
                group_words.setdefault(grp, Counter()).update(tokens)
                group_docs[grp] = group_docs.get(grp, 0) + 1
        except Exception:
            logger.warning("Description model query failed; skipping description routing")

        # Word spread for IDF weighting (more distinctive words get higher weight)
        w_spread: dict[str, int] = {}
        for cnt in group_words.values():
            for w in cnt:
                w_spread[w] = w_spread.get(w, 0) + 1
        n_grps = max(len(group_words), 1)

        logger.info(
            "Description model: %d groups, %d training tickets",
            len(group_words), sum(group_docs.values()),
        )

        def _match_desc(text: str) -> tuple[str, float] | None:
            """Score a description against group keyword profiles (TF-IDF).
            Returns (group, confidence) or None."""
            if len(text) < 10 or not group_words:
                return None
            tokens = _tokenize(text)
            if not tokens:
                return None
            scores: dict[str, float] = {}
            for grp, cnt in group_words.items():
                ndocs = max(group_docs.get(grp, 1), 1)
                s = 0.0
                for w in tokens:
                    if w in cnt:
                        tf = cnt[w] / ndocs
                        idf = n_grps / max(w_spread.get(w, 1), 1)
                        s += tf * idf
                if s > 0:
                    scores[grp] = s
            if not scores:
                return None
            ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            # Only return if there's a clear winner (>30% better than 2nd)
            if len(ranked) >= 2 and ranked[0][1] <= ranked[1][1] * 1.3:
                return None
            # Confidence based on margin over 2nd place
            if len(ranked) >= 2:
                margin = ranked[0][1] / ranked[1][1]
                conf = min(0.85, 0.50 + (margin - 1.0) * 0.35)
            else:
                conf = 0.75
            return (ranked[0][0], round(conf, 2))

        # ── 3) Fallback: it_service → natural owner (same as before) ─────────
        svc_totals_raw: dict[str, int] = {}
        for r in front_rows:
            svc = str(r.get("it_service", "(Unclassified)"))
            svc_totals_raw[svc] = svc_totals_raw.get(svc, 0) + 1

        natural_owners: dict[str, list[dict]] = {}
        for svc in sorted(svc_totals_raw.keys(), key=lambda s: svc_totals_raw[s], reverse=True):
            if svc == "(Unclassified)":
                continue
            svc_escaped = svc.replace("'", "''")
            owner_sql = f"""
                SELECT
                    COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS grp,
                    COUNT(*) AS cnt
                FROM {_TICKETS_TABLE}
                WHERE created_in IS NOT NULL
                  AND {score_type_filter}
                  AND smc_assignments > 0
                  AND TRIM(it_service) = '{svc_escaped}'
                  AND LOWER(TRIM({assignment_group_column})) NOT IN ({front_filter})
                GROUP BY grp
                ORDER BY cnt DESC
                LIMIT 5
            """
            owner_rows = self._execute(owner_sql)
            if owner_rows:
                filtered = [o for o in owner_rows if _is_group(str(o["grp"]).strip())]
                total_specialist = sum(int(o.get("cnt", 0)) for o in filtered)
                # Only keep this it_service mapping if top group has enough evidence
                if total_specialist >= 3:
                    natural_owners[svc] = [
                    {
                        "group": str(o["grp"]),
                        "tickets": int(o["cnt"]),
                        "pct": round(int(o["cnt"]) / max(total_specialist, 1) * 100, 1),
                    }
                    for o in filtered
                ]

        # ── 4) Assign each ticket to a specialist group ──────────────────────
        # Priority: real escalation > description keywords > it_service fallback
        resolver_totals: dict[str, int] = {}
        resolver_owner: dict[str, dict[str, int]] = {}  # resolver → {owner → count}
        # Track routing method per (resolver, owner) pair
        resolver_owner_method: dict[str, dict[str, dict[str, int]]] = {}
        # Track confidence per (resolver, owner) pair
        resolver_owner_conf: dict[str, dict[str, list[float]]] = {}
        resolver_unclassified: dict[str, int] = {}
        resolver_self: dict[str, int] = {}
        routing_method_counts = {
            "escalation": 0, "description": 0, "it_service": 0,
            "unclassified": 0, "self_resolved": 0,
        }

        def _add_owner(resolver: str, owner: str, method: str, confidence: float = 0.5) -> None:
            resolver_owner.setdefault(resolver, {})[owner] = (
                resolver_owner.get(resolver, {}).get(owner, 0) + 1
            )
            resolver_owner_method.setdefault(resolver, {}).setdefault(owner, {
                "escalation": 0, "description": 0, "it_service": 0,
            })
            resolver_owner_method[resolver][owner][method] += 1
            resolver_owner_conf.setdefault(resolver, {}).setdefault(owner, []).append(confidence)

        for r in front_rows:
            tid = str(r["ticket_id"])
            resolver = str(r["resolver"])
            svc = str(r.get("it_service", "(Unclassified)"))
            desc = str(r.get("ticket_desc", ""))
            resolver_totals[resolver] = resolver_totals.get(resolver, 0) + 1

            if tid in escalation_map:
                # Real escalation data — most reliable (95%)
                _add_owner(resolver, escalation_map[tid], "escalation", 0.95)
                routing_method_counts["escalation"] += 1
            else:
                # Get BOTH predictions to cross-validate
                desc_result = _match_desc(desc)  # (group, conf) or None
                svc_group = None
                svc_conf = 0.0
                if svc != "(Unclassified)":
                    owners = natural_owners.get(svc, [])
                    if owners:
                        svc_group = owners[0]["group"]
                        svc_conf = min(0.80, max(0.35, owners[0]["pct"] / 100.0))

                if desc_result and svc_group:
                    if desc_result[0] == svc_group:
                        # Both methods agree → high confidence
                        combined_conf = min(0.92, max(desc_result[1], svc_conf) + 0.10)
                        _add_owner(resolver, desc_result[0], "description", combined_conf)
                        routing_method_counts["description"] += 1
                    else:
                        # Disagree → pick the one with higher confidence
                        if desc_result[1] >= svc_conf:
                            _add_owner(resolver, desc_result[0], "description", desc_result[1])
                            routing_method_counts["description"] += 1
                        else:
                            _add_owner(resolver, svc_group, "it_service", svc_conf)
                            routing_method_counts["it_service"] += 1
                elif desc_result:
                    _add_owner(resolver, desc_result[0], "description", desc_result[1])
                    routing_method_counts["description"] += 1
                elif svc_group:
                    _add_owner(resolver, svc_group, "it_service", svc_conf)
                    routing_method_counts["it_service"] += 1
                elif svc != "(Unclassified)":
                    resolver_self[resolver] = resolver_self.get(resolver, 0) + 1
                    routing_method_counts["self_resolved"] += 1
                else:
                    resolver_unclassified[resolver] = resolver_unclassified.get(resolver, 0) + 1
                    routing_method_counts["unclassified"] += 1

        grand_total = sum(resolver_totals.values())

        # ── 5) Build resolver routing cards ──────────────────────────────────
        resolver_routing: list[dict] = []
        all_owner_agg: dict[str, int] = {}  # global aggregation for owner ranking

        for resolver in sorted(resolver_totals, key=lambda r: resolver_totals[r], reverse=True):
            total_res = resolver_totals[resolver]
            owner_map = resolver_owner.get(resolver, {})
            method_map = resolver_owner_method.get(resolver, {})
            conf_map = resolver_owner_conf.get(resolver, {})
            unclassified = resolver_unclassified.get(resolver, 0)
            self_resolved = resolver_self.get(resolver, 0)

            owner_detail = sorted(
                [{"group": g, "tickets": t, "pct": round(t / max(total_res, 1) * 100, 1),
                  "methods": method_map.get(g, {"escalation": 0, "description": 0, "it_service": 0}),
                  "confidence": round(sum(conf_map.get(g, [0.5])) / max(len(conf_map.get(g, [0.5])), 1) * 100)}
                 for g, t in owner_map.items()],
                key=lambda x: x["tickets"],
                reverse=True,
            )
            resolver_routing.append({
                "resolver": resolver,
                "totalTickets": total_res,
                "suggestedOwners": owner_detail,
                "selfResolved": self_resolved,
                "unclassified": unclassified,
            })

            for g, t in owner_map.items():
                all_owner_agg[g] = all_owner_agg.get(g, 0) + t

        # ── 6) Owner ranking ─────────────────────────────────────────────────
        owner_ranking = sorted(
            [{"group": k, "tickets": v} for k, v in all_owner_agg.items()],
            key=lambda x: x["tickets"],
            reverse=True,
        )

        # ── 7) Summary ───────────────────────────────────────────────────────
        total_unclassified = sum(resolver_unclassified.values())
        total_self = sum(resolver_self.values())
        total_routed = sum(all_owner_agg.values())

        # ── 8) Resolver list ─────────────────────────────────────────────────
        resolver_list = [
            {"group": k, "tickets": v}
            for k, v in sorted(resolver_totals.items(), key=lambda x: x[1], reverse=True)
        ]

        # ── 9) Routing details by it_service (kept for reference) ────────────
        routing_details: list[dict] = []
        for svc in sorted(svc_totals_raw.keys(), key=lambda s: svc_totals_raw[s], reverse=True):
            svc_cnt = svc_totals_raw[svc]
            owners = natural_owners.get(svc, [])
            suggested = owners[0]["group"] if owners else "(self-resolved)"
            routing_details.append({
                "itService": svc,
                "ticketCount": svc_cnt,
                "sharePct": round(svc_cnt / max(grand_total, 1) * 100, 1),
                "suggestedOwner": suggested,
                "topOwners": owners,
            })

        # ── 10) Actual arrivals: tickets actually assigned to each group ─────
        # For every group that appears in ownerRanking, count how many tickets
        # actually had that group as their final assignment_group (resolved there).
        actual_arrivals: dict[str, int] = {}
        actual_arrivals_monthly: dict[str, dict[str, int]] = {}
        predicted_monthly: dict[str, dict[str, int]] = {}

        # Build predicted monthly from front_rows + routing decisions
        for r in front_rows:
            tid = str(r["ticket_id"])
            resolver = str(r["resolver"])
            # Determine which month this ticket belongs to
            # We don't have created_in in front_rows, but we can re-derive from activity data
            # Instead, track predicted owner per ticket from above logic
            pass

        # Query actual tickets resolved by each specialist group in the same months
        owner_groups = [g["group"] for g in owner_ranking[:50]]  # top 50 groups
        if owner_groups:
            groups_in = ", ".join(
                f"'{g.replace(chr(39), chr(39)+chr(39))}'" for g in owner_groups
            )
            actual_sql = f"""
                SELECT
                    COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS grp,
                    DATE_FORMAT({local_created_expr}, 'yyyy-MM') AS month,
                    COUNT(*) AS cnt
                FROM {_TICKETS_TABLE}
                WHERE created_in IS NOT NULL
                  AND {score_type_filter}
                  AND TRIM({assignment_group_column}) IN ({groups_in})
                  {month_filter}
                GROUP BY grp, month
                ORDER BY grp, month
            """
            try:
                actual_rows = self._execute(actual_sql)
                for row in actual_rows:
                    grp = str(row["grp"]).strip()
                    m = str(row.get("month", ""))
                    cnt = int(row.get("cnt", 0))
                    actual_arrivals[grp] = actual_arrivals.get(grp, 0) + cnt
                    actual_arrivals_monthly.setdefault(grp, {})[m] = cnt
            except Exception:
                logger.warning("Actual arrivals query failed; skipping")

        # Enrich ownerRanking with actual arrival data
        owner_ranking_enriched = []
        for o in owner_ranking:
            grp = o["group"]
            owner_ranking_enriched.append({
                **o,
                "actualTickets": actual_arrivals.get(grp, 0),
                "monthlyPredicted": actual_arrivals_monthly.get(grp, {}),  # placeholder
                "monthlyActual": actual_arrivals_monthly.get(grp, {}),
            })

        return {
            "summary": {
                "totalFrontLineResolved": grand_total,
                "classifiedTickets": grand_total - total_unclassified,
                "unclassifiedTickets": total_unclassified,
                "reroutableTickets": total_routed,
                "reroutePct": round(total_routed / max(grand_total, 1) * 100, 1),
                "monthsAnalyzed": months if months else "all",
                "routingMethod": routing_method_counts,
            },
            "resolvers": resolver_list,
            "resolverRouting": resolver_routing,
            "ownerRanking": owner_ranking_enriched,
            "routingDetails": routing_details[:limit],
        }

    # ── Priority Audit ──────────────────────────────────────────────────────────

    def get_priority_audit(self, months: list[str] | None = None) -> dict:
        """Return P1/P2 ticket analysis with misclassification flags."""
        cache_key = f"priority_audit:{'|'.join(sorted(months)) if months else 'all'}"
        return self._cached(cache_key, lambda: self._fetch_priority_audit(months))

    def _fetch_priority_audit(self, months: list[str] | None) -> dict:
        priority_column = self._get_priority_column()
        status_column = self._get_status_column()
        assignment_group_column = self._get_assignment_group_column()
        issue_type_column = self._get_issue_type_column()
        local_created_expr = f"FROM_UTC_TIMESTAMP(created_in, '{_REPORTING_TIMEZONE}')"

        if not priority_column:
            return {"error": "No priority column available", "tickets": [], "summary": {}}

        month_filter = ""
        if months:
            month_list = ", ".join(f"'{m}'" for m in months)
            month_filter = f"AND DATE_FORMAT({local_created_expr}, 'yyyy-MM') IN ({month_list})"

        sql = f"""
            SELECT
                {_TICKET_KEY_COLUMN} AS ticket_key,
                {_TICKET_ID_COLUMN} AS ticket_id,
                COALESCE(NULLIF(TRIM({priority_column}), ''), 'Unknown') AS priority,
                COALESCE(NULLIF(TRIM({status_column}), ''), 'Unknown') AS status,
                COALESCE(NULLIF(TRIM({assignment_group_column}), ''), 'Unknown') AS assignment_group,
                COALESCE(NULLIF(TRIM({issue_type_column}), ''), 'Unknown') AS issue_type,
                created_in,
                COALESCE(updated_in, created_in) AS resolved_at,
                DATE_FORMAT({local_created_expr}, 'yyyy-MM') AS month
            FROM {_TICKETS_TABLE}
            WHERE (UPPER({priority_column}) LIKE '%P1%' OR UPPER({priority_column}) LIKE '%P2%')
                AND LOWER(TRIM({issue_type_column})) LIKE '%incident%'
                {month_filter}
            ORDER BY created_in DESC
        """
        rows = self._execute(sql)

        # Collect ticket IDs to check activity for IMOD and resolved timestamps
        ticket_ids = []
        for row in rows:
            tid = str(row.get("ticket_id") or "").strip()
            if tid:
                ticket_ids.append(tid)

        # Query activity table to find which tickets have IMOD and last resolved timestamp
        tickets_with_imod: set[str] = set()
        ticket_last_resolved: dict[str, datetime] = {}
        if ticket_ids:
            activity_ticket_col = self._get_activity_ticket_id_column()
            activity_content_col = self._get_activity_content_column()
            activity_date_col = self._get_activity_date_column()
            # Process in batches to avoid overly large IN clauses
            batch_size = 200
            for i in range(0, len(ticket_ids), batch_size):
                batch = ticket_ids[i:i + batch_size]
                id_placeholders = ", ".join(["?" for _ in batch])
                # IMOD check
                imod_sql = f"""
                    SELECT DISTINCT CAST({activity_ticket_col} AS STRING) AS tid
                    FROM {_ACTIVITY_TABLE}
                    WHERE CAST({activity_ticket_col} AS STRING) IN ({id_placeholders})
                      AND UPPER(COALESCE({activity_content_col}, '')) LIKE '%IMOD%'
                """
                imod_rows = self._execute(imod_sql, batch)
                for arow in imod_rows:
                    tickets_with_imod.add(str(arow.get("tid") or "").strip())

                # Last resolved timestamp from activity
                resolved_sql = f"""
                    SELECT CAST({activity_ticket_col} AS STRING) AS tid,
                           MAX({activity_date_col}) AS last_resolved
                    FROM {_ACTIVITY_TABLE}
                    WHERE CAST({activity_ticket_col} AS STRING) IN ({id_placeholders})
                      AND LOWER(COALESCE({activity_content_col}, '')) LIKE '%resolved%'
                    GROUP BY CAST({activity_ticket_col} AS STRING)
                """
                resolved_rows = self._execute(resolved_sql, batch)
                for rrow in resolved_rows:
                    tid_val = str(rrow.get("tid") or "").strip()
                    resolved_ts = self._as_datetime(rrow.get("last_resolved"))
                    if tid_val and resolved_ts:
                        ticket_last_resolved[tid_val] = resolved_ts

        p1_tickets = []
        p2_tickets = []
        p1_suspicious = []
        p2_suspicious = []

        for row in rows:
            priority_level = self._parse_priority_level(row.get("priority"))
            if priority_level not in (1, 2):
                continue

            created_dt = self._as_datetime(row.get("created_in"))
            # Use last resolved from activity if available, otherwise fall back to updated_in
            tid = str(row.get("ticket_id") or "").strip()
            resolved_dt = ticket_last_resolved.get(tid) or self._as_datetime(row.get("resolved_at"))

            resolution_hours = None
            if created_dt and resolved_dt and resolved_dt > created_dt:
                resolution_hours = round((resolved_dt - created_dt).total_seconds() / 3600, 1)

            target_hours = _SLA_TARGET_HOURS.get(priority_level, 6)
            is_breached = resolution_hours is not None and resolution_hours > target_hours

            # Suspicious = activity does NOT contain IMOD
            is_suspicious = tid not in tickets_with_imod

            status_val = str(row.get("status") or "").strip().lower()
            is_closed = "closed" in status_val or "resolved" in status_val

            ticket_data = {
                "ticketKey": row.get("ticket_key"),
                "priority": f"P{priority_level}",
                "status": row.get("status"),
                "assignmentGroup": row.get("assignment_group"),
                "issueType": row.get("issue_type"),
                "createdAt": self._to_utc_iso(created_dt),
                "resolvedAt": self._to_utc_iso(resolved_dt) if is_closed else None,
                "resolutionHours": resolution_hours if is_closed else None,
                "targetHours": target_hours,
                "isBreached": is_breached if is_closed else None,
                "isSuspicious": is_suspicious,
                "isClosed": is_closed,
                "month": row.get("month"),
                "jiraUrl": f"{_JIRA_BASE_URL}/{row.get('ticket_key')}",
            }

            if priority_level == 1:
                p1_tickets.append(ticket_data)
                if is_suspicious:
                    p1_suspicious.append(ticket_data)
            else:
                p2_tickets.append(ticket_data)
                if is_suspicious:
                    p2_suspicious.append(ticket_data)

        return {
            "summary": {
                "p1Total": len(p1_tickets),
                "p2Total": len(p2_tickets),
                "p1Closed": sum(1 for t in p1_tickets if t["isClosed"]),
                "p2Closed": sum(1 for t in p2_tickets if t["isClosed"]),
                "p1Suspicious": len(p1_suspicious),
                "p2Suspicious": len(p2_suspicious),
                "p1SlaBreached": sum(1 for t in p1_tickets if t["isBreached"]),
                "p2SlaBreached": sum(1 for t in p2_tickets if t["isBreached"]),
                "p1TargetHours": _SLA_TARGET_HOURS[1],
                "p2TargetHours": _SLA_TARGET_HOURS[2],
                "monthsAnalyzed": months if months else "all",
            },
            "p1Tickets": p1_tickets,
            "p2Tickets": p2_tickets,
            "p1Suspicious": p1_suspicious,
            "p2Suspicious": p2_suspicious,
        }

    # ── Tickets per Language ──────────────────────────────────────────────────

    def get_tickets_per_language_months(self) -> list[str]:
        return self._cached(
            "__tickets_per_language_months_v4__",
            self._fetch_tickets_per_language_months,
        )

    def _fetch_tickets_per_language_months(self) -> list[str]:
        local_created_expr = f"FROM_UTC_TIMESTAMP(created_in, '{_REPORTING_TIMEZONE}')"
        smc_assignments_column = self._get_smc_assignments_column()
        where_parts: list[str] = [
            "created_in IS NOT NULL",
            f"COALESCE(TRY_CAST({smc_assignments_column} AS DOUBLE), 0) > 0",
        ]
        params: list[Any] = []
        self._append_it_hub_filter(where_parts, params)
        self._append_incident_filter(where_parts, params)
        where_sql = "\n                AND ".join(where_parts)

        sql = f"""
            SELECT DISTINCT DATE_FORMAT({local_created_expr}, 'yyyy-MM') AS month
            FROM {_TICKETS_TABLE}
            WHERE
                {where_sql}
            ORDER BY month DESC
            LIMIT 36
        """
        rows = self._execute(sql, params)
        return [str(row.get("month") or "") for row in rows if row.get("month")]

    def get_tickets_per_language(
        self,
        months: list[str] | None = None,
        detail_limit: int = 5000,
    ) -> dict[str, Any]:
        normalised_months = sorted({str(m).strip() for m in (months or []) if str(m).strip()})
        bounded_limit = max(100, min(int(detail_limit or 5000), 10000))
        cache_key = "tickets_per_language_v5::months={months}::detail_limit={detail_limit}".format(
            months=",".join(normalised_months) if normalised_months else "__all__",
            detail_limit=bounded_limit,
        )
        return self._cached(
            cache_key,
            lambda: self._fetch_tickets_per_language(normalised_months, bounded_limit),
        )

    def _fetch_tickets_per_language(self, months: list[str], detail_limit: int) -> dict[str, Any]:
        description_column = self._get_description_column()
        smc_assignments_column = self._get_smc_assignments_column()
        timeline_column = self._get_timeline_event_column()
        status_column = self._get_status_column()
        local_created_expr = f"FROM_UTC_TIMESTAMP(created_in, '{_REPORTING_TIMEZONE}')"

        where_parts: list[str] = [
            "created_in IS NOT NULL",
            f"COALESCE(TRY_CAST({smc_assignments_column} AS DOUBLE), 0) > 0",
        ]
        params: list[Any] = []
        self._append_it_hub_filter(where_parts, params)
        self._append_incident_filter(where_parts, params)

        if months:
            placeholders = ", ".join(["?" for _ in months])
            where_parts.append(f"DATE_FORMAT({local_created_expr}, 'yyyy-MM') IN ({placeholders})")
            params.extend(months)

        where_sql = "\n                AND ".join(where_parts)
        status_select = f"{status_column} AS status" if status_column else "'Unknown' AS status"
        sql = f"""
            WITH scoped_raw AS (
                SELECT
                    {_TICKET_ID_COLUMN} AS ticket_id,
                    {_TICKET_KEY_COLUMN} AS ticket_key,
                    DATE_FORMAT({local_created_expr}, 'yyyy-MM') AS month,
                    COALESCE(CAST({description_column} AS STRING), '') AS description_raw,
                    LOWER(COALESCE(CAST({description_column} AS STRING), '')) AS description_lc,
                    {status_select},
                    ROW_NUMBER() OVER (
                        PARTITION BY {_TICKET_KEY_COLUMN}
                        ORDER BY {timeline_column} DESC
                    ) AS rn
                FROM {_TICKETS_TABLE}
                WHERE
                    {where_sql}
            ),
            scoped AS (
                SELECT ticket_id, ticket_key, month, description_raw, description_lc, status
                FROM scoped_raw
                WHERE rn = 1
            ),
            scored AS (
                SELECT
                    ticket_id, month, ticket_key, status, description_raw, description_lc,
                    (
                        CASE WHEN description_lc RLIKE '(?i)[äöüß]' THEN 2 ELSE 0 END +
                        CASE WHEN description_lc RLIKE '(?i)(^|[^a-z0-9_])(und|oder|nicht|kein|keine|ich|wir|sie|der|die|das|ein|eine|mit|fuer|für|bitte|danke|rechnung|störung|stoerung|gerät|geraet|anfrage)([^a-z0-9_]|$)' THEN 1 ELSE 0 END
                    ) AS de_score,
                    (
                        CASE WHEN description_lc RLIKE '(?i)(^|[^a-z0-9_])(the|and|or|not|cannot|unable|issue|request|please|thanks|error|device|laptop|network|password|login|log in|access|update|ticket|service)([^a-z0-9_]|$)' THEN 1 ELSE 0 END
                    ) AS en_score
                FROM scoped
            ),
            classified AS (
                SELECT
                    ticket_id, month, ticket_key, status, description_raw, de_score, en_score,
                    CASE
                        WHEN LENGTH(TRIM(description_lc)) = 0 THEN 'Other'
                        WHEN de_score > en_score THEN 'German'
                        WHEN en_score > de_score THEN 'English'
                        WHEN de_score = en_score AND de_score > 0 THEN
                            CASE
                                WHEN description_lc RLIKE '(?i)[äöüß]|(^|[^a-z0-9_])(der|die|das|nicht|für|fuer|störung|stoerung)([^a-z0-9_]|$)' THEN 'German'
                                ELSE 'English'
                            END
                        ELSE 'Other'
                    END AS language
                FROM scored
            )
            SELECT
                ticket_id, month, ticket_key, language, status, de_score, en_score,
                TRIM(REPLACE(REPLACE(SUBSTRING(description_raw, 1, 240), '\\n', ' '), '\\r', ' ')) AS description_preview
            FROM classified
            ORDER BY month DESC, ticket_key ASC
        """

        rows = self._execute(sql, params)
        aggregate_map: dict[tuple[str, str], int] = {}
        totals_by_language: dict[str, int] = {}
        totals_by_month: dict[str, int] = {}

        detail_rows: list[dict[str, Any]] = []
        for row in rows:
            month = str(row.get("month") or "")
            language = str(row.get("language") or "Other")
            key = str(row.get("ticket_key") or "")
            aggregate_map[(month, language)] = aggregate_map.get((month, language), 0) + 1
            totals_by_language[language] = totals_by_language.get(language, 0) + 1
            totals_by_month[month] = totals_by_month.get(month, 0) + 1

            if len(detail_rows) < detail_limit:
                detail_rows.append({
                    "ticket_id": str(row.get("ticket_id") or "") or None,
                    "ticket_key": key,
                    "ticket_url": f"{_JIRA_BASE_URL}/{key}" if key else None,
                    "month": month,
                    "language": language,
                    "status": str(row.get("status") or "Unknown"),
                    "de_score": int(row.get("de_score") or 0),
                    "en_score": int(row.get("en_score") or 0),
                    "description_preview": str(row.get("description_preview") or ""),
                })

        payload_rows = [
            {"month": month, "language": language, "ticket_count": int(count)}
            for (month, language), count in sorted(
                aggregate_map.items(), key=lambda item: (item[0][0], item[0][1]), reverse=True,
            )
        ]

        opened_unique_tickets = len(rows)
        language_aggregated_total = sum(totals_by_language.values())

        return {
            "rows": payload_rows,
            "details": detail_rows,
            "summary": {
                "total_tickets": language_aggregated_total,
                "totals_by_language": totals_by_language,
                "totals_by_month": totals_by_month,
                "scope": "it_hub_incident_smc_assignments_gt_0",
                "opened_unique_tickets": opened_unique_tickets,
                "language_aggregated_total": language_aggregated_total,
                "integrity_opened_vs_aggregated": opened_unique_tickets == language_aggregated_total,
                "details_returned": len(detail_rows),
                "details_limit": detail_limit,
                "reportingTimezone": self._reporting_timezone(),
            },
        }

    # ── Reassignment Ratio ────────────────────────────────────────────────────

    def get_reassignment_ratio_months(self) -> list[str]:
        return self._cached(
            "__reassignment_ratio_months_v2__",
            self._fetch_reassignment_ratio_months,
        )

    def _fetch_reassignment_ratio_months(self) -> list[str]:
        status_column = self._get_status_column()
        timeline_column = self._get_timeline_event_column()
        closed_col = self._get_closed_time_column()
        resolved_col = self._get_resolved_time_column()
        smc_assignments_column = self._get_smc_assignments_column()
        status_filter_sql = self._select_lifecycle_tickets_where_status(status_column)

        close_candidates = [c for c in [closed_col, resolved_col] if c]
        close_expr = "COALESCE(" + ", ".join(close_candidates) + ")" if close_candidates else timeline_column
        local_close_expr = f"FROM_UTC_TIMESTAMP({close_expr}, '{_REPORTING_TIMEZONE}')"

        where_parts: list[str] = [
            "created_in IS NOT NULL",
            f"{close_expr} IS NOT NULL",
            status_filter_sql,
            f"COALESCE(TRY_CAST({smc_assignments_column} AS DOUBLE), 0) > 0",
        ]
        params: list[Any] = []
        self._append_it_hub_filter(where_parts, params)
        self._append_incident_filter(where_parts, params)
        where_sql = "\n                AND ".join(where_parts)

        sql = f"""
            SELECT DISTINCT DATE_FORMAT({local_close_expr}, 'yyyy-MM') AS month
            FROM {_TICKETS_TABLE}
            WHERE
                {where_sql}
            ORDER BY month DESC
            LIMIT 36
        """
        rows = self._execute(sql, params)
        return [str(row.get("month") or "") for row in rows if row.get("month")]

    def get_reassignment_ratio_assignment_groups(self) -> list[str]:
        return self._cached(
            "__reassignment_ratio_assignment_groups_v2__",
            self._fetch_reassignment_ratio_assignment_groups,
        )

    def _fetch_reassignment_ratio_assignment_groups(self) -> list[str]:
        assignment_group_column = self._get_assignment_group_column()
        timeline_column = self._get_timeline_event_column()
        status_column = self._get_status_column()
        closed_col = self._get_closed_time_column()
        resolved_col = self._get_resolved_time_column()
        smc_assignments_column = self._get_smc_assignments_column()
        status_filter_sql = self._select_lifecycle_tickets_where_status(status_column)

        close_candidates = [c for c in [closed_col, resolved_col] if c]
        close_expr = "COALESCE(" + ", ".join(close_candidates) + ")" if close_candidates else timeline_column

        where_parts: list[str] = [
            "created_in IS NOT NULL",
            f"{close_expr} IS NOT NULL",
            status_filter_sql,
            f"COALESCE(TRY_CAST({smc_assignments_column} AS DOUBLE), 0) > 0",
        ]
        params: list[Any] = []
        self._append_it_hub_filter(where_parts, params)
        self._append_incident_filter(where_parts, params)
        where_sql = "\n                    AND ".join(where_parts)

        sql = f"""
            WITH scoped_raw AS (
                SELECT
                    {assignment_group_column} AS assignment_group,
                    {_TICKET_KEY_COLUMN} AS ticket_key,
                    ROW_NUMBER() OVER (
                        PARTITION BY {_TICKET_KEY_COLUMN}
                        ORDER BY {timeline_column} DESC
                    ) AS rn
                FROM {_TICKETS_TABLE}
                WHERE
                    {where_sql}
            )
            SELECT DISTINCT TRIM(CAST(assignment_group AS STRING)) AS assignment_group
            FROM scoped_raw
            WHERE rn = 1
              AND assignment_group IS NOT NULL
              AND TRIM(CAST(assignment_group AS STRING)) <> ''
            ORDER BY LOWER(assignment_group) ASC
        """
        rows = self._execute(sql, params)
        return [str(row.get("assignment_group") or "") for row in rows if row.get("assignment_group")]

    def get_reassignment_ratio(
        self,
        months: list[str] | None = None,
        assignment_group: str | None = None,
    ) -> dict[str, Any]:
        normalised_months = sorted({str(m).strip() for m in (months or []) if str(m).strip()})
        selected_group = (assignment_group or "").strip()
        group_key = selected_group.lower() if selected_group else "__all__"
        cache_key = "reassignment_ratio_v1::months={months}::group={group}".format(
            months=",".join(normalised_months) if normalised_months else "__all__",
            group=group_key,
        )
        return self._cached(
            cache_key,
            lambda: self._fetch_reassignment_ratio(normalised_months, selected_group),
        )

    def _fetch_reassignment_ratio(self, months: list[str], assignment_group: str) -> dict[str, Any]:
        assignment_group_column = self._get_assignment_group_column()
        smc_assignments_column = self._get_smc_assignments_column()
        smc_reassignment_column = self._get_smc_reassignment_column()
        timeline_column = self._get_timeline_event_column()
        status_column = self._get_status_column()
        closed_col = self._get_closed_time_column()
        resolved_col = self._get_resolved_time_column()
        status_filter_sql = self._select_lifecycle_tickets_where_status(status_column)

        close_candidates = [c for c in [closed_col, resolved_col] if c]
        close_expr = "COALESCE(" + ", ".join(close_candidates) + ")" if close_candidates else timeline_column
        local_close_expr = f"FROM_UTC_TIMESTAMP({close_expr}, '{_REPORTING_TIMEZONE}')"

        where_parts: list[str] = [
            "created_in IS NOT NULL",
            f"{close_expr} IS NOT NULL",
            status_filter_sql,
            f"COALESCE(TRY_CAST({smc_assignments_column} AS DOUBLE), 0) > 0",
        ]
        base_params: list[Any] = []
        self._append_it_hub_filter(where_parts, base_params)
        self._append_incident_filter(where_parts, base_params)
        if months:
            placeholders = ", ".join(["?" for _ in months])
            where_parts.append(f"DATE_FORMAT({local_close_expr}, 'yyyy-MM') IN ({placeholders})")
            base_params.extend(months)

        selected_filter_sql = "1 = 1"
        selected_filter_params_main: list[Any] = []
        selected_filter_params_breakdown: list[Any] = []
        if assignment_group:
            selected_filter_sql = "LOWER(TRIM(assignment_group)) = LOWER(TRIM(?))"
            selected_filter_params_main = [assignment_group, assignment_group]
            selected_filter_params_breakdown = [assignment_group]

        where_sql = "\n                    AND ".join(where_parts)
        sql = f"""
            WITH scoped_raw AS (
                SELECT
                    {_TICKET_KEY_COLUMN} AS ticket_key,
                    DATE_FORMAT({local_close_expr}, 'yyyy-MM') AS month,
                    TRIM(COALESCE(CAST({assignment_group_column} AS STRING), '')) AS assignment_group,
                    COALESCE(TRY_CAST({smc_reassignment_column} AS DOUBLE), 0) AS reassignment_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY {_TICKET_KEY_COLUMN}
                        ORDER BY {timeline_column} DESC
                    ) AS rn
                FROM {_TICKETS_TABLE}
                WHERE
                    {where_sql}
            ),
            scoped AS (
                SELECT
                    ticket_key, month,
                    CASE WHEN assignment_group = '' THEN 'Unknown' ELSE assignment_group END AS assignment_group,
                    reassignment_count
                FROM scoped_raw
                WHERE rn = 1
            ),
            monthly AS (
                SELECT
                    month,
                    COUNT(*) AS overall_total,
                    SUM(CASE WHEN reassignment_count > 3 THEN 1 ELSE 0 END) AS overall_gt3,
                    SUM(CASE WHEN {selected_filter_sql} THEN 1 ELSE 0 END) AS selected_total,
                    SUM(CASE WHEN {selected_filter_sql} AND reassignment_count > 3 THEN 1 ELSE 0 END) AS selected_gt3
                FROM scoped
                GROUP BY month
            )
            SELECT month, overall_total, overall_gt3, selected_total, selected_gt3
            FROM monthly
            ORDER BY month DESC
        """

        rows = self._execute(sql, base_params + selected_filter_params_main)

        breakdown_sql = f"""
            WITH scoped_raw AS (
                SELECT
                    {_TICKET_KEY_COLUMN} AS ticket_key,
                    TRIM(COALESCE(CAST({assignment_group_column} AS STRING), '')) AS assignment_group,
                    COALESCE(TRY_CAST({smc_reassignment_column} AS DOUBLE), 0) AS reassignment_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY {_TICKET_KEY_COLUMN}
                        ORDER BY {timeline_column} DESC
                    ) AS rn
                FROM {_TICKETS_TABLE}
                WHERE
                    {where_sql}
            ),
            scoped AS (
                SELECT
                    ticket_key,
                    CASE WHEN assignment_group = '' THEN 'Unknown' ELSE assignment_group END AS assignment_group,
                    reassignment_count
                FROM scoped_raw
                WHERE rn = 1
            )
            SELECT
                CAST(reassignment_count AS INT) AS reassignment_count,
                COUNT(*) AS ticket_count
            FROM scoped
            WHERE reassignment_count > 3
              AND {selected_filter_sql}
            GROUP BY CAST(reassignment_count AS INT)
            ORDER BY reassignment_count ASC
        """
        breakdown_rows = self._execute(breakdown_sql, base_params + selected_filter_params_breakdown)

        reassignment_breakdown = [
            {"reassignment_count": int(row.get("reassignment_count") or 0), "ticket_count": int(row.get("ticket_count") or 0)}
            for row in breakdown_rows
            if int(row.get("reassignment_count") or 0) > 3
        ]
        monthly_rows: list[dict[str, Any]] = []
        overall_total_sum = 0
        overall_gt3_sum = 0
        selected_total_sum = 0
        selected_gt3_sum = 0

        for row in rows:
            ot = int(row.get("overall_total") or 0)
            og = int(row.get("overall_gt3") or 0)
            st = int(row.get("selected_total") or 0)
            sg = int(row.get("selected_gt3") or 0)
            overall_total_sum += ot
            overall_gt3_sum += og
            selected_total_sum += st
            selected_gt3_sum += sg
            monthly_rows.append({
                "month": str(row.get("month") or ""),
                "overall_total": ot, "overall_gt3": og,
                "overall_ratio_pct": round((og / ot * 100.0) if ot > 0 else 0.0, 2),
                "selected_total": st, "selected_gt3": sg,
                "selected_ratio_pct": round((sg / st * 100.0) if st > 0 else 0.0, 2),
            })

        overall_ratio_pct = (overall_gt3_sum / overall_total_sum * 100.0) if overall_total_sum > 0 else 0.0
        selected_ratio_pct = (selected_gt3_sum / selected_total_sum * 100.0) if selected_total_sum > 0 else 0.0

        return {
            "monthly": monthly_rows,
            "reassignment_breakdown": reassignment_breakdown,
            "summary": {
                "formula": "reassignment_ratio = tickets_with_reassignments_gt_3 / tickets_overall",
                "threshold": 3,
                "selected_group": assignment_group or "All assignment groups",
                "selected_metrics": {
                    "tickets_overall": selected_total_sum,
                    "reassignments_gt_3": selected_gt3_sum,
                    "reassignment_ratio_pct": round(selected_ratio_pct, 2),
                },
                "overall_metrics": {
                    "tickets_overall": overall_total_sum,
                    "reassignments_gt_3": overall_gt3_sum,
                    "reassignment_ratio_pct": round(overall_ratio_pct, 2),
                },
                "delta_vs_overall_pct_points": round(selected_ratio_pct - overall_ratio_pct, 2),
                "scope": "it_hub_incident_smc_assignments_gt_0",
                "month_anchor": "closed_or_resolved_in",
                "reportingTimezone": self._reporting_timezone(),
            },
        }

    def invalidate_cache(self) -> None:
        """Clear all cached entries (e.g. triggered by a manual refresh)."""
        self._cache.clear()
        logger.info("Response cache cleared.")


@lru_cache(maxsize=1)
def get_client() -> DatabricksClient:
    """Return the process-wide singleton DatabricksClient."""
    return DatabricksClient()

