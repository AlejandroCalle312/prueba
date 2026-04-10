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

    @staticmethod
    def _duration_seconds(start: datetime | None, end: datetime | None) -> int:
        if not start or not end:
            return 0
        start_utc = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
        end_utc = end if end.tzinfo else end.replace(tzinfo=timezone.utc)
        delta = int((end_utc - start_utc).total_seconds())
        return max(delta, 0)

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
    def _business_seconds_weekdays(start: datetime, end: datetime, tz_name: str) -> int:
        if end <= start:
            return 0

        tz = ZoneInfo(tz_name)
        local_start = start.astimezone(tz)
        local_end = end.astimezone(tz)
        day_cursor = local_start.date()
        last_day = local_end.date()
        total_seconds = 0

        while day_cursor <= last_day:
            if day_cursor.weekday() < 5:
                window_start = datetime.combine(day_cursor, dt_time(_SLA_WORKDAY_START_HOUR, 0), tzinfo=tz)
                window_end = datetime.combine(day_cursor, dt_time(_SLA_WORKDAY_END_HOUR, 0), tzinfo=tz)
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
        activity_error: str | None = None
        first_activity_ts: datetime | None = None
        try:
            activity_ticket_col = self._get_activity_ticket_id_column()
            activity_date_col = self._get_activity_date_column()
            activity_content_col = self._get_activity_content_column()
            activity_sql = f"""
                SELECT
                    {activity_date_col} AS activity_time,
                    {activity_content_col} AS content
                FROM {_ACTIVITY_TABLE}
                WHERE CAST({activity_ticket_col} AS STRING) = ?
                  AND {activity_date_col} IS NOT NULL
                  AND {activity_content_col} IS NOT NULL
                ORDER BY {activity_date_col} ASC
            """
            activity_rows = self._execute(activity_sql, [baseline_ticket_id])
            for row in activity_rows:
                ts = self._as_datetime(row.get("activity_time"))
                if ts is None:
                    continue
                if first_activity_ts is None or ts < first_activity_ts:
                    first_activity_ts = ts
                from_group, to_group = self._parse_assignment_group_transitions_from_activity(
                    str(row.get("content") or "")
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
            "sla": sla,
            "meta": {
                "transitionSource": transition_source,
                "activityFallbackUsed": transition_source != "activity",
                "activityError": activity_error,
                "lifecycleStartSource": lifecycle_start_source,
                "reportingTimezone": self._reporting_timezone(),
                "scope": "closed_or_resolved",
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
            "onsite_baden::month={month}::assignee={assignee}::status={status}::group={group}"
        ).format(
            month=(month or "__all__").strip().lower(),
            assignee=(assignee or "__all__").strip().lower(),
            status=(status or "__all__").strip().lower(),
            group=(assignment_group or "__all__").strip().lower(),
        )
        payload = self._cached(
            cache_key,
            lambda: self._fetch_tickets_per_agent(
                month,
                assignee,
                status,
                assignment_group,
                strict_filters=True,
            ),
        )
        if int(payload.get("summary", {}).get("total_tickets", 0)) > 0:
            return payload

        fallback_cache_key = cache_key + "::fallback"
        return self._cached(
            fallback_cache_key,
            lambda: self._fetch_tickets_per_agent(
                month,
                assignee,
                status,
                assignment_group,
                strict_filters=False,
            ),
        )

    def _fetch_tickets_per_agent(
        self,
        month: str | None = None,
        assignee: str | None = None,
        status: str | None = None,
        assignment_group: str | None = None,
        strict_filters: bool = True,
    ) -> dict:
        issue_type_column = self._get_issue_type_column()
        assignment_group_column = self._get_assignment_group_column()
        project_column = self._get_project_column()
        assignee_column = self._get_assignee_column()
        status_column = self._get_status_column()
        priority_column = self._get_priority_column()
        sla_breach_column = self._get_sla_breach_column()
        local_created_expr = f"FROM_UTC_TIMESTAMP(created_in, '{_REPORTING_TIMEZONE}')"

        where_parts = [
            "created_in IS NOT NULL",
        ]
        params: list[Any] = []

        if assignment_group and assignment_group.strip():
            where_parts.append(f"LOWER(TRIM({assignment_group_column})) = LOWER(TRIM(?))")
            params.append(assignment_group.strip())

        if strict_filters:
            project_key_like = f"{_ONSITE_PROJECT_KEY_PREFIX.upper()}%"
            if project_column:
                where_parts.append(
                    "(LOWER(TRIM({project_col})) = LOWER(TRIM(?)) OR UPPER({key_col}) LIKE ?)".format(
                        project_col=project_column,
                        key_col=_TICKET_KEY_COLUMN,
                    )
                )
                params.extend([_ONSITE_PROJECT_NAME, project_key_like])
            else:
                where_parts.append(f"UPPER({_TICKET_KEY_COLUMN}) LIKE ?")
                params.append(project_key_like)

            issue_like_clauses: list[str] = []
            for issue_type in _ONSITE_ISSUE_TYPES:
                issue_like_clauses.append(f"LOWER({issue_type_column}) LIKE ?")
                params.append(f"%{issue_type.lower()}%")
            if issue_like_clauses:
                where_parts.append(f"({' OR '.join(issue_like_clauses)})")

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
                "scope": "strict" if strict_filters else "assignment_group_fallback",
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

    def invalidate_cache(self) -> None:
        """Clear all cached entries (e.g. triggered by a manual refresh)."""
        self._cache.clear()
        logger.info("Response cache cleared.")


@lru_cache(maxsize=1)
def get_client() -> DatabricksClient:
    """Return the process-wide singleton DatabricksClient."""
    return DatabricksClient()

