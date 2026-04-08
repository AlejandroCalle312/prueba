"""
Unit tests for the tickets-by-hour API.

Run with:
    pytest serving/srf-axsa-api/tests/ -v
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def mock_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_WORKSPACE_URL", "https://adb-test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/test")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")
    monkeypatch.setenv("CORS_ALLOWED_ORIGINS", "http://localhost:3000")


@pytest.fixture()
def mock_client() -> MagicMock:
    client = MagicMock()
    client.get_available_months.return_value = ["2025-03", "2025-02", "2025-01"]
    client.get_assignment_groups.return_value = [
        "AXPO Service Management Center",
        "AXPO Azure Cloud Support",
    ]
    client.get_tickets_by_hour.return_value = [
        {"hour": 9, "count": 5, "month": "2025-01", "year": 2025},
        {"hour": 14, "count": 8, "month": "2025-01", "year": 2025},
    ]
    client.get_ticket_by_hour.return_value = {
        "hour": 9,
        "month": "2025-01",
        "totalTickets": 2,
        "tickets": [
            {
                "ticketKey": "ITHUB-32601",
                "ticketNumber": "32601",
                "ticketUrl": "https://axpo.atlassian.net/browse/ITHUB-32601",
                "createdIn": "2025-01-12T09:15:00Z",
            },
            {
                "ticketKey": "ITHUB-32602",
                "ticketNumber": "32602",
                "ticketUrl": "https://axpo.atlassian.net/browse/ITHUB-32602",
                "createdIn": "2025-01-12T09:31:00Z",
            },
        ],
    }
    client.get_tickets_per_agent.return_value = {
        "rows": [
            {
                "month": "2026-02",
                "assignment_group": "Axpo Onsite Support CH - Baden",
                "assignee": "Jane Doe",
                "status": "Open",
                "priority": "High",
                "ticket_count": 7,
                "sla_breach_count": 1,
            }
        ],
        "summary": {
            "total_tickets": 7,
            "total_sla_breach": 1,
            "sla_metric_available": True,
        },
    }
    return client


@pytest.fixture()
def api_client(mock_client: MagicMock) -> TestClient:
    # Re-import app inside the fixture so env vars are picked up
    if "app" in sys.modules:
        del sys.modules["app"]
    if "databricks_client" in sys.modules:
        del sys.modules["databricks_client"]

    sys.path.insert(0, "serving/srf-axsa-api")
    import app as api_module
    import api.tickets_per_agent as onsite_module

    api_module.get_client = lambda: mock_client
    onsite_module.databricks_client.get_client = lambda: mock_client

    return TestClient(api_module.app)


# ---------------------------------------------------------------------------
# Tests — /health
# ---------------------------------------------------------------------------


def test_health(api_client: TestClient) -> None:
    resp = api_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "version" in body


# ---------------------------------------------------------------------------
# Tests — /api/available-months
# ---------------------------------------------------------------------------


def test_available_months_returns_list(api_client: TestClient) -> None:
    resp = api_client.get("/api/available-months")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert "2025-03" in data


# ---------------------------------------------------------------------------
# Tests — /api/assignment-groups
# ---------------------------------------------------------------------------


def test_assignment_groups_returns_list(api_client: TestClient) -> None:
    resp = api_client.get("/api/assignment-groups")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert "AXPO Service Management Center" in data


# ---------------------------------------------------------------------------
# Tests — /api/tickets-by-hour
# ---------------------------------------------------------------------------


def test_tickets_by_hour_no_params(api_client: TestClient) -> None:
    resp = api_client.get("/api/tickets-by-hour")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert data[0]["hour"] == 9


def test_tickets_by_hour_with_months(api_client: TestClient) -> None:
    resp = api_client.get("/api/tickets-by-hour?months=2025-01,2025-02")
    assert resp.status_code == 200


def test_tickets_by_hour_with_assignment_group(api_client: TestClient) -> None:
    resp = api_client.get(
        "/api/tickets-by-hour?months=2025-01&assignmentGroup=AXPO%20Service%20Management%20Center"
    )
    assert resp.status_code == 200


def test_tickets_by_hour_include_meta(api_client: TestClient, mock_client: MagicMock) -> None:
    mock_client.get_tickets_by_hour.return_value = {
        "rows": [{"hour": 9, "count": 5, "month": "2025-01", "year": 2025}],
        "meta": {"metricMode": "open", "unit": "tickets", "dataQuality": "authoritative"},
    }
    resp = api_client.get("/api/tickets-by-hour?includeMeta=true&metricMode=open")
    assert resp.status_code == 200
    body = resp.json()
    assert "rows" in body
    assert body["meta"]["metricMode"] == "open"


def test_tickets_by_hour_invalid_metric_mode(api_client: TestClient) -> None:
    resp = api_client.get("/api/tickets-by-hour?metricMode=invalid")
    assert resp.status_code == 400
    assert "Invalid metricMode" in resp.json()["detail"]


def test_tickets_by_hour_invalid_month_format(api_client: TestClient) -> None:
    resp = api_client.get("/api/tickets-by-hour?months=01-2025")
    assert resp.status_code == 400
    assert "Invalid month format" in resp.json()["detail"]


def test_tickets_by_hour_too_many_months(api_client: TestClient) -> None:
    months = ",".join([f"202{y}-{m:02d}" for y in range(3) for m in range(1, 13)])
    resp = api_client.get(f"/api/tickets-by-hour?months={months}")
    assert resp.status_code == 400
    assert "Maximum" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Tests — /api/ticket-by-hour
# ---------------------------------------------------------------------------


def test_ticket_by_hour_with_month(api_client: TestClient) -> None:
    resp = api_client.get("/api/ticket-by-hour?hour=9&month=2025-01")
    assert resp.status_code == 200
    body = resp.json()
    assert body["totalTickets"] == 2
    assert body["tickets"][0]["ticketKey"] == "ITHUB-32601"


def test_ticket_by_hour_invalid_metric_mode(api_client: TestClient) -> None:
    resp = api_client.get("/api/ticket-by-hour?hour=9&metricMode=invalid")
    assert resp.status_code == 400
    assert "Invalid metricMode" in resp.json()["detail"]


def test_ticket_by_hour_invalid_month(api_client: TestClient) -> None:
    resp = api_client.get("/api/ticket-by-hour?hour=9&month=01-2025")
    assert resp.status_code == 400
    assert "Invalid month format" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Tests — /api/tickets-per-agent
# ---------------------------------------------------------------------------


def test_tickets_per_agent(api_client: TestClient) -> None:
    resp = api_client.get("/api/tickets-per-agent?month=2026-02")
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body["rows"], list)
    assert body["summary"]["total_tickets"] == 7
    assert body["rows"][0]["assignment_group"] == "Axpo Onsite Support CH - Baden"


def test_tickets_per_agent_with_assignment_group(api_client: TestClient, mock_client: MagicMock) -> None:
    resp = api_client.get(
        "/api/tickets-per-agent?month=2026-02&assignmentGroup=AXPO%20Service%20Management%20Center"
    )
    assert resp.status_code == 200
    mock_client.get_tickets_per_agent.assert_called_with(
        month="2026-02",
        assignee=None,
        status=None,
        assignment_group="AXPO Service Management Center",
    )


def test_tickets_per_agent_invalid_month(api_client: TestClient) -> None:
    resp = api_client.get("/api/tickets-per-agent?month=02-2026")
    assert resp.status_code == 400
    assert "Invalid month format" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Tests — /api/cache/invalidate
# ---------------------------------------------------------------------------


def test_cache_invalidate_no_config(api_client: TestClient) -> None:
    resp = api_client.post(
        "/api/cache/invalidate",
        headers={"X-Cache-Token": "anything"},
    )
    # CACHE_INVALIDATION_TOKEN not set → 503
    assert resp.status_code == 503


def test_cache_invalidate_wrong_token(
    api_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CACHE_INVALIDATION_TOKEN", "secret123")
    resp = api_client.post(
        "/api/cache/invalidate",
        headers={"X-Cache-Token": "wrong"},
    )
    assert resp.status_code == 403


