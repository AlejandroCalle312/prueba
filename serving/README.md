# serving

## Purpose
Data access and read-only serving layer.

## Contains
- APIs or functions that expose governed data contracts
- Query/aggregation logic needed to serve contracts

## Must not contain
- HTML, CSS, or UI templates
- Presentation logic

## Dependency constraints
- Can know Databricks schemas conceptually
- Must not be accessed directly by UI code in `/presentation`
- Must not bypass data contracts

## Environment scope
- Production repository only
- No dev-only flags or branches

## Nomenclature
- Cross-page API services must use the platform scope name: `srf-axsa-api`.
- Page-specific assets must include the page scope in the folder name.
	Example: `tickets-per-agent-queries/` for SQL used by the Tickets per Agent page.
- `__pycache__/` folders are runtime artifacts, not source. They can be deleted safely and must not be versioned.

---

## Endpoints

### `srf-axsa-api/`
FastAPI service returning Jira ticket-creation counts grouped by hour of day.

| File | Description |
|------|-------------|
| `app.py` | FastAPI entry point — `GET /api/tickets-by-hour`, `GET /api/available-months`, `POST /api/cache/invalidate` |
| `databricks_client.py` | Thread-safe Databricks SQL connector with TTL cache and Managed Identity auth |
| `requirements.txt` | Python dependencies (FastAPI, uvicorn, databricks-sql-connector, azure-identity) |
| `tests/test_app.py` | Unit tests (pytest) |

### `tickets-per-agent-queries/`
SQL reference queries used by the Tickets per Agent analytics scope.

**API contract:** [`docs/serving/srf-axsa-api.md`](../docs/serving/srf-axsa-api.md)

**Local run:**
```bash
cd serving/srf-axsa-api
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
# Interactive docs: http://localhost:8000/docs
```

