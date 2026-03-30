import { useMemo, useState } from "react";
import "./App.css";

function App() {
  const [catalog, setCatalog] = useState("main");
  const [schema, setSchema] = useState("default");
  const [table, setTable] = useState("");
  const [limit, setLimit] = useState(100);
  const [loading, setLoading] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState({ rows: [], count: 0, catalog: "", schema: "" });
  const [preview, setPreview] = useState({
    rows: [],
    columns: [],
    count: 0,
    catalog: "",
    schema: "",
    table: "",
  });

  const columns = useMemo(() => {
    if (result.rows.length === 0) {
      return ["table_catalog", "table_schema", "table_name", "table_type"];
    }
    return Object.keys(result.rows[0]);
  }, [result.rows]);

  const previewColumns = useMemo(() => {
    if (preview.columns.length > 0) {
      return preview.columns;
    }

    if (preview.rows.length === 0) {
      return [];
    }

    return Object.keys(preview.rows[0]);
  }, [preview.columns, preview.rows]);

  async function handleLoadTables(event) {
    event.preventDefault();
    setLoading(true);
    setError("");
    setPreview({ rows: [], columns: [], count: 0, catalog: "", schema: "", table: "" });

    try {
      const query = new URLSearchParams({ catalog, schema, limit: String(limit) });
      if (table.trim()) {
        query.set("table", table.trim());
      }

      const apiBase = import.meta.env.VITE_API_BASE_URL || "";
      const response = await fetch(`${apiBase}/api/databricks/tables?${query.toString()}`);
      const payload = await response.json();

      if (!response.ok) {
        throw new Error(payload?.details?.message || payload?.error || "Unexpected API error");
      }

      setResult(payload);
    } catch (requestError) {
      setResult({ rows: [], count: 0, catalog: "", schema: "" });
      setError(requestError.message);
    } finally {
      setLoading(false);
    }
  }

  async function handlePreviewTable(row) {
    setPreviewLoading(true);
    setError("");

    try {
      const query = new URLSearchParams({
        catalog: row.table_catalog,
        schema: row.table_schema,
        table: row.table_name,
        limit: "50",
      });

      const apiBase = import.meta.env.VITE_API_BASE_URL || "";
      const response = await fetch(`${apiBase}/api/databricks/table-preview?${query.toString()}`);
      const payload = await response.json();

      if (!response.ok) {
        throw new Error(payload?.details?.message || payload?.error || "Unexpected API error");
      }

      setPreview(payload);
    } catch (requestError) {
      setPreview({ rows: [], columns: [], count: 0, catalog: "", schema: "", table: "" });
      setError(requestError.message);
    } finally {
      setPreviewLoading(false);
    }
  }

  return (
    <main className="page">
      <section className="hero">
        <p className="eyebrow">Mini proyecto local</p>
        <h1>Databricks Tables Explorer</h1>
        <p className="subtitle">
          Frontend en React + backend en Node.js con autenticacion por App Registration.
        </p>

        <form className="query-form" onSubmit={handleLoadTables}>
          <label>
            Catalog
            <input value={catalog} onChange={(event) => setCatalog(event.target.value)} required />
          </label>
          <label>
            Schema
            <input value={schema} onChange={(event) => setSchema(event.target.value)} required />
          </label>
          <label>
            Table
            <input
              value={table}
              onChange={(event) => setTable(event.target.value)}
              placeholder="Nombre o parte del nombre"
            />
          </label>
          <label>
            Limit
            <input
              type="number"
              min="1"
              max="1000"
              value={limit}
              onChange={(event) => setLimit(event.target.value)}
              required
            />
          </label>
          <button type="submit" disabled={loading}>
            {loading ? "Consultando..." : "Cargar tablas"}
          </button>
        </form>

        {error ? <p className="error">{error}</p> : null}
      </section>

      <section className="table-panel">
        <div className="table-header">
          <h2>Resultado</h2>
          <p>
            {result.catalog && result.schema
              ? `${result.catalog}.${result.schema} - ${result.count} tablas`
              : "Sin resultados todavia"}
          </p>
        </div>

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                {columns.map((column) => (
                  <th key={column}>{column}</th>
                ))}
                <th>actions</th>
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row, index) => (
                <tr key={`${row.table_name || "row"}-${index}`}>
                  {columns.map((column) => (
                    <td key={`${column}-${index}`}>{String(row[column] ?? "")}</td>
                  ))}
                  <td>
                    <button
                      type="button"
                      className="secondary-btn"
                      onClick={() => handlePreviewTable(row)}
                      disabled={previewLoading}
                    >
                      {previewLoading && preview.table === row.table_name ? "Cargando..." : "Ver contenido"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {!loading && result.rows.length === 0 ? (
            <p className="empty">No hay tablas para mostrar con el filtro actual.</p>
          ) : null}
        </div>
      </section>

      {preview.table ? (
        <section className="table-panel">
          <div className="table-header">
            <h2>Contenido de tabla</h2>
            <p>
              {preview.catalog}.{preview.schema}.{preview.table} - {preview.count} filas
            </p>
          </div>

          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  {previewColumns.map((column) => (
                    <th key={column}>{column}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {preview.rows.map((row, rowIndex) => (
                  <tr key={`preview-${rowIndex}`}>
                    {previewColumns.map((column) => (
                      <td key={`preview-${rowIndex}-${column}`}>{String(row[column] ?? "")}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>

            {!previewLoading && preview.rows.length === 0 ? (
              <p className="empty">No hay filas para mostrar en esta tabla (limite 50).</p>
            ) : null}
          </div>
        </section>
      ) : null}
    </main>
  );
}

export default App;
