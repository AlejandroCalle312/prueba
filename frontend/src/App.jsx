import { useMemo, useState } from "react";
import "./App.css";

function App() {
  const [catalog, setCatalog] = useState("main");
  const [schema, setSchema] = useState("default");
  const [limit, setLimit] = useState(100);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState({ rows: [], count: 0, catalog: "", schema: "" });

  const columns = useMemo(() => {
    if (result.rows.length === 0) {
      return ["table_catalog", "table_schema", "table_name", "table_type"];
    }
    return Object.keys(result.rows[0]);
  }, [result.rows]);

  async function handleLoadTables(event) {
    event.preventDefault();
    setLoading(true);
    setError("");

    try {
      const query = new URLSearchParams({
        catalog,
        schema,
        limit: String(limit),
      });

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
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row, index) => (
                <tr key={`${row.table_name || "row"}-${index}`}>
                  {columns.map((column) => (
                    <td key={`${column}-${index}`}>{String(row[column] ?? "")}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          {!loading && result.rows.length === 0 ? (
            <p className="empty">No hay tablas para mostrar con el filtro actual.</p>
          ) : null}
        </div>
      </section>
    </main>
  );
}

export default App;
