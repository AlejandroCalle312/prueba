const fs = require("fs");
const path = require("path");
const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const { getTables, getTablePreview } = require("./databricks");

dotenv.config({ path: path.join(__dirname, ".env"), override: true });

const app = express();
const port = Number(process.env.PORT || 3001);

app.use(cors());
app.use(express.json());

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, timestamp: new Date().toISOString() });
});

app.get("/api/databricks/tables", async (req, res) => {
  const catalog = req.query.catalog || process.env.DATABRICKS_DEFAULT_CATALOG || "main";
  const schema = req.query.schema || process.env.DATABRICKS_DEFAULT_SCHEMA || "default";
  const table = req.query.table || "";
  const limit = req.query.limit || 100;

  try {
    const tables = await getTables({ catalog, schema, limit, table });
    const resolvedCatalog = tables[0]?.table_catalog || catalog;
    const resolvedSchema = tables[0]?.table_schema || schema;

    res.json({
      catalog: resolvedCatalog,
      schema: resolvedSchema,
      table,
      count: tables.length,
      rows: tables,
    });
  } catch (error) {
    const details = error.response?.data || error.message;
    res.status(500).json({
      error: "Failed to fetch Databricks tables",
      details,
    });
  }
});

app.get("/api/databricks/table-preview", async (req, res) => {
  const catalog = req.query.catalog;
  const schema = req.query.schema;
  const table = req.query.table;
  const limit = req.query.limit || 50;

  if (!catalog || !schema || !table) {
    res.status(400).json({
      error: "Missing required query parameters: catalog, schema, table",
    });
    return;
  }

  try {
    const preview = await getTablePreview({ catalog, schema, table, limit });
    res.json({
      catalog,
      schema,
      table,
      count: preview.rows.length,
      columns: preview.columns,
      rows: preview.rows,
    });
  } catch (error) {
    const details = error.response?.data || error.message;
    res.status(500).json({
      error: "Failed to fetch Databricks table preview",
      details,
    });
  }
});

const frontendDistPath = path.resolve(__dirname, "../frontend/dist");
if (fs.existsSync(frontendDistPath)) {
  app.use(express.static(frontendDistPath));

  app.get(/^(?!\/api).*/, (_req, res) => {
    res.sendFile(path.join(frontendDistPath, "index.html"));
  });
}

app.listen(port, () => {
  console.log(`Backend listening on http://localhost:${port}`);
});
