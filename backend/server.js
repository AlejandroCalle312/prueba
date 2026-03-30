const fs = require("fs");
const path = require("path");
const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const { getTables } = require("./databricks");

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
  const limit = req.query.limit || 100;

  try {
    const tables = await getTables({ catalog, schema, limit });
    res.json({
      catalog,
      schema,
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
