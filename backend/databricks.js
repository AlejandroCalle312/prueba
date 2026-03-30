const axios = require("axios");

const tokenCache = {
  accessToken: null,
  expiresAt: 0,
};

function getEnv(name) {
  const rawValue = process.env[name];
  const value = rawValue?.trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function normalizeHost(rawHost) {
  const trimmed = rawHost.trim().replace(/\/$/, "");
  if (trimmed.startsWith("https://") || trimmed.startsWith("http://")) {
    return trimmed;
  }
  return `https://${trimmed}`;
}

function isSafeIdentifier(value) {
  return /^[A-Za-z0-9_-]+$/.test(value);
}

function quoteIdentifier(value) {
  return `\`${value}\``;
}

async function getAccessToken() {
  if (tokenCache.accessToken && Date.now() < tokenCache.expiresAt) {
    return tokenCache.accessToken;
  }

  const clientId = getEnv("DATABRICKS_CLIENT_ID");
  const clientSecret = getEnv("DATABRICKS_CLIENT_SECRET");
  const tenantId = process.env.AZURE_TENANT_ID?.trim() || process.env.DATABRICKS_TENANT_ID?.trim();

  let tokenResponse;
  if (tenantId) {
    // Azure App Registration flow (Entra ID) for Azure Databricks APIs.
    const body = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: clientId,
      client_secret: clientSecret,
      scope: "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
    });

    tokenResponse = await axios.post(
      `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
      body,
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
      },
    );
  } else {
    // Databricks-native OAuth client credentials fallback.
    const databricksHost = normalizeHost(getEnv("DATABRICKS_HOST"));
    const basicAuth = Buffer.from(`${clientId}:${clientSecret}`).toString("base64");
    const body = new URLSearchParams({
      grant_type: "client_credentials",
      scope: "all-apis",
    });

    tokenResponse = await axios.post(`${databricksHost}/oidc/v1/token`, body, {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization: `Basic ${basicAuth}`,
      },
    });
  }

  const accessToken = tokenResponse.data.access_token;
  const expiresIn = Number(tokenResponse.data.expires_in || 3600);

  tokenCache.accessToken = accessToken;
  tokenCache.expiresAt = Date.now() + Math.max(60, expiresIn - 60) * 1000;

  return accessToken;
}

async function pollStatement(databricksHost, statementId, headers) {
  const timeoutMs = 45_000;
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const response = await axios.get(
      `${databricksHost}/api/2.0/sql/statements/${statementId}`,
      { headers },
    );

    const state = response.data?.status?.state;
    if (state === "SUCCEEDED") {
      return response.data;
    }

    if (state === "FAILED" || state === "CANCELED" || state === "CLOSED") {
      const details = response.data?.status?.error?.message || "Statement execution failed";
      throw new Error(details);
    }

    await new Promise((resolve) => setTimeout(resolve, 1200));
  }

  throw new Error("Timed out waiting for Databricks SQL statement result");
}

function getStatementColumns(statementResponse) {
  return (statementResponse?.manifest?.schema?.columns || []).map((column) => column.name);
}

function toRowObjects(statementResponse) {
  const columns = statementResponse?.manifest?.schema?.columns || [];
  const rows = statementResponse?.result?.data_array || [];

  return rows.map((row) => {
    const mapped = {};
    columns.forEach((column, index) => {
      mapped[column.name] = row[index];
    });
    return mapped;
  });
}

async function runStatement(statement) {
  const databricksHost = normalizeHost(getEnv("DATABRICKS_HOST"));
  const warehouseId = getEnv("DATABRICKS_SQL_WAREHOUSE_ID");
  const accessToken = await getAccessToken();

  const headers = {
    Authorization: `Bearer ${accessToken}`,
    "Content-Type": "application/json",
  };

  const startResponse = await axios.post(
    `${databricksHost}/api/2.0/sql/statements`,
    {
      statement,
      warehouse_id: warehouseId,
      disposition: "INLINE",
      wait_timeout: "10s",
    },
    { headers },
  );

  const state = startResponse.data?.status?.state;
  const statementId = startResponse.data?.statement_id;

  let completedResponse = startResponse.data;
  if (state !== "SUCCEEDED") {
    if (!statementId) {
      throw new Error("Databricks did not return a statement_id for polling");
    }
    completedResponse = await pollStatement(databricksHost, statementId, headers);
  }

  return {
    columns: getStatementColumns(completedResponse),
    rows: toRowObjects(completedResponse),
  };
}

async function getTables({ catalog, schema, limit, table }) {
  if (!isSafeIdentifier(catalog) || !isSafeIdentifier(schema)) {
    throw new Error("Catalog and schema can only contain letters, numbers, _ and -");
  }

  const tableLimit = Number(limit) || 100;
  const boundedLimit = Math.min(Math.max(tableLimit, 1), 1000);

  const requestedCatalog = catalog;
  let activeCatalog = requestedCatalog;

  // SHOW TABLES is the most compatible way across workspaces/catalog setups.
  let showRows;
  try {
    const showTablesStatement = `SHOW TABLES IN ${quoteIdentifier(activeCatalog)}.${quoteIdentifier(schema)}`;
    showRows = (await runStatement(showTablesStatement)).rows;
  } catch (error) {
    const message = String(error?.message || "");
    const canFallbackToHive = requestedCatalog.toLowerCase() === "main" && message.includes("NO_SUCH_CATALOG_EXCEPTION");

    if (!canFallbackToHive) {
      throw error;
    }

    activeCatalog = "hive_metastore";
    const fallbackStatement = `SHOW TABLES IN ${quoteIdentifier(activeCatalog)}.${quoteIdentifier(schema)}`;
    showRows = (await runStatement(fallbackStatement)).rows;
  }

  function mapTableRow(row) {
    return {
      table_catalog: row.catalog || activeCatalog,
      table_schema: row.database || row.namespace || schema,
      table_name: row.tableName || row.table_name || "",
      table_type: row.isTemporary === "true" || row.isTemporary === true ? "TEMPORARY" : "TABLE",
    };
  }

  let normalizedRows = showRows.map(mapTableRow);

  if (normalizedRows.length === 0) {
    try {
      const schemas = (await runStatement(`SHOW SCHEMAS IN ${quoteIdentifier(activeCatalog)}`)).rows;
      const schemaNames = schemas
        .map((row) => row.databaseName || row.namespace || row.schemaName || "")
        .filter(Boolean)
        .filter((name) => name.toLowerCase() !== schema.toLowerCase())
        .filter((name) => name.toLowerCase() !== "information_schema")
        .slice(0, 25);

      for (const schemaName of schemaNames) {
        try {
          const fallbackRows = await runStatement(
            `SHOW TABLES IN ${quoteIdentifier(activeCatalog)}.${quoteIdentifier(schemaName)}`,
          );

          if (fallbackRows.rows.length > 0) {
            normalizedRows = fallbackRows.rows.map((row) => ({
              ...mapTableRow(row),
              table_schema: row.database || row.namespace || schemaName,
            }));
            break;
          }
        } catch (_schemaError) {
          // Ignore inaccessible schemas and keep searching.
        }
      }
    } catch (_schemasError) {
      // Ignore schema-discovery errors and keep empty result.
    }
  }

  if (normalizedRows.length === 0) {
    try {
      const catalogs = (await runStatement("SHOW CATALOGS")).rows;
      const catalogNames = catalogs
        .map((row) => row.catalog || row.catalog_name || row.name || "")
        .filter(Boolean)
        .filter((name) => !["system"].includes(name.toLowerCase()))
        .slice(0, 20);

      for (const catalogName of catalogNames) {
        try {
          const schemas = (await runStatement(`SHOW SCHEMAS IN ${quoteIdentifier(catalogName)}`)).rows;
          const schemaNames = schemas
            .map((row) => row.databaseName || row.namespace || row.schemaName || "")
            .filter(Boolean)
            .filter((name) => name.toLowerCase() !== "information_schema")
            .slice(0, 30);

          for (const schemaName of schemaNames) {
            try {
              const rows = await runStatement(
                `SHOW TABLES IN ${quoteIdentifier(catalogName)}.${quoteIdentifier(schemaName)}`,
              );
              if (rows.rows.length > 0) {
                activeCatalog = catalogName;
                normalizedRows = rows.rows.map((row) => ({
                  ...mapTableRow(row),
                  table_catalog: row.catalog || catalogName,
                  table_schema: row.database || row.namespace || schemaName,
                }));
                break;
              }
            } catch (_listTablesError) {
              // Ignore inaccessible schema and continue searching.
            }
          }

          if (normalizedRows.length > 0) {
            break;
          }
        } catch (_listSchemasError) {
          // Ignore inaccessible catalog and continue searching.
        }
      }
    } catch (_listCatalogsError) {
      // Ignore catalog-discovery errors and keep empty result.
    }
  }

  const tableFilter = String(table || "").trim().toLowerCase();
  if (tableFilter) {
    normalizedRows = normalizedRows.filter((row) =>
      String(row.table_name || "").toLowerCase().includes(tableFilter),
    );
  }

  return normalizedRows.slice(0, boundedLimit).map((row) => ({
    table_catalog: row.catalog || activeCatalog,
    table_schema: row.table_schema || schema,
    table_name: row.table_name || "",
    table_type: row.table_type || "TABLE",
  }));
}

async function getTablePreview({ catalog, schema, table, limit }) {
  if (!isSafeIdentifier(catalog) || !isSafeIdentifier(schema) || !isSafeIdentifier(table)) {
    throw new Error("Catalog, schema and table can only contain letters, numbers, _ and -");
  }

  const previewLimit = Number(limit) || 50;
  const boundedLimit = Math.min(Math.max(previewLimit, 1), 200);
  const statement = [
    "SELECT *",
    `FROM ${quoteIdentifier(catalog)}.${quoteIdentifier(schema)}.${quoteIdentifier(table)}`,
    `LIMIT ${boundedLimit}`,
  ].join("\n");

  return runStatement(statement);
}

module.exports = {
  getTables,
  getTablePreview,
};
