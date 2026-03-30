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

async function getTables({ catalog, schema, limit }) {
  if (!isSafeIdentifier(catalog) || !isSafeIdentifier(schema)) {
    throw new Error("Catalog and schema can only contain letters, numbers, _ and -");
  }

  const tableLimit = Number(limit) || 100;
  const boundedLimit = Math.min(Math.max(tableLimit, 1), 1000);

  const databricksHost = normalizeHost(getEnv("DATABRICKS_HOST"));
  const warehouseId = getEnv("DATABRICKS_SQL_WAREHOUSE_ID");
  const accessToken = await getAccessToken();

  const headers = {
    Authorization: `Bearer ${accessToken}`,
    "Content-Type": "application/json",
  };

  const statement = [
    "SELECT table_catalog, table_schema, table_name, table_type",
    "FROM system.information_schema.tables",
    `WHERE table_catalog = '${catalog}' AND table_schema = '${schema}'`,
    "ORDER BY table_name",
    `LIMIT ${boundedLimit}`,
  ].join("\n");

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

  return toRowObjects(completedResponse);
}

module.exports = {
  getTables,
};
