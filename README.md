# Mini proyecto: React + Node + Databricks

Proyecto local preparado para:

- Frontend en React que muestra una tabla de Databricks.
- Backend en Node.js/Express que expone API y sirve el frontend en produccion.
- Autenticacion a Databricks por App Registration (OAuth client credentials).
- Configuracion con variables de entorno para despliegue en Azure Web App.

## Estructura

- `frontend`: aplicacion React (Vite)
- `backend`: API Express + cliente Databricks

## Requisitos

- Node.js 20+
- Credenciales de App Registration con permisos en Databricks
- SQL Warehouse habilitado en Databricks

## Configuracion local

1. Configura variables en `backend/.env`.
2. Opcional: ajusta `frontend/.env` si necesitas `VITE_API_BASE_URL`.

## Ejecutar en local (modo desarrollo)

Terminal 1:

```bash
cd backend
npm run dev
```

Terminal 2:

```bash
cd frontend
npm run dev
```

Frontend: http://localhost:5173
Backend: http://localhost:3001

## Probar endpoint

```bash
curl "http://localhost:3001/api/databricks/tables?catalog=main&schema=default&limit=100"
```

## Build para produccion

```bash
cd frontend
npm run build
cd ../backend
npm start
```

Cuando existe `frontend/dist`, el backend sirve el frontend y la API desde el mismo host.

## Azure Web App

Configura estas Application Settings en Azure (no en codigo):

- `DATABRICKS_HOST`
- `DATABRICKS_CLIENT_ID`
- `DATABRICKS_CLIENT_SECRET`
- `AZURE_TENANT_ID` (si usas App Registration de Entra ID)
- `DATABRICKS_SQL_WAREHOUSE_ID`
- `DATABRICKS_DEFAULT_CATALOG` (opcional)
- `DATABRICKS_DEFAULT_SCHEMA` (opcional)
- `PORT` (opcional, Azure suele inyectarla)

Comando de inicio recomendado en Azure Web App:

```bash
node backend/server.js
```

Asegurate de desplegar tambien `frontend/dist` (compilado) junto al backend.
