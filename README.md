# SRF Databricks Web App

Web application for IT Service Management analytics, deployed on Azure Web App.

- FastAPI backend connected to Databricks SQL Warehouse.
- Static frontend serving Ticket Lifecycle and Resolution Score Engine dashboards.
- Authentication via Azure Entra ID (client credentials).
- Data sourced from Jira tickets and activity tables in Databricks (`axsa_prod_bronze`).

## Estado y memoria del proyecto

### Cerrado y validado

- Despliegue en Azure Web App operativo con pipeline en verde.
- Ticket Lifecycle alineado entre local y Web App para el mismo ticket en Databricks.
- Se corrigio la vista de actividad completa en lifecycle para mostrar Jira Activity (Raw).
- Se corrigio el parser de transiciones para evitar valores tecnicos tipo `ari:cloud:identity::user/...`.
- Se aplico cache-busting de frontend para evitar que produccion cargara JS antiguo.
- Configuracion de produccion alineada a tablas `axsa_prod_bronze` en Databricks.

### Pendiente

- Provision de nueva Landing Zone publica (solicitud IT Hub en curso).
- Cierre de permisos/acceso en Cloudflare Access segun policy corporativa.
- Cierre de IAM/SaaS para el nuevo flujo de acceso final.

### Historial breve (resumen)

- Se creo y fusiono la rama de fix para visibilidad completa de actividad de Jira.
- Se desplego el cambio a `main` y se valido en Web App.
- Se detecto diferencia entre local y Web App por configuracion/entorno y se alineo.
- Se revalido con tickets reales que la salida de lifecycle era consistente.

## Estructura

- `serving/srf-axsa-api/` — Backend FastAPI (Python, uvicorn) + cliente Databricks SQL
- `presentation/ticket-lifecycle/` — Frontend estatico (HTML, JS, CSS) con dashboards de Ticket Lifecycle y Score Engine

## Requisitos

- Python 3.12+
- Credenciales de Azure Entra ID con permisos en Databricks (o Azure CLI autenticado)
- SQL Warehouse habilitado en Databricks

## Configuracion local

1. Configura variables en `serving/srf-axsa-api/.env` (ver `.env.example`).
2. Opcional: ajusta `presentation/ticket-lifecycle/app-config.local.js` para apuntar la API a otro host.

## Ejecutar en local

Terminal 1 (backend):

```bash
cd serving/srf-axsa-api
pip install -r requirements.txt
python -m uvicorn app:app --host 127.0.0.1 --port 8000 --reload
```

Terminal 2 (frontend):

```bash
cd presentation/ticket-lifecycle
python -m http.server 3000
```

Frontend: http://localhost:3000
Backend API: http://localhost:8000

## Probar endpoint

```bash
curl "http://localhost:8000/api/ticket-lifecycle/score-engine?months=2026-02,2026-03"
```

## Azure Web App

Configura estas Application Settings en Azure:

- `DATABRICKS_WORKSPACE_URL`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_TICKETS_TABLE`
- `DATABRICKS_CLIENT_ID` (si usas client credentials)
- `DATABRICKS_CLIENT_SECRET`
- `AZURE_TENANT_ID`
- `CORS_ALLOWED_ORIGINS`

El despliegue se realiza automaticamente via GitHub Actions al hacer push a `main`.

---

## Resolution Score Engine

Dashboard analitico que evalua el rendimiento de los grupos de resolucion de incidencias basandose en datos de Jira extraidos desde Databricks.

### Que mide

Analiza **incidencias cerradas/resueltas** que pasaron por SMC (`smc_assignments = 1`) en los meses seleccionados.

### Columnas del dashboard

| Columna | Descripcion |
|---------|-------------|
| **Received** | Tickets que el grupo recibio (incluye los que luego escalo a otros grupos). Se calcula usando el historial de reasignaciones de la tabla de actividad de Jira. |
| **Resolved** | Tickets que el grupo resolvio (status = Closed o Resolved y el grupo es el asignado final). |
| **Rate** | Tasa de resolucion del grupo: `resolved / received`. Mide eficiencia individual. |
| **Share** | Cuota de resolucion del grupo respecto al total: `resolved_grupo / resolved_total`. Mide peso del grupo en el total. |
| **Avg Resolution** | Tiempo medio de resolucion de todos los tickets resueltos por el grupo (`updated_in - created_in`). |
| **Median** | Mediana del tiempo de resolucion (menos sensible a outliers que la media). |
| **Speed Score** | Puntuacion de velocidad comparada con el grupo mas lento (0-100, mayor = mas rapido). |
| **Forecast** | Prediccion del share para el proximo mes, basada en regresion lineal sobre los shares mensuales. Incluye flecha de tendencia e indicador de confianza. |

### Forecast (prediccion)

Usa **regresion lineal** sobre el share mensual de cada grupo:

1. Calcula el share (%) del grupo en cada mes seleccionado por separado.
2. Ajusta una recta `y = a + bx` a esos puntos.
3. Predice el share para el mes siguiente (`x = n`).

La flecha indica la tendencia:
- ▲ (verde): subiendo (`pendiente > 0.5`)
- ▼ (roja): bajando (`pendiente < -0.5`)
- ▶ (amarilla): estable

El indicador de confianza compara el forecast con el share actual:
- **high** (verde): el forecast sube respecto al share actual
- **medium** (amarillo): el forecast se mantiene similar
- **low** (rojo): el forecast baja respecto al share actual

Cuantos mas meses se seleccionen, mas precisa es la prediccion.

### Filtros aplicados en la query

- Solo incidencias: `type LIKE '%incident%'`
- Solo tickets que pasaron por SMC: `smc_assignments = 1`
- Solo cerrados/resueltos: `status IN ('resolved', 'closed')`
- Duracion positiva: `updated_in - created_in > 0`
- Meses filtrados por `updated_in` (fecha de cierre/resolucion en timezone Europe/Madrid)

### Datos de "Received"

El conteo de tickets recibidos por grupo se calcula cruzando la tabla de tickets con la tabla de actividad (`activity`). Se buscan eventos `updated = 'Assignment group'` con `content` en formato `"GroupA --> GroupB"`, lo que permite rastrear todos los grupos por los que paso cada ticket, no solo el grupo asignado final.

### Archivos involucrados

- `serving/srf-axsa-api/databricks_client.py` — Metodo `_fetch_score_engine()` con la query SQL principal y el calculo de forecast.
- `serving/srf-axsa-api/app.py` — Endpoint `GET /api/ticket-lifecycle/score-engine?months=...&limit=...`
- `presentation/ticket-lifecycle/app.js` — Logica del frontend (tabla, flechas de tendencia, clustering de Others).
- `presentation/ticket-lifecycle/index.html` — Estructura HTML de la tabla.
- `presentation/ticket-lifecycle/styles.css` — Estilos (barras de velocidad, badges de confianza, flechas de tendencia).


