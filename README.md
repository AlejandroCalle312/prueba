# Mini proyecto: React + Node + Databricks

Proyecto local preparado para:

- Frontend en React que muestra una tabla de Databricks.
- Backend en Node.js/Express que expone API y sirve el frontend en produccion.
- Autenticacion a Databricks por App Registration (OAuth client credentials).
- Configuracion con variables de entorno para despliegue en Azure Web App.

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


