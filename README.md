# IOL CLI (Docker)

CLI para consultar y operar con la API de InvertirOnline (IOL) desde consola.

## Requisitos
- Docker y Docker Compose

## Configuracion
1. Copia `.env.example` a `.env`
2. Completa tus credenciales y URLs:

```
IOL_USERNAME=tu_usuario
IOL_PASSWORD=tu_password
IOL_API_URL=https://api.invertironline.com
IOL_TIMEOUT=20
IOL_COMMISSION_RATE=0.0
IOL_COMMISSION_MIN=0.0
IOL_DB_PATH=data/iol_history.db
IOL_MARKET_TZ=America/Argentina/Buenos_Aires
IOL_MARKET_CLOSE_TIME=18:00
IOL_STORE_RAW=0
```

## Docker
Levantar contenedor persistente:

```
docker compose up -d --build
```

## Web dashboard (portfolio)
Levantar la web app (lee snapshots desde `data/iol_history.db`):

```
docker compose up -d --build web
```

Abrir:

```
http://localhost:8000/
```

Ejecutar comandos:

```
docker exec -it iol-cli iol auth test
```

Scheduler de snapshots (cron + catch-up al iniciar):

```
docker exec -it iol-scheduler tail -n 50 /var/log/cron.log
```

## Comandos principales

```
iol portfolio --country argentina

iol advisor context

iol advisor context --out data/context/latest.json

iol advisor context --format md

iol market quote --market bcba --symbol GGAL

iol orders list --status pendientes

iol order buy --market bcba --symbol GGAL --quantity 10 --price 1000 --plazo t0 --order-type limit

iol order sell --market bcba --symbol GGAL --quantity 10 --price 1000 --plazo t0 --order-type limit

iol order sell --market bcba --symbol ALUA --quantity 1 --price 1000 --plazo t1 --order-type limit --confirm CONFIRMAR

iol order simulate --side buy --market bcba --symbol GGAL --quantity 10 --price 1000

iol order confirm <confirmation_id>

iol order confirm <confirmation_id> --confirm CONFIRMAR

iol batch template

iol batch from-md --md reports/rebalance/ResumenRebalanceo_2026-02-07.md --out data/plans/plan_20260207_010000.json

# o (acceso rapido al ultimo reporte)
iol batch from-md --md reports/latest/ResumenRebalanceo.md --out data/plans/plan_latest.json

iol batch validate --plan data/plans/plan_20260207_010000.json --price-mode fast

iol batch run --plan data/plans/plan_20260207_010000.json

iol batch run --plan data/plans/plan_20260207_010000.json --confirm CONFIRMAR

iol fci subscribe --symbol FCI_TEST --amount 10000

iol fci redeem --symbol FCI_TEST --quantity 5

iol raw GET /api/v2/portafolio/argentina

iol snapshot run

iol snapshot catchup

iol snapshot backfill --from 2026-01-01 --to 2026-02-01

iol data query "SELECT snapshot_date, total_value FROM portfolio_snapshots ORDER BY snapshot_date"

iol data export --table portfolio_assets --format csv

iol data export --table batch_runs --format json

iol data export --table batch_ops --format json
```

## Outputs / Reports
Este repo genera 2 tipos de outputs:

1) **Context pack (fuente primaria del analisis)**
- `data/context/latest.json`
- `data/context/latest.md`

2) **Reportes Markdown del asesor**
- Por tipo:
  - `reports/analisis/`
  - `reports/macro/`
  - `reports/rebalance/`
  - `reports/dca/`
- Acceso rapido a lo ultimo (copias "latest"):
  - `reports/latest/AnalisisPortafolio.md`
  - `reports/latest/Macro.md`
  - `reports/latest/ResumenRebalanceo.md`
  - `reports/latest/PlanDCA.md`
  - `reports/latest/Seguimiento.md`

Evidencia (JSON crudo, opcional):
- Por fecha: `data/evidence/<YYYY-MM-DD>/...`
- Ultimo: `data/evidence/latest/...`

## Notas
- `IOL_API_URL` define la URL de la API (no se usa sandbox).
- El snapshot diario guarda `cash_disponible_ars`/`cash_disponible_usd` y usa `totalEnPesos` de `/api/v2/estadocuenta` para el total.
- Las ordenes individuales requieren confirmacion (prompt interactivo o `--confirm CONFIRMAR`).
- `iol batch run` solo ejecuta si pasas `--confirm CONFIRMAR` (sin eso se comporta como dry-run).
- Para automatizacion, podes ejecutar sin prompt interactivo usando `--confirm CONFIRMAR` (igual sigue siendo una orden real).
- `IOL_COMMISSION_RATE` y `IOL_COMMISSION_MIN` se usan solo para simulacion local.
- `IOL_STORE_RAW=1` guarda JSON crudo en la BD.

## Trading (protocolo seguro)
- Este proyecto puede ENVIAR ordenes reales a IOL. Usalo con extremo cuidado.
- Recomendado: el agente prepara (cotizacion/simulacion) y vos ejecutas el comando final con `--confirm CONFIRMAR`.
- Flujo sugerido:
- Paso 1 (cotizacion): `iol market quote --market bcba --symbol <SIMBOLO>`
- Paso 2 (simulacion): `iol order simulate --side buy|sell ...`
- Paso 3 (ejecucion real): `iol order buy|sell ... --confirm CONFIRMAR` o `iol order confirm <confirmation_id> --confirm CONFIRMAR`
- Para muchas ordenes: usar `iol batch validate` y luego `iol batch run --confirm CONFIRMAR` con un JSON plan temporal (ver `iol batch template`).
