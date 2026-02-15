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
IOL_MARKET_OPEN_TIME=11:00
IOL_MARKET_CLOSE_TIME=18:00
IOL_SNAPSHOT_INTERVAL_MIN=5
IOL_STORE_RAW=0
IOL_SEC_CONTACT_EMAIL=tu-email@dominio.com
# Optional override:
# IOL_SEC_USER_AGENT=CodexIOL/1.0 (tu-email@dominio.com)
IOL_SEC_USER_AGENT=
```

## Docker
Levantar contenedor persistente:

```
docker compose up -d --build
```

## Autoarranque (Windows)
- Este `docker-compose.yml` incluye `restart: unless-stopped`, asi que los contenedores se levantan solos cuando Docker arranca.
- Asegurate de tener habilitado en Docker Desktop: `Settings -> General -> Start Docker Desktop when you log in`.
- Opcional: instalar una tarea programada (por usuario) que corre `docker compose up -d` al iniciar sesion:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/install_startup_task.ps1
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

Atajo opcional (PowerShell) para no repetir `docker exec`:

```powershell
function iolc { docker exec -it iol-cli iol @args }
# ejemplo: iolc advisor context
```

Scheduler de snapshots (cron + catch-up al iniciar):

```
docker exec -it iol-scheduler tail -n 50 /var/log/cron.log
```

## Comandos principales

Nota: los comandos `iol ...` de esta seccion asumen que estas dentro del contenedor.
Si estas en host, antepone `docker exec -it iol-cli` (o usa `iolc`).

```
iol portfolio --country argentina

iol advisor context

iol advisor context --out data/context/latest.json

iol advisor context --format md

iol advisor alert create --type concentration --title "Peso alto SPY" --description "SPY supera 30%" --severity high --symbol SPY

iol advisor alert list --status open

iol advisor alert close --id 1 --reason "Se rebalanceo con compras en otros activos"

iol advisor event add --type portfolio --title "Rebalanceo parcial" --description "Se ejecuto tanda 1" --symbol SPY

iol advisor event list --type portfolio

iol advisor evidence add --symbol SPY --query "SPY expense ratio" --source-name "Issuer" --source-url "https://example.com" --claim "expense ratio ... " --confidence high --date-confidence high

iol advisor evidence list --symbol SPY --days 60

iol advisor opportunities snapshot-universe --universe bcba_cedears

iol advisor opportunities run --mode both --budget-ars 200000 --top 10

iol advisor opportunities list-runs --limit 20

iol advisor opportunities report --run-id 1 --out reports/latest/Oportunidades.md

iol advisor seguimiento --out reports/latest/Seguimiento.md

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

iol data export --table advisor_alerts --format json

iol data export --table advisor_events --format json
```

## Outputs / Reports
Este repo genera 2 tipos de outputs:

Privacidad:
- Se versiona solo la estructura de `data/` y `reports/`.
- Los archivos reales generados (reportes, evidencias, snapshots, DB) no se publican.

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

## Arquitectura de prompts
El asesor usa una arquitectura modular con un wrapper estable:

- Entrada estable: `AsesorFinanciero.md`
- Orquestador: `prompts/00_orquestador.md`
- Contrato de contexto: `prompts/10_context_pack.md`
- Prompts especializados:
  - `prompts/20_portfolio_analysis.md`
  - `prompts/30_macro_sources.md`
  - `prompts/40_symbol_web_research.md`
  - `prompts/45_opportunity_scoring.md`
  - `prompts/50_alerts_events.md`
  - `prompts/60_safe_execution.md`
- Contratos de salida/evidencia:
  - `prompts/contracts/output_schema.md`
  - `prompts/contracts/evidence_schema.md`

Flujo recomendado:
1. Cargar `AsesorFinanciero.md`.
2. Generar contexto con `iol advisor context`.
3. Enrutar por intencion via `prompts/00_orquestador.md`.
4. Emitir salida con contrato y registrar en BD cuando corresponda.

## Motor de oportunidades
Pipeline semanal semiautomatico para detectar compras nuevas y recompras (BCBA + CEDEARs):

1. Refrescar contexto:
   - `iol advisor context`
2. Refrescar universo y precios:
   - `iol advisor opportunities snapshot-universe --universe bcba_cedears`
3. Cargar evidencia web estructurada (opcional manual):
   - `iol advisor evidence add ...`
4. Fetch semiautomatico de evidencia (opcional standalone):
   - `iol advisor evidence fetch --from-context --max-symbols 15 --per-source-limit 2`
5. Ejecutar scoring/ranking (incluye auto-fetch por defecto):
   - `iol advisor opportunities run --mode both --budget-ars <ARS> --top 10`
6. Generar reporte:
   - `iol advisor opportunities report --run-id <id> --out reports/latest/Oportunidades.md`

Notas:
- No ejecuta ordenes reales.
- Para operar, usar simulacion/confirmacion del protocolo seguro.
- Desactivar auto-fetch en run: `--no-fetch-evidence`.
- Runbook rápido (11 pasos): `docs/Runbook_Oportunidades_Semanal.md`

## Notas
- `IOL_API_URL` define la URL de la API (no se usa sandbox).
- Para SEC/EDGAR usar un `User-Agent` identificado con contacto:
  - `IOL_SEC_CONTACT_EMAIL=tu-email@dominio.com` o
  - `IOL_SEC_USER_AGENT="CodexIOL/1.0 (tu-email@dominio.com)"`
  - Si no, algunos endpoints como `www.sec.gov/files/company_tickers.json` pueden devolver `403`.
- El snapshot diario guarda `cash_disponible_ars`/`cash_disponible_usd` y usa `totalEnPesos` de `/api/v2/estadocuenta` para el total.
- `iol snapshot run` evita pisar un snapshot existente si el nuevo estÃ¡ mÃ¡s lejos del horario de cierre (por ejemplo, si lo corrÃ©s durante horario de mercado). UsÃ¡ `iol snapshot run --force` para sobrescribir igualmente.
- Las ordenes individuales requieren confirmacion (prompt interactivo o `--confirm CONFIRMAR`).
- `iol batch run` solo ejecuta si pasas `--confirm CONFIRMAR` (sin eso se comporta como dry-run).
- Para automatizacion, podes ejecutar sin prompt interactivo usando `--confirm CONFIRMAR` (igual sigue siendo una orden real).
- `IOL_COMMISSION_RATE` y `IOL_COMMISSION_MIN` se usan solo para simulacion local.
- `IOL_STORE_RAW=1` guarda JSON crudo en la BD.
- `reports/latest/Seguimiento.md` es una vista resumida; la fuente de verdad para alertas/eventos es SQLite (`advisor_alerts`, `advisor_events`).

## Trading (protocolo seguro)
- Este proyecto puede ENVIAR ordenes reales a IOL. Usalo con extremo cuidado.
- Recomendado: el agente prepara (cotizacion/simulacion) y vos ejecutas el comando final con `--confirm CONFIRMAR`.
- Flujo sugerido:
- Paso 1 (cotizacion): `iol market quote --market bcba --symbol <SIMBOLO>`
- Paso 2 (simulacion): `iol order simulate --side buy|sell ...`
- Paso 3 (ejecucion real): `iol order buy|sell ... --confirm CONFIRMAR` o `iol order confirm <confirmation_id> --confirm CONFIRMAR`
- Para muchas ordenes: usar `iol batch validate` y luego `iol batch run --confirm CONFIRMAR` con un JSON plan temporal (ver `iol batch template`).
