# AGENTS.md

## Proposito de este proyecto

CodexIOL es un sistema de asesoria financiera para InvertirOnline Argentina. Incluye Python CLI, backend FastAPI, frontend Next.js, framework advisor de 5 motores y SQLite.

Este archivo reemplaza funcionalmente a `CLAUDE.md` para agentes Codex. Si ambos archivos difieren, para Codex seguir primero `AGENTS.md` y luego usar `CLAUDE.md` solo como referencia historica.

## Arquitectura general

```text
IOL API (invertironline.com)
   ->
iol_cli/              CLI Typer: scraping, snapshots, ordenes, batch, opportunities
iol_engines/          5 motores: regime, macro, smart money, opportunity, strategy
iol_advisor/          Briefings y context packs
iol_reconciliation/   Reconciliacion de flujos de caja
iol_shared/           DB queries, metricas, movers y utilidades compartidas
   ->
SQLite data/iol_history.db
   ->
iol_web/              FastAPI REST: /api/*
   ->
frontend/             Next.js 15 dashboard React/TypeScript
```

Los modulos Python no se importan entre si libremente:

- `iol_web` y `iol_engines` dependen de `iol_shared`;
- `iol_cli` coordina;
- no importar `iol_web` desde `iol_cli` ni viceversa;
- validar limites con `test_architecture_boundaries.py`.

## Entorno real

El entorno real es Docker. El frontend proxea `/api/*` a `http://web:8000`, hostname interno Docker. Fuera de Docker esa URL no resuelve.

Comandos principales:

```bash
docker compose up -d --build
docker compose up -d --build web frontend
docker exec -it iol-cli iol <comando>
docker compose logs -f frontend
docker compose logs -f web
```

No usar `preview_start`, `uvicorn` local ni pruebas UI en localhost fuera de Docker para validar el sistema real.

## Comandos de desarrollo

Backend:

```bash
pip install -e .
python -m pytest -x -q
pytest tests/test_batch.py
pytest -k advisor
```

Frontend:

```bash
cd frontend
npm install
npm run dev
npm run build && npm run start
npm run lint
npm run type-check
```

CLI utiles:

```bash
iol auth test
iol snapshot run
iol advisor context --out data/context/latest.json
iol advisor opportunities run --mode both --budget-ars 200000 --top 10
iol advisor autopilot run --cadence daily --out reports/latest/AdvisorDaily.md
iol batch validate --plan data/plans/plan.json
iol batch run --plan data/plans/plan.json --confirm CONFIRMAR
iol data query "SELECT snapshot_date, total_value FROM portfolio_snapshots"
```

## Seguridad financiera

- No ejecutar ordenes reales sin confirmacion explicita y verificable del usuario.
- Antes de cualquier orden real, pasar por `iol batch validate`.
- `--confirm CONFIRMAR` implica operacion real; no usarlo salvo autorizacion explicita.
- No exponer credenciales, tokens ni secretos en Markdown, JSON, logs o respuestas.
- `.env`, `data/` y `reports/` pueden contener datos sensibles o reales; no publicarlos ni commitearlos.

## Stack

| Capa | Tecnologia |
| --- | --- |
| CLI | Python 3.10+, Typer, Rich |
| API | FastAPI, Uvicorn, sqlite3 |
| Frontend | Next.js 15 App Router, TypeScript strict, Tailwind CSS 3 |
| Componentes | shadcn/ui, Radix UI, Lucide React, Recharts |
| Estado cliente | TanStack Query v5 |
| Forms | React Hook Form + Zod |
| Temas | next-themes, CSS variables |
| Tests | pytest |

## Variables de entorno

Copiar `.env.example` a `.env`. Variables principales:

```bash
IOL_USERNAME=
IOL_PASSWORD=
IOL_API_URL=https://api.invertironline.com
IOL_DB_PATH=data/iol_history.db
IOL_MARKET_TZ=America/Argentina/Buenos_Aires
IOL_SEC_CONTACT_EMAIL=
IOL_OPP_WATCHLIST=
```

Frontend en Docker usa `API_BASE_URL=http://web:8000`.

## Estructura relevante

- `src/iol_cli/cli.py`: router principal Typer.
- `src/iol_cli/commands_*.py`: comandos por dominio.
- `src/iol_cli/db_schema.py`: definicion de tablas SQLite.
- `src/iol_cli/db_migrations.py`: migraciones livianas.
- `src/iol_cli/iol_client.py`: wrapper HTTP para IOL API.
- `src/iol_cli/opportunities.py`: scoring/ranking de oportunidades.
- `src/iol_web/routes_api.py`: composicion de routers `/api/*`.
- `src/iol_web/api_*.py`: endpoints por dominio.
- `src/iol_engines/registry.py`: orquestador de motores.
- `src/iol_shared/portfolio_db.py`: queries reutilizables.

## Frontend

Convenciones:

- Paginas: `frontend/src/app/<feature>/page.tsx`.
- Componentes de feature: `frontend/src/features/<feature>/`.
- UI compartida: `frontend/src/components/ui/`.
- Layout: `frontend/src/components/layout/`.
- API calls: siempre via `frontend/src/lib/api.ts`.
- Tipos de dominio: `frontend/src/types/*.ts`.
- Colores: usar tokens CSS/Tailwind, no hex hardcodeado.
- Iconos: Lucide React.
- Charts: Recharts con `ResponsiveContainer`.
- No usar `any` en TypeScript.

Criterios de terminado para componentes:

- TypeScript limpio.
- Responsive entre 375px y 1280px.
- Light y dark mode.
- Estados de loading, empty y error.
- Build sin warnings.

## Motores

Los 5 motores corren en secuencia con cache en DB:

| Motor | Staleness | Fuente |
| --- | --- | --- |
| Regime | 1 dia | VIX, RSI, volatilidad |
| Macro | 1 dia | CPI Argentina, indices globales |
| Smart Money | 7 dias | SEC EDGAR 13F |
| Opportunity | adapter | Pondera senales anteriores |
| Strategy | siempre fresco | Solo lecturas de DB |

Endpoints:

- `GET /api/engines/regime`
- `GET /api/engines/macro`
- `GET /api/engines/smart-money`
- `GET /api/engines/strategy`
- `GET /api/engines/accuracy`
- `POST /api/engines/run-all`

## Base de datos

Tablas principales:

- `portfolio_snapshots`, `portfolio_assets`, `portfolio_transactions`
- `advisor_alerts`, `advisor_events`, `advisor_opportunities_runs`, `advisor_opportunities_scores`
- `opportunity_watchlist`
- `engine_regime_snapshots`, `engine_macro_snapshots`, `engine_smart_money_snapshots`, `engine_strategy_runs`
- `manual_cashflow_adjustments`, `cashflow_auto_detected`, `reconciliation_proposals`

Notas de esquema para consultas SQL directas:

- `portfolio_assets` se vincula con `portfolio_snapshots` por `snapshot_date`; no existe `snapshot_id`.
- Los valores monetarios persistidos en DB usan `total_value`. `total_value_ars` es un nombre del context pack/reporte, no una columna SQL.
- La composición por activo se calcula como `portfolio_assets.total_value * 100 / portfolio_snapshots.total_value`; no existe una columna persistida `pct_portfolio`.

En tests usar `create_temp_sqlite_db()` de `tests/tests_support.py`. No mockear la DB.

## Advisor y prompts

- `AsesorFinanciero.md`: wrapper estable de entrada.
- `prompts/00_orquestador.md`: enruta por intencion.
- `prompts/10_context_pack.md`: contrato de entrada.
- `prompts/contracts/output_schema.md`: esquema de salida.
- `prompts/contracts/evidence_schema.md`: evidencia.
- `prompts/70_advisor_daily_automation.md`: briefing diario.
- `prompts/80_advisor_weekly_automation.md`: briefing semanal.

Para automatizacion diaria/semanal usar:

```bash
iol advisor autopilot run --cadence daily
iol advisor autopilot run --cadence weekly
```

## Automatizaciones migradas desde Claude Code

El export original vive en `data/scheduled_tasks_export.json` y proviene de `Claude Code MCP scheduled-tasks + ~/.claude/scheduled-tasks/`.

Las automatizaciones recurrentes fueron recreadas en Codex:

- `daily-market-close-briefing`
- `weekly-portfolio-review`
- `engine-staleness-monitor`
- `simulation-accuracy-report`
- `bot-daily-pipeline`
- `bot-weekly-performance`

La tarea historica `review-adrdola-suscripcion` quedo pausada en Codex como revision sin ejecucion real, porque el export original estaba deshabilitado y contenia una operacion financiera.

## Mercado y dominio

- Broker: InvertirOnline Argentina.
- Mercados: BCBA y CEDEARs.
- Monedas: ARS y USD.
- Horario BCBA: 11:00 a 18:00 `America/Argentina/Buenos_Aires`.
- Fechas: ISO 8601 `YYYY-MM-DD`.

## Que no hacer

- No usar `preview_start`.
- No validar UI contra localhost fuera de Docker.
- No iniciar servidores locales para reemplazar el entorno Docker real.
- No importar `iol_web` desde `iol_cli` ni `iol_cli` desde `iol_web`.
- No mockear DB en tests.
- No agregar Jinja2, templates HTML ni static files al backend; el frontend es Next.js.
- No usar `any` en TypeScript.
- No hardcodear colores hex en componentes.
- No ejecutar ordenes reales sin `batch validate` y confirmacion explicita.
- No commitear datos reales de `data/` ni `reports/`.
