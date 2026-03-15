# CodexIOL — CLAUDE.md

Sistema de asesoría financiera para InvertirOnline (Argentina). Python CLI + FastAPI backend, Next.js frontend, 5-engine advisor framework, SQLite.

---

## Arquitectura general

```
IOL API (invertironline.com)
   ↓
iol_cli/          CLI Typer — scraping, snapshots, órdenes, batch, opportunities
iol_engines/      5 motores de señales (régimen, macro, smart money, oportunidad, estrategia)
iol_advisor/      Capa de servicio para briefings y context packs
iol_reconciliation/ Reconciliación de flujos de caja
iol_shared/       DB queries, métricas, movers (capa compartida)
   ↓
SQLite  data/iol_history.db
   ↓
iol_web/          FastAPI REST — /api/*
   ↓
frontend/         Next.js 15 — dashboard React/TypeScript
```

**Los módulos Python NO se importan entre sí libremente:** `iol_web` y `iol_engines` dependen de `iol_shared`; `iol_cli` coordina todo. Ver `test_architecture_boundaries.py`.

---

## Comandos de desarrollo

### Backend Python

```bash
pip install -e .                   # Instalar en modo dev
python -m pytest -x -q            # Tests (sale al primer fallo)
pytest tests/test_batch.py        # Un módulo
pytest -k advisor                 # Filtrar por patrón
```

### Frontend

```bash
cd frontend
npm install
npm run dev                        # Dev server en :3000
npm run build && npm run start     # Build producción
npm run lint                       # ESLint
npm run type-check                 # TypeScript
```

### Docker (entorno completo)

```bash
docker compose up -d --build                     # CLI + scheduler
docker compose up -d --build web frontend        # API (:8000) + frontend (:3000)
docker exec -it iol-cli iol <comando>
```

### CLI útiles

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

---

## Stack

| Capa | Tecnología |
|------|-----------|
| CLI | Python 3.10+, Typer, Rich |
| API | FastAPI, Uvicorn, sqlite3 |
| Frontend | Next.js 15 App Router, TypeScript strict, Tailwind CSS 3 |
| Componentes | shadcn/ui (Radix UI), Lucide React, Recharts |
| Estado cliente | TanStack Query v5 |
| Forms | React Hook Form + Zod |
| Temas | next-themes (light/dark, CSS variables) |
| Tests | pytest |

---

## Variables de entorno

Copiar `.env.example` → `.env`. Variables requeridas:

```bash
IOL_USERNAME=            # Credenciales InvertirOnline
IOL_PASSWORD=
IOL_API_URL=https://api.invertironline.com
IOL_DB_PATH=data/iol_history.db
IOL_SEC_CONTACT_EMAIL=   # Para SEC EDGAR (smart money engine)
IOL_MARKET_TZ=America/Argentina/Buenos_Aires
```

Frontend en Docker usa `API_BASE_URL=http://web:8000` (servicio interno).

---

## Estructura de módulos Python

- `src/iol_cli/cli.py` — Router principal Typer (792 líneas)
- `src/iol_cli/commands_*.py` — Comandos por dominio
- `src/iol_cli/db_schema.py` — Definición de tablas SQLite
- `src/iol_cli/db_migrations.py` — Versionado de esquema
- `src/iol_cli/iol_client.py` — HTTP wrapper para IOL API
- `src/iol_cli/opportunities.py` — Scoring/ranking de oportunidades (1000+ líneas)
- `src/iol_web/routes_api.py` — Composición de routers `/api/*`
- `src/iol_web/api_*.py` — Endpoints por dominio
- `src/iol_engines/registry.py` — Orquestador de los 5 motores
- `src/iol_shared/portfolio_db.py` — Queries reutilizables de DB

---

## Frontend — convenciones

- **Páginas:** `frontend/src/app/<feature>/page.tsx`
- **Componentes de feature:** `frontend/src/features/<feature>/`
- **UI atoms compartidos:** `frontend/src/components/ui/`
- **Layout:** `frontend/src/components/layout/` (Sidebar, Footer)
- **API calls:** siempre via `frontend/src/lib/api.ts` (tipadas)
- **Tipos de dominio:** `frontend/src/types/*.ts`
- **Colores:** usar tokens CSS (`text-foreground`, `bg-surface`, `text-muted-foreground`) — nunca hex
- **Íconos:** Lucide React únicamente
- **Charts:** Recharts con `<ResponsiveContainer>`
- **No `any` en TypeScript**

**Done criteria para cualquier componente:**
- TypeScript limpio (sin `any`)
- Responsive (375px → 1280px)
- Light + dark mode
- Estados de loading / empty / error
- Build sin warnings

---

## Los 5 motores (iol_engines)

Corren en secuencia con caché en DB para evitar re-fetch innecesario:

| # | Motor | Staleness | Fuente |
|---|-------|-----------|--------|
| 1 | Regime | 1 día | VIX, RSI, volatilidad |
| 2 | Macro | 1 día | CPI Argentina, índices globales |
| 3 | Smart Money | 7 días | SEC EDGAR 13F |
| 4 | Opportunity | adapter | Pondera señales anteriores |
| 5 | Strategy | siempre fresco | Solo lecturas de DB |

Endpoints: `GET /api/engines/signals`, `GET /api/engines/plan`

---

## Base de datos SQLite

Tablas principales (ver `src/iol_cli/db_schema.py` para definición completa):

- `portfolio_snapshots`, `portfolio_assets`, `portfolio_transactions`
- `advisor_alerts`, `advisor_events`, `advisor_opportunities_runs`, `advisor_opportunities_scores`
- `engine_regime_snapshots`, `engine_macro_snapshots`, `engine_smart_money_signals`, `engine_strategy_plans`
- `manual_cashflow_adjustments`, `cashflow_auto_detected`, `reconciliation_proposals`

**En tests:** usar `create_temp_sqlite_db()` de `tests/tests_support.py`. No mockear la DB.

---

## Arquitectura de prompts (advisor)

- `AsesorFinanciero.md` — Wrapper de entrada estable
- `prompts/00_orquestador.md` — Enruta por intención
- `prompts/10_context_pack.md` — Contrato de entrada del analista
- `prompts/contracts/output_schema.md` — Esquema de salida
- `prompts/70_advisor_daily_automation.md` / `80_advisor_weekly_automation.md` — Briefings automáticos

---

## Mercado y dominio

- **Broker:** InvertirOnline (Argentina)
- **Mercados:** BCBA, CEDEARs (ADRs cotizados en pesos)
- **Monedas:** ARS, USD
- **Horario:** 11:00–18:00 `America/Argentina/Buenos_Aires`
- **Fechas:** ISO 8601 (`YYYY-MM-DD`) en toda la codebase

---

## Qué NO hacer

- No importar `iol_web` desde `iol_cli` ni viceversa — usar `iol_shared` como puente.
- No mockear la DB en tests — usar bases temporales reales (ver `tests_support.py`).
- No agregar Jinja2, templates HTML ni static files al backend — el frontend es Next.js.
- No usar `any` en TypeScript.
- No hardcodear colores hex en componentes — usar tokens de Tailwind.
- No ejecutar órdenes reales sin pasar por `batch validate` primero.
- Los directorios `data/` y `reports/` están en `.gitignore` — no commitear datos reales.
