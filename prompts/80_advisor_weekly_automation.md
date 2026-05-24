# Advisor Weekly Automation

Objetivo: correr el weekly deep y publicar un briefing semanal estable, usando el esquema real de SQLite y el acceso Docker de Codex.

## Reglas de ejecución

- No ejecutar órdenes reales, `batch run`, ni comandos con `--confirm CONFIRMAR`.
- Usar Docker con contexto explícito `codex-desktop-linux`.
- Mantener `$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'` en los comandos Docker.
- Las consultas SQL deben empezar con `SELECT`; `iol data query` rechaza CTEs que empiezan con `WITH`.
- Las consultas SQL deben usar el esquema real:
  - `portfolio_snapshots.snapshot_date`
  - `portfolio_snapshots.total_value`
  - `portfolio_assets.snapshot_date`
  - `portfolio_assets.total_value`
  - `portfolio_assets.daily_var_pct`
  - `advisor_opportunity_candidates.candidate_status`
- No usar columnas inexistentes como `snapshot_id`, `current_value_ars`, `pct_portfolio`, `total_value_ars`, `operable_count`, `watchlist_count`, `suppressed_count` o `rejected_count`.
- Para composición, calcular el porcentaje como `portfolio_assets.total_value * 100.0 / portfolio_snapshots.total_value`.
- Si algún comando falla, indicarlo brevemente y continuar con `reports/latest/*`, `data/plans/` y, si está disponible, lecturas locales de `data/iol_history.db` solo para `SELECT`.

## Comandos estables

### 0. Preflight Docker

```powershell
$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'; docker --context codex-desktop-linux ps
```

Si falla, no reintentar con `docker exec` sin contexto.

### 1. Correr advisor semanal

```powershell
$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'; docker --context codex-desktop-linux exec iol-cli iol advisor autopilot run --cadence weekly --budget-ars 200000 --top 10 --out reports/latest/AdvisorWeekly.md --opportunity-report-out reports/latest/Oportunidades.md
```

### 2. Leer reporte semanal

Leer `reports/latest/AdvisorWeekly.md`.

### 3. Composición actual

```powershell
$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'; docker --context codex-desktop-linux exec iol-cli iol data query "SELECT pa.symbol, pa.daily_var_pct, pa.total_value, ROUND(pa.total_value * 100.0 / NULLIF(ps.total_value, 0), 4) AS pct_portfolio_calc FROM portfolio_assets pa JOIN portfolio_snapshots ps ON ps.snapshot_date = pa.snapshot_date WHERE pa.snapshot_date = (SELECT snapshot_date FROM portfolio_snapshots ORDER BY snapshot_date DESC LIMIT 1) ORDER BY pa.total_value DESC"
```

### 4. Retorno semanal por activo

```powershell
$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'; docker --context codex-desktop-linux exec iol-cli iol data query "SELECT cur.symbol, ROUND((cur.total_value - old.total_value) * 100.0 / NULLIF(old.total_value,0), 2) AS week_var_pct, cur.total_value FROM portfolio_assets cur JOIN portfolio_assets old ON old.symbol = cur.symbol WHERE cur.snapshot_date = (SELECT MAX(snapshot_date) FROM portfolio_snapshots) AND old.snapshot_date = (SELECT MAX(snapshot_date) FROM portfolio_snapshots WHERE snapshot_date <= date((SELECT MAX(snapshot_date) FROM portfolio_snapshots), '-7 day')) ORDER BY week_var_pct DESC"
```

### 5. Valor del portfolio

```powershell
$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'; docker --context codex-desktop-linux exec iol-cli iol data query "SELECT snapshot_date, total_value FROM portfolio_snapshots ORDER BY snapshot_date DESC LIMIT 8"
```

### 6. Señales de oportunidades

Conteo por estado:

```powershell
$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'; docker --context codex-desktop-linux exec iol-cli iol data query "SELECT candidate_status, COUNT(*) AS count FROM advisor_opportunity_candidates WHERE run_id = (SELECT id FROM advisor_opportunity_runs WHERE status IN ('done','ok') ORDER BY id DESC LIMIT 1) GROUP BY candidate_status ORDER BY candidate_status"
```

Operable/watchlist:

```powershell
$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'; docker --context codex-desktop-linux exec iol-cli iol data query "SELECT symbol, signal_side, signal_family, ROUND(score_total, 2) AS score_total, candidate_status, reason_summary FROM advisor_opportunity_candidates WHERE run_id = (SELECT id FROM advisor_opportunity_runs WHERE status IN ('done','ok') ORDER BY id DESC LIMIT 1) AND candidate_status IN ('operable','watchlist') ORDER BY candidate_status, score_total DESC LIMIT 25"
```

Suprimidas:

```powershell
$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'; docker --context codex-desktop-linux exec iol-cli iol data query "SELECT symbol, signal_side, signal_family, ROUND(score_total, 2) AS score_total, candidate_status, reason_summary FROM advisor_opportunity_candidates WHERE run_id = (SELECT id FROM advisor_opportunity_runs WHERE status IN ('done','ok') ORDER BY id DESC LIMIT 1) AND candidate_status = 'suppressed' ORDER BY score_total DESC"
```

Mostrar solo candidatos `operable` y `watchlist`; resumir `suppressed` como validación del sistema de resolución de conflictos.

### 7. Target estructural

```powershell
$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'; docker --context codex-desktop-linux exec iol-cli iol advisor target-weights show
```

Comparar cada activo real vs su target. Calcular desvío en puntos porcentuales:
- Desvío > 3pp: mencionar.
- Desvío > 5pp: alerta de rebalanceo.
- FCI/liquidez > 12%: alerta de cash drag.

### 8. Chequeo de FCI / ADRDOLA

```powershell
$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'; docker --context codex-desktop-linux exec iol-cli iol data query "SELECT pa.symbol, pa.quantity, pa.total_value, ROUND(pa.total_value * 100.0 / NULLIF(ps.total_value, 0), 4) AS pct_portfolio_calc FROM portfolio_assets pa JOIN portfolio_snapshots ps ON ps.snapshot_date = pa.snapshot_date WHERE pa.snapshot_date = (SELECT snapshot_date FROM portfolio_snapshots ORDER BY snapshot_date DESC LIMIT 1) AND pa.symbol LIKE '%ADRDOLA%'"
```

### 9. Comparación y planes

- Si existe `reports/latest/AdvisorWeekly_prev.md`, comparar con la semana anterior.
- Si no existe, usar semana móvil de 7 días desde la DB.
- Buscar archivos en `data/plans/` con fecha reciente. Listar planes pendientes; no ejecutarlos.

### 10. Investigación macro externa

Buscar novedades de la semana:
- Petróleo Brent/WTI y contexto geopolítico.
- S&P 500, mercados globales y VIX.
- Oro/GLD, flujos e institucionales.
- Bitcoin/IBIT, flujos y regulación.
- Argentina: IPC, ARS/USD, BCRA tasa, EMBI/riesgo país.
- EEM/emergentes: Asia, India, Brasil.
- Ray Dalio/Bridgewater, Berkshire/Buffett, Goldman Sachs, BofA, JPMorgan y flujos de ETFs.

## Formato de salida

### Revisión Semanal — semana del [fecha lunes] al [fecha viernes]

**Performance de la semana** — valor total y variación, top 3 ganadoras y perdedoras.

**Señales del sistema** — Operable / Watchlist / Suprimidas, con cantidad y razón.

**Composición vs target estructural** — tabla: activo | % actual | % target | desvío. Indicar trigger de rebalanceo: SÍ/NO.

**FCI / Liquidez** — ADRDOLA % actual vs target 8%, estado de cash drag.

**Contexto macro de la semana** — petróleo, S&P 500, oro, BTC, Argentina, EEM.

**Inteligencia institucional** — 3-4 bullets sobre grandes fondos.

**Plan para la semana** — acciones concretas, máximo 5 bullets, y órdenes pendientes.

**Alertas y riesgos** — desvíos críticos, alertas del advisor, riesgos macro.

Sé directo y accionable.
