# Advisor Daily Automation

Objetivo: ejecutar el briefing diario post-cierre sin reimplementar lógica en lenguaje natural.

## Comando estable
`$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'; docker --context codex-desktop-linux exec iol-cli iol advisor autopilot run --cadence daily --out reports/latest/AdvisorDaily.md`

## Reglas
- No ejecutar órdenes reales.
- Usar el briefing persistido como fuente principal.
- Si el status sale `blocked` o `error`, priorizar explicación y siguiente paso.
- Referenciar `reports/latest/*` solo como artefactos secundarios.
- En automatizaciones Codex, usar Docker con contexto explícito `codex-desktop-linux`; no volver a comandos bare `docker exec`.
- Para SQL auxiliar, respetar el esquema real:
  - `portfolio_assets` se une a `portfolio_snapshots` por `snapshot_date`, no por `snapshot_id`.
  - El valor monetario en DB es `total_value`; `total_value_ars` pertenece al context pack/reporte.
  - La composición se calcula con `portfolio_assets.total_value * 100 / portfolio_snapshots.total_value`; no existe `pct_portfolio` persistido.
  - Smart money vive en `engine_smart_money_snapshots`.
