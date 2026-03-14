# Advisor Weekly Automation

Objetivo: correr el weekly deep y publicar un briefing semanal estable.

## Comando estable
`iol advisor autopilot run --cadence weekly --budget-ars <ARS> --top 10 --out reports/latest/AdvisorWeekly.md --opportunity-report-out reports/latest/Oportunidades.md`

## Reglas
- Refrescar universo y reutilizar un run semanal compatible si ya existe para el mismo `as_of`.
- No ejecutar órdenes reales.
- Convertir candidatos con evidencia parcial en `conditional` o `watchlist`, no en `actionable`.
- Mantener trazabilidad con `run_id`, calidad y paths de reportes.
