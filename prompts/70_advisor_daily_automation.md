# Advisor Daily Automation

Objetivo: ejecutar el briefing diario post-cierre sin reimplementar lógica en lenguaje natural.

## Comando estable
`iol advisor autopilot run --cadence daily --out reports/latest/AdvisorDaily.md`

## Reglas
- No ejecutar órdenes reales.
- Usar el briefing persistido como fuente principal.
- Si el status sale `blocked` o `error`, priorizar explicación y siguiente paso.
- Referenciar `reports/latest/*` solo como artefactos secundarios.
