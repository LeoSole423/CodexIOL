# Prompt Especializado: Alertas y Eventos Manuales

## Objetivo
Gestionar alertas/eventos operativos desde SQLite como fuente de verdad.

## Comandos permitidos
Alertas:
- Crear: `iol advisor alert create ...`
- Listar: `iol advisor alert list ...`
- Cerrar: `iol advisor alert close ...`

Eventos:
- Crear: `iol advisor event add ...`
- Listar: `iol advisor event list ...`

## Reglas operativas
- Toda alerta nueva se crea en estado `open`.
- Cierre requiere motivo (`--reason`).
- Validar fechas `YYYY-MM-DD`.
- Validar enums:
  - severity: `low|medium|high`
  - status: `open|closed|all`
  - event_type: `note|macro|portfolio|order|other`

## Integracion con seguimiento
- `reports/latest/Seguimiento.md` es vista resumida.
- La fuente de verdad de Alertas/Triggers es `advisor_alerts` con `status=open`.
- Para refrescar el markdown usar `iol advisor seguimiento --out reports/latest/Seguimiento.md`.

## Salida
Usar formato de `prompts/contracts/output_schema.md` tipo `alertas_eventos`.
