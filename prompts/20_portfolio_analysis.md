# Prompt Especializado: Analisis de Portafolio

## Objetivo
Explicar situacion actual de la cartera y proponer acciones concretas para un perfil moderado (horizonte 6-24 meses), sin ejecutar ordenes reales.

## Inputs minimos
- `data/context/latest.json` (fuente primaria).
- `iol portfolio --country argentina` (foto operativa para validar frescura).
- Plan vigente en `reports/latest/*` si existe (`PlanDCA`, `RevisionEstrategica`, `ResumenRebalanceo`).
- Alertas/eventos desde SQLite:
  - `iol advisor alert list --status open --limit 50`
  - `iol advisor event list --limit 50`
- Opcional: `reports/latest/Seguimiento.md` para continuidad narrativa.

## Metodo de analisis
1. Leer snapshot (`as_of`, total, cash).
2. Contrastar con foto operativa (`iol portfolio`) y reportar discrepancias de timestamp/valuacion.
3. Revisar plan vigente y validar desviaciones vs estrategia en curso.
4. Revisar alertas/eventos abiertos y su impacto en prioridades.
5. Revisar concentracion (top holdings y allocation por tipo/simbolo).
6. Revisar retornos y movers (daily/weekly/monthly/ytd/yearly).
7. Validar warnings y limitaciones metodologicas.
8. Proponer 3-5 acciones con trade-off explicito.

## Guardrails
- No prometer retornos.
- No inferir causalidad fuerte sin evidencia.
- No ejecutar ordenes; solo preparar plan/simulacion.
- Si no hay plan vigente o alertas/eventos disponibles, declararlo explicitamente en supuestos.
- Si se sugiere operativa: derivar a `prompts/60_safe_execution.md`.

## Salida
Usar formato de `prompts/contracts/output_schema.md` tipo `analisis`.
