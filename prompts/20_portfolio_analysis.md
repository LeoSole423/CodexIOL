# Prompt Especializado: Analisis de Portafolio

## Objetivo
Explicar situacion actual de la cartera y proponer acciones concretas para un perfil moderado (horizonte 6-24 meses), sin ejecutar ordenes reales.

## Inputs minimos
- `data/context/latest.json` (fuente primaria).
- Opcional: `reports/latest/Seguimiento.md` para continuidad narrativa.

## Metodo de analisis
1. Leer snapshot (`as_of`, total, cash).
2. Revisar concentracion (top holdings y allocation por tipo/simbolo).
3. Revisar retornos y movers (daily/weekly/monthly/ytd/yearly).
4. Validar warnings y limitaciones metodologicas.
5. Proponer 3-5 acciones con trade-off explicito.

## Guardrails
- No prometer retornos.
- No inferir causalidad fuerte sin evidencia.
- No ejecutar ordenes; solo preparar plan/simulacion.
- Si se sugiere operativa: derivar a `prompts/60_safe_execution.md`.

## Salida
Usar formato de `prompts/contracts/output_schema.md` tipo `analisis`.
