# Prompt Especializado: Opportunity Scoring (BCBA + CEDEARs)

## Objetivo
Construir un ranking semanal de oportunidades para:
- compras nuevas (`new`)
- recompras (`rebuy`)

Salida esperada: decision final de cartera + recomendaciones (o no-operar) con razones claras.

## Flujo de trabajo
1. Actualizar contexto de cartera:
   - `iol advisor context`
2. Refrescar universo/precios:
   - `iol advisor opportunities snapshot-universe --universe bcba_cedears`
3. Ingestar evidencia web (opcional manual):
   - `iol advisor evidence add ...`
4. Fetch semiautomatico de evidencia (opcional standalone):
   - `iol advisor evidence fetch --from-context --max-symbols 15 --per-source-limit 2`
5. Ejecutar ranking (incluye auto-fetch por defecto):
   - `iol advisor opportunities run --mode both --budget-ars <monto> --top 10`
6. Generar reporte:
   - `iol advisor opportunities report --run-id <id> --out reports/latest/Oportunidades.md`

## Salida obligatoria post-run
Aplicar `prompts/contracts/output_schema.md` tipo `oportunidades` y siempre incluir:
1. Decision final: `comprar`, `recomprar`, o `no operar`.
2. Recomendacion por activo (o "sin operaciones recomendadas").
3. Por que de cada recomendacion: score, factores de riesgo, catalyst/evidencia, y flags.
4. Si no se recomienda operar, explicar concretamente por que (ej. evidencia insuficiente, score bajo, conflictos).

## Reglas de decisión
- Filtros duros:
  - liquidez
  - concentración
  - drawdown extremo
- Rebuy (`buy the dip`):
  - drawdown <= -8%
  - tesis vigente (evidencia medium/high en ventana reciente)
  - sin conflicto no resuelto de evidencia

## Scoring híbrido
- `risk` 35%
- `value` 20%
- `momentum` 35%
- `catalyst` 10%

## Sizing
- Convicción por score:
  - `>=80` -> 1.5x
  - `65-79` -> 1.0x
  - `50-64` -> 0.5x
- Aplicar caps por activo y concentración post-trade.

## Guardrail operativo
- El ranking no ejecuta órdenes reales.
- Cualquier ejecución debe pasar por `prompts/60_safe_execution.md`.
- Formato de salida: `prompts/contracts/output_schema.md` tipo `oportunidades`.
