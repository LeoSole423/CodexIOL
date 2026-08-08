# Prompt Especializado: Opportunity Scoring (BCBA + CEDEARs)

## Objetivo
Construir un ranking semanal de oportunidades para:
- compras nuevas (`new`)
- recompras (`rebuy`)

Salida esperada: decision final de cartera + recomendaciones (o no-operar) con razones claras.

## Flujo de trabajo
0. Obtener contexto de motores (`prompts/32_regime_engine.md`):
   - `iol engines regime show` + `iol engines macro show`
   - Aplicar ajuste defensivo si corresponde (bear/crisis, stress AR alto)
1. Actualizar contexto de cartera:
   - `iol advisor context`
2. Refrescar universo/precios:
   - `iol advisor opportunities snapshot-universe --universe bcba_cedears`
3. Ejecutar pasada preliminar cuantitativa (`prelim`).
4. Integrar evidencia web bidireccional (`TopN <-> Web`) en modo estricto:
   - `source_policy=strict_official_reuters`
   - simbolos = holdings + TopK preliminar
5. Aplicar overlay institucional (`prompts/37_smart_money.md`):
   - `iol engines smart-money show --min-conviction 50`
   - Ajustar conviccion por simbolo según señales 13F
6. Recalcular ranking final (`rerank`) con score hibrido y compuertas.
7. Ejecutar ranking integrado:
   - `iol advisor opportunities run --mode both --budget-ars <monto> --top 10 --web-link --web-source-policy strict_official_reuters`
   - Defaults operativos para futuras busquedas:
     - `--exclude-crypto-new`
     - `--min-volume-amount 50000`
     - `--min-operations 5`
     - `--liquidity-priority`
     - `--diversify-sectors --max-per-sector 2`
8. Generar reporte:
   - `iol advisor opportunities report --run-id <id> --out reports/latest/Oportunidades.md`

## Salida obligatoria post-run
Aplicar `prompts/contracts/output_schema.md` tipo `oportunidades` y siempre incluir:
1. Contexto de régimen (del paso 0).
2. Decision final: `comprar`, `recomprar`, o `no operar`.
3. Recomendacion por activo (o "sin operaciones recomendadas").
4. Por que de cada recomendacion: score, factores de riesgo, catalyst/evidencia, y flags.
5. Si no se recomienda operar, explicar concretamente por que (ej. evidencia insuficiente, score bajo, conflictos).

## Reglas de decision
- Filtros duros:
  - liquidez
  - exclusion de cripto para candidatos `new` (por defecto)
  - concentracion
  - drawdown extremo
- Priorizacion de ranking:
  - desempatar por liquidez (spread/operaciones/volumen)
- Diversificacion:
  - cap por sector inferido para evitar top concentrado en un solo tema
- Rebuy (`buy the dip`):
  - drawdown <= -8%
  - tesis vigente (evidencia medium/high en ventana reciente)
- Regimen bear/crisis: reducir budget operativo segun `defensive_weight_adjustment`
- Stress AR > 65: priorizar CEDEARs sobre BCBA local en el ranking

## Acople web + score (hibrido con compuertas)
- `catalyst_final = 0.6 * catalyst_actual + 0.4 * expert_signal_score`.
- Si `trusted_refs < umbral`, activar `EVIDENCE_INSUFFICIENT`.
- Si `consensus_state=conflict`, marcar `decision_gate=manual_review` (no bloquear automaticamente).
- Si falla fetch web, continuar con ranking cuantitativo y advertir en `pipeline_warnings_json`.

## Sizing
- Conviccion por score:
  - `>=80` -> 1.5x
  - `65-79` -> 1.0x
  - `50-64` -> 0.5x
- Aplicar caps por activo y concentracion post-trade.
- Si régimen bear/crisis: multiplicar budget total por `(1 + defensive_weight_adjustment)`.

## Guardrail operativo
- El ranking no ejecuta ordenes reales.
- Cualquier ejecucion debe pasar por `prompts/60_safe_execution.md`.
- Formato de salida: `prompts/contracts/output_schema.md` tipo `oportunidades`.
