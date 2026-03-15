# Overlay: Smart Money (Posicionamiento Institucional 13F)

## Objetivo
Incorporar el posicionamiento de fondos institucionales de referencia (Berkshire, ARK, Vanguard) como overlay de convicción sobre el ranking de oportunidades.

## Flujo

1. Obtener señales institucionales:
   ```
   iol engines smart-money show --min-conviction 50
   ```
   Leer: `symbol`, `net_institutional_direction`, `conviction_score`, `top_holders_added`, `top_holders_trimmed`

2. Cruzar con la lista de candidatos del ranking de oportunidades.

3. Ajustar convicción por símbolo:
   - `accumulate` + `conviction_score > 70` → refuerza la tesis; +10 puntos al catalyst
   - `distribute` → señal de alerta; marcar en el análisis aunque no bloquea automáticamente
   - `neutral` o sin señal → no modifica el scoring

## Reglas

- Los datos son trimestrales con un lag de ~45 días. No operar basado únicamente en señales 13F sin confirmar con evidencia reciente.
- `distribute` no es señal de venta automática — puede reflejar rebalanceo de cartera, no deterioro fundamental.
- Si no hay señales (`conviction_score` bajo o sin datos): omitir este overlay y continuar sin ajuste.

## Limitación

Solo se rastrean 3 fondos: Berkshire Hathaway, ARK Invest, Vanguard. El universo cubierto es parcial y sesgado hacia CEDEARs (acciones US).

## Salida esperada
Overlay sobre el ranking de oportunidades: lista de símbolos con nota de refuerzo o alerta institucional.
