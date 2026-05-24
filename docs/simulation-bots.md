# Simulation Bots

Guia operativa para entender, ejecutar, auditar y debuggear los bots de simulacion de CodexIOL.

Los bots son simuladores. No envian ordenes reales a InvertirOnline. Aun asi, usalos siempre dentro de Docker y nunca combines estos flujos con `batch run --confirm CONFIRMAR` salvo que estes haciendo una operacion real autorizada fuera de esta guia.

## Familias

| Familia | Comandos | Horizonte | Fuente principal | Ejecucion |
| --- | --- | --- | --- | --- |
| Daily | `iol simulate ...` | diario / rotacion corta | oportunidades + motores | senal T, orden pendiente, fill T+1 |
| Swing | `iol simulate swing ...` | 3 a 10 dias | TA + oportunidad + regimen/macro | senal T, orden pendiente, fill T+1 |
| Event | `iol simulate event ...` | reactivo | eventos de motores | evento T, orden pendiente, fill T+1 |

### Daily

Los daily bots usan presets `conservative`, `balanced` y `growth`. Toman candidatos de `advisor_opportunity_candidates`, ponderan scores y restricciones del preset, y generan compras/ventas simuladas.

Uso tipico:

```powershell
docker exec iol-cli iol simulate bots
docker exec iol-cli iol simulate run --bot-config balanced --date-from 2026-05-01 --date-to 2026-05-22
docker exec iol-cli iol simulate list --limit 20
docker exec iol-cli iol simulate show --run-id 17 --trades
docker exec iol-cli iol simulate live-step --bots all --as-of 2026-05-22 --json
```

### Swing

Los swing bots usan presets `swing-conservative`, `swing-balanced` y `swing-aggressive`. Mantienen posiciones varios dias, aplican stop loss, take profit, trailing stop, time stop y senales tecnicas.

Uso tipico:

```powershell
docker exec iol-cli iol simulate swing bots
docker exec iol-cli iol simulate swing run --bot swing-balanced --date-from 2026-05-01 --date-to 2026-05-22
docker exec iol-cli iol simulate swing list --limit 20
docker exec iol-cli iol simulate swing show --run-id 26 --trades
docker exec iol-cli iol simulate swing live-step --bots all --as-of 2026-05-22 --json
```

### Event

Los event bots usan presets `event-defensive`, `event-opportunistic` y `event-adaptive`. Detectan cambios discretos en motores: regimen, volatilidad, macro stress, risk-on/off y smart money.

Uso tipico:

```powershell
docker exec iol-cli iol simulate event bots
docker exec iol-cli iol simulate event detect --as-of 2026-05-22 --json
docker exec iol-cli iol simulate event run --bot event-adaptive --date-from 2026-05-01 --date-to 2026-05-22
docker exec iol-cli iol simulate event list --limit 20
docker exec iol-cli iol simulate event show --run-id 18 --trades
docker exec iol-cli iol simulate event live-step --bots all --as-of 2026-05-22 --json
```

## Modelo De Ejecucion

Las simulaciones nuevas usan senal T y ejecucion T+1:

1. El bot detecta una entrada/salida en la fecha T.
2. Persiste una orden pendiente en una tabla T+1.
3. En el siguiente `live-step` o dia de backtest, intenta ejecutar en `symbol_daily_ohlcv.open`.
4. Si no existe `open`, usa `market_symbol_snapshots.last_price` como fallback y lo marca en `cost_model_json`.

Fuentes de precio:

| `price_source` | Interpretacion |
| --- | --- |
| `symbol_daily_ohlcv.open` | T+1/open confiable |
| `fallback_last_price` | T+1 parcial, sin open disponible |
| `same_day_snapshot` | ejecucion legacy o interna no T+1 real |
| missing/null | dato incompleto, revisar |

## Costos Netos

Los runs nuevos tienen `cost_model_version`. Eso significa que el retorno es neto de:

- comision broker;
- derechos de mercado;
- IVA cuando corresponde;
- slippage;
- sizing neto para evitar cash negativo.

Campos persistidos por trade:

- `gross_amount_ars`
- `net_amount_ars`
- `commission_ars`
- `market_fee_ars`
- `iva_ars`
- `slippage_ars`
- `total_cost_ars`
- `execution_price`
- `instrument_type`
- `cost_model_json`

Regla de lectura:

- `cost_model_version IS NULL`: run legacy, retorno bruto o no comparable.
- `cost_model_version IS NOT NULL`: run nuevo, retorno neto.
- No compares un run legacy/stale contra uno nuevo neto sin aclarar el cambio de base.

Variables relevantes:

```powershell
IOL_SIM_COMMISSION_TIER=gold
IOL_SIM_INCLUDE_IVA=1
IOL_SIM_INCLUDE_MARKET_FEES=1
IOL_SIM_DEFAULT_INSTRUMENT_TYPE=stock
IOL_SIM_INSTRUMENT_OVERRIDES=AL30:bond,GGAL:stock
IOL_SIM_MAX_DAILY_VOLUME_PCT=0.02
```

## Tablas Principales

| Familia | Runs | Trades | Pendientes T+1 |
| --- | --- | --- | --- |
| Daily | `simulation_runs` | `simulation_trades` | `simulation_pending_orders` |
| Swing | `swing_simulation_runs` | `swing_simulation_trades` | `swing_pending_orders` |
| Event | `event_simulation_runs` | `event_simulation_trades` | `event_pending_orders` |

Datos de mercado:

- `market_symbol_snapshots`: precios `last_price`, volumen y snapshot diario.
- `symbol_daily_ohlcv`: `open`, `high`, `low`, `close`, volumen diario.
- `symbol_intraday_ticks`: ticks acumulados por snapshot.

## Diagnostico T+1 / OHLCV

Comando principal:

```powershell
docker exec iol-cli iol simulate t1-health --as-of 2026-05-26
docker exec iol-cli iol simulate t1-health --as-of 2026-05-26 --symbols
docker exec iol-cli iol simulate t1-health --as-of 2026-05-26 --json
```

Interpreta:

- `active_pending_total`: ordenes pendientes en runs activos.
- `stale_pending_total`: pendientes antiguas en runs stale; deberian ser 0 o estar canceladas.
- `symbols_needed`: universo que necesita OHLCV/open para simulaciones.
- `open_ready`: cuantos tienen `symbol_daily_ohlcv.open` para `as_of`.
- `fallback_needed`: cuantos caerian a `fallback_last_price`.
- `missing_price`: no hay open ni last_price.
- `fallback_last_price_high`: falta cobertura OHLCV/open.

En feriados o domingos, `open_ready=0` para una fecha futura puede ser normal. Despues de un snapshot de mercado abierto, `open_ready` deberia subir y `fallback_needed` deberia bajar.

## Pipeline Diario Seguro

Despues del cierre de mercado:

```powershell
$env:DOCKER_CONFIG='C:\Users\leone\.codex\docker-config-automation'
docker exec iol-cli iol snapshot run
docker exec iol-cli iol advisor opportunities run --mode both --budget-ars 200000 --top 10
docker exec iol-cli iol simulate live-step --bots all --json
docker exec iol-cli iol simulate swing live-step --bots all --json
docker exec iol-cli iol simulate event live-step --bots all --json
docker exec iol-cli iol simulate t1-health --json
```

No ejecutes:

```powershell
docker exec iol-cli iol batch run --confirm CONFIRMAR
```

Ese comando es para ordenes reales y queda fuera del flujo de simulacion.

## Debugging

### El bot no opera

1. Ver si hay datos de oportunidades:

```powershell
docker exec iol-cli iol advisor opportunities list-runs --limit 5
```

2. Ver si el bot encolo algo:

```powershell
docker exec iol-cli iol simulate t1-health --json
```

3. Ver anomalies:

```powershell
docker exec iol-cli iol simulate list --limit 20 --json
docker exec iol-cli iol simulate swing list --limit 20 --json
docker exec iol-cli iol simulate event list --limit 20 --json
```

Si no hay trades pero hay `pending_t1_orders`, no es falla: la orden espera el siguiente dia de ejecucion.

### Una orden queda pending

Usar:

```powershell
docker exec iol-cli iol simulate t1-health --symbols
```

Revisar:

- `open_ready` bajo;
- `fallback_needed` alto;
- `missing_price` mayor a 0;
- `oldest_active_signal_date` demasiado viejo.

Si pertenece a un run `stale`, deberia quedar `cancelled` por migracion. Si no ocurre, correr un comando CLI que invoque `init_db`, por ejemplo:

```powershell
docker exec iol-cli iol simulate t1-health --json
```

### PnL raro

Revisar el trade con `show --trades` y buscar:

- `execution_price`;
- `total_cost_ars`;
- `cost_model_json`;
- `price_source`;
- `liquidity_warning`.

Daily:

```powershell
docker exec iol-cli iol simulate show --run-id <ID> --trades
```

Swing:

```powershell
docker exec iol-cli iol simulate swing show --run-id <ID> --trades
```

Event:

```powershell
docker exec iol-cli iol simulate event show --run-id <ID> --trades
```

### `fallback_last_price` alto

Esto significa que la simulacion no encontro `symbol_daily_ohlcv.open` para los simbolos que necesitaba.

Revisar:

```powershell
docker exec iol-cli iol simulate t1-health --symbols
```

Causas comunes:

- el mercado estuvo cerrado;
- todavia no corrio `snapshot run` en fecha de mercado;
- el simbolo no estaba en portfolio/watchlist/candidatos/pendientes al momento del snapshot;
- `get_quote` no trajo `apertura` para ese simbolo;
- mercado/simbolo mal normalizado.

### Event bots sin operaciones

Primero detectar eventos:

```powershell
docker exec iol-cli iol simulate event detect --as-of 2026-05-22 --json
```

Si no hay eventos, el bot no tiene por que operar. Si hay eventos pero no opera, revisar:

- reglas del preset;
- cooldown;
- candidatos disponibles;
- `min_engine_score`;
- max positions y cash reserve.

### Metricas null

Flags comunes:

- `null_drawdown`
- `null_event_count`
- `no_week_trades`
- `no_week_steps`
- `duplicate_running`
- `stale`

Ver listados JSON:

```powershell
docker exec iol-cli iol simulate list --limit 50 --json
docker exec iol-cli iol simulate swing list --limit 50 --json
docker exec iol-cli iol simulate event list --limit 50 --json
```

## Checklist Post-Mercado

1. `snapshot run` termino OK.
2. `advisor opportunities run` genero candidatos.
3. `t1-health` muestra pendientes esperables y buena cobertura de `open`.
4. `live-step` daily/swing/event corrio sin errores.
5. Trades nuevos tienen `cost_model_version`.
6. Trades nuevos tienen `total_cost_ars` no null.
7. `price_source` idealmente es `symbol_daily_ohlcv.open`.
8. `fallback_last_price` se explica por mercado cerrado o falta puntual de OHLCV.
9. No hay `duplicate_running`.
10. No hay pendientes viejas activas.

## Consultas SQL Utiles

Pendientes activas:

```sql
SELECT 'daily' AS family, COUNT(*) AS n, MIN(signal_date) AS oldest
FROM simulation_pending_orders p
JOIN simulation_runs r ON r.id=p.run_id
WHERE p.status='pending' AND r.status='running'
UNION ALL
SELECT 'swing', COUNT(*), MIN(signal_date)
FROM swing_pending_orders p
JOIN swing_simulation_runs r ON r.id=p.run_id
WHERE p.status='pending' AND r.status='running'
UNION ALL
SELECT 'event', COUNT(*), MIN(signal_date)
FROM event_pending_orders p
JOIN event_simulation_runs r ON r.id=p.run_id
WHERE p.status='pending' AND r.status='running';
```

Cobertura de open:

```sql
SELECT symbol, MAX(trade_date) AS latest_open_date
FROM symbol_daily_ohlcv
WHERE open IS NOT NULL
GROUP BY symbol
ORDER BY symbol;
```

Costos por run daily:

```sql
SELECT run_id,
       COUNT(*) AS trades,
       SUM(total_cost_ars) AS total_costs,
       AVG(total_cost_ars) AS avg_cost
FROM simulation_trades
GROUP BY run_id
ORDER BY run_id DESC;
```

## Relacion Con Automatizaciones

`simulation-accuracy-report` debe usar:

```powershell
docker exec iol-cli iol simulate t1-health --json
docker exec iol-cli iol simulate list --limit 50 --json
docker exec iol-cli iol simulate swing list --limit 50 --json
docker exec iol-cli iol simulate event list --limit 50 --json
```

El reporte semanal debe separar:

- precision de senales H1/H5;
- salud operativa de bots;
- realismo de simulacion;
- retorno neto vs legacy bruto;
- cobertura de costos;
- cobertura T+1/open.
