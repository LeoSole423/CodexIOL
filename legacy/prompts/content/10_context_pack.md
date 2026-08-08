# Context Pack Contract

Este prompt define como leer y validar `iol advisor context`.

## Paso 1: Generar contexto
- Comando base: `iol advisor context`.
- Para persistencia:
  - JSON: `iol advisor context --out data/context/latest.json`.
  - Markdown: `iol advisor context --format md --out data/context/latest.md`.

## Paso 2: Validar warnings
- `DB_NOT_FOUND`: no hay DB local accesible.
- `NO_SNAPSHOTS`: DB sin snapshots.
- `SNAPSHOT_OLD`: snapshot desactualizado.
- `RETURNS_IGNORE_CASHFLOWS`: retornos sin ajuste por aportes/retiros.

## Paso 3: Remediacion minima
- Si aparece `DB_NOT_FOUND`, `NO_SNAPSHOTS` o `SNAPSHOT_OLD`:
  1. Ejecutar `iol snapshot catchup`.
  2. Reintentar `iol advisor context`.

## Paso 4: Campos prioritarios para analisis
- `as_of`
- `snapshot.total_value_ars`
- `assets.top_by_value`
- `returns.*`
- `movers.*`
- `allocation.*`
- `warnings`

## Paso 5: Politica de consistencia
- Si la consulta requiere intradia/tiempo real: contrastar con `iol portfolio --country argentina`.
- Si hay discrepancias, reportar ambas fuentes y su timestamp.
