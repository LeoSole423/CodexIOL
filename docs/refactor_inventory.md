# Refactor Inventory

Estado inicial de la limpieza estructural.

## Duplicado
- `src/iol_cli/advisor_context.py` y `src/iol_web/db.py` repetían acceso a snapshots, assets, series y allocation.
- `src/iol_reconciliation/service.py` y `src/iol_web/routes_api.py` repetían normalización de moneda/movimientos, cash snapshots y agregación de movimientos importados.
- `src/iol_web/metrics.py` y `src/iol_cli/advisor_context.py` repetían lógica de retornos y fechas objetivo.
- `src/iol_web/movers.py` y `src/iol_cli/advisor_context.py` repetían unión de movers por símbolo.

## Legacy
- ~~Frontend Jinja2 (`src/iol_web/templates/`, `src/iol_web/static/`, `routes_pages.py`, `templates.py`) eliminado — reemplazado por Next.js en `frontend/`.~~ ✅ Completado (2026-03-15)
- Backend conservaba tolerancia a esquemas viejos en órdenes/runs; se mantiene donde todavía protege bases existentes.

## Monolitos
- `src/iol_cli/cli.py` concentra orquestación y lógica de dominio.
- `src/iol_web/routes_api.py` mezcla helpers, calidad, advisor, reconciliación, returns, inflation y movers.
- `src/iol_cli/db.py` mezclaba runtime, schema y migraciones livianas.

## Acoplamiento
- `src/iol_reconciliation/service.py` dependía de `iol_web.db`.
- `src/iol_web/metrics.py` dependía del módulo web de DB para tipos.
- `src/iol_web/routes_api.py` centralizaba endpoints de advisor y reconciliación en el mismo router general.

## Cambios aplicados en esta fase
- Se crea `iol_shared` como capa interna compartida para DB, métricas, movers y utilidades de conciliación.
- `iol_reconciliation` deja de depender de `iol_web`.
- `iol_web` separa routers de advisor y reconciliación sin cambiar rutas públicas.
- `iol_cli.db` pasa a orquestar schema + migraciones desde módulos dedicados.
