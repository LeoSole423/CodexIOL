# AGENTS.md

## Propósito

CodexIOL es exclusivamente un CLI operativo para la API de InvertirOnline. El paquete activo vive en `src/iol_cli` y no usa SQLite, web, advisor, motores ni simulaciones.

`src/iol_review` es un subsistema independiente de revisión mensual: solo puede leer estado, carteras, órdenes y movimientos. Nunca importa comandos operativos ni llama endpoints mutantes. Sus artefactos son recomendaciones auditables, no instrucciones de trading.

## Continuidad de cartera

Antes de analizar la cartera, sugerir aportes o interpretar tareas programadas, leer `config/investor-profile.toml`, la revisión mensual más reciente y `reports/monthly/2026-08-transition-plan.md`. Este último es el documento local canónico para objetivos, bandas, transición y acciones humanas ya ejecutadas. Sus etapas pendientes son contexto analítico y nunca autorización para operar.

No mezclar el subsistema activo con los informes y planes históricos en `reports/latest/`, `reports/analisis/`, `reports/rebalance/`, `reports/dca/`, `reports/macro/`, `data/plans/`, bases SQLite ni `data/scheduled_tasks_export.json`: pertenecen a legacy y solo sirven como historia. Sus comandos de advisor, motores, queries y planes batch no existen en la arquitectura activa.

Si datos actuales contradicen el plan, informar la contradicción y revisar la tesis; no aplicar objetivos históricos automáticamente. Una actualización debe conservar por separado los hechos ejecutados y las propuestas futuras.

## Entorno

El flujo soportado es Docker:

```powershell
docker compose up -d --build
docker exec -it iol-cli iol --help
docker exec -it iol-cli iol auth test
```

## Seguridad financiera

- Nunca ejecutar operaciones reales durante desarrollo, tests o validación manual.
- Compra y venta requieren `orders prepare` seguido de `orders execute <ticket-id>` y confirmación interactiva exacta `CONFIRMAR`.
- Cancelaciones, FCI y requests genéricos mutantes también requieren confirmación.
- No agregar flags que eviten la confirmación interactiva.
- No reintentar automáticamente POST, PUT, PATCH o DELETE.
- No exponer credenciales, tokens, payloads privados ni cuerpos de error.
- No publicar ni commitear `.env`, `data/`, `reports/` o tickets.
- Las automatizaciones de revisión no pueden invocar `iol orders prepare`, `iol orders execute`, `iol orders cancel`, comandos FCI ni `iol api request` mutante.
- Las propuestas de revisión no contienen cantidades, montos, tickets, payloads ni son elegibles automáticamente para operar.

## Arquitectura activa

- `config.py`: configuración mínima.
- `iol_client.py`: autenticación y HTTP.
- `commands_*.py`: grupos Typer.
- `tickets.py`: preparación y ejecución única de órdenes.
- `legacy/`: archivo histórico, excluido del paquete y sin soporte.

El código activo nunca debe importar desde `legacy`, FastAPI, SQLite, advisor, engines, frontend o reconciliación.

## Terminado

- `python -m pytest -q`
- `iol --help`
- build Docker exitoso.
- pruebas HTTP completamente simuladas; nunca usar credenciales reales.
