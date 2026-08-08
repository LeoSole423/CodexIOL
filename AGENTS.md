# AGENTS.md

## Propósito

CodexIOL es exclusivamente un CLI operativo para la API de InvertirOnline. El paquete activo vive en `src/iol_cli` y no usa SQLite, web, advisor, motores ni simulaciones.

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
