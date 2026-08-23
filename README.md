# CodexIOL CLI

Cliente de consola minimalista para consultar y operar una cuenta de InvertirOnline.

> **Advertencia:** la URL predeterminada es la API real de IOL. Los comandos de ejecución, cancelación, FCI y requests HTTP mutantes pueden afectar dinero real. Ningún ejemplo de esta guía ejecuta una operación.

## Configuración

1. Copiar `.env.example` a `.env`.
2. Completar `IOL_USERNAME` e `IOL_PASSWORD`.
3. Construir el único servicio:

```powershell
docker compose up -d --build
```

Ejecutar comandos con:

```powershell
docker exec -it iol-cli iol auth test
docker exec -it iol-cli iol account status
```

## Comandos

```text
iol auth test

iol account status
iol account portfolio [--country argentina|estados_Unidos]
iol account movements [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--country ...] [--currency ...]

iol market quote --market bcba --symbol GGAL
iol market instruments [--country ...]
iol market panels --instrument <nombre> [--country ...]
iol market panel-quotes --instrument <nombre> --panel <nombre> [--country ...]

iol orders list [--status ...] [--from ...] [--to ...] [--country ...] [--number ...]
iol orders get <number>
iol orders cancel <number>
iol orders prepare --side buy|sell --market bcba --symbol GGAL ...
iol orders execute <ticket-id>

iol funds subscribe --symbol <fci> --amount <monto> [--validate]
iol funds redeem --symbol <fci> --quantity <cantidad> [--validate]

iol api request GET /api/v2/portafolio/argentina
iol api request POST /api/... --json '{...}'
```

## Flujo seguro de órdenes

Preparar una orden no se conecta al endpoint operativo:

```powershell
docker exec -it iol-cli iol orders prepare --side buy --market bcba --symbol GGAL --quantity 10 --price 1000 --plazo t0 --order-type limit
```

El resultado contiene un `id`, el payload, un resumen estimado y su vencimiento. Para ejecutarla:

```powershell
docker exec -it iol-cli iol orders execute <ticket-id>
```

El CLI vuelve a mostrar el ticket y exige escribir `CONFIRMAR`. Un ticket vence, no puede ejecutarse dos veces y nunca se incluye en Git. La comisión mostrada es una estimación configurada localmente; prevalece la liquidación de IOL.

`orders cancel`, operaciones FCI sin `--validate` y `api request` con POST/PUT/PATCH/DELETE también exigen confirmación interactiva. GET y HEAD son de lectura.

## Errores y privacidad

- El CLI no imprime tokens ni cuerpos de error devueltos por IOL.
- `.env`, tickets, bases históricas y reportes están ignorados por Git.
- Las operaciones mutantes no tienen reintento HTTP automático.
- El código anterior está archivado y no soportado en [`legacy/`](legacy/README.md).

La referencia de migración está en [`docs/MIGRATION.md`](docs/MIGRATION.md).

## Revisión mensual con Codex

CLI operativo para IOL y soporte separado para revisión mensual en modo lectura. La API configurada puede operar sobre una cuenta real: las operaciones exigen confirmación manual y la revisión mensual nunca crea ni ejecuta órdenes.

## Uso de revisión mensual

Copiá `config/investor-profile.example.toml` a `config/investor-profile.toml`, completalo y validalo:

```powershell
docker exec -it iol-cli iol-review profile validate
docker exec -it iol-cli iol-review context export
```

El contexto se guarda en `data/review/` sin tokens ni respuestas HTTP crudas. Los informes y artefactos de revisión se guardan en `reports/monthly/` y `data/review/`; ambos directorios están ignorados por Git. Una propuesta es análisis, nunca una orden: para operar se sigue usando manualmente `iol orders prepare` y luego `iol orders execute` con confirmación explícita.

Los comandos disponibles son `iol-review profile validate`, `iol-review context export [--as-of YYYY-MM-DD] [--out PATH]` e `iol-review decision record REVIEW_ID --status accepted|rejected|modified --notes TEXT`.
