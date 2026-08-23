# AGENTS.md

## Propósito

CodexIOL es exclusivamente un CLI operativo para la API de InvertirOnline. El paquete activo vive en `src/iol_cli` y no usa SQLite, web, advisor, motores ni simulaciones.

`src/iol_review` es un subsistema independiente de revisión mensual: solo puede leer estado, carteras, órdenes y movimientos. Nunca importa comandos operativos ni llama endpoints mutantes. Sus artefactos son recomendaciones auditables, no instrucciones de trading.

`src/iol_mcp` implementa la superficie MCP consumida por ChatGPT. Es estrictamente de solo lectura: entrega cartera, perfil y memoria de inversión normalizados, pero no puede preparar, ejecutar ni cancelar operaciones.

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

## MCP

- El MCP es una frontera arquitectónica de seguridad, no una restricción de interfaz. Nunca exponer herramientas de preparación, ejecución o cancelación de órdenes, FCI, transferencias, requests arbitrarios ni ninguna acción que modifique la cuenta.
- `iol_mcp` sólo puede depender del gateway de lectura de `iol_review`; no puede importar comandos operativos, tickets, API genérica, `IOLClient` ni `legacy`.
- Todas las tools MCP deben ser explícitas, estructuradas, anotadas como `read_only_hint=true`, idempotentes y de universo cerrado. No añadir herramientas genéricas de HTTP, archivos o consultas arbitrarias.
- IOL es la fuente de verdad para cartera, efectivo, órdenes y movimientos actuales. El perfil privado y los artefactos de revisión son memoria histórica; nunca presentarlos como datos actuales.
- Aplicar minimización y redacción antes de cada respuesta MCP. Nunca devolver credenciales, tokens, cabeceras, cookies, respuestas HTTP crudas, DNI/CUIT, datos de cuenta, email, domicilio, rutas locales o stack traces.
- El servicio MCP se publica localmente en Docker y se conecta a ChatGPT mediante un túnel seguro o un despliegue remoto autenticado. No exponerlo directamente a Internet ni publicar el puerto en todas las interfaces.

## Git y GitHub

### Principios

- Git es parte de la superficie de seguridad del proyecto: un commit o push nunca debe publicar credenciales, datos de cuenta, informes privados ni artefactos de operación.
- Antes de modificar archivos, inspeccionar `git status --short --branch`, la rama actual y el remoto configurado. Un árbol de trabajo sucio puede contener cambios legítimos del usuario: no descartarlos, sobrescribirlos ni incluirlos por defecto.
- Usar ramas con prefijo `codex/` para trabajo nuevo. No crear, cambiar, fusionar ni borrar ramas salvo que la tarea lo requiera o el usuario lo autorice.
- `main` es la rama de integración. Nunca hacer commits ni pushes directos a `main`: los cambios se integran mediante un PR desde una rama `codex/<tema>`.
- Tras completar una implementación y pasar las verificaciones requeridas, publicar automáticamente la rama, abrir o actualizar un PR hacia `main` y fusionarlo. Antes del merge, verificar que la rama objetivo sigue siendo `main`, que el árbol está limpio, que los checks obligatorios y las revisiones exigidas por GitHub están aprobados y que el PR no tiene conflictos.
- Si el push, PR, checks, revisión requerida, protección de rama o merge falla, no intentar eludirlo: informar el bloqueo y conservar la rama para revisión humana. No cambiar configuración de GitHub ni protecciones de rama automáticamente.
- Nunca usar `--force`, `--force-with-lease`, `git reset --hard`, `git clean`, `git checkout --`, `git restore` destructivo ni reescribir historia, salvo instrucción inequívoca del usuario.

### Preparación y revisión de commits

- Crear commits pequeños, coherentes y con un único propósito. No mezclar refactors no relacionados, cambios del usuario o archivos generados con una implementación funcional.
- No usar `git add -A`, `git add .` ni staging masivo. Agregar explícitamente los archivos revisados; una excepción requiere inspeccionar antes la lista exacta de rutas.
- Antes de cada commit, revisar `git diff --check`, `git diff --cached --name-only` y `git diff --cached --stat`. Revisar el diff completo de archivos que afecten autenticación, IOL, Docker, CI, dependencias, documentación de seguridad o configuración.
- Ejecutar las verificaciones pertinentes antes de confirmar. Como mínimo para cambios de código: `python -m pytest -q`, `iol --help`, `iol-review --help` cuando corresponda y build Docker si afecta paquete, imagen o dependencias. No ocultar fallos; si una verificación no puede ejecutarse, explicarlo antes del commit.
- Usar mensajes de commit imperativos y descriptivos. No incluir nombres de cuentas, símbolos de cartera, saldos, datos personales ni secretos en mensajes de commit, ramas, tags, PRs o logs publicados.

### Privacidad y control de secretos

- Nunca forzar el agregado de archivos ignorados. Están prohibidos en Git `.env`, perfiles reales, tickets, `data/`, `reports/`, snapshots, decisiones, contextos, logs, caches y respuestas HTTP, salvo archivos vacíos `.gitkeep` ya aprobados explícitamente.
- Antes de cada commit y push, verificar que el índice no contiene rutas privadas y hacer una búsqueda de secretos sobre el contenido staged. Buscar como mínimo credenciales IOL, access/refresh tokens, cabeceras `Authorization`, cookies, claves privadas y tokens de GitHub. Informar sólo cantidades y rutas seguras; nunca imprimir una coincidencia sensible.
- Si se detecta un secreto staged, detener el commit/push, quitar únicamente ese archivo del índice de forma no destructiva y pedir instrucciones. Si un secreto ya fue publicado, detenerse y avisar al usuario para revocarlo y limpiar el historial mediante un proceso acordado.
- Verificar con `git check-ignore` que los archivos privados siguen ignorados. No leer, copiar, mostrar ni incluir sus valores en salidas, documentación o commits.

### Publicación y cierre

- Antes de hacer push, confirmar rama, remoto, commit objetivo y que `git status --short` no contiene cambios no intencionales. Hacer push sólo de la rama `codex/` de trabajo.
- Después del push, verificar el tracking branch y el commit remoto con `git status --short --branch` y `git log -1 --oneline`. Crear o actualizar el PR contra `main`, esperar/consultar los checks y fusionar sólo cuando estén aprobados.
- Si el remoto avanzó o existe un conflicto, actualizar la rama mediante un procedimiento no destructivo y volver a ejecutar las verificaciones; si no es posible, informar la divergencia y solicitar dirección.

## Arquitectura activa

- `config.py`: configuración mínima.
- `iol_client.py`: autenticación y HTTP.
- `commands_*.py`: grupos Typer.
- `tickets.py`: preparación y ejecución única de órdenes.
- `iol_mcp/`: integración MCP de solo lectura para ChatGPT.
- `legacy/`: archivo histórico, excluido del paquete y sin soporte.

El código activo nunca debe importar desde `legacy`, FastAPI, SQLite, advisor, engines, frontend o reconciliación.

## Terminado

- `python -m pytest -q`
- `iol --help`
- build Docker exitoso.
- pruebas HTTP completamente simuladas; nunca usar credenciales reales.
