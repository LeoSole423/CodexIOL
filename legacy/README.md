# Archivo histórico de CodexIOL

Este directorio conserva el sistema anterior como referencia: frontend Next.js, backend FastAPI, advisor, motores, simulaciones, reconciliación, CLI extendido, prompts, automatizaciones, documentación y tests.

El contenido de `legacy/`:

- no forma parte del paquete `iol-cli`;
- no se copia a la imagen Docker activa;
- no se ejecuta en la suite de tests activa;
- no se considera mantenido ni funcional;
- puede depender de módulos, servicios y esquemas que ya no están disponibles.

Los cambios locales que existían antes del archivado fueron preservados con sus archivos. Los datos personales en `data/`, `reports/` y `.env` no fueron trasladados aquí.

Para recuperar una capacidad antigua, tratarla como una implementación nueva: revisar seguridad, dependencias, datos y pruebas antes de copiar cualquier pieza al paquete activo.
