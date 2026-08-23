# Revisión mensual de cartera

`iol-review` separa la información de cartera de la ejecución. Solo consulta estado de cuenta, cartera Argentina, cartera Estados Unidos, órdenes y movimientos; no tiene comandos ni dependencias para comprar, vender, cancelar, operar FCI o enviar requests mutantes.

## Preparación

1. Copiar `config/investor-profile.example.toml` a `config/investor-profile.toml`.
2. Completar objetivo, horizonte, liquidez, tolerancia a pérdidas, mercados e instrumentos permitidos.
3. Ejecutar `docker exec -it iol-cli iol-review profile validate`.

El perfil y los artefactos generados son privados: Git los ignora. El export incluye un hash reproducible, estados de fuente y advertencias, pero no tokens ni cuerpos HTTP crudos.

## Flujo

La tarea mensual local de Codex produce un informe Markdown, evidencia JSON y propuestas JSON. Distingue hechos, inferencias, pronósticos y preferencias. Toda recomendación se compara expresamente con no modificar la cartera.

La continuidad vigente puede referenciar un plan privado bajo `reports/monthly/`, declarado mediante `continuity_document` en el perfil. Ese documento conserva por separado objetivos actuales, hechos humanos ya ejecutados y propuestas pendientes. Los informes de `reports/latest/`, `reports/analisis/`, `reports/rebalance/`, `reports/dca/`, `reports/macro/` y los planes de `data/plans/` pertenecen a legacy: son historia, no instrucciones activas ni autorización para operar.

Si el contexto actual contradice el plan de continuidad, la revisión debe informar la contradicción y reevaluar la tesis. Nunca aplica automáticamente bandas, etapas pendientes ni planes históricos.

Las nuevas ideas quedan como `research` y `eligible_for_order=false`. Ninguna propuesta incluye cantidades, monto, ticket ni payload. Una eventual operación continúa siendo una decisión humana separada, mediante el CLI `iol` y su confirmación interactiva.

La revisión es información para decidir; no garantiza rendimiento ni reemplaza asesoramiento financiero profesional.
