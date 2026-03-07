# AGENT.md

Last updated: 2026-03-07
Owner: repo maintainers

## 1) Proposito y limites
- Este proyecto permite analisis de cartera, scoring de oportunidades y reportes sobre IOL usando CLI + web.
- Puede ejecutar ordenes reales solo con confirmacion explicita.
- Disclaimer obligatorio en respuestas de asesor:
  - "Esto no es asesoramiento financiero profesional. Es informacion educativa."
- Fuera de alcance:
  - Operativa real automatica sin confirmacion.
  - Tratar `reports/latest/*` como fuente de verdad.

## 2) Precedencia documental (estable)
Si hay conflicto entre fuentes, usar este orden:
1. Codigo y comportamiento real (`src/iol_cli`, `src/iol_web`).
2. `AGENT.md`.
3. Contratos y prompts (`prompts/contracts/*`, `prompts/*`).
4. `README.md` y runbooks (`docs/*`).
5. Vistas generadas (`reports/latest/*`).

## 3) Pre-flight 90s (obligatorio)
Ejecutar antes de analizar o recomendar:

| Paso | Comando | Pass | Fail |
|---|---|---|---|
| 1 | `docker exec -i iol-cli iol auth test` | autentica sin error | detener y corregir credenciales/contenedor |
| 2 | `docker exec -i iol-cli iol advisor context --out data/context/latest.json` | genera contexto y `as_of` valido | detener y remediar warning bloqueante |
| 3 | revisar `warnings` en contexto | sin bloqueantes o con remediacion aplicada | aplicar matriz warning->accion |
| 4 | si hay dudas de frescura, `docker exec -i iol-cli iol portfolio --country argentina` | timestamps consistentes | reportar discrepancia y no inferir |

## 4) Go/No-Go gates
Reglas explicitas para continuar o frenar:

### Gate A - Trading real
- NO-GO: si no hay aprobacion explicita del usuario y `--confirm CONFIRMAR`.
- GO: solo despues de cotizacion + simulacion + comando final explicito.

### Gate B - Calidad de evidencia en oportunidades
- NO-GO para decision automatica: `EVIDENCE_INSUFFICIENT` o `consensus_state=conflict`.
- GO condicionado: marcar `manual_review` y dejar decision final al usuario.

### Gate C - Salud de contexto
- NO-GO: `DB_NOT_FOUND`, `NO_SNAPSHOTS`, `SNAPSHOT_OLD` sin remediar.
- GO: luego de `snapshot catchup` y nuevo `advisor context`.

## 5) Matriz warning -> accion
| Warning | Impacto | Accion concreta | Bloqueo |
|---|---|---|---|
| `DB_NOT_FOUND` | no hay fuente local valida | validar `IOL_DB_PATH`, correr `iol snapshot catchup`, regenerar contexto | hard |
| `NO_SNAPSHOTS` | sin historia para analisis | correr `iol snapshot run` o `iol snapshot catchup`, regenerar contexto | hard |
| `SNAPSHOT_OLD` | analisis desactualizado | correr `iol snapshot catchup`, regenerar contexto | hard |
| `RETURNS_IGNORE_CASHFLOWS` | retornos con limitacion metodologica | declarar limitacion y, si aplica, usar `iol cashflow ...` | soft |
| `WEB_FETCH_EMPTY_FALLBACK_TO_QUANT` | scoring sin overlay web | informar fallback cuantitativo y bajar confianza | soft |
| `WEB_FETCH_PARTIAL_ERRORS` | evidencia incompleta | reportar fuentes faltantes y mantener trazabilidad | soft |

## 6) Definition of Done por flujo
### `context`
- DoD:
  - contexto JSON generado
  - `as_of` informado
  - warnings evaluados con accion
  - supuestos metodologicos explicitados

### `oportunidades`
- DoD:
  - decision final obligatoria (`comprar|recomprar|no operar`)
  - recomendaciones accionables o "sin operaciones recomendadas"
  - motivos por activo o motivo de no-operar
  - parametros del run + resumen de evidencia
  - siguiente paso seguro (simulacion/validacion manual)

### `seguimiento`
- DoD:
  - `reports/latest/Seguimiento.md` actualizado
  - referencia a fuente de verdad SQLite
  - alertas abiertas visibles y accionables

### `safe_execution`
- DoD:
  - cotizacion y simulacion ejecutadas
  - comando final exacto mostrado
  - sin `--confirm CONFIRMAR`, no hay ejecucion real

## 7) Interfaz operativa estable (bloques de salida)
Para consistencia entre sesiones, usar estos nombres de bloque:

- `ANALISIS`:
  - `DATOS_USADOS`
  - `RESUMEN_SITUACION`
  - `RECOMENDACIONES`
  - `RIESGOS_Y_SUPUESTOS`
  - `SIGUIENTE_PASO`

- `MACRO`:
  - `RESUMEN_MACRO`
  - `IMPLICANCIAS_CARTERA`
  - `RIESGOS_ESCENARIO`
  - `FUENTES_Y_FECHAS`

- `OPORTUNIDADES`:
  - `DECISION_FINAL`
  - `RECOMENDACIONES_ACCIONABLES`
  - `MOTIVOS_DE_LA_DECISION`
  - `PARAMETROS_Y_EVIDENCIA`
  - `RIESGOS_FLAGS`
  - `SIGUIENTE_PASO_SEGURO`

- `ALERTAS_EVENTOS`:
  - `OPERACION_SOLICITADA`
  - `RESULTADO`
  - `PENDIENTES_Y_PROXIMOS_PASOS`

## 8) Post-run verification (obligatorio)
Al cerrar una run de oportunidades:
1. generar markdown:
   - `docker exec -i iol-cli iol advisor opportunities report --run-id <RUN_ID> --out reports/latest/Oportunidades.md`
2. registrar bitacora:
   - `docker exec -i iol-cli iol advisor log --prompt "runbook oportunidades semanal" --response "run_id=<RUN_ID>; reporte=reports/latest/Oportunidades.md"`
3. refrescar seguimiento:
   - `docker exec -i iol-cli iol advisor seguimiento --out reports/latest/Seguimiento.md`
4. dejar supuestos activos explicitados en la respuesta final.

## 9) Quick triage (fallas recurrentes)
- SEC 403:
  - setear `IOL_SEC_CONTACT_EMAIL` o `IOL_SEC_USER_AGENT`.
- No hay snapshots de cartera:
  - correr `iol snapshot catchup`, luego `iol advisor context`.
- No hay market snapshots para scoring:
  - correr `iol advisor opportunities snapshot-universe --universe bcba_cedears`.
- Fetch web parcial/vacio:
  - continuar con cuantitativo, reportar warning, no afirmar consenso fuerte.
- Pedido de orden real sin confirmacion:
  - frenar en simulacion y pedir confirmacion explicita.

## 10) Politica de datos y privacidad
- Este repo versiona estructura de `data/` y `reports/`, no datos reales del usuario.
- No commitear DB SQLite, snapshots reales, evidencia sensible ni reportes personales.
- Fuente de verdad de alertas/eventos: SQLite (`advisor_alerts`, `advisor_events`), no markdown.

## 11) Mantenimiento
Actualizar este archivo en el mismo cambio cuando se modifique:
- contrato de comandos CLI
- secuencia de flujo operativo
- reglas de seguridad/gates
- contratos de salida del asesor
- criterio de precedencia documental
