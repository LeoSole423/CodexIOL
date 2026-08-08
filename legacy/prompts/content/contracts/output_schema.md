# Output Schema

Formato comun de respuesta para prompts del asesor.

## Tipo: analisis
1. Datos usados (`as_of`, warnings).
2. Resumen de situacion (1-3 lineas).
3. Recomendaciones accionables (3-5 items).
4. Riesgos y supuestos (1-3 items).
5. Siguiente paso sugerido (1 linea).

## Tipo: macro
1. Resumen macro (global + local).
2. Implicancias para cartera.
3. Riesgos de escenario.
4. Fuentes citadas con fecha.

## Tipo: investigacion_simbolos
1. Simbolos consultados y estado de validacion en IOL.
2. Hallazgos externos por simbolo.
3. Conflictos o incertidumbre.
4. Evidencia guardada (paths).

## Tipo: alertas_eventos
1. Operacion solicitada (create/list/close/add).
2. Resultado (ids, estado).
3. Pendientes y proximos pasos.

## Tipo: oportunidades
1. Decision final obligatoria:
   - `comprar`, `recomprar`, o `no operar`.
2. Recomendaciones accionables:
   - Si hay operacion: lista de activos con tipo (`new|rebuy`), monto y niveles.
   - Si no hay operacion: declarar explicitamente `sin operaciones recomendadas`.
3. Motivos de la decision:
   - Explicar el por que de cada recomendacion (o del no-operar) con score, riesgo y evidencia.
4. Parametros y evidencia del run (`as_of`, `mode`, `budget_ars`, `top_n`, resumen `evidence_fetch`).
5. Riesgos/flags clave y condicion de invalidacion.
6. Siguiente paso sugerido (simulacion y validacion manual).
