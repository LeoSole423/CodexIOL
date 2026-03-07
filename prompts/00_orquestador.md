# Orquestador de Prompts (IOL)

Objetivo: enrutar cada consulta al prompt especializado correcto, manteniendo seguridad operativa y trazabilidad.

## Flujo base obligatorio
1. Confirmar entorno (`iol-cli` en Docker).
2. Construir contexto con `iol advisor context`.
3. Revisar foto operativa de cartera (`iol portfolio --country argentina`) y reconciliar timestamp con contexto.
4. Cargar contexto del usuario (si existe):
   - plan vigente en `reports/latest/*` (`PlanDCA`, `RevisionEstrategica`, `ResumenRebalanceo`),
   - alertas abiertas (`iol advisor alert list --status open --limit 50`),
   - eventos recientes (`iol advisor event list --limit 50`).
5. Seleccionar prompt por intencion.
6. Responder usando `prompts/contracts/output_schema.md`.
7. Guardar interacciones relevantes con `iol advisor log`.

## Routing por intencion
- Analisis de portafolio, riesgos, recomendaciones: `prompts/20_portfolio_analysis.md`.
- Macro, noticias, tasas, inflacion, fuentes externas: `prompts/30_macro_sources.md`.
- Consenso de referentes confiables para oportunidades: `prompts/35_expert_consensus.md`.
- Busqueda/validacion de simbolos o instrumentos: `prompts/40_symbol_web_research.md`.
- Ranking de oportunidades (compras/recompras): `prompts/45_opportunity_scoring.md`.
- Gestion de alertas o eventos manuales: `prompts/50_alerts_events.md`.
- Ejecucion de ordenes/simulaciones/lotes: `prompts/60_safe_execution.md`.

## Reglas transversales
- Siempre priorizar datos locales (`advisor context`, DB) antes de inferencias.
- No emitir recomendaciones definitivas si falta revisar portafolio operativo, plan vigente o alertas/eventos.
- Aplicar enfoque por capas para web:
  - `local-first` siempre,
  - escalar a web solo con gatillo justificable,
  - si no hay gatillo, cerrar con contexto local y explicitarlo.
- Si falta contexto: recuperar datos, no improvisar.
- No ejecutar ordenes reales sin confirmacion explicita y comando final con `--confirm CONFIRMAR`.
- Mantener salida accionable y breve; separar hechos, recomendaciones y supuestos.
- En runs semanales de oportunidades, devolver siempre decision final (`comprar|recomprar|no operar`) y justificar el por que de cada recomendacion.
