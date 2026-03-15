# Orquestador de Prompts (IOL)

Objetivo: enrutar cada consulta al prompt especializado correcto, manteniendo seguridad operativa y trazabilidad.

## Flujo base obligatorio
1. Confirmar entorno (`iol-cli` en Docker).
2. Construir contexto con `iol advisor context`.
3. Seleccionar prompt por intencion.
4. Responder usando `prompts/contracts/output_schema.md`.
5. Guardar interacciones relevantes con `iol advisor log`.

## Routing por intencion
- Regimen de mercado, señales de motores, macro computada: `prompts/32_regime_engine.md`.
- Smart money, 13F, posicionamiento institucional: `prompts/37_smart_money.md`.
- Analisis de portafolio, riesgos, recomendaciones: `prompts/20_portfolio_analysis.md`.
- Macro via fuentes externas (investigacion profunda): `prompts/30_macro_sources.md`.
- Consenso de referentes confiables para oportunidades: `prompts/35_expert_consensus.md`.
- Busqueda/validacion de simbolos o instrumentos: `prompts/40_symbol_web_research.md`.
- Ranking de oportunidades (compras/recompras): `prompts/45_opportunity_scoring.md`.
- Gestion de alertas o eventos manuales: `prompts/50_alerts_events.md`.
- Ejecucion de ordenes/simulaciones/lotes: `prompts/60_safe_execution.md`.

## Reglas transversales
- Siempre priorizar datos locales (`advisor context`, DB) antes de inferencias.
- Si falta contexto: recuperar datos, no improvisar.
- No ejecutar ordenes reales sin confirmacion explicita y comando final con `--confirm CONFIRMAR`.
- Mantener salida accionable y breve; separar hechos, recomendaciones y supuestos.
- En runs semanales de oportunidades, devolver siempre decision final (`comprar|recomprar|no operar`) y justificar el por que de cada recomendacion.
- Antes de cualquier analisis de oportunidades o portafolio: ejecutar `prompts/32_regime_engine.md` como paso 0.
