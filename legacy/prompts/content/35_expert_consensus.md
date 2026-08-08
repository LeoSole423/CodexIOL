# Prompt Especializado: Consenso de Referentes Confiables

## Objetivo
Agregar una capa de validacion cualitativa al ranking cuantitativo usando referentes financieros confiables en sitios de renombre.

## Politica de fuentes (estricta)
- Modo por defecto: `strict_official_reuters`.
- Fuentes permitidas:
  - Oficiales/regulatorias/issuer (`source_tier=official`), por ejemplo SEC/filings.
  - Reuters (`source_tier=reuters`).
- Fuentes fuera de politica estricta no deben sumar al consenso de referentes.

## Flujo obligatorio (TopN <-> Web)
1. Recibir simbolos desde ranking preliminar (TopK) y holdings actuales.
2. Buscar evidencia reciente por simbolo en fuentes permitidas.
3. Guardar evidencia con metadata estandar en `notes` JSON:
   - `expert_name`
   - `org`
   - `source_tier`
   - `stance` (`bullish|neutral|bearish`)
   - `topic`
   - `run_stage` (`prelim|rerank`)
4. Recalcular score/catalyst usando la evidencia recolectada.

## Reglas de consenso
- `consensus_state=aligned`: no hay conflicto bullish vs bearish.
- `consensus_state=mixed`: predominan neutros o mezcla suave.
- `consensus_state=conflict`: coexistencia de senales bullish y bearish fuertes.

## Politica ante conflicto
- No bloquear automaticamente el activo.
- Marcar `decision_gate=manual_review`.
- En salida, mostrar aviso claro y dejar decision final al usuario.

## Salida minima
- Resumen de consenso por simbolo (`trusted_refs`, `expert_signal`, `consensus_state`).
- Senal operativa (`auto` vs `manual_review`).
- Trazabilidad de fuentes (URL + fechas).
