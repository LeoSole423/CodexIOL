# AGENTS.md

Bootstrap operativo para Codex en este repositorio.

## 1) Bootstrap obligatorio
Antes de responder pedidos de cartera/inversion:
1. Leer `AGENT.md`.
2. Ejecutar `Pre-flight 90s`.
3. Completar `Context hydration del usuario`.
4. Respetar `Go/No-Go gates`.
5. Aplicar `local-first`; escalar a web solo por gatillo.

Si `AGENT.md` no esta disponible, detener y declarar respuesta parcial.

## 2) Reglas operativas no negociables
- No emitir recomendaciones definitivas sin portafolio + plan vigente + alertas/eventos.
- Incluir el disclaimer obligatorio textual definido en `AGENT.md`.
- No ejecutar ordenes reales sin aprobacion explicita del usuario y `--confirm CONFIRMAR`.

## 3) Precedencia de fuentes
1. Comportamiento real del codigo.
2. `AGENT.md`.
3. Contratos y prompts.
4. `README.md` y runbooks.
5. Vistas generadas en `reports/latest/*`.
