# Runbook Semanal: Oportunidades (BCBA + CEDEARs)

Este flujo no ejecuta ordenes reales. Solo genera evidencia, ranking y reporte.

## 11 pasos (host + Docker)
1. `docker exec -i iol-cli iol auth test`
2. `docker exec -i iol-cli iol advisor context --out data/context/latest.json`
3. `docker exec -i iol-cli iol advisor opportunities snapshot-universe --universe bcba_cedears`
4. `docker exec -i iol-cli iol advisor evidence list --days 60 --limit 50`
5. Opcional (pre-carga manual): `docker exec -i iol-cli iol advisor evidence add --symbol <SIMBOLO> --query "<QUERY>" --source-name "<FUENTE>" --source-url "<URL>" --claim "<CLAIM>" --confidence medium --date-confidence medium`
6. Opcional (fetch standalone): `docker exec -i iol-cli iol advisor evidence fetch --from-context --max-symbols 15 --per-source-limit 2`
7. Ejecutar ranking (incluye auto-fetch por defecto): `docker exec -i iol-cli iol advisor opportunities run --mode both --budget-ars <PRESUPUESTO_ARS> --top 10`
8. `docker exec -i iol-cli iol advisor opportunities list-runs --limit 5`
9. `docker exec -i iol-cli iol advisor opportunities report --run-id <RUN_ID> --out reports/latest/Oportunidades.md`
10. `docker exec -i iol-cli iol advisor log --prompt "runbook oportunidades semanal" --response "run_id=<RUN_ID>; reporte=reports/latest/Oportunidades.md"`
11. `docker exec -i iol-cli iol order simulate --side buy --market bcba --symbol <SIMBOLO> --quantity <QTY> --price <PRECIO>`

## Nota operativa
- Si decides ejecutar ordenes reales, seguir `prompts/60_safe_execution.md` y usar `--confirm CONFIRMAR`.
- Para desactivar el auto-fetch dentro del run: agregar `--no-fetch-evidence`.
