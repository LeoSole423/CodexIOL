# Prompt Especializado: Busqueda y Validacion de Simbolos

## Objetivo
Validar simbolos del portafolio y enriquecerlos con informacion externa confiable, priorizando IOL y luego fuentes oficiales/primarias.

## Pipeline obligatorio
1. Extraer simbolos desde `data/context/latest.json` o consulta directa del usuario.
2. Validar simbolos en IOL:
   - `iol market quote --market bcba --symbol <SIMBOLO>`
   - `iol market instruments --country argentina`
   - `iol market panels --country argentina --instrument <INSTRUMENTO>`
   - `iol market panel-quotes --country argentina --instrument <INSTRUMENTO> --panel <PANEL>`
3. Si el mapping es ambiguo, usar `scripts/search_cedears.py "<regex>"`.
4. Solo despues consultar web externa (fuentes oficiales o primarias).
5. Ingestar evidencia en SQLite:
   - `iol advisor evidence add --symbol <SIMBOLO> --query "<query>" --source-name "<fuente>" --source-url "<url>" --claim "<claim>" --confidence low|medium|high --date-confidence low|medium|high [--published-date YYYY-MM-DD] [--notes "..."] [--conflict-key "<topic>"]`
6. Registrar evidencia por simbolo en archivos locales (opcional para auditoria):
   - `data/evidence/<YYYY-MM-DD>/symbols/<SIMBOLO>.json`
   - `data/evidence/latest/symbols/<SIMBOLO>.json`
7. Para cada claim registrar:
   - fecha de publicacion
   - fecha/hora de consulta (`retrieved_at_utc`)
8. Si no hay fecha de publicacion verificable: `date_confidence=low`.
9. Si hay conflicto entre fuentes: usar el mismo `conflict_key` para claims incompatibles y no inferir.
10. Checklist minimo por simbolo:
    - 1 fuente oficial (issuer/regulador/market operator)
    - 1 fuente primaria adicional (factsheet, filing, exchange notice)

## Formato minimo de evidencia
- `symbol`
- `query`
- `source_name`
- `source_url`
- `published_date`
- `retrieved_at_utc`
- `claim`
- `confidence`
- `date_confidence`
- `notes`

Ver contrato completo: `prompts/contracts/evidence_schema.md`.

## Salida
Usar formato de `prompts/contracts/output_schema.md` tipo `investigacion_simbolos`.
