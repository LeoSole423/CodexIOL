# Evidence Schema (simbolos web)

## Objeto minimo por claim
```json
{
  "symbol": "SPY",
  "query": "SPY expense ratio",
  "source_name": "Issuer/Official site",
  "source_url": "https://example.com",
  "published_date": "2026-02-10",
  "retrieved_at_utc": "2026-02-14T18:20:00Z",
  "claim": "Expense ratio is X",
  "confidence": "high",
  "date_confidence": "high",
  "notes": "Any caveat"
}
```

## Alineacion SQLite (`advisor_evidence`)
- `symbol` -> `symbol`
- `query` -> `query`
- `source_name` -> `source_name`
- `source_url` -> `source_url`
- `published_date` -> `published_date`
- `retrieved_at_utc` -> `retrieved_at_utc`
- `claim` -> `claim`
- `confidence` -> `confidence`
- `date_confidence` -> `date_confidence`
- `notes` -> `notes`
- `conflict_key` -> `conflict_key` (opcional)

## Reglas
- `published_date`: `YYYY-MM-DD` o `null`.
- Si `published_date` es `null`, usar `date_confidence=low`.
- `confidence`: `low|medium|high`.
- `source_url` obligatorio y trazable.
- Guardar una lista de objetos por archivo de simbolo.
