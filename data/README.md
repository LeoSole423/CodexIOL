# Data Directory Policy

Este directorio contiene datos locales y potencialmente sensibles.

Regla del repositorio:
- Se versiona solo la estructura de carpetas (`.gitkeep` y este `README.md`).
- No se versionan snapshots, evidencias, bases SQLite ni salidas reales del usuario.

Subcarpetas esperadas:
- `data/context/`
- `data/evidence/`
- `data/cache/`
- `data/plans/`
