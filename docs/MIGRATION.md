# Migración desde CodexIOL histórico

El proyecto ahora es únicamente un cliente operativo de IOL. No existe migración de la base histórica y su contenido permanece intacto e ignorado por Git.

| Comando anterior | Comando actual |
| --- | --- |
| `iol portfolio` | `iol account portfolio` |
| `iol market ...` | `iol market ...` |
| `iol orders list/get/cancel` | `iol orders list/get/cancel` |
| `iol order simulate` | `iol orders prepare` |
| `iol order confirm` | `iol orders execute` |
| `iol order buy/sell` | `iol orders prepare` y luego `iol orders execute` |
| `iol fci subscribe/redeem` | `iol funds subscribe/redeem` |
| `iol raw` | `iol api request` |
| `iol movements sync` | `iol account movements` (consulta directa, sin persistencia) |

Los grupos `advisor`, `batch`, `cashflow`, `data`, `engines`, `reconcile`, `simulate`, `snapshot` y `web` fueron retirados del CLI activo. Su implementación histórica está bajo `legacy/` y no recibe mantenimiento.
