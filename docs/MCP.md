# CodexIOL MCP: integración privada de solo lectura

`iol-mcp` implementa un servidor MCP estándar con Streamable HTTP. Entrega contexto normalizado para que ChatGPT investigue y analice la cartera; no opera dinero ni expone la API genérica de IOL.

## Límites de seguridad

- No existen herramientas para preparar, ejecutar o cancelar órdenes, operar FCI, transferir dinero ni invocar endpoints arbitrarios.
- IOL puede usar internamente una autenticación y una consulta de movimientos implementada como POST, pero esos detalles no son herramientas MCP ni quedan alcanzables por ChatGPT.
- Las respuestas excluyen credenciales, tokens, cookies, headers, respuestas HTTP crudas y datos personales innecesarios.
- Las revisiones, decisiones y el plan de transición son memoria histórica. Las tenencias actuales sólo provienen de IOL y siempre incluyen timestamps y advertencias de cobertura.

## Ejecución local

```powershell
docker compose up -d --build
docker compose logs iol-mcp
```

El servicio queda en `http://127.0.0.1:8000/mcp`. Está ligado a localhost deliberadamente; no cambiarlo a una interfaz pública.

Para desarrollo por stdio:

```powershell
$env:IOL_MCP_TRANSPORT = "stdio"
iol-mcp
```

## Conexión con ChatGPT

ChatGPT web requiere un servidor MCP remoto. Para un servicio que permanece en esta máquina, conectá el endpoint local mediante Secure MCP Tunnel. Para un despliegue remoto, agregá TLS, autenticación, gestión de secretos, rate limiting y logs sin datos financieros; no copies `.env` ni el perfil privado a una imagen o repositorio.

En ChatGPT, agregá la app MCP en Developer Mode, configurá el endpoint seguro y revisá la lista de herramientas detectadas. Debe contener exactamente:

```text
get_portfolio_snapshot
get_investor_profile
get_movements
get_investment_history
get_asset_context
get_capabilities
```

Si aparece una herramienta adicional o una que permita una acción mutante, detené el despliegue: es un fallo de seguridad.

## Uso esperado

1. Pedí a ChatGPT que consulte `get_portfolio_snapshot` y `get_investor_profile`.
2. Para una tesis anterior, usá `get_investment_history`; para una posición puntual, `get_asset_context` con mercado explícito.
3. Investigá el mercado con fuentes externas y separá hechos, inferencias y pronósticos.
4. Si decidís operar, salí de ChatGPT y usá el CLI: `iol orders prepare` seguido por `iol orders execute` y la confirmación exacta `CONFIRMAR`.
