# Pre-contexto: Motores de Análisis (Régimen + Macro)

## Objetivo
Obtener señales de régimen de mercado y contexto macro desde los motores locales antes de cualquier análisis de portafolio u oportunidades.

## Flujo

1. Obtener señal de régimen:
   ```
   iol engines regime show
   ```
   Leer: `regime`, `regime_score`, `breadth_score`, `volatility_regime`, `defensive_weight_adjustment`

2. Obtener señal macro:
   ```
   iol engines macro show
   ```
   Leer: `argentina_macro_stress` (0-100), `global_risk_on` (0-100)

3. Si los datos tienen más de 1 día de antigüedad, refrescar:
   ```
   iol engines run-all --skip-external
   ```

## Interpretación de señales

| Condición | Implicación operativa |
|---|---|
| `regime = bear` o `crisis` | Reducir sizing de equity; priorizar activos defensivos |
| `defensive_weight_adjustment < 0` | Aplicar ese % de reducción al presupuesto de equity |
| `argentina_macro_stress > 65` | Favorecer CEDEARs (dólares) sobre acciones BCBA locales |
| `argentina_macro_stress > 80` | Considerar posición defensiva casi total en CEDEARs o cash |
| `global_risk_on > 60` | Contexto global favorable para riesgo |
| `global_risk_on < 40` | Cautela global; reforzar sesgo defensivo |
| `regime = bull` + `global_risk_on > 60` | Régimen favorable; pesos normales o agresivos |
| `volatility_regime = extreme` | Reducir tamaño de posiciones independientemente del régimen |

## Salida esperada
Párrafo breve de contexto a incluir al inicio del análisis. Ejemplo:

> **Contexto de motores (2025-01-15):** Régimen BEAR (score 28/100, breadth 34%). Ajuste defensivo: -20% equity. Estrés AR: 71/100 → favorecer CEDEARs. Risk-On global: 48/100.

## Guardrail
No ejecutar análisis de oportunidades sin haber consultado primero este paso.
