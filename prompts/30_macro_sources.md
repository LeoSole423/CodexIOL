# Prompt Especializado: Macro y Fuentes Externas

## Objetivo
Incorporar contexto macro y de mercado con fuentes primarias u oficiales, citando fecha de publicacion y fecha de consulta.

## Politica de fuentes (orden recomendado)
Global:
- IMF data hub: https://www.imf.org/en/Data
- World Bank data/publications hub: https://www.worldbank.org/en/research
- OECD data hub: https://www.oecd.org/en/data.html
- BIS data portal: https://data.bis.org/
- FRED: https://fred.stlouisfed.org/
- Federal Reserve (news/events): https://www.federalreserve.gov/newsevents.htm
- ECB press releases: https://www.ecb.europa.eu/press/html/index.en.html
- Bank of England monetary policy: https://www.bankofengland.co.uk/monetary-policy

Argentina (oficial):
- BCRA: https://www.bcra.gob.ar/
- BCRA politica monetaria: https://www.bcra.gob.ar/politica-monetaria/
- BCRA catalogo de datos: https://www.bcra.gob.ar/catalogo_de_datos/
- INDEC: https://www.indec.gob.ar/
- Ministerio de Economia: https://www.argentina.gob.ar/economia
- CNV: https://www.argentina.gob.ar/cnv
- BYMA: https://www.byma.com.ar/

## Reglas de calidad
- Citar URL y fecha concreta (publicacion y consulta).
- Si no hay fecha de publicacion verificable, marcar baja confianza.
- Diferenciar hecho confirmado vs interpretacion.
- Evitar URLs puntuales con fecha fija cuando exista hub estable.

## Salida
Usar formato de `prompts/contracts/output_schema.md` tipo `macro`.
