# Decentraland Data Platform — Diseño

**Fecha:** 2026-08-20 (actualizado 2026-08-21 con capa staging y Lambda decode)
**Estado:** validado por secciones en conversación; pendiente revisión final del documento

## 1. Objetivo

Proyecto de portfolio de data engineering. Pipeline serverless en AWS que:
extrae datos on-chain de Decentraland (logs crudos de contratos en Ethereum y
Polygon) y precios del token MANA, los procesa en una arquitectura medallion
(bronze/staging/silver/gold/platinum) y los sirve en un dashboard público.

Restricciones: costo ~$0/mes (free tiers de AWS, GCP y Streamlit), todo
reproducible desde el repo (Terraform), y maximizar valor demostrable en
entrevistas (decodificación ABI propia, dbt, Step Functions, Terraform, CI/CD).

## 2. Fuentes de datos

- **On-chain**: BigQuery public datasets `crypto_ethereum` y `crypto_polygon`.
  Query filtrada por direcciones de contratos de Decentraland (Marketplace v1/v2,
  LANDRegistry, EstateRegistry, colecciones de wearables, Bids, tienda de
  colecciones en Polygon) y por partición de fecha. Los logs vienen crudos
  (topics/data en hex) — la decodificación es nuestra. Free tier permanente de
  BigQuery: 1 TB de queries/mes; la extracción diaria consume GBs.
- **Precios**: CoinGecko API pública — OHLCV/market data diaria de MANA.

## 3. Data lake medallion (un solo bucket S3)

| Capa | Contenido | Quién escribe |
|---|---|---|
| `bronze/onchain_logs/chain=<c>/dt=<d>/` | logs hex crudos, parquet | Lambda `extract_onchain` |
| `bronze/mana_prices/dt=<d>/` | respuesta CoinGecko casi cruda | Lambda `extract_prices` |
| `staging/decoded_events/chain=<c>/dt=<d>/` | eventos decodificados y normalizados | Lambda `decode` |
| `silver/` | `nft_sales`, `mana_prices_daily`, eventos limpios | dbt |
| `gold/` | `daily_marketplace_kpis`, `asset_category_stats`, `mana_price_vs_activity` | dbt |
| `platinum/` | extractos ultra-agregados (KB), público-legibles | Lambda `export_platinum` |

Un solo bucket con prefijos (más simple para IAM y lifecycle). Glue Data Catalog
como metastore; todo consultable desde Athena.

**Platinum** es la capa de servicio: el dashboard lee estos parquets por HTTPS
sin credenciales AWS y sin generar queries de Athena por visita. Argumento:
"gold es para analistas con SQL; platinum es el contrato de datos con aplicaciones".

## 4. Frontera Python / SQL

Decisión clave del diseño (dbt-athena no soporta Python models):

- **Python — Lambda `decode` (bronze → staging)**, trabajo a nivel de registro:
  identificar evento por `topics[0]`, decodificar topics/data con `eth_abi`
  (tipos declarados por evento), normalizar unidades (wei→MANA, direcciones,
  timestamps UTC).
- **SQL/dbt (staging → silver → gold)**, trabajo a nivel de conjunto:
  deduplicación por `(tx_hash, log_index)`, armado de la entidad "venta"
  joineando eventos de la misma transacción (`OrderSuccessful` + `Transfer`),
  unificación de los caminos de venta (Marketplace, Bids, tienda Polygon) con
  UNION, clasificación por categoría vía seeds, enriquecimiento con precio USD,
  filtrado de no-ventas (mints, cancelaciones), tests de calidad.

La experiencia del usuario decodificando manualmente (substrings de hex) se
luce en `tests/test_decode.py`: decodificación manual como oráculo contra
`eth_abi`, más validación de N ventas contra Etherscan/Polygonscan.

## 5. Datos de referencia

dbt seeds (CSVs versionados en `dbt/seeds/`): direcciones de contratos,
categorías de assets, firmas de eventos. Se descartó DynamoDB: los joins pasan
en Athena, el versionado por git es superior para referencia chica, y no hay
estado operacional que guardar (la idempotencia por partición de día hace de
watermark).

## 6. Orquestación y operación

Step Functions (Standard, free tier), disparado por EventBridge cron 06:00 UTC:

```
[extract_onchain ∥ extract_prices] → decode → dbt build (silver→gold + tests) → export_platinum
```

- Retries declarativos (2 intentos, backoff exponencial) por paso.
- Rama Catch → SNS → email en fallos.
- dbt tests fallando frenan el pipeline antes de publicar platinum.
- Idempotencia: reescribir la partición del día es seguro.
- Backfill: mismo state machine con input `{"start_date": ..., "end_date": ...}`;
  default "ayer".
- dbt-core + dbt-athena como Lambda container image (no entra en zip de 250 MB);
  build estimado 1-3 min, muy por debajo del límite de 15 min. Camino de escalado
  documentado: mover esa tarea a ECS Fargate si el build creciera 10x.
- IAM de mínimo privilegio por Lambda (extract escribe solo bronze; decode lee
  bronze y escribe staging; dbt lee staging y escribe silver/gold; export escribe
  platinum). Service account de GCP en SSM Parameter Store.
- CloudWatch logs con retención 7 días.

## 7. Consumo

Dashboard Streamlit en Streamlit Community Cloud (gratis), leyendo `platinum/`
por HTTPS. Vistas: KPIs del marketplace, precio MANA vs actividad, análisis por
categoría de asset, y página "About" con el diagrama de arquitectura.

## 8. Infraestructura y CI/CD

- **Terraform** para todo (elegido sobre SAM por valor de CV y cobertura de
  recursos de datos); región us-east-1; nunca crear recursos por consola.
- **GitHub Actions**: en PRs `pytest` + `ruff` + `terraform validate/plan` +
  `dbt parse`; en main, build de imágenes + `terraform apply` con OIDC
  (sin access keys guardadas).

## 9. Testing

- **pytest**: chunking/particionado de extracción, lógica de decode (manual vs
  eth_abi con fixtures de logs reales).
- **dbt tests**: unicidad, not_null, freshness, rangos de precio en silver y gold.
- **Validación cruzada**: ventas decodificadas comparadas contra exploradores
  de bloques.

## 10. Orden de construcción

1. Terraform base (bucket, catalog, IAM) + Lambda de precios → primer dato en bronze.
2. Lambda BigQuery → bronze on-chain.
3. Lambda decode → staging.
4. dbt: silver + tests.
5. dbt: gold + export platinum.
6. Step Functions + EventBridge.
7. Dashboard + README con diagrama.

## Fuera de alcance (YAGNI)

Streaming/tiempo real, DynamoDB, Redshift, Airflow/MWAA, QuickSight, más chains
que Ethereum y Polygon, API de servicio propia.
