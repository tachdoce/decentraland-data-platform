# Decentraland Data Platform

Proyecto de portfolio de data engineering: pipeline serverless en AWS que extrae
datos on-chain de Decentraland (logs de contratos en Ethereum y Polygon) y precios
de MANA, los procesa en una arquitectura medallion y los sirve en un dashboard
público. Presupuesto objetivo: ~$0/mes (free tiers).

## Arquitectura en 5 líneas

- **Fuentes**: BigQuery public datasets (`crypto_ethereum`, `crypto_polygon`) para logs crudos; CoinGecko para precios MANA.
- **Lake medallion en un solo bucket S3**: `bronze/` (crudo) → `staging/` (decodificado) → `silver/` (entidades limpias) → `gold/` (KPIs) → `platinum/` (extractos públicos para el dashboard).
- **Frontera Python/SQL**: Python (Lambda `decode` con eth_abi) para transformación a nivel registro; dbt-athena para todo lo set-based (dedup, joins, agregaciones). dbt-athena NO soporta Python models.
- **Orquestación**: Step Functions — extracciones en paralelo → decode → dbt build → export platinum. EventBridge cron diario 06:00 UTC. SNS para alertas.
- **Consumo**: Streamlit Community Cloud leyendo parquets de `platinum/` por HTTPS (sin Athena en vivo, sin credenciales).

## Estructura

| Carpeta | Rol |
|---|---|
| `terraform/` | Toda la infra (S3, Lambdas, Step Functions, Glue/Athena, IAM, SNS) |
| `ingestion/onchain/`, `ingestion/prices/` | Lambdas de extracción → bronze |
| `decode/` | Lambda Python bronze → staging (decodificación ABI con eth_abi) |
| `dbt/` | Proyecto dbt-athena: staging → silver → gold. Referencia en `seeds/` |
| `platinum_export/` | Lambda gold → platinum |
| `dashboard/` | App Streamlit |
| `tests/` | pytest; `test_decode.py` valida eth_abi contra decodificación manual |
| `docs/superpowers/specs/` | Diseños validados con el usuario |
| `docs/superpowers/plans/` | Planes de implementación derivados de cada spec |

## Convenciones y decisiones

- Región: **us-east-1**. Usuario CLI: `terraform-admin`.
- IaC solo con Terraform (elegido sobre SAM); nunca crear recursos por consola.
- Nunca commitear: tfstate, keys de GCP, .tfvars con secretos (ver .gitignore).
- Secrets (service account GCP) en SSM Parameter Store.
- Lambdas con deps pesadas (BigQuery, eth_abi, dbt) van como container image; livianas como zip.
- Extracción incremental e idempotente por partición `chain=/dt=`; backfill = mismo código con `{start_date, end_date}`.
- Datos de referencia (contratos, categorías) como dbt seeds — se descartó DynamoDB a propósito.
- dbt tests que fallan frenan el pipeline antes de publicar platinum.
- CI en GitHub Actions: pytest + ruff + terraform validate/plan + dbt parse en PRs; deploy con OIDC en main.

## Comandos frecuentes

```bash
cd terraform && terraform plan     # SIEMPRE leer el plan antes de aplicar
cd terraform && terraform apply
aws lambda invoke --function-name <fn> --payload '{"date":"YYYY-MM-DD"}' --cli-binary-format raw-in-base64-out /dev/stdout
cd dbt && dbt build                # transforma y testea silver+gold
pytest tests/
```

## Contexto del usuario

Data engineer con ~5 años en The Sandbox; fuerte en datos, aprendiendo infra/IaC —
explicar los recursos de Terraform y conceptos de AWS al introducirlos. Idioma: español.

## Flujo de trabajo

Diseños nuevos se validan por secciones y quedan en `docs/superpowers/specs/`;
de cada spec sale un plan en `docs/superpowers/plans/` que se ejecuta con
checkpoints. Orden de construcción acordado: 1) Terraform base + Lambda precios,
2) Lambda BigQuery→bronze, 3) decode, 4) dbt silver, 5) gold+platinum,
6) Step Functions, 7) dashboard+README.
