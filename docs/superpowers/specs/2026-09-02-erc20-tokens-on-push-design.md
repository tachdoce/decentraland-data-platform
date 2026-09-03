# erc20-tokens-on-push: landing → bronze.erc20_tokens → silver.dim_erc20_tokens

Design validated with the user on 2026-09-02. Clones the contracts-on-push
pattern for the new hand-curated reference file `reference/erc20_tokens.csv`
(payment tokens seen in marketplace trades, with their price ticker and
decimals).

## 1. Goal

Publishing `reference/erc20_tokens.csv` to `landing/erc20_tokens/` must
automatically: validate the file, snapshot it into `bronze.erc20_tokens`
(parquet, partitioned by `dt`), and rebuild `silver.dim_erc20_tokens` with its
dbt tests — alerting Slack on any failure. This dimension will later feed the
prices pipeline (DefiLlama lookups by contract address) and USD valuation of
trades (`raw_amount / 10^decimals * price_usd`).

## 2. Decisions (validated)

- **Dedicated Lambda** `load-erc20-tokens` cloned from `load-contracts`, with
  its own `validate.py`. No generic reference-CSV loader yet: with two tables
  the abstraction does not pay for itself. Rule agreed: a third reference
  table triggers the extraction of a shared loader.
  `ingestion/common/partitions.py` is already shared and reused as is.
- **Full flow, not bronze-only**: the state machine also runs dbt to build
  `silver.dim_erc20_tokens` (selector `source:bronze.erc20_tokens+`), so the
  feature ships end to end.
- **Key is `(chain_id, contract_address)`**. `fsym` is informative and NOT
  unique: GALA v1 (`0x15d4…`) and GALA v2 (`0xd1d2…`) both carry `GALA`.
- `dt` is the run date (full snapshot on publish), same semantics as
  `bronze.contracts`.
- The native-ETH placeholder row (`0x0000000000000000000000000000000000000000`)
  is a regular row; downstream price lookups map it to `coingecko:ethereum`.
- Frog Coin (FRG) was removed from the CSV by the user: it is not indexed by
  any free price source. Trades paid in unknown tokens will value to NULL USD
  downstream (out of scope here).

## 3. CSV contract and validation

File: `reference/erc20_tokens.csv`, header exactly
`chain_id,contract_address,name,fsym,decimals`.

`ingestion/erc20_tokens/validate.py` — pure module, whole-file semantics (any
defect raises `ValueError` with `line N: …`; nothing is written):

- Decode `utf-8-sig` (BOM tolerant), accept CRLF.
- Exact header match.
- `chain_id` ∈ {1, 137} (rows today are all mainnet; rule ready for Polygon).
- `contract_address` matches `^0x[0-9a-f]{40}$` (lowercase enforced).
- `name` non-empty, no commas (simple comma-split parser, as in contracts).
- `fsym` matches `^[A-Z0-9]{1,10}$`; duplicates allowed.
- `decimals` integer in [0, 36].
- Reject duplicate `(chain_id, contract_address)`.
- Reject empty file and header-with-no-rows.

## 4. Lambda load-erc20-tokens (zip)

`ingestion/erc20_tokens/handler.py`, mirror of the contracts handler:

1. Read S3 records (`Records[].s3.bucket.name` / `object.key`, unquoted).
2. `parse_and_validate(body)` → rows.
3. Write in-memory snappy parquet with an explicit `pa.schema`
   (`chain_id int32, contract_address string, name string, fsym string,
   decimals int32`) to
   `bronze/erc20_tokens/dt=YYYY-MM-DD/erc20_tokens.parquet` (run date, UTC).
4. `register_partition("erc20_tokens", run_date, bucket)` (shared module).

Packaging: zip via `archive_file` (handler, validate, `common/partitions.py`,
`__init__.py` files), runtime python3.13, timeout 60, memory 512, AWSSDKPandas
layer for pyarrow, env `ATHENA_WORKGROUP`.

## 5. Glue table bronze.erc20_tokens (Terraform)

`terraform/table_erc20_tokens.tf`, clone of `table_contracts.tf`:
EXTERNAL_TABLE, parquet classification, ParquetHiveSerDe, partition key `dt`
(string), location `s3://${local.bucket_name}/bronze/erc20_tokens/`. Columns:
`chain_id int, contract_address string, name string, fsym string,
decimals int`. No partition projection (infrequent snapshots; the Lambda
registers partitions explicitly).

## 6. Orchestration: erc20-tokens-on-push

`terraform/step_functions_erc20_tokens.tf`, clone of the contracts one:

- EventBridge rule `erc20-tokens-on-push`: `aws.s3` / `Object Created` /
  lake bucket / key wildcard `landing/erc20_tokens/*.csv`. The shared
  `aws_s3_bucket_notification` already enables EventBridge — do NOT add
  another one.
- State machine `erc20-tokens-on-push`:
  - `LoadErc20Tokens`: reshapes the EventBridge event into the S3 `Records`
    shape (same `Parameters` trick as contracts) and invokes the Lambda.
  - `RunDbt`: invokes the existing `run-dbt` Lambda with
    `Parameters = { select = "source:bronze.erc20_tokens+" }` — no
    `dbt_runner` code change (the handler already accepts `select`).
  - Both tasks: retry on `Lambda.ServiceException` /
    `Lambda.TooManyRequestsException` (5s, 2 attempts, backoff 2) and
    `Catch States.ALL → NotifyFailure` (`ResultPath $.error`).
  - `NotifyFailure`: SNS publish to `decentraland-alerts` with the custom
    contract `{source: "step-functions", component: "erc20-tokens",
    status: "FAILED", detail, execution_url}` → `FailExecution`.

## 7. IAM and observability

- `load-erc20-tokens-role` + inline policy `landing-read-bronze-write`:
  `GetObject` on `landing/erc20_tokens/*`, `PutObject` on
  `bronze/erc20_tokens/*`, bucket `GetBucketLocation`/`ListBucket`,
  `athena-results/*` read/write, Athena start/get on the tagged workgroup,
  Glue get/create-partition scoped to catalog + bronze DB + the new table.
- `erc20-tokens-on-push-sfn-role` (`invoke-lambdas-publish-alerts`):
  InvokeFunction on both Lambdas + `sns:Publish` on the alerts topic.
- `erc20-tokens-on-push-events-role` (`start-erc20-tokens-on-push`):
  `states:StartExecution`.
- Existing `run-dbt` policy: add `s3:GetObject` on `bronze/erc20_tokens/*`
  (today it only reads `bronze/contracts/*`).
- `alerts.tf`: add `load-erc20-tokens` to `local.monitored_lambdas`.
- Log group `/aws/lambda/load-erc20-tokens`, 7-day retention.
- Tags: Lambda group `component=ingestion-erc20-tokens, layer=bronze`;
  orchestration resources `component=orchestration, layer=ops`.

## 8. dbt: silver.dim_erc20_tokens

`dbt/models/silver/dim_erc20_tokens.sql`:

```sql
{{ config(materialized='table') }}

SELECT
    t.chain_id,
    t.contract_address,
    t.name,
    t.fsym,
    t.decimals,
    t.dt AS snapshot_dt
FROM {{ source('bronze', 'erc20_tokens') }} t
WHERE t.dt = {{ max_partition_dt(source('bronze', 'erc20_tokens')) }}
```

`materialized='table'` is mandatory (`max_partition_dt` reads `$partitions`,
unsupported in Athena views). No surrogate key for now (user decision):
joins use the natural key `(chain_id, contract_address)`. Adding
`sk_contract` via `dbt_utils.generate_surrogate_key` is deferred — it would
be the project's first `dbt_utils` use and carries packaging costs
(`packages.yml`, `dbt deps` in the run-dbt image).

- `sources.yml`: add source `bronze.erc20_tokens` with column descriptions.
- `schema.yml`: `not_null` on all six columns; `accepted_values` `[1, 137]`
  on `chain_id`.
- Singular tests: `assert_dim_erc20_tokens_unique_key.sql`
  (`(chain_id, contract_address)` unique) and
  `assert_dim_erc20_tokens_not_empty.sql`.

## 9. Testing & verification

`tests/test_erc20_tokens.py`, mirror of `test_contracts.py`:

- The committed `reference/erc20_tokens.csv` passes the validator.
- Parametrized rejections: wrong header, unknown chain_id, malformed or
  uppercase address, lowercase fsym, decimals out of range or non-integer,
  duplicate key, empty file.
- BOM/CRLF acceptance, `partition_key` format, parquet roundtrip against the
  pyarrow schema, handler test with fake S3/Athena clients.

Manual verification after deploy: publish the CSV, watch the
`erc20-tokens-on-push` execution succeed, query
`silver.dim_erc20_tokens` in Athena (22 rows) and confirm `dbt build`
tests passed in the run-dbt logs.

## 10. Documentation

`reference/README.md`, new `## erc20_tokens.csv` section: origin (symbols and
names resolved on-chain via CoinGecko contract lookup + Ethplorer; decimals
verified against DefiLlama batch responses), inclusion criterion (tokens seen
as payment currency in marketplace trades), per-column semantics — including
`fsym` non-uniqueness (GALA v1/v2), the native-ETH placeholder row, and the
decimals exceptions (USDC/USDT=6, GALA/CUBE/GMT=8, rest 18) — and the publish
snippet (`aws s3 cp reference/erc20_tokens.csv s3://…/landing/erc20_tokens/`).

## 11. Out of scope (YAGNI)

- Prices ingestion (DefiLlama) and the price join — next spec.
- Any use of `decimals` in the decode Lambda.
- Generic reference-CSV loader (revisit at the third reference table).
- Polygon token rows (validation already accepts chain_id 137 when needed).
