# token-prices-extraction: DefiLlama daily USD prices → bronze.token_prices

Design validated with the user on 2026-09-02. First half of the prices phase:
extraction to bronze only. The silver model (dedup + price join design) will be
brainstormed separately once bronze is running, and lands in the same PR /
branch (`token-prices-extraction`).

## 1. Goal

Daily USD prices for every token in `silver.dim_erc20_tokens`, from
**2019-01-01** onward, extracted from DefiLlama into `bronze.token_prices` —
append-only, partitioned by `month=YYYY-MM`, manually triggered (no cron),
with Slack alerting on failure. These prices will later value marketplace
trades (`raw_amount / 10^decimals * price_usd`).

## 2. Source decisions (validated empirically in conversation)

- **Source: DefiLlama** (`coins.llama.fi`). Free, no API key, no secrets.
  Lookup by contract address (`ethereum:0x…`), which eliminates ticker
  collisions (NCT, VOLT, GMT) and covers delisted tokens (WOOL, NCT, CUBE).
  History reaches ~Oct 2017 for MANA; user chose 2019-01-01 as start.
- Alternatives rejected: CoinGecko free (365-day history cap), CryptoCompare
  (now requires a CoinDesk API key), Binance (MANAUSDT only since 2020-08).
- **Grid rule**: the `/chart` endpoint returns, for each requested daily
  tick, the nearest real sample — its timestamp drifts around midnight and
  can cross calendar days. Therefore `dt` ALWAYS comes from the requested
  grid (`start + n × 1d`), never from the returned timestamp. The returned
  timestamp is kept as `price_ts` for drift auditing.
- **Daily price semantics**: `dt = D` is the price at grid point
  `D 00:00 UTC` (day open).
- **Native ETH**: the zero-address row maps to DefiLlama id
  `coingecko:ethereum`; every other token maps to `ethereum:<address>`.
- **Gaps**: a token/day DefiLlama has no price for (e.g. PRIME before 2023)
  produces NO row — never NULL rows. Downstream joins resolve missing
  prices with LEFT JOIN.
- Rate limits: undocumented; empirically ~480 req/min returned all 200s.
  Our worst case (full backfill) is ~24 batch calls. In-Lambda retry with
  exponential backoff on 429/5xx (3 attempts, 30s request timeout); a chunk
  that still fails aborts the whole invocation (fail loud → Slack).

## 3. Lambda extract-token-prices (zip)

`ingestion/token_prices/{__init__,handler,defillama}.py` — zip Lambda,
python3.13, AWSSDKPandas layer (pyarrow), timeout 300s, memory 512.

Steps:

1. **Token universe from Athena** (user decision — same pattern as
   extract-onchain-logs reading silver.dim_contracts): `SELECT chain_id,
   contract_address FROM silver.dim_erc20_tokens` via
   `athena:StartQueryExecution` + poll + `GetQueryResults`. Adding a token
   to the CSV automatically prices it on the next run; zero code changes.
2. **Date range**: payload `{}` → today (UTC) only; payload
   `{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}` → backfill.
   Same code for both (repo convention: backfill = same Lambda).
3. **Fetch**: `/chart` returns HTTP 400 when `coins × span` exceeds 500
   total points (found empirically during the backfill; bisected to exactly
   500). Coins therefore go in batches of ≤10 per call and the range in
   chunks of ≤50 days:
   `https://coins.llama.fi/chart/<up-to-10-coins>?start=<unix>&span=<≤50>&period=1d`.
   Full 2019→today backfill ≈ 56 chunks × 3 batches ≈ 168 calls.
4. **Rows**: one per (token, grid day) with a returned price. `dt` from the
   grid; `price_ts` and `confidence` from the response; `extracted_at` = run
   timestamp (dedup key for the future silver model).
5. **Write append-only** (user decision — same pattern as
   bronze.ethereum_logs): one timestamped parquet per touched month
   partition, `bronze/token_prices/month=YYYY-MM/token_prices_<extracted_at>.parquet`.
   Re-runs never overwrite; duplicates are resolved later in silver by
   latest `extracted_at`.
6. **Register partitions**: `ALTER TABLE ADD IF NOT EXISTS PARTITION` per
   touched month. `ingestion/common/partitions.py` is parameterised by table
   but hardcodes `dt =` — extend it with an optional partition-column
   argument (default `dt`, so existing callers are untouched) rather than
   duplicating it.

## 4. Glue table bronze.token_prices (Terraform)

`terraform/table_token_prices.tf`: EXTERNAL_TABLE, parquet classification,
ParquetHiveSerDe, partition key `month` (string), location
`s3://${local.bucket_name}/bronze/token_prices/`. No partition projection
(the Lambda registers partitions explicitly).

| column | type | notes |
|---|---|---|
| `chain_id` | int | from dim_erc20_tokens |
| `contract_address` | string | lowercase; zero address = native ETH |
| `dt` | date | grid day (00:00 UTC) |
| `price_usd` | double | DefiLlama price |
| `price_ts` | timestamp | actual returned sample timestamp |
| `confidence` | double | DefiLlama confidence field |
| `extracted_at` | timestamp | run timestamp; future dedup key |
| `month` (partition) | string | `YYYY-MM` |

Rationale for monthly partitioning: prices are tiny (~22 rows/day); daily
partitions would create ~2,800 3-KB files (S3/Athena small-files problem).
Monthly gives ~84 partitions of ~660 rows.

## 5. Orchestration: token-prices state machine (manual, no cron)

`terraform/step_functions_token_prices.tf`, skeleton cloned from
erc20-tokens-on-push but WITHOUT an EventBridge trigger (user decision: no
cron; global orchestration arrives in phase 8) and WITHOUT a RunDbt step
(bronze-only scope — an empty dbt selector would fail; the step gets added
with the silver model):

- State machine `token-prices`: single task `ExtractTokenPrices` (payload
  passes through, so a manual execution can carry `{start_date, end_date}`),
  retry on `Lambda.ServiceException`/`Lambda.TooManyRequestsException`
  (5s, 2 attempts, backoff 2), `Catch States.ALL → NotifyFailure`
  (SNS custom contract, `component = "token-prices"`) → `FailExecution`.
- Trigger: manual only —
  `aws stepfunctions start-execution --state-machine-arn … --input '{...}'`.
- No dbt selector, no sources.yml entry yet (added with the silver model).

## 6. IAM and observability

- `extract-token-prices-role` + inline policy:
  - `s3:PutObject` on `bronze/token_prices/*` only.
  - `s3:GetBucketLocation`/`ListBucket` on the bucket; read/write on
    `athena-results/*`.
  - `athena:StartQueryExecution`/`GetQueryExecution`/`GetQueryResults` on
    the tagged workgroup.
  - Glue read (`GetDatabase/GetTable/GetPartition/GetPartitions`) on the
    catalog, `bronze` and `silver` databases, `silver.dim_erc20_tokens`
    (Athena reads it for the token universe; Glue table resource for
    silver tables is account-wide `table/silver/*` since dbt-created tables
    have no Terraform ARN) plus `s3:GetObject` on `silver/dim_erc20_tokens/*`
    (Athena reads table data with caller credentials).
  - Glue `CreatePartition`/`BatchCreatePartition` on `bronze.token_prices`.
- `token-prices-sfn-role`: InvokeFunction on the Lambda + `sns:Publish` on
  the alerts topic.
- `alerts.tf`: add `extract-token-prices` to `local.monitored_lambdas`.
- Log group `/aws/lambda/extract-token-prices`, 7-day retention.
- Tags: Lambda group `component=ingestion-token-prices, layer=bronze`;
  state machine `component=orchestration, layer=ops`.
- Outbound HTTPS to coins.llama.fi: Lambdas run outside any VPC, so no
  networking changes.

## 7. Testing & verification

`tests/test_token_prices.py`:

- Response parsing against a real DefiLlama JSON fixture (checked into
  `tests/fixtures/`), including the timestamp-drift case: grid `dt` must
  come from `start + n × 1d`, not from the returned timestamp.
- Native-ETH mapping (`0x0…0` → `coingecko:ethereum`) and the reverse
  mapping of responses back to `(chain_id, contract_address)`.
- Chunking: a 2019→2026 range splits into ≤120-day chunks covering every
  day exactly once.
- Missing token/day in the response → no row emitted.
- Month-partition grouping: rows land in the right
  `month=YYYY-MM/token_prices_<ts>.parquet` keys.
- Parquet roundtrip against the pyarrow schema.
- Handler test with fake Athena (token universe), fake HTTP and fake S3.
- `ingestion/common/partitions.py` extension: existing tests still pass
  unchanged (default `dt`), new test for `month` partitions.

Manual verification after deploy: run the state machine with
`{"start_date": "2019-01-01", "end_date": "<today>"}`, confirm SUCCEEDED,
then in Athena: row count ≈ 22 tokens × ~2,800 days (minus pre-listing
gaps: UNI from 2020-09, APE from 2022-03, PRIME from 2023, WILD/ASH/WOOL
etc. from their launch dates), MANA count = full range, spot-check MANA
2021-12-01 ≈ 4.61 USD (validated in conversation).

## 8. Documentation

- CLAUDE.md workflow line for phase 4 (prices) updated when the full phase
  (bronze + silver) ships.
- This spec + its plan in `docs/superpowers/`.

## 9. Out of scope (deferred)

- **silver.token_prices** (dedup by latest `extracted_at`, price-sanity
  tests): separate brainstorming once bronze runs; same branch and PR.
- The price join / USD valuation of trades (needs the silver model).
- Cron / global orchestration (phase 8).
- Hourly granularity (unreliable pre-2021), Polygon tokens, CoinGecko
  contrast source.
