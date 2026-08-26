# Silver currency transfers — design

Date: 2026-08-26
Status: validated with the user (sections discussed and approved in chat)

## Goal

Decode ERC-20 `Transfer` events from the bronze log tables into clean,
queryable silver fact tables — one per chain — so gold can compute MANA
flows and, later, sale amounts paid in any token.

## Key decisions

1. **SQL decoding in dbt, no decode Lambda for this entity.** The ERC-20
   `Transfer` event is simple enough to decode with Athena string/number
   functions. The Python `decode` Lambda (eth_abi) is reserved for complex
   events (ERC-721/1155, marketplace). This narrows the original
   Python/SQL boundary: record-level decoding stays in Python only when
   SQL cannot express it reasonably.
2. **One model per chain, no cross-chain union.** `ethereum_currency_transfers`
   and `polygon_currency_transfers`, each reading its own bronze table.
   No `chain_id` column: the chain is in the table name.
3. **No join to `dim_contracts`.** Bronze holds every log of the
   transactions touching curated contracts, which includes transfers of
   payment tokens we do not track. Those rows are kept on purpose: the
   future sales parsing needs them to price sales paid in arbitrary
   tokens. Filtering by contract happens downstream (gold).
4. **Incremental append with a 20-day window.** `materialized='incremental'`,
   `incremental_strategy='append'`, partitioned by `dt`. Each run loads
   `(max(dt) of the model itself, max(dt) + 20 days]`. Windows never
   overlap, so `append` never duplicates; `insert_overwrite` was
   deliberately not used. Backfill = run `dbt build` repeatedly until the
   window catches up with bronze.
5. **Window bounds via the `max_partition_dt` macro** (`macros/max_partition_dt.sql`):
   a scalar subquery over `"<schema>"."<table>$partitions"` (zero bytes
   scanned). Called with `this`. Verified empirically: Athena engine v3
   prunes bronze partitions from the subquery result — the first
   incremental run scanned ~0.7 MB against a ~17 GB table.
6. **Bootstrap branch.** On the first build (`is_incremental()` false) the
   table and its `$partitions` do not exist, so the model uses a fixed
   lower bound instead: `dt <= '2017-09-06'` for Ethereum (MANA
   deployment). Polygon will get its own bootstrap date (its bronze
   min(dt)).

## Model

`dbt/models/silver/ethereum_currency_transfers.sql` (implemented;
`polygon_currency_transfers` will mirror it):

- Filter: `topics[1] = keccak('Transfer(address,address,uint256)')`
  and `cardinality(topics) = 3` (ERC-721 Transfer carries 4 topics).
- Decoded columns: `token_address` (emitter), `from_address` /
  `to_address` (last 20 bytes of topics 2/3), `amount_raw` — the uint256
  word split into two 48-bit halves recombined in `decimal(38,0)`,
  covering amounts < 2^96.
- Lineage columns: `bronze_extracted_at`, `processed_at`.
- Grain: one row per `(transaction_hash, log_index)`.
- No dedup step: bronze is append-only, so a re-extracted day would
  produce duplicate logs. Accepted for now; the uniqueness test below is
  the tripwire.

## Tests (to implement)

- `not_null` on `transaction_hash`, `log_index`, `block_timestamp`,
  `token_address`, `from_address`, `to_address`, `amount_raw`, `dt`.
- Singular test: unique `(transaction_hash, log_index)` — catches both
  decode bugs and bronze re-extraction duplicates.
- ~~Overflow guard test~~ replaced by a model filter (decision below):
  transfers with any of the high 160 bits of `data` set are excluded in
  the model's WHERE, so the split decoding can never truncate.
- Failing tests stop the pipeline before platinum, per project convention.

## Validation (2017 trial)

Backfill Ethereum through 2017 by running `dbt build` in a loop (each run
advances 20 days), then check in Athena: rows per `dt`, no duplicates, no
overflow, spot-check amounts against known MANA events (e.g. ~5k
transfers/day around the 2017-09-15 post-crowdsale distribution).

## Known limitations

- **Stall on an empty window**: if a full 20-day window contains zero
  transfers, no partition is written, `max(dt)` does not advance and the
  model repeats the same empty window forever. Not expected on Ethereum;
  revisit for sparse chains/events (fix: advance from bronze's calendar
  instead of the model's own partitions).
- **Amounts >= 2^96 are excluded, not truncated.** Found in practice on
  2018-04-25: a bogus MANA Transfer of ~1.16e77 from the 2018
  overflow-exploit era (tx `0xde99cab6...`, log_index 27). Such events are
  not real economic activity, so the model drops them
  (`substr(data, 3, 40) = '00…0'`); the original overflow test was
  removed since the WHERE enforces the rule by construction.
- **Workgroup scan cap (1 GB/query)** is the constraint that sized the
  window; if daily bronze volume grows, shrink the window before raising
  the cap.
