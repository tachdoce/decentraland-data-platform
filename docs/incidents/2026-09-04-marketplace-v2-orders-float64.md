# Incident: float64-corrupted total_price in staging.ethereum_marketplace_v2_orders

**Date detected:** 2026-09-04
**Date resolved:** 2026-09-04 (full table rewrite)
**Affected table:** `staging.ethereum_marketplace_v2_orders`
**Affected column:** `total_price` (decimal(38,0))

## Symptom

Rows carried prices with float64 rounding artifacts, e.g.
`9997000000000000786432` where the on-chain value is exactly
`9997000000000000000000` (9997 MANA). Any value above 2^53 wei
(~0.009 ETH/MANA) was potentially off by up to a few million wei.

## Root cause

The initial backfill ran on 2026-08-27 19:12–19:15 UTC, **before** the
merge of PR #1 (19:27 UTC), with an intermediate working-tree version of
the handler that routed `total_price` through a float64 step. Values
larger than 2^53 lost precision before being written as decimal.

The merged code in `decode/ethereum_marketplace_v2_handler.py` is
correct (`int(hex, 16)` → `Decimal`, never float): re-invoking the
deployed Lambda reproduced exact values. This was a data-only incident;
no code change was required.

## Diagnosis evidence

- Bronze hex `0x...21df03ea59fbc140000` for tx
  `0x7455894c5a6d11da74a2c06d490bc7788ad5cdf7134b7baab4b719ec5708b088`
  (log_index 118) decodes to `9997000000000000000000` exactly, while the
  staging parquet written on 2026-08-27 stored `9997000000000000786432`
  — physically as decimal128(38,0), so the float roundtrip happened at
  write time in the Lambda, not in Athena.
- `int(float(9997 * 10**18)) == 9997000000000000786432` confirms the
  float64 signature.

## Remediation (rewrite recipe)

1. Baseline distinct grain per event via Athena:
   OrderCreated 142,716 / OrderSuccessful 23,910 / OrderCancelled 9,987.
2. `aws s3 mv --recursive` the table prefix to
   `backup/ethereum_marketplace_v2_orders_20260904/` (safe to delete
   after verification; data is fully reproducible from bronze).
3. Re-invoke `decode-ethereum-marketplace-v2-orders` over
   2018-10-11..2026-07-18 in quarterly `{start_date, end_date}` chunks,
   3 in parallel. The 8 quarters 2021-Q3..2023-Q2 exceeded the Athena
   workgroup bytes-scanned limit and were re-run as monthly chunks.
4. Verify:
   - distinct grain identical to baseline (no events lost or gained);
   - zero rows with the known artifact value;
   - 482 rows across the affected days joined back to bronze hex with
     zero mismatches.
   Remaining ~171 duplicate OrderCreated rows at grain come from
   duplicate logs in bronze (re-extractions) and are deduped downstream
   in dbt as usual.

`staging.ethereum_legacy_marketplace_auctions` (same handler silhouette,
same backfill era) was spot-checked against bronze hex (998 rows,
April 2018): clean.

## Lessons

- Never run a backfill with unmerged working-tree code; deploy from the
  merged commit first.
- After any backfill of a decoded numeric column, verify a sample of
  large values (> 2^53) against the raw bronze hex — float corruption
  produces plausible-looking numbers that pass not_null/range tests.
