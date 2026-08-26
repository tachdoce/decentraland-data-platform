"""Builds the Athena query that extracts and flattens ERC-721 and
ERC-1155 Transfer events for curated contracts.

token_id / quantity stay as raw 64-char hex: all numeric decoding happens
in pandas (Python ints are arbitrary precision; Athena tops out at 64-bit
integers / 38-digit decimals, while LAND token ids use the high 128 bits).
"""

import re

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

TOPIC_721_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TOPIC_1155_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TOPIC_1155_BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"

# One TransferBatch log can carry up to this many token ids.
MAX_BATCH_IDS = 4000


def build_query(start_date: str, end_date: str) -> str:
    for d in (start_date, end_date):
        if not _DT_RE.match(d):
            raise ValueError(f"invalid date {d!r}, expected YYYY-MM-DD")

    return f"""WITH logs AS (
    SELECT el.*, c.erc_type
    FROM "bronze"."ethereum_logs" AS el
    INNER JOIN "silver"."dim_contracts" AS c ON c.chain_id = 1
        AND c.contract_address = el.address
        AND (
            (c.erc_type = 721  AND el.topics[1] = '{TOPIC_721_TRANSFER}') OR
            (c.erc_type = 1155 AND el.topics[1] IN ('{TOPIC_1155_BATCH}', '{TOPIC_1155_SINGLE}'))
        )
    WHERE el.dt BETWEEN '{start_date}' AND '{end_date}'
),
transfers AS (
    SELECT transaction_hash,
        log_index,
        block_timestamp,
        address AS contract_address,
        erc_type,
        substr(topics[4], 3) AS token_id_hex,
        lpad('1', 64, '0') AS quantity_hex,
        concat('0x', substr(topics[2], 27)) AS from_address,
        concat('0x', substr(topics[3], 27)) AS to_address,
        extracted_at AS bronze_extracted_at,
        dt
    FROM logs
    WHERE topics[1] = '{TOPIC_721_TRANSFER}'
        AND cardinality(topics) = 4
),
single_transfers AS (
    SELECT transaction_hash,
        log_index,
        block_timestamp,
        address AS contract_address,
        erc_type,
        substr(data, 3, 64) AS token_id_hex,
        substr(data, 67, 64) AS quantity_hex,
        concat('0x', substr(topics[3], 27)) AS from_address,
        concat('0x', substr(topics[4], 27)) AS to_address,
        extracted_at AS bronze_extracted_at,
        dt
    FROM logs
    WHERE topics[1] = '{TOPIC_1155_SINGLE}'
        AND cardinality(topics) = 4
),
pre_batch_transfers AS (
    -- strip '0x' + the two offset words; layout left: [N][ids...][M][values...]
    SELECT transaction_hash,
        log_index,
        block_timestamp,
        address AS contract_address,
        erc_type,
        substr(data, 131, length(data) - 130) AS data,
        concat('0x', substr(topics[3], 27)) AS from_address,
        concat('0x', substr(topics[4], 27)) AS to_address,
        extracted_at AS bronze_extracted_at,
        dt
    FROM logs
    WHERE topics[1] = '{TOPIC_1155_BATCH}'
        AND cardinality(topics) = 4
        AND length(data) >= 386
        AND substr(data, 3, 64) = lpad('40', 64, '0')
),
pre2_batch_transfers AS (
    -- ids and values arrays always have equal length, so each half is
    -- [count word][elements]; drop the count word from each half
    SELECT transaction_hash,
        log_index,
        block_timestamp,
        contract_address,
        erc_type,
        substr(data, 65, length(data) / 2 - 64) AS token_ids,
        substr(data, 65 + length(data) / 2, length(data) / 2 - 64) AS quantities,
        from_address,
        to_address,
        bronze_extracted_at,
        dt
    FROM pre_batch_transfers
),
split_limiters AS (
    SELECT CAST(_limit AS integer) AS _limit
    FROM UNNEST(sequence(1, 64 * {MAX_BATCH_IDS}, 64)) AS t(_limit)
),
batch_transfers AS (
    SELECT p.transaction_hash,
        p.log_index,
        p.block_timestamp,
        p.contract_address,
        p.erc_type,
        substr(p.token_ids, s._limit, 64) AS token_id_hex,
        substr(p.quantities, s._limit, 64) AS quantity_hex,
        p.from_address,
        p.to_address,
        p.bronze_extracted_at,
        p.dt
    FROM pre2_batch_transfers AS p
    INNER JOIN split_limiters AS s ON length(p.token_ids) > s._limit - 1
)
SELECT * FROM transfers
UNION ALL
SELECT * FROM single_transfers
UNION ALL
SELECT * FROM batch_transfers"""
