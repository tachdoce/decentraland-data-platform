"""Builds the Athena query that extracts ERC-20 Transfer events.

The whole decode fits in SQL: one static data word (value) and
from/to as indexed topics. Python only converts value hex ->
decimal (uint256 exceeds bigint). No join to dim_contracts:
untracked payment tokens are kept on purpose for sales parsing.
"""

import re

TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def build_query(start_date: str, end_date: str) -> str:
    for d in (start_date, end_date):
        if not _DT_RE.match(d):
            raise ValueError(f"invalid date {d!r}, expected YYYY-MM-DD")

    # cardinality = 3: ERC-721 Transfer shares topic0 but has 4 topics.
    # substr(data, 3, 40) = zeros drops amounts >= 2^96: bogus events
    # from the 2018 overflow-exploit era, and it guarantees the value
    # fits decimal(38,0).
    return f"""SELECT transaction_hash,
    log_index,
    block_timestamp,
    address AS token_address,
    concat('0x', substr(topics[2], 27)) AS from_address,
    concat('0x', substr(topics[3], 27)) AS to_address,
    substr(data, 3, 64) AS amount_hex,
    extracted_at AS bronze_extracted_at,
    dt
FROM "bronze"."ethereum_logs"
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
    AND topics[1] = '{TRANSFER_TOPIC}'
    AND cardinality(topics) = 3
    AND length(data) = 66
    AND substr(data, 3, 40) = '{'0' * 40}'"""
