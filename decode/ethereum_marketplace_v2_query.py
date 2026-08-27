"""Builds the Athena query that extracts Marketplace V2 order events.

Same silhouette as the LegacyMarketplace decoder: every event is
static words, so the whole decode fits in SQL. Events are emitted by
the MarketplaceProxy; unlike the legacy contract, data word 1 carries
the NFT contract address (nftAddress), so no downstream attribution is
needed. Python only converts hex -> decimal string / Decimal /
timestamp.
"""

import re

MARKETPLACE_V2_PROXY = "0x8e5660b4ab70168b5a6feea0e0315cb49c8cd539"
ORDER_CREATED_TOPIC = (
    "0x84c66c3f7ba4b390e20e8e8233e2a516f3ce34a72749e4f12bd010dfba238039"
)
ORDER_CANCELLED_TOPIC = (
    "0x0325426328de5b91ae4ad8462ad4076de4bcaf4551e81556185cacde5a425c6b"
)
ORDER_SUCCESSFUL_TOPIC = (
    "0x695ec315e8a642a74d450a4505eeea53df699b47a7378c7d752e97d5b16eb9bb"
)

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def build_query(start_date: str, end_date: str) -> str:
    for d in (start_date, end_date):
        if not _DT_RE.match(d):
            raise ValueError(f"invalid date {d!r}, expected YYYY-MM-DD")

    return f"""SELECT transaction_hash,
    log_index,
    block_timestamp,
    CASE topics[1]
        WHEN '{ORDER_CREATED_TOPIC}' THEN 'OrderCreated'
        WHEN '{ORDER_CANCELLED_TOPIC}' THEN 'OrderCancelled'
        WHEN '{ORDER_SUCCESSFUL_TOPIC}' THEN 'OrderSuccessful'
    END AS event_name,
    concat('0x', substr(data, 3, 64)) AS order_id,
    topics[2] AS asset_id_hex,
    concat('0x', substr(data, 91, 40)) AS contract_address,
    concat('0x', substr(topics[3], 27)) AS seller,
    CASE WHEN topics[1] = '{ORDER_SUCCESSFUL_TOPIC}'
        THEN concat('0x', substr(topics[4], 27)) END AS buyer,
    CASE WHEN topics[1] IN ('{ORDER_CREATED_TOPIC}', '{ORDER_SUCCESSFUL_TOPIC}')
        THEN substr(data, 131, 64) END AS total_price_hex,
    CASE WHEN topics[1] = '{ORDER_CREATED_TOPIC}'
        THEN substr(data, 195, 64) END AS expires_at_hex,
    extracted_at AS bronze_extracted_at,
    dt
FROM "bronze"."ethereum_logs"
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
    AND address = '{MARKETPLACE_V2_PROXY}'
    AND topics[1] IN ('{ORDER_CREATED_TOPIC}',
        '{ORDER_CANCELLED_TOPIC}',
        '{ORDER_SUCCESSFUL_TOPIC}')
    AND length(data) = CASE topics[1]
        WHEN '{ORDER_CREATED_TOPIC}' THEN 258
        WHEN '{ORDER_SUCCESSFUL_TOPIC}' THEN 194
        ELSE 130 END
    AND cardinality(topics) = CASE topics[1]
        WHEN '{ORDER_SUCCESSFUL_TOPIC}' THEN 4
        ELSE 3 END"""
