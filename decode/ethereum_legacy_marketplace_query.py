"""Builds the Athena query that extracts LegacyMarketplace auction events.

Like Wyvern, the whole decode fits in SQL: every event is static words.
AuctionCreated/Cancelled/Successful share topics (assetId, seller;
Successful adds winner) and data word 0 is the bytes32 auction id — the
NFT token id is the assetId topic, and the NFT contract address is not
in the event at all (attribution is silver work). Python only converts
hex -> decimal string / Decimal / timestamp.
"""

import re

LEGACY_MARKETPLACE = "0xb3bca6f5052c7e24726b44da7403b56a8a1b98f8"
AUCTION_CREATED_TOPIC = (
    "0x9493ae82b9872af74473effb9d302efba34e0df360a99cc5e577cd3f28e3cab2"
)
AUCTION_CANCELLED_TOPIC = (
    "0x88bd2ba46f3dc2567144331c35bd4c5ced3d547d8828638a152ddd9591c137a6"
)
AUCTION_SUCCESSFUL_TOPIC = (
    "0xedcc7e1c269bc295dc24e74dc46b129c8449e6b0544af73b57c4201b78d119db"
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
        WHEN '{AUCTION_CREATED_TOPIC}' THEN 'AuctionCreated'
        WHEN '{AUCTION_CANCELLED_TOPIC}' THEN 'AuctionCancelled'
        WHEN '{AUCTION_SUCCESSFUL_TOPIC}' THEN 'AuctionSuccessful'
    END AS event_name,
    concat('0x', substr(data, 3, 64)) AS auction_id,
    topics[2] AS asset_id_hex,
    concat('0x', substr(topics[3], 27)) AS seller,
    CASE WHEN topics[1] = '{AUCTION_SUCCESSFUL_TOPIC}'
        THEN concat('0x', substr(topics[4], 27)) END AS winner,
    CASE WHEN topics[1] IN ('{AUCTION_CREATED_TOPIC}', '{AUCTION_SUCCESSFUL_TOPIC}')
        THEN substr(data, 67, 64) END AS total_price_hex,
    CASE WHEN topics[1] = '{AUCTION_CREATED_TOPIC}'
        THEN substr(data, 131, 64) END AS expires_at_hex,
    extracted_at AS bronze_extracted_at,
    dt
FROM "bronze"."ethereum_logs"
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
    AND address = '{LEGACY_MARKETPLACE}'
    AND topics[1] IN ('{AUCTION_CREATED_TOPIC}',
        '{AUCTION_CANCELLED_TOPIC}',
        '{AUCTION_SUCCESSFUL_TOPIC}')
    AND length(data) = CASE topics[1]
        WHEN '{AUCTION_CREATED_TOPIC}' THEN 194
        WHEN '{AUCTION_SUCCESSFUL_TOPIC}' THEN 130
        ELSE 66 END
    AND cardinality(topics) = CASE topics[1]
        WHEN '{AUCTION_SUCCESSFUL_TOPIC}' THEN 4
        ELSE 3 END"""
