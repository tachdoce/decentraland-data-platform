"""Builds the Athena query that extracts Wyvern OrdersMatched sales.

Unlike Seaport, the whole decode fits in SQL: the event data is three
static words (buyHash, sellHash, price) and maker/taker are indexed
topics. Python only converts price hex -> decimal (exceeds bigint).
"""

import re

ORDERS_MATCHED_TOPIC = (
    "0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9"
)
WYVERN_V1 = "0x7be8076f4ea4a4ad08075c2508e481d6c946d12b"
WYVERN_V23 = "0x7f268357a8c2552623316e2562d90e642bb538e5"

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def build_query(start_date: str, end_date: str) -> str:
    for d in (start_date, end_date):
        if not _DT_RE.match(d):
            raise ValueError(f"invalid date {d!r}, expected YYYY-MM-DD")

    return f"""SELECT transaction_hash,
    log_index,
    block_timestamp,
    address AS wyvern_address,
    concat('0x', substr(data, 3, 64)) AS buy_hash,
    concat('0x', substr(data, 67, 64)) AS sell_hash,
    concat('0x', substr(topics[2], 27)) AS maker,
    concat('0x', substr(topics[3], 27)) AS taker,
    substr(data, 131, 64) AS price_hex,
    topics[4] AS metadata,
    extracted_at AS bronze_extracted_at,
    dt
FROM "bronze"."ethereum_logs"
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
    AND address IN ('{WYVERN_V1}', '{WYVERN_V23}')
    AND topics[1] = '{ORDERS_MATCHED_TOPIC}'
    AND cardinality(topics) = 4
    AND length(data) = 194"""
