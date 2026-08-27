"""Builds the Athena query that fetches raw Seaport OrderFulfilled logs.

Filter-and-fetch only: all ABI decoding happens in Python
(seaport_parser), because the data blob nests two dynamic arrays of
structs — unreadable as SQL substr arithmetic. No address filter:
topic0 already identifies the event.
"""

import re

from decode.seaport_parser import ORDER_FULFILLED_TOPIC

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def build_query(start_date: str, end_date: str) -> str:
    for d in (start_date, end_date):
        if not _DT_RE.match(d):
            raise ValueError(f"invalid date {d!r}, expected YYYY-MM-DD")

    return f"""SELECT transaction_hash,
    log_index,
    block_timestamp,
    address,
    topics,
    data,
    extracted_at,
    dt
FROM "bronze"."ethereum_logs"
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
    AND topics[1] = '{ORDER_FULFILLED_TOPIC}'"""
