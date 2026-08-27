"""Builds the Athena query that extracts V3/V4 marketplace Traded events.

Like Seaport (and unlike the V1/V2 decoders), the event payload is a
nested dynamic struct — per-asset `bytes extra` makes word positions
variable — so the query only filters and fetches raw topics/data; all
ABI decoding happens in ethereum_marketplace_trades_parser.
"""

import re

TRADED_TOPIC = "0xaaecdfa7e74e704650fcb273f630f42f68974eff42bfffc1732cf30db9e4685b"
MARKETPLACE_V3 = "0x2d6b3508f9aca32d2550f92b2addba932e73c1ff"
MARKETPLACE_V4 = "0x1b67d0e31eeb6b52d8eeed71d3616c2f5b33b8e7"

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
    AND address IN ('{MARKETPLACE_V3}', '{MARKETPLACE_V4}')
    AND topics[1] = '{TRADED_TOPIC}'
    AND cardinality(topics) = 3"""
