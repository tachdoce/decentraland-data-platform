"""Helpers shared by all the decode Lambdas (nft transfers, seaport,
wyvern, erc20 and the dcl marketplaces: legacy, v2 and trades)."""

from datetime import datetime, timedelta, timezone


def parse_event(event: dict) -> tuple[str, str]:
    event = event or {}
    default = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
    start = event.get("start_date") or default
    end = event.get("end_date") or start
    if start > end:
        raise ValueError(f"start_date {start} after end_date {end}")
    return start, end
