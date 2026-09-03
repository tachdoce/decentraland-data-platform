"""Pure helpers for the DefiLlama coins API (no AWS or network calls).

Grid rule: /chart returns, for each requested hourly tick, the nearest real
sample; its timestamp drifts around the tick and can cross hour or day
boundaries. grid_ts/dt therefore ALWAYS come from the requested grid, never
from the returned timestamp (kept separately as price_ts for drift auditing).

Hourly availability is a short rolling window for most tokens (majors like
ETH reach further back); missing ticks simply yield no rows.
"""

import datetime

BASE_URL = "https://coins.llama.fi"
# Native-ETH placeholder used by marketplace trades paid in ETH
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
CHAIN_SLUGS = {1: "ethereum"}
HOUR = 3600
# /chart returns HTTP 400 when coins x span exceeds 500 total points
# (bisected empirically: 500 OK, 501+ fails, regardless of the split).
MAX_POINTS_PER_CALL = 500
MAX_COINS_PER_CALL = 10
MAX_CHUNK_HOURS = MAX_POINTS_PER_CALL // MAX_COINS_PER_CALL


def coin_id(chain_id: int, address: str) -> str:
    if chain_id not in CHAIN_SLUGS:
        raise ValueError(f"unsupported chain_id {chain_id}")
    if address == ZERO_ADDRESS:
        return "coingecko:ethereum"
    return f"{CHAIN_SLUGS[chain_id]}:{address}"


def chunk_range(
    start: datetime.date, end: datetime.date, max_hours: int = MAX_CHUNK_HOURS
) -> list[tuple[datetime.datetime, int]]:
    """Split the hourly ticks of [start, end] (dates, inclusive: 00:00 of
    start through 23:00 of end) into (chunk_start, span_hours) covering
    every hour exactly once."""
    # Naive datetimes are UTC by repo convention (parquet timestamps are
    # timezone-free); combine() avoids the DTZ lint on the constructor.
    cursor = datetime.datetime.combine(start, datetime.time.min)
    grid_end = datetime.datetime.combine(end, datetime.time.min) + datetime.timedelta(
        hours=24
    )
    chunks = []
    while cursor < grid_end:
        span = min(max_hours, int((grid_end - cursor).total_seconds()) // HOUR)
        chunks.append((cursor, span))
        cursor += datetime.timedelta(hours=span)
    return chunks


def _epoch(tick: datetime.datetime) -> int:
    return int(tick.replace(tzinfo=datetime.timezone.utc).timestamp())


def chart_url(coin_ids: list[str], chunk_start: datetime.datetime, span: int) -> str:
    coins = ",".join(coin_ids)
    return (
        f"{BASE_URL}/chart/{coins}?start={_epoch(chunk_start)}&span={span}&period=1h"
    )


def parse_chart_response(
    payload: dict,
    chunk_start: datetime.datetime,
    span: int,
    id_map: dict[str, tuple[int, str]],
) -> list[dict]:
    """Flatten a /chart payload into grid-aligned hourly rows.

    Each sample is assigned to the nearest grid hour; samples rounding
    outside [0, span) are dropped; on a collision the sample closest to
    its grid tick wins. Coins absent from the payload yield no rows.
    """
    start_epoch = _epoch(chunk_start)
    rows = []
    for cid, coin in payload.get("coins", {}).items():
        chain_id, address = id_map[cid]
        best: dict[int, dict] = {}  # grid index -> sample
        for sample in coin.get("prices", []):
            ts = sample["timestamp"]
            index = round((ts - start_epoch) / HOUR)
            if not 0 <= index < span:
                continue
            distance = abs(ts - (start_epoch + index * HOUR))
            if index in best and best[index]["distance"] <= distance:
                continue
            best[index] = {"sample": sample, "distance": distance}
        for index in sorted(best):
            sample = best[index]["sample"]
            grid_ts = chunk_start + datetime.timedelta(hours=index)
            rows.append(
                {
                    "chain_id": chain_id,
                    "contract_address": address,
                    "grid_ts": grid_ts,
                    "dt": grid_ts.date(),
                    "price_usd": float(sample["price"]),
                    "price_ts": datetime.datetime.fromtimestamp(
                        sample["timestamp"], tz=datetime.timezone.utc
                    ).replace(tzinfo=None),
                    "confidence": float(coin.get("confidence", 0.0)),
                }
            )
    return rows


def batch_coins(
    coin_ids: list[str], max_per_call: int = MAX_COINS_PER_CALL
) -> list[list[str]]:
    return [
        coin_ids[i : i + max_per_call]
        for i in range(0, len(coin_ids), max_per_call)
    ]


def month_key(day: datetime.date) -> str:
    return f"{day.year:04d}-{day.month:02d}"
