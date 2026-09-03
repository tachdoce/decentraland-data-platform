"""Pure helpers for the DefiLlama coins API (no AWS or network calls).

Grid rule: /chart returns, for each requested daily tick, the nearest real
sample; its timestamp drifts around midnight and can cross calendar days.
dt therefore ALWAYS comes from the requested grid, never from the returned
timestamp (kept separately as price_ts for drift auditing).
"""

import datetime

BASE_URL = "https://coins.llama.fi"
# Native-ETH placeholder used by marketplace trades paid in ETH
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
CHAIN_SLUGS = {1: "ethereum"}
# /chart returns HTTP 400 when coins x span exceeds 500 total points
# (bisected empirically: 500 OK, 501+ fails, regardless of the split).
MAX_POINTS_PER_CALL = 500
MAX_COINS_PER_CALL = 10
MAX_CHUNK_DAYS = MAX_POINTS_PER_CALL // MAX_COINS_PER_CALL


def coin_id(chain_id: int, address: str) -> str:
    if chain_id not in CHAIN_SLUGS:
        raise ValueError(f"unsupported chain_id {chain_id}")
    if address == ZERO_ADDRESS:
        return "coingecko:ethereum"
    return f"{CHAIN_SLUGS[chain_id]}:{address}"


def chunk_range(
    start: datetime.date, end: datetime.date, max_days: int = MAX_CHUNK_DAYS
) -> list[tuple[datetime.date, int]]:
    """Split [start, end] (inclusive) into (chunk_start, span) covering
    every day exactly once."""
    chunks = []
    cursor = start
    while cursor <= end:
        span = min(max_days, (end - cursor).days + 1)
        chunks.append((cursor, span))
        cursor += datetime.timedelta(days=span)
    return chunks


def _epoch(day: datetime.date) -> int:
    return int(
        datetime.datetime(
            day.year, day.month, day.day, tzinfo=datetime.timezone.utc
        ).timestamp()
    )


def chart_url(coin_ids: list[str], chunk_start: datetime.date, span: int) -> str:
    coins = ",".join(coin_ids)
    return f"{BASE_URL}/chart/{coins}?start={_epoch(chunk_start)}&span={span}&period=1d"


def parse_chart_response(
    payload: dict,
    chunk_start: datetime.date,
    span: int,
    id_map: dict[str, tuple[int, str]],
) -> list[dict]:
    """Flatten a /chart payload into grid-aligned rows.

    Each sample is assigned to the nearest grid day; samples rounding
    outside [0, span) are dropped; on a collision the sample closest to
    its grid point wins. Coins absent from the payload yield no rows.
    """
    start_epoch = _epoch(chunk_start)
    rows = []
    for cid, coin in payload.get("coins", {}).items():
        chain_id, address = id_map[cid]
        best: dict[int, dict] = {}  # grid index -> sample
        for sample in coin.get("prices", []):
            ts = sample["timestamp"]
            index = round((ts - start_epoch) / 86400)
            if not 0 <= index < span:
                continue
            distance = abs(ts - (start_epoch + index * 86400))
            if index in best and best[index]["distance"] <= distance:
                continue
            best[index] = {"sample": sample, "distance": distance}
        for index in sorted(best):
            sample = best[index]["sample"]
            rows.append(
                {
                    "chain_id": chain_id,
                    "contract_address": address,
                    "dt": chunk_start + datetime.timedelta(days=index),
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
