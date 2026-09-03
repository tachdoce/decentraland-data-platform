"""Pure validation/parsing for the curated ERC-20 tokens CSV.

Whole-file semantics: any defect raises ValueError and nothing is written.
No AWS or network calls here.
"""

import csv
import datetime
import io
import re

EXPECTED_HEADER = [
    "chain_id",
    "contract_address",
    "name",
    "fsym",
    "decimals",
]

KNOWN_CHAIN_IDS = {1, 137}
ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
# Price-source ticker (informative, NOT unique: GALA v1/v2 share it)
FSYM_RE = re.compile(r"^[A-Z0-9]{1,10}$")


def parse_and_validate(csv_bytes: bytes) -> list[dict]:
    # utf-8-sig: tolerate a BOM from curators re-saving in Excel
    text = csv_bytes.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError("CSV is empty")
    if header != EXPECTED_HEADER:
        raise ValueError(f"bad header: expected {EXPECTED_HEADER}, got {header}")

    rows, seen = [], set()
    for lineno, raw in enumerate(reader, start=2):
        if not raw:
            continue  # trailing blank line
        if len(raw) != len(EXPECTED_HEADER):
            raise ValueError(
                f"line {lineno}: expected {len(EXPECTED_HEADER)} fields, got {len(raw)}"
            )
        chain_raw, address, name, fsym, decimals_raw = raw

        try:
            chain_id = int(chain_raw)
        except ValueError:
            raise ValueError(f"line {lineno}: chain_id is not an integer: {chain_raw!r}")
        if chain_id not in KNOWN_CHAIN_IDS:
            raise ValueError(
                f"line {lineno}: unknown chain_id {chain_id} (known: {sorted(KNOWN_CHAIN_IDS)})"
            )

        if not ADDRESS_RE.match(address):
            raise ValueError(
                f"line {lineno}: invalid address (must be 0x + 40 lowercase hex): {address!r}"
            )

        if not name:
            raise ValueError(f"line {lineno}: name must not be empty")

        if not FSYM_RE.match(fsym):
            raise ValueError(
                f"line {lineno}: invalid fsym (must be 1-10 uppercase A-Z/0-9): {fsym!r}"
            )

        try:
            decimals = int(decimals_raw)
        except ValueError:
            raise ValueError(
                f"line {lineno}: decimals is not an integer: {decimals_raw!r}"
            )
        if not 0 <= decimals <= 36:
            raise ValueError(f"line {lineno}: decimals out of range [0, 36]: {decimals}")

        key = (chain_id, address)
        if key in seen:
            raise ValueError(
                f"line {lineno}: duplicate (chain_id, contract_address): {key}"
            )
        seen.add(key)

        rows.append(
            {
                "chain_id": chain_id,
                "contract_address": address,
                "name": name,
                "fsym": fsym,
                "decimals": decimals,
            }
        )

    if not rows:
        raise ValueError("CSV has a header but no data rows")
    return rows


def partition_key(run_date: datetime.date) -> str:
    return f"bronze/erc20_tokens/dt={run_date.isoformat()}/erc20_tokens.parquet"
