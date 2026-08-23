"""Pure validation/parsing for the curated contracts CSV.

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
    "contract_name",
    "first_mint_dt",
    "extract_from_dt",
    "dcl_contract",
    "erc_type",
]

KNOWN_CHAIN_IDS = {1, 137}
# -1 = ignore, 0 = type not yet classified (likely marketplace or similar),
# 20/721/1155 = ERC token standards.
KNOWN_ERC_TYPES = {-1, 0, 20, 721, 1155}
ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
# Strict guard: Python >= 3.11 fromisoformat() accepts compact forms like
# 20010101, so the format must be enforced explicitly.
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_date(raw: str, lineno: int, field: str) -> datetime.date:
    if not DATE_RE.match(raw):
        raise ValueError(f"line {lineno}: {field} must be YYYY-MM-DD, got {raw!r}")
    try:
        return datetime.date.fromisoformat(raw)
    except ValueError:
        raise ValueError(f"line {lineno}: {field} is not a valid date: {raw!r}")


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
        (
            chain_raw,
            address,
            name,
            first_mint_raw,
            extract_from_raw,
            dcl_raw,
            erc_raw,
        ) = raw

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

        first_mint = _parse_date(first_mint_raw, lineno, "first_mint_dt")
        extract_from = _parse_date(extract_from_raw, lineno, "extract_from_dt")

        if dcl_raw not in ("TRUE", "FALSE"):
            raise ValueError(
                f"line {lineno}: dcl_contract must be TRUE or FALSE, got {dcl_raw!r}"
            )
        dcl_contract = dcl_raw == "TRUE"

        try:
            erc_type = int(erc_raw)
        except ValueError:
            raise ValueError(f"line {lineno}: erc_type is not an integer: {erc_raw!r}")
        if erc_type not in KNOWN_ERC_TYPES:
            raise ValueError(
                f"line {lineno}: unknown erc_type {erc_type} (known: {sorted(KNOWN_ERC_TYPES)})"
            )

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
                "contract_name": name,
                "first_mint_dt": first_mint,
                "extract_from_dt": extract_from,
                "dcl_contract": dcl_contract,
                "erc_type": erc_type,
            }
        )

    if not rows:
        raise ValueError("CSV has a header but no data rows")
    return rows


def partition_key(run_date: datetime.date) -> str:
    return f"bronze/contracts/dt={run_date.isoformat()}/contracts.parquet"
