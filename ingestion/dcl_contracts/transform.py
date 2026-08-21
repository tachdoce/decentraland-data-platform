"""Pure transformation logic for the Decentraland contract registry snapshot.

No AWS or network calls here — everything is unit-testable without mocks.
"""

import datetime

CHAIN_IDS = {"mainnet": 1, "matic": 137}


def validate_payload(data: dict) -> None:
    """Reject payloads that must never become a snapshot."""
    if not isinstance(data, dict) or not data:
        raise ValueError("addresses.json payload is empty or not an object")
    for network in CHAIN_IDS:
        contracts = data.get(network)
        if not isinstance(contracts, dict) or not contracts:
            raise ValueError(f"network '{network}' is missing or has no contracts")
        bad = [n for n, a in contracts.items() if not str(a).startswith("0x")]
        if bad:
            raise ValueError(f"network '{network}' has non-address values: {bad[:3]}")


def flatten(data: dict) -> list[dict]:
    """{network: {name: address}} -> rows keyed by (chain_id, contract_address)."""
    rows = []
    for network, chain_id in CHAIN_IDS.items():
        for name, address in data[network].items():
            rows.append(
                {
                    "chain_id": chain_id,
                    "contract_address": address.lower(),
                    "contract_name": name.strip(),
                }
            )
    return rows


def partition_key(run_date: datetime.date) -> str:
    return f"bronze/dcl_contracts/dt={run_date.isoformat()}/contracts.parquet"
