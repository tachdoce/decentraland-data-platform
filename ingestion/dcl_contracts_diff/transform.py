"""Pure day-over-day diff logic for dcl_contracts snapshots.

No AWS or network calls here — everything is unit-testable without mocks.
Contract keys are (chain_id, contract_address), matching the rest of the
platform; renamed means the same key with a different contract_name.
"""


def diff_snapshots(today_rows: list[dict], yesterday_rows: list[dict]) -> dict:
    """Compare two snapshots and classify changes as added/removed/renamed."""
    if not today_rows:
        raise ValueError("today's snapshot has no rows; refusing to diff")

    today = {(r["chain_id"], r["contract_address"]): r for r in today_rows}
    yesterday = {(r["chain_id"], r["contract_address"]): r for r in yesterday_rows}

    changes = {"added": [], "removed": [], "renamed": []}
    for key, r in today.items():
        old = yesterday.get(key)
        if old is None:
            changes["added"].append(r)
        elif old["contract_name"] != r["contract_name"]:
            changes["renamed"].append(
                {
                    "chain_id": r["chain_id"],
                    "contract_address": r["contract_address"],
                    "old_name": old["contract_name"],
                    "new_name": r["contract_name"],
                }
            )
    changes["removed"] = [r for key, r in yesterday.items() if key not in today]
    return changes


def summarize(changes: dict, max_lines: int = 10) -> str:
    """Human-readable summary for the Slack alert: counts + first changes."""
    lines = [
        (
            f"{len(changes['added'])} added, {len(changes['removed'])} removed, "
            f"{len(changes['renamed'])} renamed"
        )
    ]
    details = []
    for r in changes["added"]:
        details.append(
            f"added chain_id={r['chain_id']} {r['contract_address']} {r['contract_name']}"
        )
    for r in changes["removed"]:
        details.append(
            f"removed chain_id={r['chain_id']} {r['contract_address']} {r['contract_name']}"
        )
    for r in changes["renamed"]:
        details.append(
            f"renamed chain_id={r['chain_id']} {r['contract_address']} "
            f"{r['old_name']} -> {r['new_name']}"
        )
    if len(details) > max_lines:
        hidden = len(details) - max_lines
        details = details[:max_lines] + [f"... {hidden} more"]
    return "\n".join(lines + details)
