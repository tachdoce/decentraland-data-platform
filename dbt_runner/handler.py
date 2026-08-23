"""run-dbt Lambda: builds the dbt models that depend on bronze.contracts.

Invoked by the contracts-on-push state machine after a successful CSV
ingestion. Event: {} for the default selector, {"select": "<selector>"}
for manual runs. Raises on any dbt failure so Step Functions catches it.
"""

import os

from dbt.cli.main import dbtRunner

DEFAULT_SELECT = "source:bronze.contracts+"


def run_dbt(select: str) -> None:
    project_dir = os.environ["DBT_PROJECT_DIR"]
    # Lambda's filesystem is read-only except /tmp: dbt artifacts and logs
    # must be redirected there.
    result = dbtRunner().invoke(
        [
            "build",
            "--select", select,
            "--project-dir", project_dir,
            "--profiles-dir", project_dir,
            "--target-path", "/tmp/dbt-target",
            "--log-path", "/tmp/dbt-logs",
        ]
    )
    if not result.success:
        raise RuntimeError(
            f"dbt build failed: {result.exception or 'model or test failures'}"
        )
    # dbt reports success with zero nodes for a selector that matches
    # nothing (e.g. a typo): surface it instead of silently doing nothing.
    if not getattr(result.result, "results", None):
        raise RuntimeError(f"dbt selection {select!r} matched no nodes")


def handler(event, context):
    select = (event or {}).get("select") or DEFAULT_SELECT
    run_dbt(select)
    return {"select": select}
