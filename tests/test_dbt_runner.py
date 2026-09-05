from types import SimpleNamespace
from typing import ClassVar

import pytest

import dbt_runner.handler as h


class FakeDbtRunner:
    """Captures invoke args and returns a canned result."""

    calls: ClassVar[list[list[str]]] = []
    success = True
    nodes = 1

    def invoke(self, args):
        FakeDbtRunner.calls.append(list(args))
        results = [object()] * FakeDbtRunner.nodes
        return SimpleNamespace(
            success=FakeDbtRunner.success,
            exception=None,
            result=SimpleNamespace(results=results),
        )


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("DBT_PROJECT_DIR", "/var/task/dbt")


def _wire(monkeypatch):
    FakeDbtRunner.calls = []
    FakeDbtRunner.success = True
    FakeDbtRunner.nodes = 1
    monkeypatch.setattr(h, "dbtRunner", FakeDbtRunner)


def test_default_selector(env, monkeypatch):
    _wire(monkeypatch)

    result = h.handler({}, None)

    args = FakeDbtRunner.calls[0]
    assert args[:3] == ["build", "--select", "source:bronze.contracts+"]
    assert "--project-dir" in args and "/var/task/dbt" in args
    assert "--target-path" in args and "/tmp/dbt-target" in args
    assert "--exclude" in args
    assert args[args.index("--exclude") + 1] == "tag:manual"
    assert result == {"select": "source:bronze.contracts+"}


def test_selector_override(env, monkeypatch):
    _wire(monkeypatch)

    result = h.handler({"select": "dim_contracts"}, None)

    assert FakeDbtRunner.calls[0][:3] == ["build", "--select", "dim_contracts"]
    assert result == {"select": "dim_contracts"}
    assert "--exclude" in FakeDbtRunner.calls[0]


def test_manual_tag_always_excluded(env, monkeypatch):
    # Models tagged 'manual' (e.g. fct_monthly_nft_prices) need a mandatory
    # var: any automatic selection reaching them would fail. The Lambda must
    # exclude the tag no matter which selector the event carries.
    _wire(monkeypatch)

    h.handler({"select": "source:bronze.token_prices+"}, None)

    args = FakeDbtRunner.calls[0]
    assert args[args.index("--exclude") + 1] == "tag:manual"


def test_dbt_failure_raises(env, monkeypatch):
    _wire(monkeypatch)
    FakeDbtRunner.success = False

    with pytest.raises(RuntimeError, match="dbt build failed"):
        h.handler({}, None)


def test_empty_selection_raises(env, monkeypatch):
    # dbt returns success with zero nodes for a typo'd selector; the handler
    # must treat that as a failure instead of silently doing nothing.
    _wire(monkeypatch)
    FakeDbtRunner.nodes = 0

    with pytest.raises(RuntimeError, match="matched no nodes"):
        h.handler({"select": "nonexistent_model"}, None)
