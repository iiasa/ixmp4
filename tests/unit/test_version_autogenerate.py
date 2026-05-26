"""Unit tests for ixmp4.data.versions.autogenerate.

These tests cover:
  * pure-Python utility functions (_normalize, _extract_body)
  * MigrateOperation classes and their .reverse() methods
  * renderer functions
  * the _compare_version_triggers comparator (with a mocked autogen context)

No real database is required; PostgreSQL interactions are fully mocked.
"""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from alembic.operations.ops import CreateTableOp, DropTableOp, UpgradeOps

from ixmp4.data.versions.autogenerate import (
    DropVersionTriggersOp,
    SyncVersionTriggersOp,
    _compare_version_triggers,
    _extract_body,
    _normalize,
    _render_drop_version_triggers,
    _render_sync_version_triggers,
)


class TestNormalize:
    def test_collapses_multiple_spaces(self) -> None:
        assert _normalize("a    b") == "a b"

    def test_strips_leading_and_trailing_whitespace(self) -> None:
        assert _normalize("  hello  ") == "hello"

    def test_newlines_replaced_by_single_space(self) -> None:
        assert _normalize("a\nb\nc") == "a b c"

    def test_tabs_replaced_by_single_space(self) -> None:
        assert _normalize("a\tb") == "a b"

    def test_mixed_whitespace(self) -> None:
        assert _normalize("  a \n  b \t c  ") == "a b c"

    def test_empty_string(self) -> None:
        assert _normalize("") == ""

    def test_already_normalised_unchanged(self) -> None:
        assert _normalize("hello world") == "hello world"


class TestExtractBody:
    def test_extracts_between_outer_dollar_signs(self) -> None:
        src = "CREATE FUNCTION f() RETURNS trigger AS $$ BODY_HERE $$ LANGUAGE plpgsql"
        assert "BODY_HERE" in _extract_body(src)

    def test_preserves_inner_plpgsql(self) -> None:
        src = "CREATE FUNCTION f() AS $$ begin return null; end $$ language plpgsql;"
        body = _extract_body(src)
        assert "begin" in body
        assert "return null;" in body

    def test_does_not_include_outer_delimiters(self) -> None:
        src = "CREATE FUNCTION f() AS $$ body $$ language plpgsql"
        body = _extract_body(src)
        assert "$$" not in body


class TestSyncVersionTriggersOpReverse:
    def test_existing_table_reverses_to_sync_op(self) -> None:
        op = SyncVersionTriggersOp("run", "run_version")
        rev = op.reverse()
        assert isinstance(rev, SyncVersionTriggersOp)
        assert rev.table_name == "run"
        assert rev.version_table_name == "run_version"
        assert not rev.is_new_table

    def test_new_table_reverses_to_drop_op(self) -> None:
        op = SyncVersionTriggersOp("run", "run_version", is_new_table=True)
        rev = op.reverse()
        assert isinstance(rev, DropVersionTriggersOp)
        assert rev.table_name == "run"
        assert rev.version_table_name == "run_version"

    def test_custom_transaction_table_preserved_in_reverse(self) -> None:
        op = SyncVersionTriggersOp("run", "run_version", "custom_tx")
        rev = op.reverse()
        assert rev.transaction_table_name == "custom_tx"

    def test_default_transaction_table_is_transaction(self) -> None:
        op = SyncVersionTriggersOp("run", "run_version")
        assert op.transaction_table_name == "transaction"


class TestDropVersionTriggersOpReverse:
    def test_reverses_to_sync_op_with_is_new_table(self) -> None:
        op = DropVersionTriggersOp("run", "run_version")
        rev = op.reverse()
        assert isinstance(rev, SyncVersionTriggersOp)
        assert rev.is_new_table is True
        assert rev.table_name == "run"
        assert rev.version_table_name == "run_version"

    def test_custom_transaction_table_preserved_in_reverse(self) -> None:
        op = DropVersionTriggersOp("run", "run_version", "custom_tx")
        rev = op.reverse()
        assert rev.transaction_table_name == "custom_tx"

    def test_double_reverse_roundtrips(self) -> None:
        """reverse(reverse(op)) should produce the same kind of op as op."""
        original = SyncVersionTriggersOp("run", "run_version", is_new_table=True)
        roundtripped = original.reverse().reverse()
        assert type(roundtripped) is type(original)
        assert roundtripped.table_name == original.table_name


class TestRenderers:
    def test_render_sync_default_tx_table(self) -> None:
        op = SyncVersionTriggersOp("run", "run_version")
        result = _render_sync_version_triggers(None, op)  # type: ignore[arg-type]
        assert result == "op.sync_version_triggers('run', 'run_version')"

    def test_render_sync_custom_tx_table(self) -> None:
        op = SyncVersionTriggersOp("run", "run_version", "custom_tx")
        result = _render_sync_version_triggers(None, op)  # type: ignore[arg-type]
        assert "transaction_table_name='custom_tx'" in result

    def test_render_sync_no_default_tx_in_output(self) -> None:
        """When using the default transaction table, it should not appear
        in the output.
        """
        op = SyncVersionTriggersOp("run", "run_version")
        result = _render_sync_version_triggers(None, op)  # type: ignore[arg-type]
        assert "transaction_table_name" not in result

    def test_render_drop_default_tx_table(self) -> None:
        op = DropVersionTriggersOp("run", "run_version")
        result = _render_drop_version_triggers(None, op)  # type: ignore[arg-type]
        assert result == "op.drop_version_triggers('run', 'run_version')"

    def test_render_drop_custom_tx_table(self) -> None:
        op = DropVersionTriggersOp("run", "run_version", "custom_tx")
        result = _render_drop_version_triggers(None, op)  # type: ignore[arg-type]
        assert "transaction_table_name='custom_tx'" in result

    @pytest.mark.parametrize("table_name", ["run", "my_table", "opt_sca"])
    def test_render_sync_table_name_quoted(self, table_name: str) -> None:
        op = SyncVersionTriggersOp(table_name, f"{table_name}_version")
        result = _render_sync_version_triggers(None, op)  # type: ignore[arg-type]
        assert f"'{table_name}'" in result


# Shared procedure body used across comparator tests.
_BODY = " declare tx_id INT; begin return null; end "
_DEFINITION = f"CREATE FUNCTION foo() RETURNS trigger AS $$ {_BODY} $$ LANGUAGE plpgsql"


def _make_mock_triggers(
    definition: str = _DEFINITION,
    tx_table: str = "transaction",
) -> Any:
    """Build a fake PostgresVersionTriggers instance (enough for the comparator)."""
    proc = SimpleNamespace(
        definition_context={"transaction_tablename": tx_table},
        definition=definition,
    )
    return SimpleNamespace(version_procedure=proc)


def _make_autogen_ctx(dialect_name: str = "postgresql") -> Any:
    dialect = SimpleNamespace(name=dialect_name)
    conn = MagicMock()
    conn.dialect = dialect
    ctx = MagicMock()
    ctx.connection = conn
    return ctx


def _make_upgrade_ops(existing: list[Any] | None = None) -> "UpgradeOps":
    """Return a SimpleNamespace with a real list in .ops."""
    return cast("UpgradeOps", SimpleNamespace(ops=list(existing or [])))


class TestCompareVersionTriggers:
    """Tests for the _compare_version_triggers Alembic comparator hook."""

    def _run_comparator(
        self,
        *,
        dialect_name: str = "postgresql",
        registry: dict[tuple[str, str], Any],
        table_exists: bool = True,
        proc_source: str | None = None,
        existing_ops: list[Any] | None = None,
    ) -> "UpgradeOps":
        """Run the comparator with mocked dependencies; return upgrade_ops."""
        ctx = _make_autogen_ctx(dialect_name)
        upgrade_ops = _make_upgrade_ops(existing_ops)

        mock_inspector = MagicMock()
        mock_inspector.has_table.return_value = table_exists

        with (
            patch(
                "ixmp4.data.versions.autogenerate.sa.inspect",
                return_value=mock_inspector,
            ),
            patch(
                "ixmp4.data.versions.autogenerate._get_proc_source",
                return_value=proc_source,
            ),
            patch("ixmp4.data.versions.PostgresVersionTriggers") as MockPVT,
        ):
            MockPVT._registry = registry
            _compare_version_triggers(ctx, upgrade_ops, [])

        return upgrade_ops

    def test_non_postgresql_dialect_produces_no_ops(self) -> None:
        upgrade_ops = self._run_comparator(
            dialect_name="sqlite",
            registry={("run", "run_version"): _make_mock_triggers()},
            table_exists=True,
        )
        assert upgrade_ops.ops == []

    def test_matching_proc_emits_no_op(self) -> None:
        """When the stored procedure body matches the expected body, nothing
        is emitted.
        """
        from ixmp4.data.versions.autogenerate import _extract_body, _normalize

        stored = _normalize(_extract_body(_DEFINITION))
        upgrade_ops = self._run_comparator(
            registry={("run", "run_version"): _make_mock_triggers()},
            table_exists=True,
            proc_source=stored,
        )
        sync_ops = [o for o in upgrade_ops.ops if isinstance(o, SyncVersionTriggersOp)]
        assert len(sync_ops) == 0

    def test_missing_proc_emits_sync_op(self) -> None:
        """When pg_proc has no entry for the function, a SyncVersionTriggersOp
        is queued.
        """
        upgrade_ops = self._run_comparator(
            registry={("run", "run_version"): _make_mock_triggers()},
            table_exists=True,
            proc_source=None,  # function not found in pg_proc
        )
        sync_ops = [o for o in upgrade_ops.ops if isinstance(o, SyncVersionTriggersOp)]
        assert len(sync_ops) == 1
        assert sync_ops[0].table_name == "run"
        assert sync_ops[0].version_table_name == "run_version"
        assert not sync_ops[0].is_new_table

    def test_changed_proc_emits_sync_op(self) -> None:
        """When the stored source differs from expected, a SyncVersionTriggersOp
        is queued.
        """
        upgrade_ops = self._run_comparator(
            registry={("run", "run_version"): _make_mock_triggers()},
            table_exists=True,
            proc_source="completely different body content",
        )
        sync_ops = [o for o in upgrade_ops.ops if isinstance(o, SyncVersionTriggersOp)]
        assert len(sync_ops) == 1

    def test_changed_proc_carries_correct_transaction_table(self) -> None:
        triggers = _make_mock_triggers(tx_table="custom_tx")
        upgrade_ops = self._run_comparator(
            registry={("run", "run_version"): triggers},
            table_exists=True,
            proc_source="different body",
        )
        assert getattr(upgrade_ops.ops[0], "transaction_table_name") == "custom_tx"

    def test_new_table_in_pending_create_emits_sync_op_with_flag(self) -> None:
        """A table that Alembic is about to create gets a SyncVersionTriggersOp
        with is_new_table=True.
        """
        create_op = MagicMock(spec=CreateTableOp)
        create_op.table_name = "run"

        upgrade_ops = self._run_comparator(
            registry={("run", "run_version"): _make_mock_triggers()},
            table_exists=False,  # table doesn't exist in DB yet
            existing_ops=[create_op],
        )
        sync_ops = [o for o in upgrade_ops.ops if isinstance(o, SyncVersionTriggersOp)]
        assert len(sync_ops) == 1
        assert sync_ops[0].is_new_table is True

    def test_table_not_existing_and_not_pending_emits_no_op(self) -> None:
        """If the table doesn't exist and isn't being created, nothing is emitted."""
        upgrade_ops = self._run_comparator(
            registry={("run", "run_version"): _make_mock_triggers()},
            table_exists=False,
            existing_ops=[],  # no CreateTableOp
        )
        sync_ops = [o for o in upgrade_ops.ops if isinstance(o, SyncVersionTriggersOp)]
        assert len(sync_ops) == 0

    def test_table_in_pending_drop_emits_no_op(self) -> None:
        """Tables scheduled for deletion are skipped; PostgreSQL drops
        triggers automatically.
        """
        drop_op = MagicMock(spec=DropTableOp)
        drop_op.table_name = "run"

        upgrade_ops = self._run_comparator(
            registry={("run", "run_version"): _make_mock_triggers()},
            table_exists=True,
            existing_ops=[drop_op],
        )
        # No additional ops should have been appended.
        new_ops = [o for o in upgrade_ops.ops if not isinstance(o, MagicMock)]
        assert len(new_ops) == 0

    def test_multiple_registry_entries_each_evaluated(self) -> None:
        """Every entry in the registry is inspected independently."""
        registry = {
            ("run", "run_version"): _make_mock_triggers(),
            ("model", "model_version"): _make_mock_triggers(),
        }
        upgrade_ops = self._run_comparator(
            registry=registry,
            table_exists=True,
            proc_source=None,  # both procs missing -> two SyncVersionTriggersOps
        )
        sync_ops = [o for o in upgrade_ops.ops if isinstance(o, SyncVersionTriggersOp)]
        table_names = {o.table_name for o in sync_ops}
        assert table_names == {"run", "model"}
