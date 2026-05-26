"""Alembic autogenerate support for :class:`.PostgresVersionTriggers`.

The module is imported by :mod:`ixmp4.db.migrations.env` so that the
hooks are active whenever Alembic runs.

Example migration:

    def upgrade():
        op.sync_version_triggers("run", "run_version")


    def downgrade():
        op.sync_version_triggers("run", "run_version")
"""

import logging
import re
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from alembic.autogenerate import comparators, renderers
from alembic.operations import MigrateOperation, Operations
from alembic.operations.ops import CreateTableOp, DropTableOp
from alembic.util.langhelpers import DispatchPriority

if TYPE_CHECKING:
    from alembic.autogenerate.api import AutogenContext
    from alembic.operations.ops import UpgradeOps

logger = logging.getLogger(__name__)


@Operations.register_operation("sync_version_triggers")
class SyncVersionTriggersOp(MigrateOperation):
    """Recreate (sync) the versioning trigger procedure for a table pair.

    Registered as ``op.sync_version_triggers(...)`` on the Alembic
    :class:`~alembic.operations.Operations` object.
    """

    def __init__(
        self,
        table_name: str,
        version_table_name: str,
        transaction_table_name: str = "transaction",
        *,
        is_new_table: bool = False,
    ) -> None:
        self.table_name = table_name
        self.version_table_name = version_table_name
        self.transaction_table_name = transaction_table_name
        # Internal flag: True when emitted for a brand-new table so that
        # reverse() produces a DropVersionTriggersOp instead of another sync.
        self.is_new_table = is_new_table

    @classmethod
    def sync_version_triggers(
        cls,
        operations: Operations,
        table_name: str,
        version_table_name: str,
        transaction_table_name: str = "transaction",
    ) -> None:
        """Recreate the versioning triggers for *table_name* / *version_table_name*."""
        op = cls(table_name, version_table_name, transaction_table_name)
        operations.invoke(op)

    def reverse(self) -> "SyncVersionTriggersOp | DropVersionTriggersOp":
        if self.is_new_table:
            # The inverse of creating triggers for a new table is dropping the
            # procedure.  (The triggers themselves are dropped automatically by
            # PostgreSQL when their table is dropped.)
            return DropVersionTriggersOp(
                self.table_name,
                self.version_table_name,
                self.transaction_table_name,
            )
        return SyncVersionTriggersOp(
            self.table_name,
            self.version_table_name,
            self.transaction_table_name,
        )


@Operations.register_operation("drop_version_triggers")
class DropVersionTriggersOp(MigrateOperation):
    """Drop the versioning trigger procedure (and its dependent triggers) for a table.

    Registered as ``op.drop_version_triggers(...)`` on the Alembic
    :class:`~alembic.operations.Operations` object.  Typically appears in the
    ``downgrade()`` function of a migration that creates a versioned table.
    """

    def __init__(
        self,
        table_name: str,
        version_table_name: str,
        transaction_table_name: str = "transaction",
    ) -> None:
        self.table_name = table_name
        self.version_table_name = version_table_name
        self.transaction_table_name = transaction_table_name

    @classmethod
    def drop_version_triggers(
        cls,
        operations: Operations,
        table_name: str,
        version_table_name: str,
        transaction_table_name: str = "transaction",
    ) -> None:
        """Drop the versioning triggers for *table_name* / *version_table_name*."""
        op = cls(table_name, version_table_name, transaction_table_name)
        operations.invoke(op)

    def reverse(self) -> SyncVersionTriggersOp:
        return SyncVersionTriggersOp(
            self.table_name,
            self.version_table_name,
            self.transaction_table_name,
            is_new_table=True,
        )


@Operations.implementation_for(SyncVersionTriggersOp)
def _impl_sync_version_triggers(
    operations: Operations, op: SyncVersionTriggersOp
) -> None:
    conn = operations.get_bind()
    if conn.dialect.name != "postgresql":
        logger.info(
            "Skipping sync_version_triggers for dialect '%s'.", conn.dialect.name
        )
        return

    from ixmp4.data.versions import PostgresVersionTriggers

    triggers = PostgresVersionTriggers._registry.get(
        (op.table_name, op.version_table_name)
    )
    if triggers is None:
        # Fallback for calls with custom / non-registered table names.
        meta = sa.MetaData()
        data_table = sa.Table(op.table_name, meta, autoload_with=conn)
        version_table = sa.Table(op.version_table_name, meta, autoload_with=conn)
        transaction_table = sa.Table(
            op.transaction_table_name, meta, autoload_with=conn
        )
        triggers = PostgresVersionTriggers(data_table, version_table, transaction_table)

    triggers.sync_entities(conn)


@Operations.implementation_for(DropVersionTriggersOp)
def _impl_drop_version_triggers(
    operations: Operations, op: DropVersionTriggersOp
) -> None:
    conn = operations.get_bind()
    if conn.dialect.name != "postgresql":
        logger.info(
            "Skipping drop_version_triggers for dialect '%s'.", conn.dialect.name
        )
        return

    proc_name = f"{op.table_name}_version_procedure"
    # CASCADE drops the function and all triggers that depend on it.
    conn.execute(sa.text(f"DROP FUNCTION IF EXISTS {proc_name}() CASCADE"))


@renderers.dispatch_for(SyncVersionTriggersOp)
def _render_sync_version_triggers(
    autogen_context: "AutogenContext", op: SyncVersionTriggersOp
) -> str:
    extra = (
        f", transaction_table_name={op.transaction_table_name!r}"
        if op.transaction_table_name != "transaction"
        else ""
    )
    return (
        f"op.sync_version_triggers({op.table_name!r}, {op.version_table_name!r}{extra})"
    )


@renderers.dispatch_for(DropVersionTriggersOp)
def _render_drop_version_triggers(
    autogen_context: "AutogenContext", op: DropVersionTriggersOp
) -> str:
    extra = (
        f", transaction_table_name={op.transaction_table_name!r}"
        if op.transaction_table_name != "transaction"
        else ""
    )
    return (
        f"op.drop_version_triggers({op.table_name!r}, {op.version_table_name!r}{extra})"
    )


_WHITESPACE = re.compile(r"\s+")


def _normalize(text: str) -> str:
    """Collapse all whitespace runs to a single space for comparison."""
    return _WHITESPACE.sub(" ", text).strip()


def _extract_body(definition: str) -> str:
    """Return the PL/pgSQL body (between the outer ``$$`` delimiters)."""
    start = definition.find("$$") + 2
    end = definition.rfind("$$")
    return definition[start:end]


def _get_proc_source(conn: Any, proc_name: str) -> str | None:
    """Return the ``prosrc`` for *proc_name* from ``pg_proc``, or *None*."""
    row = conn.execute(
        sa.text("SELECT prosrc FROM pg_proc WHERE proname = :name"),
        {"name": proc_name},
    ).fetchone()
    return row[0] if row else None


@comparators.dispatch_for("schema", priority=DispatchPriority.LAST)
def _compare_version_triggers(
    autogen_context: "AutogenContext",
    upgrade_ops: "UpgradeOps",
    schemas: list[str | None],
) -> None:
    """Detect versioning triggers whose SQL differs from the current code."""
    conn = autogen_context.connection
    if conn is None or conn.dialect.name != "postgresql":
        return

    from ixmp4.data.versions import PostgresVersionTriggers

    inspector = sa.inspect(conn)

    # Tables being created or dropped by other comparators in this same run.
    # CreateTableOp is added by Alembic's built-in schema comparator before
    # our hook runs, so these sets are already fully populated.
    pending_create: set[str] = {
        op.table_name for op in upgrade_ops.ops if isinstance(op, CreateTableOp)
    }
    pending_drop: set[str] = {
        op.table_name for op in upgrade_ops.ops if isinstance(op, DropTableOp)
    }

    for (table_name, version_table_name), triggers in list(
        PostgresVersionTriggers._registry.items()
    ):
        table_exists = inspector.has_table(table_name)

        if not table_exists and table_name not in pending_create:
            # Table doesn't exist and isn't being created; nothing to do.
            continue

        if table_name in pending_drop:
            # Table is being dropped; PostgreSQL drops the triggers
            # automatically when the table is dropped.
            continue

        if not table_exists:
            # New table: the CreateTableOp is already queued; append the
            # trigger-creation op so it runs immediately after.
            logger.debug(
                "New versioned table '%s' detected; queueing trigger creation.",
                table_name,
            )
            upgrade_ops.ops.append(
                SyncVersionTriggersOp(
                    table_name,
                    version_table_name,
                    triggers.version_procedure.definition_context[
                        "transaction_tablename"
                    ],
                    is_new_table=True,
                )
            )
            continue

        # Existing table: compare stored procedure source with expected body.
        proc_name = f"{table_name}_version_procedure"
        current_src = _get_proc_source(conn, proc_name)
        expected_body = _extract_body(triggers.version_procedure.definition)

        if current_src is None or _normalize(current_src) != _normalize(expected_body):
            logger.debug(
                "Version trigger mismatch detected for table '%s'.", table_name
            )
            upgrade_ops.ops.append(
                SyncVersionTriggersOp(
                    table_name,
                    version_table_name,
                    triggers.version_procedure.definition_context[
                        "transaction_tablename"
                    ],
                )
            )
