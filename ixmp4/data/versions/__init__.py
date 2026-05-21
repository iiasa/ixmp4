import logging
from typing import Any, ClassVar, cast

from sqlalchemy import Connection, FromClause, Table, event, inspect, schema
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.sql import ColumnCollection, ColumnElement

from ixmp4.base_exceptions import ProgrammingError

from .ddl import (
    DeleteTrigger,
    InsertTrigger,
    UpdateTrigger,
    VersionProcedure,
)
from .model import BaseVersionModel as BaseVersionModel
from .model import Operation as Operation
from .transaction import Transaction as Transaction

logger = logging.getLogger(__name__)
default_tx_id_column_key = "transaction_id"
default_end_tx_id_column_key = "end_transaction_id"
default_op_column_key = "operation_type"


class PostgresVersionTriggers(object):
    """
    Represents a set of triggers on a source table that record changes
    to a version table.

    All instances are recorded in :attr:`_registry` (keyed by
    ``(table_name, version_table_name)``) so that the Alembic autogenerate
    comparator in :mod:`ixmp4.data.versions.autogenerate` can detect when
    trigger SQL has changed and needs a migration.

    .. code:: python

        from sqlalchemy import orm
        from toolkit.db.types import String

        from ixmp4.data import versions
        from ixmp4.data.base.db import BaseModel

        class Example(BaseModel):
            # ...
            name: String = orm.mapped_column(unique=True)

        class ExampleVersion(versions.BaseVersionModel):
            # ...
            name: String # omit any constraints

        version_triggers = versions.PostgresVersionTriggers(
            Example.__table__, ExampletVersion.__table__
        )

    A set of listeners will be attached to the provided tables.
    On creation (for example via ``BaseModel.metadata.create_all()``)
    each source table will also emit statements to create the triggers.
    On deletion (f.e. ``.drop_all()``) the triggers will also be dropped.

    Since these triggers support alembic autogeneration, a migration
    will contain the necessary syncronization code after changing something
    that affects the triggers.
    """

    version_procedure: VersionProcedure
    insert_trigger: InsertTrigger
    update_trigger: UpdateTrigger
    delete_trigger: DeleteTrigger

    entities: list[DeleteTrigger | InsertTrigger | UpdateTrigger | VersionProcedure]
    table: Table
    version_table: Table
    versioned_columns: ColumnCollection[str, ColumnElement[Any]]

    # Global registry for the Alembic autogenerate comparator.
    _registry: ClassVar[dict[tuple[str, str], "PostgresVersionTriggers"]] = {}

    def __init__(
        self,
        table: Table | type[DeclarativeBase],
        version_table: Table | type[DeclarativeBase],
        transaction_table: Table = cast(Table, Transaction.__table__),
        transaction_id_column: ColumnElement[int] | None = None,
        end_transaction_id_column: ColumnElement[int] | None = None,
        operation_type_column: ColumnElement[int] | None = None,
    ) -> None:
        table = self._extract_arg_table(table, "table")
        version_table = self._extract_arg_table(version_table, "version_table")

        logger.debug(
            "Initializing version triggers for tables "
            f"'{table.name}' and '{version_table.name}'."
        )

        self.table = table
        self.version_table = version_table

        if transaction_id_column is None:
            transaction_id_column = self.column_or_exception(
                "transaction_id_column", default_tx_id_column_key, version_table
            )
        if end_transaction_id_column is None:
            end_transaction_id_column = self.column_or_exception(
                "end_transaction_id_column", default_end_tx_id_column_key, version_table
            )
        if operation_type_column is None:
            operation_type_column = self.column_or_exception(
                "operation_type_column", default_op_column_key, version_table
            )

        self.check_primary_key(table, version_table, transaction_id_column)

        self.versioned_columns: ColumnCollection[str, ColumnElement[Any]] = (
            ColumnCollection()
        )
        for column in table.columns:
            version_column = version_table.columns.get(column.key)
            if version_column is not None and self.column_corresponds(
                column, version_column
            ):
                self.versioned_columns.add(column)

        self.version_procedure = VersionProcedure(
            table,
            version_table,
            transaction_table,
            self.versioned_columns,
            transaction_id_column,
            end_transaction_id_column,
        )
        self.insert_trigger = InsertTrigger(table, self.version_procedure)
        self.update_trigger = UpdateTrigger(table, self.version_procedure)
        self.delete_trigger = DeleteTrigger(table, self.version_procedure)
        self.entities = [
            self.version_procedure,
            self.insert_trigger,
            self.update_trigger,
            self.delete_trigger,
        ]

        PostgresVersionTriggers._registry[(table.name, version_table.name)] = self
        self.create_listeners()

    def _extract_arg_table(
        self, arg: Table | type[DeclarativeBase], argname: str
    ) -> Table:
        if isinstance(arg, Table):
            return arg
        if issubclass(arg, DeclarativeBase):
            local_table = inspect(arg).local_table
            if isinstance(local_table, Table):
                return local_table

            raise ProgrammingError(
                f"Argument '{argname}' must be an sqlalchemy model mapped to a table."
            )

        raise ProgrammingError(
            f"Argument '{argname}' must be `Table` or sqlalchemy model"
            f" not {arg.__class__.__name__}"
        )

    def column_or_exception(
        self, arg_name: str, def_col_name: str, table: Table
    ) -> ColumnElement[Any]:
        try:
            return table.columns[def_col_name]
        except KeyError:
            raise ProgrammingError(
                f"Provide the `{arg_name}` argument or add `{def_col_name}`"
                f"column to version table '{table.name}'.."
            )

    def column_corresponds(
        self, col: ColumnElement[Any], other: ColumnElement[Any]
    ) -> bool:
        return str(col.type) == str(other.type)

    def check_primary_key(
        self,
        table: Table,
        version_table: Table,
        transaction_id_column: ColumnElement[int],
    ) -> None:
        expected_version_pk = ColumnCollection(table.primary_key.columns.items())
        expected_version_pk.add(transaction_id_column)
        current_pk = version_table.primary_key.columns

        for expected_col in expected_version_pk:
            current_col = current_pk.get(expected_col.key)
            if current_col is None or not self.column_corresponds(
                expected_col, current_col
            ):
                raise ProgrammingError(
                    "Version table primary key must consist of "
                    f"original primary key and '{transaction_id_column.key}'.\n"
                    f"Expected: {expected_version_pk}\n"
                    f"Current: {current_pk}"
                )

    def sync_entities(self, con: Session | Connection) -> None:
        """Recreate all DDL entities for this trigger group on a supplied connection."""
        self.drop_entities(con)
        self.create_entities(con)

    def create_entities(self, con: Session | Connection) -> None:
        """Create all DDL entities for this trigger group on a supplied connection."""

        for ent in self.entities:
            ddl = schema.DDL(ent.to_create_sql()).execute_if(dialect="postgresql")  # type: ignore[no-untyped-call]
            con.execute(ddl)

    def drop_entities(self, con: Session | Connection) -> None:
        """Drop all DDL entities for this trigger group on a supplied connection."""

        for ent in reversed(self.entities):
            ddl = schema.DDL(ent.to_drop_sql()).execute_if(  # type: ignore[no-untyped-call]
                dialect="postgresql"
            )
            con.execute(ddl)

    def create_listeners(self) -> None:
        """Creates listeners for trigger creation when using
        `BaseModel.metadata.create_all(...)`, for example when testing."""
        for ent in self.entities:
            ddl = schema.DDL(ent.to_create_sql()).execute_if(  # type: ignore[no-untyped-call]
                dialect="postgresql"
            )
            event.listen(self.table, "after_create", ddl)

        for ent in reversed(self.entities):
            ddl = schema.DDL(ent.to_drop_sql()).execute_if(  # type: ignore[no-untyped-call]
                dialect="postgresql"
            )
            event.listen(self.table, "before_drop", ddl)
