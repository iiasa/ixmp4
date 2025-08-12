import logging
from typing import Any, cast

from sqlalchemy import Connection, FromClause, Table, event, schema
from sqlalchemy.orm import Session
from sqlalchemy.sql import ColumnCollection, ColumnElement

from ixmp4.core.exceptions import ProgrammingError

from .ddl import (
    DeleteTrigger,
    InsertTrigger,
    UpdateTrigger,
    VersionProcedure,
)
from .model import DefaultVersionModel as DefaultVersionModel
from .model import Operation as Operation
from .repository import VersionRepository as VersionRepository
from .transaction import Transaction as Transaction
from .transaction import TransactionRepository as TransactionRepository

logger = logging.getLogger(__name__)
default_tx_id_column_key = "transaction_id"
default_end_tx_id_column_key = "end_transaction_id"
default_op_column_key = "operation_type"


class PostgresVersionTriggers(object):
    version_procedure: VersionProcedure
    insert_trigger: InsertTrigger
    update_trigger: UpdateTrigger
    delete_trigger: DeleteTrigger

    entities: list[DeleteTrigger | InsertTrigger | UpdateTrigger | VersionProcedure]

    def __init__(
        self,
        table: Table | FromClause,
        version_table: Table | FromClause,
        transaction_table: Table = cast(Table, Transaction.__table__),
        transaction_id_column: ColumnElement[int] | None = None,
        end_transaction_id_column: ColumnElement[int] | None = None,
        operation_type_column: ColumnElement[int] | None = None,
    ) -> None:
        if not isinstance(table, Table):
            raise ProgrammingError(
                f"Argument 'table' must be `Table` not {table.__class__.__name__}"
            )
        if not isinstance(version_table, Table):
            raise ProgrammingError(
                "Argument 'version_table' must be `Table` not "
                f"`{version_table.__class__.__name__}`"
            )

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

        versioned_columns: ColumnCollection[str, ColumnElement[Any]] = (
            ColumnCollection()
        )
        for column in table.columns:
            version_column = version_table.columns.get(column.key)
            if version_column is not None and self.column_corresponds(
                column, version_column
            ):
                versioned_columns.add(column)

        self.version_procedure = VersionProcedure(
            table,
            version_table,
            transaction_table,
            versioned_columns,
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

        self.create_listeners()

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
        self.drop_entities(con)
        self.create_entities(con)

    def create_entities(self, con: Session | Connection) -> None:
        for ent in self.entities:
            ddl = schema.DDL(ent.to_create_sql()).execute_if(dialect="postgresql")  # type: ignore[no-untyped-call]
            con.execute(ddl)

    def drop_entities(self, con: Session | Connection) -> None:
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
