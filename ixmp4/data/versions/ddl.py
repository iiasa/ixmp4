from typing import Any

from sqlalchemy import Table
from sqlalchemy.sql import ColumnCollection, ColumnElement

from .model import Operation


class VersionProcedure(object):
    signature_template = "%(tablename)s_version_procedure()"
    definition_template = """
    returns trigger as $$
        declare tx_id INT;
    begin
        begin
            insert into %(transaction_tablename)s (issued_at)
            values (NOW()) returning id into tx_id;
        end;

        if (TG_OP='DELETE') then
            update %(version_tablename)s set %(end_transaction_id_column)s = tx_id
            where id in (select id from OLD_TABLE)
            and %(end_transaction_id_column)s is NULL;

            insert into %(version_tablename)s (%(versioned_column_names)s,
            %(transaction_id_column)s, operation_type)
            select %(versioned_column_names)s, tx_id, %(op_delete)s
            from OLD_TABLE;
        elsif (TG_OP='UPDATE') then
            update %(version_tablename)s set %(end_transaction_id_column)s = tx_id
            where id in (select id from NEW_TABLE)
            and %(end_transaction_id_column)s is NULL;

            insert into %(version_tablename)s (%(versioned_column_names)s,
            %(transaction_id_column)s, operation_type)
            select %(versioned_column_names)s, tx_id, %(op_update)s
            from NEW_TABLE;
        elsif (TG_OP='INSERT') then
            insert into %(version_tablename)s (%(versioned_column_names)s,
            %(transaction_id_column)s, operation_type)
            select %(versioned_column_names)s, tx_id, %(op_insert)s
            from NEW_TABLE;
        end if;
        return null;
    end
    $$ language plpgsql;
    """
    definition_context: dict[str, Any]
    signature_context: dict[str, Any]
    definition: str
    signature: str

    def __init__(
        self,
        table: Table,
        version_table: Table,
        transaction_table: Table,
        versioned_columns: ColumnCollection[str, ColumnElement[Any]],
        transaction_id_column: ColumnElement[int],
        end_transaction_id_column: ColumnElement[int],
    ):
        tablename = table.name
        version_tablename = version_table.name
        transaction_tablename = transaction_table.name

        versioned_column_names = ", ".join(versioned_columns.keys())

        transaction_id_column = getattr(
            transaction_id_column, "description", transaction_id_column
        )
        end_transaction_id_column = getattr(
            end_transaction_id_column, "description", end_transaction_id_column
        )
        self.definition_context = {
            "transaction_id_column": transaction_id_column,
            "end_transaction_id_column": end_transaction_id_column,
            "tablename": tablename,
            "version_tablename": version_tablename,
            "transaction_tablename": transaction_tablename,
            "versioned_column_names": versioned_column_names,
            "op_delete": str(Operation.DELETE.value),
            "op_update": str(Operation.UPDATE.value),
            "op_insert": str(Operation.INSERT.value),
        }
        self.signature_context = {"tablename": tablename}
        self.signature = self.signature_template % self.signature_context
        self.definition = self.definition_template % self.definition_context

    def to_create_sql(self) -> str:
        return f"CREATE OR REPLACE FUNCTION {self.signature}\n{self.definition}"

    def to_drop_sql(self) -> str:
        return f"DROP FUNCTION IF EXISTS {self.signature};"


class VersionTrigger(object):
    signature_template: str
    definition_template: str
    tablename: str
    definition_context: dict[str, Any]
    signature_context: dict[str, Any]
    definition: str
    signature: str

    def __init__(self, table: Table, procedure: VersionProcedure):
        self.tablename = table.name

        self.definition_context = {
            "procedure_sig": procedure.signature,
            "tablename": self.tablename,
        }
        self.signature_context = {
            "tablename": self.tablename,
        }

        self.definition = self.definition_template % self.definition_context
        self.signature = self.signature_template % self.signature_context

    def to_create_sql(self) -> str:
        return f"CREATE OR REPLACE TRIGGER {self.signature}\n{self.definition}"

    def to_drop_sql(self) -> str:
        return f"DROP TRIGGER {self.signature} ON {self.tablename};"


class InsertTrigger(VersionTrigger):
    signature_template = "%(tablename)s_version_trigger_insert"
    definition_template = """after insert on %(tablename)s
    referencing
        new table as NEW_TABLE
    for each statement EXECUTE function %(procedure_sig)s;"""


class UpdateTrigger(VersionTrigger):
    signature_template = "%(tablename)s_version_trigger_update"
    definition_template = """after update on %(tablename)s
    referencing
        old table as OLD_TABLE
        new table as NEW_TABLE
    for each statement EXECUTE function %(procedure_sig)s;"""


class DeleteTrigger(VersionTrigger):
    signature_template = "%(tablename)s_version_trigger_delete"
    definition_template = """after delete on %(tablename)s
    referencing
    old table as OLD_TABLE
    for each statement EXECUTE function %(procedure_sig)s;"""
