import copy
from typing import Any, Iterable

import pandas as pd

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationItemUsageError
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db.unit import Unit

from .. import Column, ColumnRepository, base
from ..utils import validate_data_json
from .docs import ParameterDocsRepository
from .model import Parameter

PANDAS_SQL_TYPE_MAP = {
    "int16": db.Integer,
    "int32": db.Integer,
    "int64": db.Integer,
    "float32": db.Float,
    "float64": db.Float,
    "object": db.Text,
}


class ParameterRepository(
    base.Creator[Parameter],
    base.Retriever[Parameter],
    base.Enumerator[Parameter],
    abstract.ParameterRepository,
):
    model_class = Parameter

    UsageError = OptimizationItemUsageError

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = ParameterDocsRepository(*args, **kwargs)
        self.columns = ColumnRepository(*args, **kwargs)

        from .filter import OptimizationParameterFilter

        self.filter_class = OptimizationParameterFilter

    def _add_column(
        self,
        run_id: int,
        parameter_id: int,
        column_name: str,
        indexset_name: str,
        **kwargs,
    ) -> None:
        r"""Adds a Column to a Parameter.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which the
            :class:`ixmp4.data.abstract.optimization.Parameter` is defined.
        parameter_id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Parameter`.
        column_name : str
            The name of the Column, which must be unique in connection with the names of
            :class:`ixmp4.data.abstract.Run` and
            :class:`ixmp4.data.abstract.optimization.Parameter`.
        indexset_name : str
            The name of the :class:`ixmp4.data.abstract.optimization.IndexSet` the
            Column will be linked to.
        \*\*kwargs: any
            Keyword arguments to be passed to
            :func:`ixmp4.data.abstract.optimization.Column.create`.
        """
        indexset = self.backend.optimization.indexsets.get(
            run_id=run_id, name=indexset_name
        )
        self.columns.create(
            name=column_name,
            constrained_to_indexset=indexset.id,
            dtype=pd.Series(indexset.elements).dtype.name,
            parameter_id=parameter_id,
            unique=True,
            **kwargs,
        )

    def add(
        self,
        run_id: int,
        name: str,
    ) -> Parameter:
        parameter = Parameter(name=name, run__id=run_id)
        parameter.set_creation_info(auth_context=self.backend.auth_context)
        self.session.add(parameter)

        return parameter

    @guard("view")
    def get(self, run_id: int, name: str) -> Parameter:
        exc = db.select(Parameter).where(
            (Parameter.name == name) & (Parameter.run__id == run_id)
        )
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Parameter.NotFound

    @guard("view")
    def get_by_id(self, id: int) -> Parameter:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise Parameter.NotFound(id=id)

        return obj

    @guard("edit")
    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
        **kwargs,
    ) -> Parameter:
        # Convert to list to avoid enumerate() splitting strings to letters
        if isinstance(constrained_to_indexsets, str):
            constrained_to_indexsets = list(constrained_to_indexsets)
        if column_names and len(column_names) != len(constrained_to_indexsets):
            raise self.UsageError(
                f"While processing Parameter {name}: \n"
                "`constrained_to_indexsets` and `column_names` not equal in length! "
                "Please provide the same number of entries for both!"
            )
        # TODO: activate something like this if each column must be indexed by a unique
        # indexset
        # if len(constrained_to_indexsets) != len(set(constrained_to_indexsets)):
        #     raise self.UsageError("Each dimension must be constrained to a unique indexset!") # noqa
        if column_names and len(column_names) != len(set(column_names)):
            raise self.UsageError(
                f"While processing Parameter {name}: \n"
                "The given `column_names` are not unique!"
            )

        parameter = super().create(
            run_id=run_id,
            name=name,
            **kwargs,
        )
        for i, name in enumerate(constrained_to_indexsets):
            self._add_column(
                run_id=run_id,
                parameter_id=parameter.id,
                column_name=column_names[i] if column_names else name,
                indexset_name=name,
            )

        return parameter

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    @guard("edit")
    def add_data(self, parameter_id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, dict):
            try:
                data = pd.DataFrame.from_dict(data=data)
            except ValueError as e:
                raise Parameter.DataInvalid(str(e)) from e

        parameter = self.get_by_id(id=parameter_id)

        missing_columns = set(["values", "units"]) - set(data.columns)
        if missing_columns:
            raise OptimizationItemUsageError(
                "Parameter.data must include the column(s): "
                f"{', '.join(missing_columns)}!"
            )

        # Can use a set for now, need full column if we care about order
        for unit_name in set(data["units"]):
            try:
                self.backend.units.get(name=unit_name)
            except Unit.NotFound as e:
                # TODO Add a helpful hint on how to check defined Units
                raise Unit.NotFound(
                    message=f"'{unit_name}' is not defined for this Platform!"
                ) from e

        index_list = [column.name for column in parameter.columns]
        existing_data = pd.DataFrame(parameter.data)
        if not existing_data.empty:
            existing_data.set_index(index_list, inplace=True)
        parameter.data = (
            data.set_index(index_list).combine_first(existing_data).reset_index()
        ).to_dict(orient="list")

        self.session.commit()

    def create_temporary_optimization_data_table(
        self, columns: list[Column], name: str, value_type: str
    ) -> db.Table:
        # Create backbone of the temp table
        _ = db.Table(
            f"temp_optimization_{name}_data",
            self.model_class.metadata,
            prefixes=["TEMPORARY"],
        )

        # Add columns dynamically
        unique_columns: list[db.SAColumn] = []
        for column in columns:
            sqlalchemy_column: db.SAColumn = (
                db.SAColumn(column.name, PANDAS_SQL_TYPE_MAP[column.dtype])
                if self.dialect.name == "sqlite"
                else db.SAColumn(column.name, db.JsonType)
            )
            _ = db.Table(
                f"temp_optimization_{name}_data",
                self.model_class.metadata,
                sqlalchemy_column,
                extend_existing=True,
            )
            unique_columns.append(sqlalchemy_column)

        # Return with always-present columns and dynamic UniqueConstraint
        return (
            db.Table(
                f"temp_optimization_{name}_data",
                self.model_class.metadata,
                db.SAColumn("values", PANDAS_SQL_TYPE_MAP[value_type]),
                db.SAColumn("units", db.Text),
                db.UniqueConstraint(*unique_columns),
                extend_existing=True,
            )
            if self.dialect.name == "sqlite"
            else db.Table(
                f"temp_optimization_{name}_data",
                self.model_class.metadata,
                db.SAColumn("values", db.JsonType),
                db.SAColumn("units", db.JsonType),
                db.UniqueConstraint(*unique_columns),
                extend_existing=True,
            )
        )

    def extract_json_field_to_table(
        self, name: str
    ) -> db.sql.expression.TableValuedAlias:
        return (
            db.func.json_each(
                db.func.json_extract(Parameter.data, f"$.{name}")
            ).table_valued("value", "key", name=name)
            if self.dialect.name == "sqlite"
            else db.func.jsonb_each(
                db.func.jsonb_extract_path(Parameter.data, f"$.{name}")
            ).table_valued("value", "key", name=name)
        )

    def create_temporary_table_select_statement(
        self, parameter_id: int, columns: list[Column]
    ) -> db.sql.Select:
        # Extract JSON fields to table-valued functions
        values_table = self.extract_json_field_to_table("values")
        units_table = self.extract_json_field_to_table("units")
        column_tables = [
            self.extract_json_field_to_table(column.name) for column in columns
        ]
        column_values = [
            column_table.c.value.label(column_table.name)
            for column_table in column_tables
        ]

        # Select all desired columns
        select_statement = (
            db.select(
                *column_values,
                values_table.c.value.label("values"),
                units_table.c.value.label("units"),
            )
            # Mark cartesian product as intentional
            .join_from(Parameter, values_table, db.true())
            .join_from(
                values_table, units_table, values_table.c.key == units_table.c.key
            )
        )
        for select_column in column_tables:
            select_statement = select_statement.join_from(
                values_table, select_column, values_table.c.key == select_column.c.key
            )

        return select_statement.where(Parameter.id == parameter_id)

    # TODO aside from hardcoding the str names, haven't found a proper workaround
    # Postgres might not like single quotes being kept or so:
    # https://stackoverflow.com/questions/12170842/could-not-determine-data-type-of-parameter-1-in-python-pgsql
    # Prepare for double-list-comprehension
    def column_name(self, column: db.SAColumn) -> str:
        return (
            column.name
            # if self.dialect.name == "sqlite"
            # else db.func.to_jsonb(column.name)
        )

    def json_group_array(self, column: db.SAColumn) -> Any:
        return (
            db.func.json_group_array(column)
            if self.dialect.name == "sqlite"
            else db.func.jsonb_build_array(column)
        )

    # TODO figure out type hints once implementation is final
    def create_subquery_for_data_update(
        self, data_table: db.Table
    ) -> db.sql.expression.ScalarSelect:
        # Use double-list-comprehension
        column_list = [
            f(column)
            for column in data_table.c
            for f in (self.column_name, self.json_group_array)
        ]
        return (
            db.select(db.func.json_object(*column_list)).scalar_subquery()
            if self.dialect.name == "sqlite"
            else db.func.jsonb_build_object(*column_list)  # type: ignore
        )

    @guard("edit")
    def add_data_json(
        self, parameter_id: int, data: dict[str, Any] | pd.DataFrame
    ) -> None:
        if isinstance(data, dict):
            data = pd.DataFrame.from_dict(data=data)

        missing_columns = set(["values", "units"]) - set(data.columns)
        assert (
            not missing_columns
        ), f"Parameter.data must include the column(s): {', '.join(missing_columns)}!"

        # Can use a set for now, need full column if we care about order
        for unit_name in set(data["units"]):
            try:
                self.backend.units.get(name=unit_name)
            except Unit.NotFound as e:
                # TODO Add a helpful hint on how to check defined Units
                raise Unit.NotFound(
                    message=f"'{unit_name}' is not defined for this Platform!"
                ) from e

        # Validate data manually:
        data_to_validate = copy.deepcopy(data)
        data_to_validate.drop(["values", "units"], axis=1, inplace=True)
        columns = [column for column in self.columns.list(parameter_id=parameter_id)]
        validate_data_json(data=data_to_validate, columns=columns)

        # Create temporary table from data in JSON field
        # TODO Is there a better way to find the type?
        value_type = str(data["values"].dtype)
        temp_table = self.create_temporary_optimization_data_table(
            columns, name=f"parameter_{parameter_id}", value_type=value_type
        )
        temp_table.create(self.session.connection())

        # Fill temporary table with existing values of Parameter.data
        select_statement = self.create_temporary_table_select_statement(
            parameter_id=parameter_id, columns=columns
        )
        names = [column.name for column in columns]
        unique_columns = copy.deepcopy(names)
        names.extend(["values", "units"])
        insert_statement = db.insert(temp_table).from_select(
            names=names, select=select_statement
        )
        self.session.execute(insert_statement)

        # Upsert new data to temporary table
        executemany_data = data.to_dict(orient="records")

        # This works because we only support two backends
        # NB executemany needs sth like [{"x": 11, "y": 12}, {"x": 13, "y": 14}]
        upsert_statement = (
            db.sqlite_insert(temp_table).values(executemany_data)
            if self.dialect.name == "sqlite"
            else db.pg_insert(temp_table).values(executemany_data)
        )
        # Mypy is only able to verify this works with sinle-line upsert_statement
        upsert_statement = upsert_statement.on_conflict_do_update(  # type: ignore[attr-defined]
            index_elements=unique_columns,
            set_=dict(
                values=upsert_statement.excluded["values"],  # type: ignore[attr-defined]
                units=upsert_statement.excluded["units"],  # type: ignore[attr-defined]
            ),
        )
        self.session.execute(upsert_statement)

        # Convert temp table to JSON, update parameter.data
        insert_subquery = self.create_subquery_for_data_update(data_table=temp_table)
        update_statement = (
            db.update(Parameter)
            .where(Parameter.id == parameter_id)
            .values(data=insert_subquery)
            # TODO this hardcoding works on postgres
            # .values(
            #     data=db.func.jsonb_build_object(
            #         "Indexset 0",
            #         column_list[1],
            #         "values",
            #         column_list[3],
            #         "units",
            #         column_list[5],
            #     )
            # )
        )
        self.session.execute(update_statement)

        # TODO
        # I thought something like this wouldn't be necessary with temporary tables.
        # We do use settings to persist temp tables after session.commit() with SQLite:
        # https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#using-temporary-tables-with-sqlite
        # So maybe only needed for SQLite. Don't know if it's ever hurtful, though.
        temp_table.drop(self.session.connection())
        self.model_class.metadata.remove(temp_table)

        self.session.commit()

    @guard("view")
    def list(self, *args, **kwargs) -> Iterable[Parameter]:
        return super().list(*args, **kwargs)
