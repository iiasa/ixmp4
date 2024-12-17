from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

import pandas as pd

from ixmp4 import db
from ixmp4.core.exceptions import (
    OptimizationDataValidationError,
    OptimizationItemUsageError,
)
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import ColumnRepository, base, utils
from .docs import TableDocsRepository
from .model import Table, TableData, TableIndexsetAssociation


class TableRepository(
    base.Creator[Table],
    base.Retriever[Table],
    base.Enumerator[Table],
    abstract.TableRepository,
):
    model_class = Table

    UsageError = OptimizationItemUsageError

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)
        self.docs = TableDocsRepository(*args)
        self.columns = ColumnRepository(*args)

        from .filter import OptimizationTableFilter

        self.filter_class = OptimizationTableFilter

    def add(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Table:
        table = Table(name=name, run__id=run_id)
        indexsets = self.backend.optimization.indexsets.list(
            name__in=constrained_to_indexsets, run_id=run_id
        )

        for i in range(len(indexsets)):
            _ = TableIndexsetAssociation(
                table=table,
                indexset=indexsets[i],
                column_name=column_names[i] if column_names else None,
            )

        self.session.add(table)

        return table

    @guard("view")
    def get(self, run_id: int, name: str) -> Table:
        exc = db.select(Table).where((Table.name == name) & (Table.run__id == run_id))
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Table.NotFound

    @guard("view")
    def get_by_id(self, id: int) -> Table:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise Table.NotFound(id=id)

        return obj

    @guard("edit")
    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Table:
        if column_names and len(column_names) != len(constrained_to_indexsets):
            raise self.UsageError(
                f"While processing Table {name}: \n"
                "`constrained_to_indexsets` and `column_names` not equal in length! "
                "Please provide the same number of entries for both!"
            )
        # TODO: activate something like this if each column must be indexed by a unique
        # indexset
        # if len(constrained_to_indexsets) != len(set(constrained_to_indexsets)):
        #     raise self.UsageError("Each dimension must be constrained to a unique indexset!") # noqa
        if column_names and len(column_names) != len(set(column_names)):
            raise self.UsageError(
                f"While processing Table {name}: \n"
                "The given `column_names` are not unique!"
            )

        table = super().create(
            run_id=run_id,
            name=name,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )
        return table

    @guard("view")
    def list(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> Iterable[Table]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    @guard("edit")
    def add_data(self, table_id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        table = self.get_by_id(id=table_id)

        data = pd.DataFrame.from_dict(
            data=utils.validate_data(
                host=table,
                data=data,
                columns=table._indexsets,
                column_names=table.column_names,
            )
        )

        column_names = table.column_names if table.column_names else table.indexsets

        # Ensure column order is the same as table.indexsets
        data = data[column_names]
        renames = {name: f"value_{i}" for i, name in enumerate(column_names)}
        data.rename(renames, axis="columns", inplace=True)

        bulk_insert_enabled_data = cast(
            list[dict[str, str]], data.to_dict(orient="records")
        )

        try:
            self.session.execute(
                db.insert(TableData).values(table__id=table_id),
                bulk_insert_enabled_data,
            )
        except db.IntegrityError as e:
            self.session.rollback()
            raise OptimizationDataValidationError from e

        self.session.commit()
