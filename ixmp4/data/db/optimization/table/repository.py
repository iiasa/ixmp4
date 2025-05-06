from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

import pandas as pd

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationItemUsageError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import TableDocsRepository
from .model import Table, TableIndexsetAssociation


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

        table.set_creation_info(auth_context=self.backend.auth_context)
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

        if column_names and len(column_names) != len(set(column_names)):
            raise self.UsageError(
                f"While processing Table {name}: \n"
                "The given `column_names` are not unique!"
            )

        return super().create(
            run_id=run_id,
            name=name,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )

    @guard("view")
    def list(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> Iterable[Table]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    @guard("edit")
    def add_data(self, id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, dict):
            data = pd.DataFrame.from_dict(data=data)
        table = self.get_by_id(id=id)

        table.data = cast(
            types.JsonDict,
            pd.concat([pd.DataFrame.from_dict(table.data), data]).to_dict(
                orient="list"
            ),
        )

        self.session.commit()

    @guard("edit")
    def remove_data(self, id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, dict):
            data = pd.DataFrame.from_dict(data=data)

        table = self.get_by_id(id=id)
        index_list = table.column_names if table.column_names else table.indexset_names
        existing_data = pd.DataFrame(table.data)
        if not existing_data.empty:
            existing_data.set_index(index_list, inplace=True)
        data.set_index(index_list, inplace=True)

        remaining_data = existing_data[~existing_data.index.isin(data.index)]
        if not remaining_data.index.empty:
            remaining_data.reset_index(inplace=True)

        table.data = cast(types.JsonDict, remaining_data.to_dict(orient="list"))
