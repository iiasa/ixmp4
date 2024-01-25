from typing import Any, Iterable

import pandas as pd

from ixmp4 import db
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import Column, ColumnRepository, base
from .docs import TableDocsRepository
from .model import Table


class TableRepository(
    base.Creator[Table],
    base.Retriever[Table],
    base.Enumerator[Table],
    abstract.TableRepository,
):
    model_class = Table

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = TableDocsRepository(*args, **kwargs)
        self.columns = ColumnRepository(*args, **kwargs)

        from .filter import OptimizationTableFilter

        self.filter_class = OptimizationTableFilter

    def add_column(
        self,
        run_id: int,
        table_id: int,
        column_name: str,
        indexset_name: str,
        **kwargs,
    ) -> Column:
        indexset = self.backend.optimization.indexsets.get(
            run_id=run_id, name=indexset_name
        )

        return self.columns.create(
            table_id=table_id,
            name=column_name,
            dtype=pd.Series(indexset.elements).dtype.name,
            constrained_to_indexset=indexset.id,
            unique=True,
            **kwargs,
        )

    def add(
        self,
        run_id: int,
        name: str,
    ) -> Table:
        table = Table(name=name, run__id=run_id, **self.get_creation_info())
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
        constrained_to_indexsets: list[str],  # TODO: try passing a str to this
        dimension_names: list[str] | None = None,  # TODO: ensure the right number
        **kwargs,
    ) -> Table:
        if dimension_names and len(dimension_names) != len(constrained_to_indexsets):
            raise ValueError(
                "`constrained_to_indexsets` and `dimension_names` not equal in length! "
                "Please provide the same number of entries for both!"
            )
        # TODO: activate something like this if each column must be indexed by a unique
        # indexset
        # if len(constrained_to_indexsets) != len(set(constrained_to_indexsets)):
        #     raise ValueError("Each dimension must be constrained to a unique indexset!") # noqa
        table = super().create(
            run_id=run_id,
            name=name,
            **kwargs,
        )
        for i, name in enumerate(constrained_to_indexsets):
            _ = self.add_column(
                run_id=run_id,
                table_id=table.id,
                column_name=dimension_names[i] if dimension_names else name,
                indexset_name=name,
            )

        return table

    @guard("view")
    def list(self, *args, **kwargs) -> Iterable[Table]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    @guard("edit")
    def add_data(self, table_id: int, data: pd.DataFrame | dict[str, Any]):
        exc = db.update(Table).where(Table.id == table_id).values(data=data)

        self.session.execute(exc)
        self.session.commit()
        return self.get_by_id(table_id)
