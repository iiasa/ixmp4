from typing import Iterable

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

    def create_column(
        self,
        run_id: int,
        column_name: str,
        column_dtype: str,
        indexset_name: str,
        **kwargs,
    ) -> Column:
        indexset_id = self.backend.optimization.indexsets.get(
            run_id=run_id, name=indexset_name
        ).id

        return self.columns.create(
            name=column_name,
            dtype=column_dtype,
            constrained_to_indexset=indexset_id,
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

        #     if constrained_to_indexsets:
        #     for column_name, column_data, indexset_name in zip(
        #         data.keys(), data.values(), constrained_to_indexsets
        #     ):
        #         # Note: pd.api.types.infer_dtype might also work, but not on single
        #         # ints
        #         # TODO Make sure "object" is the correct dtype, maybe otherwise use
        #         # infer_dtype
        #         column = self.create_column(
        #             run_id=run_id,
        #             column_name=column_name,
        #             column_dtype=pd.Series(column_data).dtype.name,
        #             indexset_name=indexset_name,
        #         )
        #         columns.append(column)
        # else:
        #     for column_name, column_data in data.items():
        #         column = self.create_column(
        #             run_id=run_id,
        #             column_name=column_name,
        #             column_dtype=pd.Series(column_data).dtype.name,
        #             indexset_name=column_name,
        #         )
        #         columns.append(column)

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

        # data: dict[str, Any],
        # constrained_to_indexsets: list[str] | None = None,

    @guard("edit")
    def create(
        self,
        run_id: int,
        name: str,
        **kwargs,
    ) -> Table:
        return super().create(
            run_id=run_id,
            name=name,
            **kwargs,
        )

    @guard("edit")
    def update(
        self, name: str, value: float, unit_name: str, run_id: int, **kwargs
    ) -> Table:
        unit_id = self.backend.units.get(unit_name).id
        exc = (
            db.update(Table)
            .where(
                Table.run__id == run_id,
                Table.name == name,
            )
            .values(value=value, unit__id=unit_id)
            .returning(Table)
        )

        table: Table = self.session.execute(exc).scalar_one()
        self.session.commit()
        return table

    @guard("view")
    def list(self, *args, **kwargs) -> Iterable[Table]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)
