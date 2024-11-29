from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

import pandas as pd

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationItemUsageError
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import ColumnRepository, base
from .docs import TableDocsRepository
from .model import Table


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

    def _add_column(  # type: ignore[no-untyped-def]
        self,
        run_id: int,
        table_id: int,
        column_name: str,
        indexset_name: str,
        **kwargs,
    ) -> None:
        r"""Adds a Column to a Table.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which the
            :class:`ixmp4.data.abstract.optimization.Table` is defined.
        table_id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Table`.
        column_name : str
            The name of the Column, which must be unique in connection with the names of
            :class:`ixmp4.data.abstract.Run` and
            :class:`ixmp4.data.abstract.optimization.Table`.
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
            dtype=pd.Series(indexset.data).dtype.name,
            table_id=table_id,
            unique=True,
            **kwargs,
        )

    def add(self, run_id: int, name: str) -> Table:
        table = Table(name=name, run__id=run_id)
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

        table = super().create(run_id=run_id, name=name)
        for i, name in enumerate(constrained_to_indexsets):
            self._add_column(
                run_id=run_id,
                table_id=table.id,
                column_name=column_names[i] if column_names else name,
                indexset_name=name,
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
        if isinstance(data, dict):
            data = pd.DataFrame.from_dict(data=data)
        table = self.get_by_id(id=table_id)

        table.data = pd.concat([pd.DataFrame.from_dict(table.data), data]).to_dict(
            orient="list"
        )  # type: ignore[assignment]

        self.session.add(table)
        self.session.commit()
