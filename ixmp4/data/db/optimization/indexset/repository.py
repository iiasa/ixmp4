from collections.abc import Generator
from typing import TYPE_CHECKING, Any, List, Literal, Sequence, cast

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.data.db.optimization.equation.model import (
    EquationIndexsetAssociation,
)
from ixmp4.data.db.optimization.equation.repository import EquationRepository
from ixmp4.data.db.optimization.parameter.model import (
    ParameterIndexsetAssociation,
)
from ixmp4.data.db.optimization.parameter.repository import ParameterRepository
from ixmp4.data.db.optimization.table.model import TableIndexsetAssociation
from ixmp4.data.db.optimization.table.repository import TableRepository
from ixmp4.data.db.optimization.variable.model import (
    VariableIndexsetAssociation,
)
from ixmp4.data.db.optimization.variable.repository import VariableRepository

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

import logging

import pandas as pd

from ixmp4 import db
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import base, utils
from .docs import IndexSetDocsRepository
from .model import IndexSet, IndexSetData

log = logging.getLogger(__name__)


class IndexSetRepository(
    base.Creator[IndexSet],
    base.Deleter[IndexSet],
    base.Retriever[IndexSet],
    base.Enumerator[IndexSet],
    abstract.IndexSetRepository,
):
    model_class = IndexSet

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)
        self.docs = IndexSetDocsRepository(*args)

        from .filter import OptimizationIndexSetFilter

        self.filter_class = OptimizationIndexSetFilter

        self._linked_columns_lookup = {
            "table": (
                TableIndexsetAssociation.table__id,
                TableIndexsetAssociation.indexset__id,
            ),
            "parameter": (
                ParameterIndexsetAssociation.parameter__id,
                ParameterIndexsetAssociation.indexset__id,
            ),
            "equation": (
                EquationIndexsetAssociation.equation__id,
                EquationIndexsetAssociation.indexset__id,
            ),
            "variable": (
                VariableIndexsetAssociation.variable__id,
                VariableIndexsetAssociation.indexset__id,
            ),
        }

    def add(self, run_id: int, name: str) -> IndexSet:
        indexset = IndexSet(run__id=run_id, name=name)
        self.session.add(indexset)
        return indexset

    @guard("view")
    def get(self, run_id: int, name: str) -> IndexSet:
        exc = db.select(IndexSet).where(
            (IndexSet.name == name) & (IndexSet.run__id == run_id)
        )
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise IndexSet.NotFound

    @guard("view")
    def get_by_id(self, id: int) -> IndexSet:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise IndexSet.NotFound(id=id)

        return obj

    @guard("edit")
    def create(self, run_id: int, name: str) -> IndexSet:
        return super().create(run_id=run_id, name=name)

    @guard("edit")
    def delete(self, id: int) -> None:
        super().delete(id=id)

    @guard("view")
    def list(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> list[IndexSet]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> pd.DataFrame:
        return super().tabulate(**kwargs).rename(columns={"_data_type": "data_type"})

    @guard("edit")
    def add_data(
        self, id: int, data: float | int | str | List[float] | List[int] | List[str]
    ) -> None:
        _data = data if isinstance(data, list) else [data]

        if len(_data) == 0:
            return  # nothing to be done

        indexset = self.get_by_id(id=id)

        bulk_insert_enabled_data = [{"value": str(d)} for d in _data]
        try:
            self.session.execute(
                db.insert(IndexSetData).values(indexset__id=id),
                bulk_insert_enabled_data,
            )
        except db.IntegrityError as e:
            self.session.rollback()
            raise indexset.DataInvalid from e

        # Due to _data's limitation above, __name__ will always be that
        indexset._data_type = cast(
            Literal["float", "int", "str"], type(_data[0]).__name__
        )

        self.session.commit()

    @guard("edit")
    def remove_data(
        self,
        id: int,
        data: float | int | str | List[float] | List[int] | List[str],
        remove_dependent_data: bool = True,
    ) -> None:
        if not bool(data):
            return

        _data = [str(d) for d in data] if isinstance(data, list) else [str(data)]

        if remove_dependent_data:
            repos: dict[
                Literal["table", "parameter", "equation", "variable"],
                TableRepository
                | ParameterRepository
                | EquationRepository
                | VariableRepository,
            ] = {
                "table": self.backend.optimization.tables,
                "parameter": self.backend.optimization.parameters,
                "equation": self.backend.optimization.equations,
                "variable": self.backend.optimization.variables,
            }
            for kind, ids in self.find_all_linked_item_ids(id=id):
                self.remove_invalid_linked_data(
                    repo=repos[kind],
                    ids=ids,
                    indexset_name=self.get_by_id(id=id).name,
                    data=_data,
                )

        result = self.session.execute(
            db.delete(IndexSetData).where(
                IndexSetData.indexset__id == id,
                IndexSetData.value.in_(_data),
            )
        )

        if result.rowcount == 0:
            log.info(f"No data were removed! Are {data} registered to IndexSet {id}?")
            return None
        elif result.rowcount != len(_data):
            log.info(f"Not all items in `data` were registered for IndexSet {id}!")

        # Expire session to refresh IndexSets stored in it
        self.session.commit()

    def _find_linked_item_ids(
        self, id: int, item_kind: Literal["table", "parameter", "equation", "variable"]
    ) -> Sequence[int]:
        """Finds all items of `item_kind` linked to an IndexSet in `session.

        Parameters
        ----------
        id : int
            The id of the IndexSet we are looking for.
        item_kind : Literal["table", "parameter", "equation", "variable"]
            The type of item we are looking for.

        Returns
        -------
        list of int
            A list of ids of items of `item_kind` linked to the IndexSet.
        """

        column_clause, compare_column = self._linked_columns_lookup[item_kind]

        statement = db.select(column_clause).where(compare_column == id)

        return self.session.scalars(statement).all()

    @guard("view")
    def find_all_linked_item_ids(
        self, id: int
    ) -> Generator[
        tuple[Literal["table", "parameter", "equation", "variable"], Sequence[int]],
        Any,
        None,
    ]:
        """Finds all optimization items linked to an IndexSet.

        This is done by iterating over all possible kinds and yielding the ids of linked
        items.

        Parameters
        ----------
        id : int
            The id of the IndexSet we are looking for.

        Yields
        ------
        (item kind, list of ids)
            A tuple with the item kind being one of
            {'table', 'parameter', 'equation', 'variable'} and a list of integer ids
            representing linked items.
        """
        item_kinds: set[Literal["table", "parameter", "equation", "variable"]] = {
            "table",
            "parameter",
            "equation",
            "variable",
        }
        for kind in item_kinds:
            yield (kind, self._find_linked_item_ids(id=id, item_kind=kind))

    @guard("edit")
    def remove_invalid_linked_data(
        self,
        repo: TableRepository
        | ParameterRepository
        | EquationRepository
        | VariableRepository,
        ids: Sequence[int],
        indexset_name: str,
        data: List[str],
    ) -> None:
        """Remove invalid data from linked optimization items.

        Parameters
        ----------
        repo : TableRepository | ParameterRepository | EquationRepository | VariableRepository
            The repository including the linked items.
        ids : Sequence[int]
            The IDs of items linked to the IndexSet with `indexset_name`.
        indexset_name : str
            The name of the IndexSet from which data is to be removed.
        data : list[str]
            The data to be removed from `indexset_name` in str format.
        """  # noqa: E501
        for item in repo.list(id__in=ids):
            # Convert existing data for manipulation
            df = pd.DataFrame(item.data)

            if df.empty:
                continue  # nothing to do

            # Identify column/dimension names to target; item and name must be linked
            columns = utils._find_columns_linked_to_indexset(
                item=item, name=indexset_name
            )

            # Prepare data template to exclude
            invalid_data = {columns[i]: data for i in range(len(columns))}

            # Prepare stored data to remove
            # NOTE if any linked column is not of type str, this may be incorrect
            remove_data = df[df[columns].isin(invalid_data).any(axis=1)]

            repo.remove_data(item.id, remove_data)
