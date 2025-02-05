from typing import TYPE_CHECKING, List, Literal, cast

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

import logging

import pandas as pd

from ixmp4 import db
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import IndexSetDocsRepository
from .model import IndexSet, IndexSetData

log = logging.getLogger(__name__)


class IndexSetRepository(
    base.Creator[IndexSet],
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

    @guard("view")
    def list(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> list[IndexSet]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> pd.DataFrame:
        return super().tabulate(**kwargs).rename(columns={"_data_type": "data_type"})

    @guard("edit")
    def add_data(
        self,
        indexset_id: int,
        data: float | int | str | List[float] | List[int] | List[str],
    ) -> None:
        indexset = self.get_by_id(id=indexset_id)
        _data = data if isinstance(data, list) else [data]

        bulk_insert_enabled_data = [{"value": str(d)} for d in _data]
        try:
            self.session.execute(
                db.insert(IndexSetData).values(indexset__id=indexset_id),
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
        indexset_id: int,
        data: float | int | str | List[float] | List[int] | List[str],
    ) -> None:
        _data = [str(d) for d in data] if isinstance(data, list) else [str(data)]

        result = self.session.execute(
            db.delete(IndexSetData).where(
                IndexSetData.indexset__id == indexset_id,
                IndexSetData.value.in_(_data),
            )
        )

        if result.rowcount == 0:
            log.info(
                f"No data were removed! Are {data} registered to IndexSet "
                f"{indexset_id}?"
            )
            return None
        elif result.rowcount != len(_data):
            log.info(
                "Not all items in `data` were registered for IndexSet "
                f"{indexset_id}!"
            )

        # Expire session to refresh IndexSets stored in it
        self.session.commit()
