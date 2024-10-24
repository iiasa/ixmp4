from typing import List

import pandas as pd

from ixmp4 import db
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import IndexSetDocsRepository
from .model import IndexSet, IndexSetData


class IndexSetRepository(
    base.Creator[IndexSet],
    base.Retriever[IndexSet],
    base.Enumerator[IndexSet],
    abstract.IndexSetRepository,
):
    model_class = IndexSet

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = IndexSetDocsRepository(*args, **kwargs)

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
    def create(self, run_id: int, name: str, **kwargs) -> IndexSet:
        return super().create(run_id=run_id, name=name, **kwargs)

    @guard("view")
    def list(self, *args, **kwargs) -> list[IndexSet]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        result = super().tabulate(*args, **kwargs).drop(labels="data_type", axis=1)
        result.insert(
            loc=0,
            column="data",
            value=[self.get_by_id(id=indexset_id).data for indexset_id in result.id],
        )
        return result

    @guard("edit")
    def add_data(
        self,
        indexset_id: int,
        data: float | int | List[float | int | str] | str,
    ) -> None:
        indexset = self.get_by_id(id=indexset_id)
        if not isinstance(data, list):
            data = [data]
        # TODO If adding rows one by one is too expensive, look into executemany pattern
        for value in data:
            self.session.add(
                IndexSetData(indexset=indexset, indexset__id=indexset_id, value=value)
            )

        try:
            self.session.flush()
        except db.IntegrityError as e:
            self.session.rollback()
            raise indexset.DataInvalid from e

        indexset.data_type = type(data[0]).__name__

        self.session.add(indexset)
        self.session.commit()
