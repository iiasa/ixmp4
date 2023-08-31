from typing import Iterable, List

import pandas as pd

from ixmp4 import db
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import IndexSetDocsRepository
from .model import IndexSet


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
        indexset = IndexSet(run__id=run_id, name=name, **self.get_creation_info())
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
    def list(self, *args, **kwargs) -> Iterable[IndexSet]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    @guard("edit")
    def add_elements(
        self,
        indexset_id: int,
        elements: int | List[int | str] | str,
    ) -> None:
        indexset = self.get_by_id(id=indexset_id)
        if not isinstance(elements, list):
            elements = [elements]
        if indexset.elements is None:
            indexset.elements = elements
        else:
            indexset.elements = indexset.elements + elements

        self.session.add(indexset)
        self.session.commit()
