from datetime import datetime
from typing import ClassVar

import pandas as pd

from ixmp4.core.base import BaseFacade, BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import IndexSet as IndexSetModel
from ixmp4.data.abstract import Run


class IndexSet(BaseModelFacade):
    _model: IndexSetModel
    NotFound: ClassVar = IndexSetModel.NotFound
    NotUnique: ClassVar = IndexSetModel.NotUnique

    @property
    def id(self) -> int:
        return self._model.id

    @property
    def name(self) -> str:
        return self._model.name

    @property
    def elements(self) -> list[float | int | str]:
        return self._model.elements

    def add(self, elements: float | int | list[float | int | str] | str) -> None:
        """Adds elements to an existing IndexSet."""
        self.backend.optimization.indexsets.add_elements(
            indexset_id=self._model.id, elements=elements
        )
        self._model.elements = self.backend.optimization.indexsets.get(
            run_id=self._model.run__id, name=self._model.name
        ).elements

    @property
    def run_id(self) -> int:
        return self._model.run__id

    @property
    def created_at(self) -> datetime | None:
        return self._model.created_at

    @property
    def created_by(self) -> str | None:
        return self._model.created_by

    @property
    def docs(self):
        try:
            return self.backend.optimization.indexsets.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description):
        if description is None:
            self.backend.optimization.indexsets.docs.delete(self.id)
        else:
            self.backend.optimization.indexsets.docs.set(self.id, description)

    @docs.deleter
    def docs(self):
        try:
            self.backend.optimization.indexsets.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<IndexSet {self.id} name={self.name}>"


class IndexSetRepository(BaseFacade):
    _run: Run

    def __init__(self, _run: Run, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._run = _run

    def create(self, name: str) -> IndexSet:
        indexset = self.backend.optimization.indexsets.create(
            run_id=self._run.id,
            name=name,
        )
        return IndexSet(_backend=self.backend, _model=indexset)

    def get(self, name: str) -> IndexSet:
        indexset = self.backend.optimization.indexsets.get(
            run_id=self._run.id, name=name
        )
        return IndexSet(_backend=self.backend, _model=indexset)

    def list(self, name: str | None = None) -> list[IndexSet]:
        indexsets = self.backend.optimization.indexsets.list(
            run_id=self._run.id, name=name
        )
        return [
            IndexSet(
                _backend=self.backend,
                _model=i,
            )
            for i in indexsets
        ]

    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self.backend.optimization.indexsets.tabulate(
            run_id=self._run.id, name=name
        )
