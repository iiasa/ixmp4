from datetime import datetime
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from . import InitKwargs

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.core.base import BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import IndexSet as IndexSetModel
from ixmp4.data.abstract import Run

from .base import Creator, Deleter, Lister, Retriever, Tabulator


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
    def data(self) -> list[float] | list[int] | list[str]:
        return self._model.data

    def add(
        self, data: float | int | str | list[float] | list[int] | list[str]
    ) -> None:
        """Adds data to an existing IndexSet."""
        self.backend.optimization.indexsets.add_data(id=self._model.id, data=data)
        self._model = self.backend.optimization.indexsets.get(
            run_id=self._model.run__id, name=self._model.name
        )

    def remove(
        self,
        data: float | int | str | list[float] | list[int] | list[str],
        remove_dependent_data: bool = True,
    ) -> None:
        """Removes data from an existing IndexSet."""
        self.backend.optimization.indexsets.remove_data(
            id=self._model.id, data=data, remove_dependent_data=remove_dependent_data
        )
        self._model = self.backend.optimization.indexsets.get(
            run_id=self._model.run__id, name=self._model.name
        )

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
    def docs(self) -> str | None:
        try:
            return self.backend.optimization.indexsets.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self.backend.optimization.indexsets.docs.delete(self.id)
        else:
            self.backend.optimization.indexsets.docs.set(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self.backend.optimization.indexsets.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<IndexSet {self.id} name={self.name}>"


class IndexSetRepository(
    Creator[IndexSet, IndexSetModel],
    Deleter[IndexSet, IndexSetModel],
    Retriever[IndexSet, IndexSetModel],
    Lister[IndexSet, IndexSetModel],
    Tabulator[IndexSet, IndexSetModel],
):
    def __init__(self, _run: Run, **kwargs: Unpack["InitKwargs"]) -> None:
        super().__init__(_run=_run, **kwargs)
        self._backend_repository = self.backend.optimization.indexsets
        self._model_type = IndexSet

    def create(self, name: str) -> IndexSet:
        return super().create(name=name)
