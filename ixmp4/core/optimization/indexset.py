from datetime import datetime

import pandas as pd
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.optimization.indexset.dto import IndexSet as IndexSetDto
from ixmp4.data.optimization.indexset.exceptions import (
    IndexSetDataInvalid,
    IndexSetDeletionPrevented,
    IndexSetNotFound,
    IndexSetNotUnique,
)
from ixmp4.data.optimization.indexset.filter import IndexSetFilter
from ixmp4.data.optimization.indexset.service import IndexSetService

from .base import BaseOptimizationFacadeObject, BaseOptimizationServiceFacade


class IndexSet(BaseOptimizationFacadeObject[IndexSetService, IndexSetDto]):
    NotUnique = IndexSetNotUnique
    NotFound = IndexSetNotFound
    DeletionPrevented = IndexSetDeletionPrevented
    DataInvalid = IndexSetDataInvalid

    @property
    def id(self) -> int:
        return self.dto.id

    @property
    def name(self) -> str:
        return self.dto.name

    @property
    def run_id(self) -> int:
        return self.dto.run__id

    @property
    def data(self) -> list[float] | list[int] | list[str]:
        return self.dto.data

    @property
    def created_at(self) -> datetime | None:
        return self.dto.created_at

    @property
    def created_by(self) -> str | None:
        return self.dto.created_by

    @property
    def docs(self) -> str | None:
        try:
            return self._service.get_docs(self.id).description
        except DocsNotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self._service.delete_docs(self.id)
        else:
            self._service.set_docs(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self._service.delete_docs(self.id)
        # TODO: silently failing
        except DocsNotFound:
            return None

    def add_data(
        self, data: float | int | str | list[float] | list[int] | list[str]
    ) -> None:
        """Adds data to the IndexSet."""
        self._run.require_lock()
        self._service.add_data(id=self.dto.id, data=data)
        self.refresh()

    def remove_data(
        self, data: float | int | str | list[float] | list[int] | list[str]
    ) -> None:
        """Removes data from the IndexSet.

        If `data` is `None` (the default), remove all data. Otherwise, data must specify
        all indexed columns. All other keys/columns are ignored.
        """
        self._run.require_lock()
        self._service.remove_data(self.dto.id, data)
        self.refresh()

    def delete(self) -> None:
        self._run.require_lock()
        self._service.delete_by_id(self.dto.id)

    def _get_service(self, backend: Backend) -> IndexSetService:
        return backend.optimization.indexsets

    def __str__(self) -> str:
        return f"<IndexSet {self.id} name={self.name}>"


class IndexSetServiceFacade(
    BaseOptimizationServiceFacade[IndexSet | int | str, IndexSetDto, IndexSetService]
):
    def _get_service(self, backend: Backend) -> IndexSetService:
        return backend.optimization.indexsets

    def _get_item_id(self, key: IndexSet | int | str) -> int:
        if isinstance(key, IndexSet):
            id = key.id
        elif isinstance(key, int):
            id = key
        elif isinstance(key, str):
            dto = self._service.get(self._run.id, key)
            id = dto.id
        else:
            raise TypeError("Invalid argument: Must be `IndexSet`, `int` or `str`.")

        return id

    def create(self, name: str) -> IndexSet:
        self._run.require_lock()
        dto = self._service.create(self._run.id, name)
        return IndexSet(self._backend, dto, run=self._run)

    def delete(self, x: IndexSet | int | str) -> None:
        self._run.require_lock()
        id = self._get_item_id(x)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> IndexSet:
        dto = self._service.get(self._run.id, name)
        return IndexSet(self._backend, dto, run=self._run)

    def list(self, **kwargs: Unpack[IndexSetFilter]) -> list[IndexSet]:
        indexsets = self._service.list(**kwargs)
        return [IndexSet(self._backend, dto, run=self._run) for dto in indexsets]

    def tabulate(self, **kwargs: Unpack[IndexSetFilter]) -> pd.DataFrame:
        return self._service.tabulate(run__id=self._run.id, **kwargs).drop(
            columns=["run__id"]
        )
