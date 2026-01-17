from datetime import datetime

import pandas as pd
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.core.docs import DocsDescriptor
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

    docs: DocsDescriptor[IndexSetService, IndexSetDto] = DocsDescriptor()
    """Optimization IndexSet docs."""

    @property
    def id(self) -> int:
        """Unique id."""
        return self._dto.id

    @property
    def name(self) -> str:
        """IndexSet name."""
        return self._dto.name

    @property
    def run_id(self) -> int:
        """Run id."""
        return self._dto.run__id

    @property
    def data(self) -> list[float] | list[int] | list[str]:
        """Data contained in the index set."""
        return self._dto.data

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

    def add_data(
        self, data: float | int | str | list[float] | list[int] | list[str]
    ) -> None:
        """Adds data to the IndexSet."""
        self._run.require_lock()
        self._service.add_data(id=self._dto.id, data=data)
        self._refresh()

    def remove_data(
        self, data: float | int | str | list[float] | list[int] | list[str]
    ) -> None:
        """Removes data from the IndexSet.

        If `data` is `None` (the default), remove all data. Otherwise, data must specify
        all indexed columns. All other keys/columns are ignored.
        """
        self._run.require_lock()
        self._service.remove_data(self._dto.id, data)
        self._refresh()

    def delete(self) -> None:
        self._run.require_lock()
        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> IndexSetService:
        return backend.optimization.indexsets

    def __str__(self) -> str:
        return f"<IndexSet {self.id} name={self.name}>"

    def __repr__(self) -> str:
        return str(self)


class IndexSetServiceFacade(
    BaseOptimizationServiceFacade[IndexSet | int | str, IndexSetDto, IndexSetService]
):
    """Used to manage index sets for a specific run."""

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
        """Create a new index set for this run.

        .. code:: python

            run.optimization.indexsets.create("Years")
            #> <IndexSet 1 name='Years'>

        """
        self._run.require_lock()
        dto = self._service.create(self._run.id, name)
        return IndexSet(self._backend, dto, run=self._run)

    def delete(self, x: IndexSet | int | str) -> None:
        """Delete an index set for the run.

        .. code:: python

            run.optimization.indexsets.delete("Years")

        """
        self._run.require_lock()
        id = self._get_item_id(x)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> IndexSet:
        """Retrieve an index set by name for this run.

        .. code:: python

            run.optimization.indexsets.get_by_name("Years")
            #> <IndexSet 1 name='Years'>

        """
        dto = self._service.get(self._run.id, name)
        return IndexSet(self._backend, dto, run=self._run)

    def list(self, **kwargs: Unpack[IndexSetFilter]) -> list[IndexSet]:
        r"""List index sets for this run.

        .. code:: python

            run.optimization.indexsets.list()
            #> [<IndexSet 1 name='Years'>]

        """
        indexsets = self._service.list(**kwargs)
        return [IndexSet(self._backend, dto, run=self._run) for dto in indexsets]

    def tabulate(self, **kwargs: Unpack[IndexSetFilter]) -> pd.DataFrame:
        r"""Tabulate index sets for this run.

        .. code:: python

            run.optimization.indexsets.tabulate()
            #>    name    id
            # 0  Years   1

        """
        kwargs["run__id"] = self._run.id
        return self._service.tabulate(**kwargs).drop(columns=["run__id"])
