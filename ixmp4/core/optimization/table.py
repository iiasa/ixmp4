from datetime import datetime
from typing import Any

import pandas as pd
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.optimization.table.dto import Table as TableDto
from ixmp4.data.optimization.table.exceptions import (
    TableDataInvalid,
    TableDeletionPrevented,
    TableNotFound,
    TableNotUnique,
)
from ixmp4.data.optimization.table.filter import TableFilter
from ixmp4.data.optimization.table.service import TableService

from .base import BaseOptimizationFacadeObject, BaseOptimizationServiceFacade


class Table(BaseOptimizationFacadeObject[TableService, TableDto]):
    NotUnique = TableNotUnique
    NotFound = TableNotFound
    DeletionPrevented = TableDeletionPrevented
    DataInvalid = TableDataInvalid

    @property
    def id(self) -> int:
        return self._dto.id

    @property
    def name(self) -> str:
        return self._dto.name

    @property
    def run_id(self) -> int:
        return self._dto.run__id

    @property
    def data(self) -> dict[str, list[float] | list[int] | list[str]]:
        return self._dto.data

    @property
    def indexset_names(self) -> list[str] | None:
        return self._dto.indexset_names

    @property
    def column_names(self) -> list[str] | None:
        return self._dto.column_names

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

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

    def add_data(self, data: dict[str, Any] | pd.DataFrame) -> None:
        """Adds data to the Table."""
        self._run.require_lock()
        self._service.add_data(id=self._dto.id, data=data)
        self._refresh()

    def remove_data(self, data: dict[str, Any] | pd.DataFrame | None = None) -> None:
        """Removes data from the Table.

        If `data` is `None` (the default), remove all data. Otherwise, data must specify
        all indexed columns. All other keys/columns are ignored.
        """
        self._run.require_lock()
        self._service.remove_data(id=self._dto.id, data=data)
        self._refresh()

    def delete(self) -> None:
        self._run.require_lock()
        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> TableService:
        return backend.optimization.tables

    def __str__(self) -> str:
        return f"<Table {self.id} name={self.name}>"


class TableServiceFacade(
    BaseOptimizationServiceFacade[Table | int | str, TableDto, TableService]
):
    def _get_service(self, backend: Backend) -> TableService:
        return backend.optimization.tables

    def _get_item_id(self, key: Table | int | str) -> int:
        if isinstance(key, Table):
            id = key.id
        elif isinstance(key, int):
            id = key
        elif isinstance(key, str):
            dto = self._service.get(self._run.id, key)
            id = dto.id
        else:
            raise TypeError("Invalid argument: Must be `Table`, `int` or `str`.")

        return id

    def create(
        self,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Table:
        self._run.require_lock()
        dto = self._service.create(
            self._run.id, name, constrained_to_indexsets, column_names
        )
        return Table(self._backend, dto, run=self._run)

    def delete(self, x: Table | int | str) -> None:
        self._run.require_lock()
        id = self._get_item_id(x)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Table:
        dto = self._service.get(self._run.id, name)
        return Table(self._backend, dto, run=self._run)

    def list(self, **kwargs: Unpack[TableFilter]) -> list[Table]:
        tables = self._service.list(**kwargs)
        return [Table(self._backend, dto, run=self._run) for dto in tables]

    def tabulate(self, **kwargs: Unpack[TableFilter]) -> pd.DataFrame:
        kwargs["run__id"] = self._run.id
        return self._service.tabulate(**kwargs).drop(columns=["run__id"])
