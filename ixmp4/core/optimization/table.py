from datetime import datetime
from typing import Any, List

import pandas as pd
from typing_extensions import Unpack

from ixmp4.core.docs import DocsDescriptor
from ixmp4.data.backend import Backend
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
    Filter = TableFilter
    NotUnique = TableNotUnique
    NotFound = TableNotFound
    DeletionPrevented = TableDeletionPrevented
    DataInvalid = TableDataInvalid

    docs: DocsDescriptor[TableService, TableDto] = DocsDescriptor()
    """Optimization Table docs."""

    @property
    def id(self) -> int:
        """Unique id."""
        return self._dto.id

    @property
    def name(self) -> str:
        """Table name."""
        return self._dto.name

    @property
    def run_id(self) -> int:
        """Run id."""
        return self._dto.run__id

    @property
    def data(self) -> dict[str, list[float] | list[int] | list[str]]:
        """Raw data dictionary for this table."""
        return self._dto.data

    @property
    def indexset_names(self) -> list[str]:
        """Names of index sets constraining this table."""
        return self._dto.indexset_names

    @property
    def column_names(self) -> list[str] | None:
        """Names of columns for this table."""
        return self._dto.column_names

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

    def add_data(self, data: dict[str, Any] | pd.DataFrame) -> None:
        """Adds data to the Table.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        self._service.add_data(id=self._dto.id, data=data)
        self._refresh()

    def remove_data(self, data: dict[str, Any] | pd.DataFrame | None = None) -> None:
        """Removes data from the Table.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        If `data` is `None` (the default), remove all data. Otherwise, data must specify
        all indexed columns. All other keys/columns are ignored.

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        self._service.remove_data(id=self._dto.id, data=data)
        self._refresh()

    def delete(self) -> None:
        """Delete this Table.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> TableService:
        return backend.optimization.tables

    def __str__(self) -> str:
        return f"<Table name='{self.name}' id={self.id}>"

    def __repr__(self) -> str:
        return str(self)


class TableServiceFacade(
    BaseOptimizationServiceFacade[Table | int | str, TableDto, TableService]
):
    """Used to manage tables for a specific run."""

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
        constrained_to_indexsets: List[str],
        column_names: List[str] | None = None,
    ) -> Table:
        """Create a new table for this run.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        .. code:: python

            run.optimization.tables.create("CostTable", ["region", "year"])
            #> <Table 1 name='CostTable'>

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        dto = self._service.create(
            self._run.id, name, constrained_to_indexsets, column_names
        )
        return Table(self._backend, dto, run=self._run)

    def delete(self, x: Table | int | str) -> None:
        """Delete a table for the run.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        .. code:: python

            run.optimization.tables.delete("CostTable")

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        id = self._get_item_id(x)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Table:
        """Retrieve a table by name for this run.

        .. code:: python

            run.optimization.tables.get_by_name("CostTable")
            #> <Table 1 name='CostTable'>

        """
        dto = self._service.get(self._run.id, name)
        return Table(self._backend, dto, run=self._run)

    def list(self, **kwargs: Unpack[TableFilter]) -> list[Table]:
        r"""List tables for this run.

        .. code:: python

            run.optimization.tables.list()
            #> [<Table 1 name='CostTable'>]

        """
        kwargs["run__id"] = self._run.id
        tables = self._service.list(**kwargs)
        return [Table(self._backend, dto, run=self._run) for dto in tables]

    def tabulate(self, **kwargs: Unpack[TableFilter]) -> pd.DataFrame:
        r"""Tabulate tables for this run.

        .. code:: python

            run.optimization.tables.tabulate()
            #>    name    id
            # 0  CostTable 1

        """
        kwargs["run__id"] = self._run.id
        return self._service.tabulate(**kwargs).drop(columns=["run__id"])
