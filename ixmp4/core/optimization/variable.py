from datetime import datetime
from typing import Any, cast

import pandas as pd
from typing_extensions import Unpack

from ixmp4.core.docs import DocsDescriptor
from ixmp4.data.backend import Backend
from ixmp4.data.optimization.variable.dto import Variable as VariableDto
from ixmp4.data.optimization.variable.exceptions import (
    VariableDataInvalid,
    VariableDeletionPrevented,
    VariableNotFound,
    VariableNotUnique,
)
from ixmp4.data.optimization.variable.filter import VariableFilter
from ixmp4.data.optimization.variable.service import VariableService

from .base import BaseOptimizationFacadeObject, BaseOptimizationServiceFacade


class Variable(BaseOptimizationFacadeObject[VariableService, VariableDto]):
    Filter = VariableFilter
    NotUnique = VariableNotUnique
    NotFound = VariableNotFound
    DeletionPrevented = VariableDeletionPrevented
    DataInvalid = VariableDataInvalid

    docs: DocsDescriptor[VariableService, VariableDto] = DocsDescriptor()
    """Optimization Variable docs."""

    @property
    def id(self) -> int:
        """Unique id."""
        return self._dto.id

    @property
    def name(self) -> str:
        """Variable name."""
        return self._dto.name

    @property
    def run_id(self) -> int:
        """Run id."""
        return self._dto.run__id

    @property
    def data(self) -> dict[str, list[float] | list[int] | list[str]]:
        """Raw data dictionary for this variable."""
        return self._dto.data

    @property
    def levels(self) -> list[float]:
        """Level values associated with this variable."""
        return cast(list[float], self._dto.data.get("levels", []))

    @property
    def marginals(self) -> list[float]:
        """Marginal values for this variable."""
        return cast(list[float], self._dto.data.get("marginals", []))

    @property
    def indexset_names(self) -> list[str] | None:
        """Names of index sets constraining this variable."""
        return self._dto.indexset_names

    @property
    def column_names(self) -> list[str] | None:
        """Names of columns for this variable."""
        return self._dto.column_names

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

    def add_data(self, data: dict[str, Any] | pd.DataFrame) -> None:
        """Adds data to the Variable.

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
        """Removes data from the Variable.

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
        """Delete this Variable from the run.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        .. code:: python

            run.optimization.variables.delete("Production")

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> VariableService:
        return backend.optimization.variables

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"

    def __repr__(self) -> str:
        return str(self)


class VariableServiceFacade(
    BaseOptimizationServiceFacade[Variable | int | str, VariableDto, VariableService]
):
    """Used to manage optimization variables for a specific run."""

    def _get_service(self, backend: Backend) -> VariableService:
        return backend.optimization.variables

    def _get_item_id(self, key: Variable | int | str) -> int:
        if isinstance(key, Variable):
            id = key.id
        elif isinstance(key, int):
            id = key
        elif isinstance(key, str):
            dto = self._service.get(self._run.id, key)
            id = dto.id
        else:
            raise TypeError("Invalid argument: Must be `Variable`, `int` or `str`.")

        return id

    def create(
        self,
        name: str,
        constrained_to_indexsets: list[str] | None = None,
        column_names: list[str] | None = None,
    ) -> Variable:
        """Create a new optimization variable for the run.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        .. code:: python

            run.optimization.variables.create("Production")
            #> <Variable 1 name='Production'>

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        dto = self._service.create(
            self._run.id, name, constrained_to_indexsets, column_names
        )
        return Variable(self._backend, dto, run=self._run)

    def delete(self, x: Variable | int | str) -> None:
        """Delete a variable from the run.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        .. code:: python

            run.optimization.variables.delete("Production")

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        id = self._get_item_id(x)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Variable:
        """Get a variable by name for this run.

        .. code:: python

            run.optimization.variables.get_by_name("Production")
            #> <Variable 1 name='Production'>

        """
        dto = self._service.get(self._run.id, name)
        return Variable(self._backend, dto, run=self._run)

    def list(self, **kwargs: Unpack[VariableFilter]) -> list[Variable]:
        r"""List variables defined for this run.

        .. code:: python

            run.optimization.variables.list()
            #> [<Variable 1 name='Production'>]

        """
        variables = self._service.list(**kwargs)
        return [Variable(self._backend, dto, run=self._run) for dto in variables]

    def tabulate(self, **kwargs: Unpack[VariableFilter]) -> pd.DataFrame:
        r"""Tabulate variables for this run.

        .. code:: python

            run.optimization.variables.tabulate()
            #>    name    id
            # 0  Production 1

        """
        kwargs["run__id"] = self._run.id
        return self._service.tabulate(**kwargs).drop(columns=["run__id"])
