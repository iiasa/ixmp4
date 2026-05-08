from datetime import datetime
from typing import Any, cast

import pandas as pd
from typing_extensions import Unpack

from ixmp4.core.docs import DocsDescriptor
from ixmp4.data.backend import Backend
from ixmp4.data.optimization.parameter.dto import Parameter as ParameterDto
from ixmp4.data.optimization.parameter.exceptions import (
    ParameterDataInvalid,
    ParameterDeletionPrevented,
    ParameterNotFound,
    ParameterNotUnique,
)
from ixmp4.data.optimization.parameter.filter import ParameterFilter
from ixmp4.data.optimization.parameter.service import ParameterService

from .base import BaseOptimizationFacadeObject, BaseOptimizationServiceFacade


class Parameter(BaseOptimizationFacadeObject[ParameterService, ParameterDto]):
    Filter = ParameterFilter
    NotUnique = ParameterNotUnique
    NotFound = ParameterNotFound
    DeletionPrevented = ParameterDeletionPrevented
    DataInvalid = ParameterDataInvalid

    docs: DocsDescriptor[ParameterService, ParameterDto] = DocsDescriptor()
    """Optimization Parameter docs."""

    @property
    def id(self) -> int:
        """Unique id."""
        return self._dto.id

    @property
    def name(self) -> str:
        """Parameter name."""
        return self._dto.name

    @property
    def run_id(self) -> int:
        """Run id."""
        return self._dto.run__id

    @property
    def data(self) -> dict[str, list[float] | list[int] | list[str]]:
        """Raw data dictionary for this parameter."""
        return self._dto.data

    @property
    def values(self) -> list[float]:
        """List of numeric values for this parameter."""
        return cast(list[float], self._dto.data.get("values", []))

    @property
    def units(self) -> list[str]:
        """List of units associated with the parameter values."""
        return cast(list[str], self._dto.data.get("units", []))

    @property
    def indexset_names(self) -> list[str]:
        """Names of index sets constraining this parameter."""
        return self._dto.indexset_names

    @property
    def column_names(self) -> list[str] | None:
        """Names of columns for this parameter."""
        return self._dto.column_names

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

    def add_data(self, data: dict[str, Any] | pd.DataFrame) -> None:
        """Adds data to the Parameter.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        self._service.add_data(id=self._dto.id, data=data)
        self._dto = self._service.get(run_id=self._dto.run__id, name=self._dto.name)

    def remove_data(self, data: dict[str, Any] | pd.DataFrame | None = None) -> None:
        """Removes data from the Parameter.

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
        self._dto = self._service.get(run_id=self._dto.run__id, name=self._dto.name)

    def delete(self) -> None:
        """Delete this Parameter from the run.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> ParameterService:
        return backend.optimization.parameters

    def __str__(self) -> str:
        return f"<Parameter {self.id} name={self.name}>"

    def __repr__(self) -> str:
        return str(self)


class ParameterServiceFacade(
    BaseOptimizationServiceFacade[Parameter | int | str, ParameterDto, ParameterService]
):
    """Used to manage parameters for a specific run."""

    def _get_service(self, backend: Backend) -> ParameterService:
        return backend.optimization.parameters

    def _get_item_id(self, key: Parameter | int | str) -> int:
        if isinstance(key, Parameter):
            id = key.id
        elif isinstance(key, int):
            id = key
        elif isinstance(key, str):
            dto = self._service.get(self._run.id, key)
            id = dto.id
        else:
            raise TypeError("Invalid argument: Must be `Parameter`, `int` or `str`.")

        return id

    def create(
        self,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Parameter:
        """Create a new parameter for the run.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        .. code:: python

            run.optimization.parameters.create("Cost", ["Region", "Year"])
            #> <Parameter 1 name='Cost'>

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        dto = self._service.create(
            self._run.id, name, constrained_to_indexsets, column_names
        )
        return Parameter(self._backend, dto, run=self._run)

    def delete(self, x: Parameter | int | str) -> None:
        """Delete a parameter from the run.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        .. code:: python

            run.optimization.parameters.delete("Cost")

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        id = self._get_item_id(x)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Parameter:
        """Retrieve a parameter by name for this run.

        .. code:: python

            run.optimization.parameters.get_by_name("Cost")
            #> <Parameter 1 name='Cost'>

        """
        dto = self._service.get(self._run.id, name)
        return Parameter(self._backend, dto, run=self._run)

    def list(self, **kwargs: Unpack[ParameterFilter]) -> list[Parameter]:
        r"""List parameters for this run.

        .. code:: python

            run.optimization.parameters.list()
            #> [<Parameter 1 name='Cost'>]

        """
        parameters = self._service.list(**kwargs)
        return [Parameter(self._backend, dto, run=self._run) for dto in parameters]

    def tabulate(self, **kwargs: Unpack[ParameterFilter]) -> pd.DataFrame:
        r"""Tabulate parameters for this run.

        .. code:: python

            run.optimization.parameters.tabulate()
            #>    name    id
            # 0  Cost    1

        """
        kwargs["run__id"] = self._run.id
        return self._service.tabulate(**kwargs).drop(columns=["run__id"])
