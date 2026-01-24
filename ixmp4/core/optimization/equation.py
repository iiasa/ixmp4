from datetime import datetime
from typing import Any, cast

import pandas as pd
from typing_extensions import Unpack

from ixmp4.core.docs import DocsDescriptor
from ixmp4.data.backend import Backend
from ixmp4.data.optimization.equation.dto import Equation as EquationDto
from ixmp4.data.optimization.equation.exceptions import (
    EquationDataInvalid,
    EquationDeletionPrevented,
    EquationNotFound,
    EquationNotUnique,
)
from ixmp4.data.optimization.equation.filter import EquationFilter
from ixmp4.data.optimization.equation.service import EquationService

from .base import BaseOptimizationFacadeObject, BaseOptimizationServiceFacade


class Equation(BaseOptimizationFacadeObject[EquationService, EquationDto]):
    NotUnique = EquationNotUnique
    NotFound = EquationNotFound
    DeletionPrevented = EquationDeletionPrevented
    DataInvalid = EquationDataInvalid

    docs: DocsDescriptor[EquationService, EquationDto] = DocsDescriptor()
    """Optimization Equation docs."""

    @property
    def id(self) -> int:
        """Unique id."""
        return self._dto.id

    @property
    def name(self) -> str:
        """Equation name."""
        return self._dto.name

    @property
    def run_id(self) -> int:
        """Run id."""
        return self._dto.run__id

    @property
    def data(self) -> dict[str, list[float] | list[int] | list[str]]:
        """Raw data dictionary for this equation."""
        return self._dto.data

    @property
    def levels(self) -> list[float]:
        """Level values associated with this equation."""
        return cast(list[float], self._dto.data.get("levels", []))

    @property
    def marginals(self) -> list[float]:
        """Marginal values for this equation."""
        return cast(list[float], self._dto.data.get("marginals", []))

    @property
    def indexset_names(self) -> list[str] | None:
        """Names of index sets constraining this equation."""
        return self._dto.indexset_names

    @property
    def column_names(self) -> list[str] | None:
        """Names of columns for this equation."""
        return self._dto.column_names

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

    def add_data(self, data: dict[str, Any] | pd.DataFrame) -> None:
        """Adds data to the Equation."""
        self._run.require_lock()
        self._service.add_data(id=self._dto.id, data=data)
        self._refresh()

    def remove_data(self, data: dict[str, Any] | pd.DataFrame | None = None) -> None:
        """Removes data from the Equation.

        If `data` is `None` (the default), remove all data. Otherwise, data must specify
        all indexed columns. All other keys/columns are ignored.
        """
        self._run.require_lock()
        self._service.remove_data(id=self._dto.id, data=data)
        self._refresh()

    def delete(self) -> None:
        self._run.require_lock()
        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> EquationService:
        return backend.optimization.equations

    def __str__(self) -> str:
        return f"<Equation {self.id} name={self.name}>"


class EquationServiceFacade(
    BaseOptimizationServiceFacade[Equation | int | str, EquationDto, EquationService]
):
    """Used to manage equations for a specific run."""

    def _get_service(self, backend: Backend) -> EquationService:
        return backend.optimization.equations

    def _get_item_id(self, key: Equation | int | str) -> int:
        if isinstance(key, Equation):
            id = key.id
        elif isinstance(key, int):
            id = key
        elif isinstance(key, str):
            dto = self._service.get(self._run.id, key)
            id = dto.id
        else:
            raise TypeError("Invalid argument: Must be `Equation`, `int` or `str`.")

        return id

    def create(
        self,
        name: str,
        constrained_to_indexsets: list[str] | None = None,
        column_names: list[str] | None = None,
    ) -> Equation:
        """Create a new equation for this run.

        .. code:: python

            run.optimization.equations.create("Balance")
            #> <Equation 1 name='Balance'>

        """
        self._run.require_lock()
        dto = self._service.create(
            self._run.id,
            name,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )
        return Equation(self._backend, dto, run=self._run)

    def delete(self, x: Equation | int | str) -> None:
        """Delete an equation from the run.

        .. code:: python

            run.optimization.equations.delete("Balance")

        """
        self._run.require_lock()
        id = self._get_item_id(x)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Equation:
        """Retrieve an equation by name for this run.

        .. code:: python

            run.optimization.equations.get_by_name("Balance")
            #> <Equation 1 name='Balance'>

        """
        dto = self._service.get(self._run.id, name)
        return Equation(self._backend, dto, run=self._run)

    def list(self, **kwargs: Unpack[EquationFilter]) -> list[Equation]:
        r"""List equations for this run.

        .. code:: python

            run.optimization.equations.list()
            #> [<Equation 1 name='Balance'>]

        """
        equations = self._service.list(**kwargs)
        return [Equation(self._backend, dto, run=self._run) for dto in equations]

    def tabulate(self, **kwargs: Unpack[EquationFilter]) -> pd.DataFrame:
        r"""Tabulate equations for this run.

        .. code:: python

            run.optimization.equations.tabulate()
            #>    name    id
            # 0  Balance 1

        """
        kwargs["run__id"] = self._run.id
        return self._service.tabulate(**kwargs).drop(columns=["run__id"])
