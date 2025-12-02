from datetime import datetime
from typing import Any, cast

import pandas as pd
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.data.docs.repository import DocsNotFound
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
    def data(self) -> dict[str, list[float] | list[int] | list[str]]:
        return self.dto.data

    @property
    def levels(self) -> list[float]:
        return cast(list[float], self.dto.data.get("levels", []))

    @property
    def marginals(self) -> list[float]:
        return cast(list[float], self.dto.data.get("marginals", []))

    @property
    def indexset_names(self) -> list[str] | None:
        return self.dto.indexset_names

    @property
    def column_names(self) -> list[str] | None:
        return self.dto.column_names

    @property
    def created_at(self) -> datetime | None:
        return self.dto.created_at

    @property
    def created_by(self) -> str | None:
        return self.dto.created_by

    @property
    def docs(self) -> str | None:
        try:
            return self.service.get_docs(self.id).description
        except DocsNotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self.service.delete_docs(self.id)
        else:
            self.service.set_docs(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self.service.delete_docs(self.id)
        # TODO: silently failing
        except DocsNotFound:
            return None

    def add_data(self, data: dict[str, Any] | pd.DataFrame) -> None:
        """Adds data to the Equation."""
        self.run.require_lock()
        self.service.add_data(id=self.dto.id, data=data)
        self.refresh()

    def remove_data(self, data: dict[str, Any] | pd.DataFrame | None = None) -> None:
        """Removes data from the Equation.

        If `data` is `None` (the default), remove all data. Otherwise, data must specify
        all indexed columns. All other keys/columns are ignored.
        """
        self.run.require_lock()
        self.service.remove_data(id=self.dto.id, data=data)
        self.refresh()

    def delete(self) -> None:
        self.run.require_lock()
        self.service.delete_by_id(self.dto.id)

    def get_service(self, backend: Backend) -> EquationService:
        return backend.optimization.equations

    def __str__(self) -> str:
        return f"<Equation {self.id} name={self.name}>"


class EquationServiceFacade(
    BaseOptimizationServiceFacade[Equation | int | str, EquationDto, EquationService]
):
    def get_service(self, backend: Backend) -> EquationService:
        return backend.optimization.equations

    def get_item_id(self, key: Equation | int | str) -> int:
        if isinstance(key, Equation):
            id = key.id
        elif isinstance(key, int):
            id = key
        elif isinstance(key, str):
            dto = self.service.get(self.run.id, key)
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
        self.run.require_lock()
        dto = self.service.create(
            self.run.id,
            name,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )
        return Equation(self.backend, dto, run=self.run)

    def delete(self, x: Equation | int | str) -> None:
        self.run.require_lock()
        id = self.get_item_id(x)
        self.service.delete_by_id(id)

    def get_by_name(self, name: str) -> Equation:
        dto = self.service.get(self.run.id, name)
        return Equation(self.backend, dto, run=self.run)

    def list(self, **kwargs: Unpack[EquationFilter]) -> list[Equation]:
        equations = self.service.list(**kwargs)
        return [Equation(self.backend, dto, run=self.run) for dto in equations]

    def tabulate(self, **kwargs: Unpack[EquationFilter]) -> pd.DataFrame:
        return self.service.tabulate(run__id=self.run.id, **kwargs).drop(
            columns=["run__id"]
        )
