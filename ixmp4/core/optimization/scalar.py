from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.core.unit import Unit
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.optimization.scalar.dto import Scalar as ScalarDto
from ixmp4.data.optimization.scalar.exceptions import (
    ScalarDeletionPrevented,
    ScalarNotFound,
    ScalarNotUnique,
)
from ixmp4.data.optimization.scalar.filter import ScalarFilter
from ixmp4.data.optimization.scalar.service import ScalarService
from ixmp4.data.unit.dto import Unit as UnitDto
from ixmp4.data.unit.service import UnitService

from .base import BaseOptimizationFacadeObject, BaseOptimizationServiceFacade

if TYPE_CHECKING:
    from ixmp4.core.run import Run


class Scalar(BaseOptimizationFacadeObject[ScalarService, ScalarDto]):
    NotUnique = ScalarNotUnique
    NotFound = ScalarNotFound
    DeletionPrevented = ScalarDeletionPrevented

    def __init__(self, backend: Backend, dto: ScalarDto, run: "Run"):
        super().__init__(backend, dto, run)
        self.units = backend.units

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
    def value(self) -> float:
        """Associated value."""
        return self.dto.value

    @value.setter
    def value(self, value: float) -> None:
        self._run.require_lock()
        self.service.update(self.dto.id, value)
        self.dto.value = value

    @property
    def unit(self) -> UnitDto:
        """Associated unit."""
        return self.dto.unit

    @unit.setter
    def unit(self, value: str | UnitDto) -> None:
        self._run.require_lock()
        if isinstance(value, str):
            unit = self.units.get_by_name(value)
        else:
            unit = value
        self.dto = self.service.update_by_id(self.dto.id, unit_name=unit.name)

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

    def update(
        self, value: int | float | None = None, unit_name: str | None = None
    ) -> None:
        """Adds data to the Scalar."""
        self._run.require_lock()
        self.service.update_by_id(self.dto.id, value=value, unit_name=unit_name)
        self.refresh()

    def delete(self) -> None:
        self._run.require_lock()
        self.service.delete_by_id(self.dto.id)

    def _get_service(self, backend: Backend) -> ScalarService:
        return backend.optimization.scalars

    def __str__(self) -> str:
        return f"<Scalar {self.id} name={self.name}>"


class ScalarServiceFacade(
    BaseOptimizationServiceFacade[Scalar | int | str, ScalarDto, ScalarService]
):
    units: UnitService

    def __init__(self, backend: Backend, run: "Run"):
        super().__init__(backend, run)
        self.units = backend.units

    def _get_service(self, backend: Backend) -> ScalarService:
        return backend.optimization.scalars

    def _get_item_id(self, key: Scalar | int | str) -> int:
        if isinstance(key, Scalar):
            id = key.id
        elif isinstance(key, int):
            id = key
        elif isinstance(key, str):
            dto = self._service.get(self._run.id, key)
            id = dto.id
        else:
            raise TypeError("Invalid argument: Must be `Scalar`, `int` or `str`.")

        return id

    def create(self, name: str, value: float, unit: str | Unit | None = None) -> Scalar:
        self._run.require_lock()
        if isinstance(unit, Unit):
            unit_name = unit.name
        elif isinstance(unit, str):
            unit_name = unit
        else:
            # TODO: provide logging information about None-units being converted
            # if unit is None, assume that this is a dimensionless scalar (unit = "")
            dimensionless_unit = self.units.get_or_create(name="")
            unit_name = dimensionless_unit.name

        try:
            dto = self._service.create(
                self._run.id, name, value=value, unit_name=unit_name
            )
        except Scalar.NotUnique as e:
            raise Scalar.NotUnique(
                message=f"Scalar '{name}' already exists! "
                "Did you mean to call Scalar.update()?"
            ) from e
        return Scalar(self._backend, dto, run=self._run)

    def delete(self, x: Scalar | int | str) -> None:
        self._run.require_lock()
        id = self._get_item_id(x)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Scalar:
        dto = self._service.get(self._run.id, name)
        return Scalar(self._backend, dto, run=self._run)

    def list(self, **kwargs: Unpack[ScalarFilter]) -> list[Scalar]:
        scalars = self._service.list(**kwargs)
        return [Scalar(self._backend, dto, run=self._run) for dto in scalars]

    def tabulate(self, **kwargs: Unpack[ScalarFilter]) -> pd.DataFrame:
        return self._service.tabulate(run__id=self._run.id, **kwargs).drop(
            columns=["run__id"]
        )
