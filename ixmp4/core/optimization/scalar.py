import logging
from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd
from typing_extensions import Unpack

from ixmp4.core.docs import DocsDescriptor
from ixmp4.core.unit import Unit
from ixmp4.data.backend import Backend
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

logger = logging.getLogger(__name__)


class Scalar(BaseOptimizationFacadeObject[ScalarService, ScalarDto]):
    NotUnique = ScalarNotUnique
    NotFound = ScalarNotFound
    DeletionPrevented = ScalarDeletionPrevented

    docs: DocsDescriptor[ScalarService, ScalarDto] = DocsDescriptor()
    """Optimization Scalar docs."""

    def __init__(self, backend: Backend, dto: ScalarDto, run: "Run"):
        super().__init__(backend, dto, run)
        self.units = backend.units

    @property
    def id(self) -> int:
        """Unique id."""
        return self._dto.id

    @property
    def name(self) -> str:
        """Scalar name."""
        return self._dto.name

    @property
    def run_id(self) -> int:
        """Run id."""
        return self._dto.run__id

    @property
    def value(self) -> float:
        """Associated value."""
        return self._dto.value

    @value.setter
    def value(self, value: float) -> None:
        self._run.require_lock()
        self._service.update_by_id(self._dto.id, value=value)
        self._dto.value = value

    @property
    def unit(self) -> UnitDto:
        """Associated unit."""
        return self._dto.unit

    @unit.setter
    def unit(self, value: str | UnitDto) -> None:
        self._run.require_lock()
        if isinstance(value, str):
            unit = self.units.get_by_name(value)
        else:
            unit = value
        self._dto = self._service.update_by_id(self._dto.id, unit_name=unit.name)

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

    def update(
        self, value: int | float | None = None, unit_name: str | None = None
    ) -> None:
        """Updates data on the Scalar."""
        self._run.require_lock()
        if unit_name is None:
            logger.info(
                "Received `None` as unit name, using dimensionless "
                "scalar unit with blank name: ''."
            )
            dimensionless_unit = self.units.get_or_create(name="")
            unit_name = dimensionless_unit.name
        self._service.update_by_id(self._dto.id, value=value, unit_name=unit_name)
        self._refresh()

    def delete(self) -> None:
        self._run.require_lock()
        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> ScalarService:
        return backend.optimization.scalars

    def __str__(self) -> str:
        return f"<Scalar {self.id} name={self.name}>"

    def __repr__(self) -> str:
        return str(self)


class ScalarServiceFacade(
    BaseOptimizationServiceFacade[Scalar | int | str, ScalarDto, ScalarService]
):
    """Used to manage scalars for a specific run."""

    units: UnitService

    def __init__(self, backend: Backend, run: "Run"):
        super().__init__(backend, run)
        self.units = backend.units

    def _get_service(self, backend: Backend) -> ScalarService:
        return backend.optimization.scalars

    def _get_item_id(self, ref: Scalar | int | str) -> int:
        if isinstance(ref, Scalar):
            id = ref.id
        elif isinstance(ref, int):
            id = ref
        elif isinstance(ref, str):
            dto = self._service.get(self._run.id, ref)
            id = dto.id
        else:
            raise TypeError("Invalid argument: Must be `Scalar`, `int` or `str`.")

        return id

    def create(self, name: str, value: float, unit: str | Unit | None = None) -> Scalar:
        """Create a scalar for the run.

        .. code:: python

            run.optimization.scalars.create("discount", 0.05, unit="")
            #> <Scalar 1 name='discount'>

        """
        self._run.require_lock()
        if isinstance(unit, Unit):
            unit_name = unit.name
        elif isinstance(unit, str):
            unit_name = unit
        else:
            logger.info(
                "Received `None` as unit, using dimensionless "
                "scalar unit with blank name: ''."
            )
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
        """Delete a scalar for the run.

        .. code:: python

            run.optimization.scalars.delete("discount")

        """
        self._run.require_lock()
        id = self._get_item_id(x)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Scalar:
        """Retrieve a scalar by name for this run.

        .. code:: python

            run.optimization.scalars.get_by_name("discount")
            #> <Scalar 1 name='discount'>

        """
        dto = self._service.get(self._run.id, name)
        return Scalar(self._backend, dto, run=self._run)

    def list(self, **kwargs: Unpack[ScalarFilter]) -> list[Scalar]:
        r"""List scalars for this run.

        .. code:: python

            run.optimization.scalars.list()
            #> [<Scalar 1 name='discount'>]

        """
        scalars = self._service.list(**kwargs)
        return [Scalar(self._backend, dto, run=self._run) for dto in scalars]

    def tabulate(self, **kwargs: Unpack[ScalarFilter]) -> pd.DataFrame:
        r"""Tabulate scalars for this run.

        .. code:: python

            run.optimization.scalars.tabulate()
            #>    name    value  unit
            # 0  discount 0.05   ""

        """
        kwargs["run__id"] = self._run.id
        return self._service.tabulate(**kwargs).drop(columns=["run__id"])
