from collections.abc import Iterable
from datetime import datetime

import pandas as pd
from typing_extensions import Unpack

from ixmp4.rewrite.backend import Backend
from ixmp4.rewrite.core.base import BaseFacade
from ixmp4.rewrite.data.docs.repository import DocsNotFound
from ixmp4.rewrite.data.unit.dto import Unit as UnitModel
from ixmp4.rewrite.data.unit.filter import UnitFilter
from ixmp4.rewrite.data.unit.repositories import (
    UnitDeletionPrevented,
    UnitNotFound,
    UnitNotUnique,
)


class Unit(BaseFacade):
    dto: UnitModel
    NotUnique = UnitNotUnique
    NotFound = UnitNotFound
    DeletionPrevented = UnitDeletionPrevented

    def __init__(self, backend: Backend, dto: UnitModel) -> None:
        super().__init__(backend)
        self.dto = dto

    @property
    def id(self) -> int:
        return self.dto.id

    @property
    def name(self) -> str:
        return self.dto.name

    @property
    def created_at(self) -> datetime | None:
        return self.dto.created_at

    @property
    def created_by(self) -> str | None:
        return self.dto.created_by

    def delete(self) -> None:
        self._backend.units.delete_by_id(self.dto.id)

    @property
    def docs(self) -> str | None:
        try:
            return self._backend.units.get_docs(self.id).description
        except DocsNotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self._backend.units.delete_docs(self.id)
        else:
            self._backend.units.set_docs(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self._backend.units.delete_docs(self.id)
        # TODO: silently failing
        except DocsNotFound:
            return None

    def __str__(self) -> str:
        return f"<Unit {self.id} name={self.name}>"


class UnitRepository(BaseFacade):
    def create(self, name: str) -> Unit:
        if name != "" and name.strip() == "":
            raise ValueError("Using a space-only unit name is not allowed.")
        if name == "dimensionless":
            raise ValueError(
                "Unit name 'dimensionless' is reserved, use an empty string '' instead."
            )
        dto = self._backend.units.create(name)
        return Unit(backend=self._backend, dto=dto)

    def delete(self, x: Unit | int | str) -> None:
        if isinstance(x, Unit):
            id = x.id
        elif isinstance(x, int):
            id = x
        elif isinstance(x, str):
            dto = self._backend.units.get_by_name(x)
            id = dto.id
        else:
            raise TypeError("Invalid argument: Must be `Unit`, `int` or `str`.")

        self._backend.units.delete_by_id(id)

    def get(self, name: str) -> Unit:
        dto = self._backend.units.get_by_name(name)
        return Unit(backend=self._backend, dto=dto)

    def list(self, **kwargs: Unpack[UnitFilter]) -> list[Unit]:
        units = self._backend.units.list(**kwargs)
        return [Unit(backend=self._backend, dto=u) for u in units]

    def tabulate(self, **kwargs: Unpack[UnitFilter]) -> pd.DataFrame:
        return self._backend.units.tabulate(**kwargs)

    def _get_unit_id(self, unit: str) -> int | None:
        # NOTE leaving this check for users without mypy
        if isinstance(unit, str):
            obj = self._backend.units.get_by_name(unit)
            return obj.id
        else:
            raise ValueError(f"Invalid reference to unit: {unit}")

    def get_docs(self, name: str) -> str | None:
        unit_id = self._get_unit_id(name)
        if unit_id is None:
            return None
        try:
            return self._backend.units.get_docs(dimension__id=unit_id).description
        except DocsNotFound:
            return None

    def set_docs(self, name: str, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(name=name)
            return None
        unit_id = self._get_unit_id(name)
        if unit_id is None:
            return None
        return self._backend.units.set_docs(
            dimension__id=unit_id, description=description
        ).description

    def delete_docs(self, name: str) -> None:
        # TODO: this function is failing silently, which we should avoid
        unit_id = self._get_unit_id(name)
        if unit_id is None:
            return None
        try:
            self._backend.units.delete_docs(dimension__id=unit_id)
            return None
        except DocsNotFound:
            return None

    def list_docs(
        self, id: int | None = None, id__in: Iterable[int] | None = None
    ) -> Iterable[str]:
        return [
            item.description
            for item in self._backend.units.list_docs(
                dimension__id=id, dimension__id__in=id__in
            )
        ]
