from datetime import datetime

import pandas as pd
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.core.base import BaseDocsServiceFacade, BaseFacadeObject
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.unit.dto import Unit as UnitDto
from ixmp4.data.unit.exceptions import (
    UnitDeletionPrevented,
    UnitNotFound,
    UnitNotUnique,
)
from ixmp4.data.unit.filter import UnitFilter
from ixmp4.data.unit.service import UnitService


class Unit(BaseFacadeObject[UnitService, UnitDto]):
    dto: UnitDto
    NotUnique = UnitNotUnique
    NotFound = UnitNotFound
    DeletionPrevented = UnitDeletionPrevented

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

    def delete(self) -> None:
        self.service.delete_by_id(self.dto.id)

    def get_service(self, backend: Backend) -> UnitService:
        return backend.units

    def __str__(self) -> str:
        return f"<Unit {self.id} name={self.name}>"


class UnitServiceFacade(BaseDocsServiceFacade[Unit | int | str, Unit, UnitService]):
    def get_service(self, backend: Backend) -> UnitService:
        return backend.units

    def get_item_id(self, ref: Unit | int | str) -> int:
        if isinstance(ref, Unit):
            return ref.id
        elif isinstance(ref, int):
            return ref
        elif isinstance(ref, str):
            dto = self.service.get_by_name(ref)
            return dto.id
        else:
            raise ValueError(f"Invalid reference to unit: {ref}")

    def create(self, name: str) -> Unit:
        if name != "" and name.strip() == "":
            raise ValueError("Using a space-only unit name is not allowed.")
        if name == "dimensionless":
            raise ValueError(
                "Unit name 'dimensionless' is reserved, use an empty string '' instead."
            )
        dto = self.service.create(name)
        return Unit(self.backend, dto)

    def delete(self, ref: Unit | int | str) -> None:
        id = self.get_item_id(ref)
        self.service.delete_by_id(id)

    def get_by_name(self, name: str) -> Unit:
        dto = self.service.get_by_name(name)
        return Unit(self.backend, dto)

    def list(self, **kwargs: Unpack[UnitFilter]) -> list[Unit]:
        units = self.service.list(**kwargs)
        return [Unit(self.backend, dto) for dto in units]

    def tabulate(self, **kwargs: Unpack[UnitFilter]) -> pd.DataFrame:
        return self.service.tabulate(**kwargs)
