from datetime import datetime

import pandas as pd

from ixmp4.core.base import BaseFacade, BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import Unit as UnitModel


class Unit(BaseModelFacade):
    _model: UnitModel
    NotUnique = UnitModel.NotUnique
    NotFound = UnitModel.NotFound
    DeletionPrevented = UnitModel.DeletionPrevented

    @property
    def id(self) -> int:
        return self._model.id

    @property
    def name(self) -> str:
        return self._model.name

    @property
    def created_at(self) -> datetime | None:
        return self._model.created_at

    @property
    def created_by(self) -> str | None:
        return self._model.created_by

    def delete(self):
        self.backend.units.delete(self._model.id)

    @property
    def docs(self):
        try:
            return self.backend.units.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description):
        if description is None:
            self.backend.units.docs.delete(self.id)
        else:
            self.backend.units.docs.set(self.id, description)

    @docs.deleter
    def docs(self):
        try:
            self.backend.units.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Unit {self.id} name={self.name}>"


class UnitRepository(BaseFacade):
    def create(
        self,
        name: str,
    ) -> Unit:
        if name != "" and name.strip() == "":
            raise ValueError("Using a space-only unit name is not allowed.")
        if name == "dimensionless":
            raise ValueError(
                "Unit name 'dimensionless' is reserved, use an empty string '' instead."
            )
        model = self.backend.units.create(name)
        return Unit(_backend=self.backend, _model=model)

    def delete(self, x: Unit | int | str):
        if isinstance(x, Unit):
            id = x.id
        elif isinstance(x, int):
            id = x
        elif isinstance(x, str):
            model = self.backend.units.get(x)
            id = model.id
        else:
            raise TypeError("Invalid argument: Must be `Unit`, `int` or `str`.")

        self.backend.units.delete(id)

    def get(self, name: str) -> Unit:
        model = self.backend.units.get(name)
        return Unit(_backend=self.backend, _model=model)

    def list(self, name: str | None = None) -> list[Unit]:
        units = self.backend.units.list(name=name)
        return [Unit(_backend=self.backend, _model=u) for u in units]

    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self.backend.units.tabulate(name=name)

    def _get_unit_id(self, unit: str) -> int | None:
        if unit is None:
            return None
        elif isinstance(unit, str):
            obj = self.backend.units.get(unit)
            return obj.id
        else:
            raise ValueError(f"Invalid reference to unit: {unit}")

    def get_docs(self, name: str) -> str | None:
        unit_id = self._get_unit_id(name)
        if unit_id is None:
            return None
        try:
            return self.backend.units.docs.get(dimension_id=unit_id).description
        except DocsModel.NotFound:
            return None

    def set_docs(self, name: str, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(name=name)
            return None
        unit_id = self._get_unit_id(name)
        if unit_id is None:
            return None
        return self.backend.units.docs.set(
            dimension_id=unit_id, description=description
        ).description

    def delete_docs(self, name: str) -> None:
        # TODO: this function is failing silently, which we should avoid
        unit_id = self._get_unit_id(name)
        if unit_id is None:
            return None
        try:
            self.backend.units.docs.delete(dimension_id=unit_id)
            return None
        except DocsModel.NotFound:
            return None
