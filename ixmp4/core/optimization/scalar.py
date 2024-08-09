from datetime import datetime
from typing import ClassVar, Iterable

import pandas as pd

from ixmp4.core.base import BaseFacade, BaseModelFacade
from ixmp4.core.unit import Unit
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import Run
from ixmp4.data.abstract import Scalar as ScalarModel


class Scalar(BaseModelFacade):
    _model: ScalarModel
    NotFound: ClassVar = ScalarModel.NotFound
    NotUnique: ClassVar = ScalarModel.NotUnique

    @property
    def id(self) -> int:
        return self._model.id

    @property
    def name(self) -> str:
        return self._model.name

    @property
    def value(self) -> float:
        """Associated value."""
        return self._model.value

    @value.setter
    def value(self, value: float):
        self._model.value = value
        self.backend.optimization.scalars.update(
            id=self._model.id,
            value=self._model.value,
            unit_id=self._model.unit.id,
        )

    @property
    def unit(self):
        """Associated unit."""
        return self._model.unit

    @unit.setter
    def unit(self, unit: str | Unit):
        if isinstance(unit, Unit):
            unit = unit
        else:
            unit_model = self.backend.units.get(unit)
            unit = Unit(_backend=self.backend, _model=unit_model)
        self._model = self.backend.optimization.scalars.update(
            id=self._model.id,
            value=self._model.value,
            unit_id=unit.id,
        )

    @property
    def run_id(self) -> int:
        return self._model.run__id

    @property
    def created_at(self) -> datetime | None:
        return self._model.created_at

    @property
    def created_by(self) -> str | None:
        return self._model.created_by

    @property
    def docs(self):
        try:
            return self.backend.optimization.scalars.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description):
        if description is None:
            self.backend.optimization.scalars.docs.delete(self.id)
        else:
            self.backend.optimization.scalars.docs.set(self.id, description)

    @docs.deleter
    def docs(self):
        try:
            self.backend.optimization.scalars.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Scalar {self.id} name={self.name}>"


class ScalarRepository(BaseFacade):
    _run: Run

    def __init__(self, _run: Run, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._run = _run

    def create(self, name: str, value: float, unit: str | Unit | None = None) -> Scalar:
        if isinstance(unit, Unit):
            unit_name = unit.name
        elif isinstance(unit, str):
            unit_name = unit
        else:
            # TODO: provide logging information about None-units being converted
            # if unit is None, assume that this is a dimensionless scalar (unit = "")
            dimensionless_unit = self.backend.units.create(name="")
            unit_name = dimensionless_unit.name

        try:
            model = self.backend.optimization.scalars.create(
                name=name, value=value, unit_name=unit_name, run_id=self._run.id
            )
        except Scalar.NotUnique as e:
            raise Scalar.NotUnique(
                message=f"Scalar '{name}' already exists! Did you mean to call "
                "run.optimization.scalars.update()?"
            ) from e
        return Scalar(_backend=self.backend, _model=model)

    def get(self, name: str) -> Scalar:
        model = self.backend.optimization.scalars.get(run_id=self._run.id, name=name)
        return Scalar(_backend=self.backend, _model=model)

    def list(self, name: str | None = None) -> Iterable[Scalar]:
        scalars = self.backend.optimization.scalars.list(run_id=self._run.id, name=name)
        return [
            Scalar(
                _backend=self.backend,
                _model=i,
            )
            for i in scalars
        ]

    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self.backend.optimization.scalars.tabulate(
            run_id=self._run.id, name=name
        )
