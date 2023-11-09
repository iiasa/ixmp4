from datetime import datetime
from typing import ClassVar, Iterable

import pandas as pd

from ixmp4.core.base import BaseFacade, BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import Run, Unit
from ixmp4.data.abstract import Scalar as ScalarModel


class Scalar(BaseModelFacade):
    _model: ScalarModel
    _run: Run
    _unit: Unit
    NotFound: ClassVar = ScalarModel.NotFound
    NotUnique: ClassVar = ScalarModel.NotUnique

    def __init__(
        self,
        name: str,
        value: float,
        unit: Unit,
        _run: Run,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._run = _run
        self._value = value
        self._unit = unit
        if getattr(self, "_model", None) is None:
            try:
                self._model = self.backend.optimization.scalars.get(
                    run_id=self._run.id,
                    name=name,
                )
                # TODO: provide logging information if Scalar already exists
            except ScalarModel.NotFound:
                self._model = self.backend.optimization.scalars.create(
                    name=name,
                    value=value,
                    unit_id=self._unit.id,
                    run_id=self._run.id,
                )

    @property
    def id(self) -> int:
        return self._model.id

    @property
    def name(self) -> str:
        return self._model.name

    @property
    def value(self) -> float:
        """Associated value."""
        return self._value

    @property
    def unit(self):
        """Associated unit."""
        return self._unit

    @property
    def run(self):
        return self._run

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

    def create(self, name: str, value: float, unit_id: int) -> ScalarModel:
        try:
            return self.backend.optimization.scalars.create(
                name=name, value=value, unit_id=unit_id, run_id=self._run.id
            )
        except Scalar.NotUnique as e:
            raise Scalar.NotUnique(
                message=f"Scalar '{name}' already exists! Did you mean to call "
                "run.optimization.scalars.update()?"
            ) from e

    def update(self, name: str, value: float, unit_id: int) -> ScalarModel:
        return self.backend.optimization.scalars.update(
            name=name, value=value, unit_id=unit_id, run_id=self._run.id
        )

    def list(self, name: str | None = None) -> Iterable[Scalar]:
        scalars = self.backend.optimization.scalars.list(run_id=self._run.id, name=name)
        return [
            Scalar(
                _backend=self.backend,
                _model=i,
                _run=self._run,
                unit=self.backend.units.get_by_id(i.unit__id),
                name=i.name,
                value=i.value,
            )
            for i in scalars
        ]

    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self.backend.optimization.scalars.tabulate(name=name)
