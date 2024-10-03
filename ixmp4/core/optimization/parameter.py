from datetime import datetime
from typing import Any, ClassVar, Iterable

import pandas as pd

from ixmp4.core.base import BaseFacade, BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import Parameter as ParameterModel
from ixmp4.data.abstract import Run
from ixmp4.data.abstract.optimization import Column


class Parameter(BaseModelFacade):
    _model: ParameterModel
    NotFound: ClassVar = ParameterModel.NotFound
    NotUnique: ClassVar = ParameterModel.NotUnique

    @property
    def id(self) -> int:
        return self._model.id

    @property
    def name(self) -> str:
        return self._model.name

    @property
    def run_id(self) -> int:
        return self._model.run__id

    @property
    def data(self) -> dict[str, Any]:
        return self._model.data

    def add(self, data: dict[str, Any] | pd.DataFrame) -> None:
        """Adds data to an existing Parameter."""
        self.backend.optimization.parameters.add_data(
            parameter_id=self._model.id, data=data
        )
        self._model.data = self.backend.optimization.parameters.get(
            run_id=self._model.run__id, name=self._model.name
        ).data

    @property
    def values(self) -> list:
        return self._model.data.get("values", [])

    @property
    def units(self) -> list:
        return self._model.data.get("units", [])

    @property
    def constrained_to_indexsets(self) -> list[str]:
        return [column.indexset.name for column in self._model.columns]

    @property
    def columns(self) -> list[Column]:
        return self._model.columns

    @property
    def created_at(self) -> datetime | None:
        return self._model.created_at

    @property
    def created_by(self) -> str | None:
        return self._model.created_by

    @property
    def docs(self):
        try:
            return self.backend.optimization.parameters.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description):
        if description is None:
            self.backend.optimization.parameters.docs.delete(self.id)
        else:
            self.backend.optimization.parameters.docs.set(self.id, description)

    @docs.deleter
    def docs(self):
        try:
            self.backend.optimization.parameters.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Parameter {self.id} name={self.name}>"


class ParameterRepository(BaseFacade):
    _run: Run

    def __init__(self, _run: Run, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._run = _run

    def create(
        self,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Parameter:
        model = self.backend.optimization.parameters.create(
            name=name,
            run_id=self._run.id,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )
        return Parameter(_backend=self.backend, _model=model)

    def get(self, name: str) -> Parameter:
        model = self.backend.optimization.parameters.get(run_id=self._run.id, name=name)
        return Parameter(_backend=self.backend, _model=model)

    def list(self, name: str | None = None) -> Iterable[Parameter]:
        parameters = self.backend.optimization.parameters.list(
            run_id=self._run.id, name=name
        )
        return [
            Parameter(
                _backend=self.backend,
                _model=i,
            )
            for i in parameters
        ]

    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self.backend.optimization.parameters.tabulate(
            run_id=self._run.id, name=name
        )
