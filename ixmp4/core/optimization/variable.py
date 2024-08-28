from datetime import datetime
from typing import Any, ClassVar, Iterable

import pandas as pd

from ixmp4.core.base import BaseFacade, BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import OptimizationVariable as VariableModel
from ixmp4.data.abstract import Run
from ixmp4.data.abstract.optimization import Column


class Variable(BaseModelFacade):
    _model: VariableModel
    NotFound: ClassVar = VariableModel.NotFound
    NotUnique: ClassVar = VariableModel.NotUnique

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
        """Adds data to an existing Variable."""
        self.backend.optimization.variables.add_data(
            variable_id=self._model.id, data=data
        )
        self._model.data = self.backend.optimization.variables.get(
            run_id=self._model.run__id, name=self._model.name
        ).data

    def remove_data(self) -> None:
        """Removes data from an existing Variable."""
        self.backend.optimization.variables.remove_data(variable_id=self._model.id)
        self._model.data = self.backend.optimization.variables.get(
            run_id=self._model.run__id, name=self._model.name
        ).data

    @property
    def levels(self) -> list:
        return self._model.data.get("levels", [])

    @property
    def marginals(self) -> list:
        return self._model.data.get("marginals", [])

    @property
    def constrained_to_indexsets(self) -> list[str]:
        return (
            [column.indexset.name for column in self._model.columns]
            if self._model.columns
            else []
        )

    @property
    def columns(self) -> list[Column] | None:
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
            return self.backend.optimization.variables.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description):
        if description is None:
            self.backend.optimization.variables.docs.delete(self.id)
        else:
            self.backend.optimization.variables.docs.set(self.id, description)

    @docs.deleter
    def docs(self):
        try:
            self.backend.optimization.variables.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"


class VariableRepository(BaseFacade):
    _run: Run

    def __init__(self, _run: Run, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._run = _run

    def create(
        self,
        name: str,
        constrained_to_indexsets: str | list[str] | None = None,
        column_names: list[str] | None = None,
    ) -> Variable:
        model = self.backend.optimization.variables.create(
            name=name,
            run_id=self._run.id,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )
        return Variable(_backend=self.backend, _model=model)

    def get(self, name: str) -> Variable:
        model = self.backend.optimization.variables.get(run_id=self._run.id, name=name)
        return Variable(_backend=self.backend, _model=model)

    def list(self, name: str | None = None) -> Iterable[Variable]:
        variables = self.backend.optimization.variables.list(
            run_id=self._run.id, name=name
        )
        return [
            Variable(
                _backend=self.backend,
                _model=i,
            )
            for i in variables
        ]

    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self.backend.optimization.variables.tabulate(
            run_id=self._run.id, name=name
        )
