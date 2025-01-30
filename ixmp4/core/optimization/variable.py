from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from . import InitKwargs

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.core.base import BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import OptimizationVariable as VariableModel
from ixmp4.data.abstract import Run
from ixmp4.data.abstract.optimization import Column

from .base import Lister, Retriever, Tabulator


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
        """Adds data to the Variable."""
        self.backend.optimization.variables.add_data(
            variable_id=self._model.id, data=data
        )
        self._model = self.backend.optimization.variables.get(
            run_id=self._model.run__id, name=self._model.name
        )

    def remove_data(self) -> None:
        """Removes all data from the Variable."""
        self.backend.optimization.variables.remove_data(variable_id=self._model.id)
        self._model = self.backend.optimization.variables.get(
            run_id=self._model.run__id, name=self._model.name
        )

    @property
    def levels(self) -> list[float]:
        levels: list[float] = self._model.data.get("levels", [])
        return levels

    @property
    def marginals(self) -> list[float]:
        marginals: list[float] = self._model.data.get("marginals", [])
        return marginals

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
    def docs(self) -> str | None:
        try:
            return self.backend.optimization.variables.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self.backend.optimization.variables.docs.delete(self.id)
        else:
            self.backend.optimization.variables.docs.set(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self.backend.optimization.variables.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"


class VariableRepository(
    Retriever[Variable, VariableModel],
    Lister[Variable, VariableModel],
    Tabulator[Variable, VariableModel],
):
    def __init__(self, _run: Run, **kwargs: Unpack["InitKwargs"]) -> None:
        super().__init__(_run=_run, **kwargs)
        self._backend_repository = self.backend.optimization.variables
        self._model_type = Variable

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
