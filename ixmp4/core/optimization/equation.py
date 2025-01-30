from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from . import InitKwargs

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.core.base import BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import Equation as EquationModel
from ixmp4.data.abstract import Run
from ixmp4.data.abstract.optimization import Column

from .base import Creator, Lister, Retriever, Tabulator


class Equation(BaseModelFacade):
    _model: EquationModel
    NotFound: ClassVar = EquationModel.NotFound
    NotUnique: ClassVar = EquationModel.NotUnique

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
        """Adds data to the Equation."""
        self.backend.optimization.equations.add_data(
            equation_id=self._model.id, data=data
        )
        self._model = self.backend.optimization.equations.get(
            run_id=self._model.run__id, name=self._model.name
        )

    def remove_data(self) -> None:
        """Removes all data from the Equation."""
        self.backend.optimization.equations.remove_data(equation_id=self._model.id)
        self._model = self.backend.optimization.equations.get(
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
    def docs(self) -> str | None:
        try:
            return self.backend.optimization.equations.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self.backend.optimization.equations.docs.delete(self.id)
        else:
            self.backend.optimization.equations.docs.set(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self.backend.optimization.equations.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Equation {self.id} name={self.name}>"


class EquationRepository(
    Creator[Equation, EquationModel],
    Retriever[Equation, EquationModel],
    Lister[Equation, EquationModel],
    Tabulator[Equation, EquationModel],
):
    def __init__(self, _run: Run, **kwargs: Unpack["InitKwargs"]) -> None:
        super().__init__(_run=_run, **kwargs)
        self._backend_repository = self.backend.optimization.equations
        self._model_type = Equation

    def create(
        self,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Equation:
        return super().create(
            name=name,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )
