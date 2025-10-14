from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.data.abstract import Equation as EquationModel
from ixmp4.rewrite.data.docs.repository import DocsNotFound

from .base import (
    Creator,
    Deleter,
    Lister,
    OptimizationBaseFacade,
    Retriever,
    Tabulator,
)

if TYPE_CHECKING:
    from ixmp4.rewrite.core.run import Run

    from . import InitKwargs


class Equation(OptimizationBaseFacade):
    _model: EquationModel

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
    def data(self) -> dict[str, list[float] | list[int] | list[str]]:
        return self._model.data

    def add(self, data: dict[str, Any] | pd.DataFrame) -> None:
        """Adds data to the Equation."""
        self._run.require_lock()
        self._backend.optimization.equations.add_data(id=self._model.id, data=data)
        self._model = self._backend.optimization.equations.get(
            run_id=self._model.run__id, name=self._model.name
        )

    # TODO Make name of these functions consistent across items
    def remove_data(self, data: dict[str, Any] | pd.DataFrame | None = None) -> None:
        """Removes data from the Equation.

        If `data` is `None` (the default), remove all data. Otherwise, data must specify
        all indexed columns. All other keys/columns are ignored.
        """
        self._run.require_lock()
        self._backend.optimization.equations.remove_data(id=self._model.id, data=data)
        self._model = self._backend.optimization.equations.get(
            run_id=self._model.run__id, name=self._model.name
        )

    @property
    def levels(self) -> list[float]:
        return cast(list[float], self._model.data.get("levels", []))

    @property
    def marginals(self) -> list[float]:
        return cast(list[float], self._model.data.get("marginals", []))

    @property
    def indexset_names(self) -> list[str] | None:
        return self._model.indexset_names

    @property
    def column_names(self) -> list[str] | None:
        return self._model.column_names

    @property
    def created_at(self) -> datetime | None:
        return self._model.created_at

    @property
    def created_by(self) -> str | None:
        return self._model.created_by

    @property
    def docs(self) -> str | None:
        try:
            return self._backend.optimization.equations.get_docs(self.id).description
        except DocsNotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self._backend.optimization.equations.delete_docs(self.id)
        else:
            self._backend.optimization.equations.set_docs(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self._backend.optimization.equations.delete_docs(self.id)
        # TODO: silently failing
        except DocsNotFound:
            return None

    def __str__(self) -> str:
        return f"<Equation {self.id} name={self.name}>"


class EquationRepository(
    Creator[Equation, EquationModel],
    Deleter[Equation, EquationModel],
    Retriever[Equation, EquationModel],
    Lister[Equation, EquationModel],
    Tabulator[Equation, EquationModel],
):
    def __init__(self, _run: "Run", **kwargs: Unpack["InitKwargs"]) -> None:
        super().__init__(_run=_run, **kwargs)
        self._backend_repository = self._backend.optimization.equations
        self._model_type = Equation

    def create(
        self,
        name: str,
        constrained_to_indexsets: list[str] | None = None,
        column_names: list[str] | None = None,
    ) -> Equation:
        return super().create(
            name=name,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )
