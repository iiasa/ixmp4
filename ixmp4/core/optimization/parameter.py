from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, cast

if TYPE_CHECKING:
    from . import InitKwargs

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.core.base import BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import Parameter as ParameterModel
from ixmp4.data.abstract import Run, Unit

from .base import Creator, Deleter, Lister, Retriever, Tabulator


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
    def data(self) -> dict[str, list[float] | list[int] | list[str]]:
        return self._model.data

    def add(self, data: dict[str, Any] | pd.DataFrame) -> None:
        """Adds data to the Parameter."""
        self.backend.optimization.parameters.add_data(id=self._model.id, data=data)
        self._model = self.backend.optimization.parameters.get(
            run_id=self._model.run__id, name=self._model.name
        )

    def remove(self, data: dict[str, Any] | pd.DataFrame) -> None:
        """Removes data from the Parameter.

        The data must specify all indexed columns. All other keys/columns are ignored.
        """
        self.backend.optimization.parameters.remove_data(id=self._model.id, data=data)
        self._model = self.backend.optimization.parameters.get(
            run_id=self._model.run__id, name=self._model.name
        )

    @property
    def values(self) -> list[float]:
        return cast(list[float], self._model.data.get("values", []))

    @property
    def units(self) -> list[Unit]:
        return cast(list[Unit], self._model.data.get("units", []))

    @property
    def indexset_names(self) -> list[str]:
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
            return self.backend.optimization.parameters.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self.backend.optimization.parameters.docs.delete(self.id)
        else:
            self.backend.optimization.parameters.docs.set(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self.backend.optimization.parameters.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Parameter {self.id} name={self.name}>"


class ParameterRepository(
    Creator[Parameter, ParameterModel],
    Deleter[Parameter, ParameterModel],
    Retriever[Parameter, ParameterModel],
    Lister[Parameter, ParameterModel],
    Tabulator[Parameter, ParameterModel],
):
    def __init__(self, _run: Run, **kwargs: Unpack["InitKwargs"]) -> None:
        super().__init__(_run=_run, **kwargs)
        self._backend_repository = self.backend.optimization.parameters
        self._model_type = Parameter

    def create(
        self,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Parameter:
        return super().create(
            name=name,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )
