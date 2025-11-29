from datetime import datetime
from typing import Any, cast

import pandas as pd
from typing_extensions import Unpack

from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.optimization.parameter.dto import Parameter as ParameterDto
from ixmp4.data.optimization.parameter.filter import ParameterFilter
from ixmp4.data.optimization.parameter.repositories import (
    ParameterDataInvalid,
    ParameterDeletionPrevented,
    ParameterNotFound,
    ParameterNotUnique,
)
from ixmp4.data.optimization.parameter.service import ParameterService

from .base import BaseOptimizationFacadeObject, BaseOptimizationServiceFacade


class Parameter(BaseOptimizationFacadeObject[ParameterService, ParameterDto]):
    NotUnique = ParameterNotUnique
    NotFound = ParameterNotFound
    DeletionPrevented = ParameterDeletionPrevented
    DataInvalid = ParameterDataInvalid

    @property
    def id(self) -> int:
        return self.dto.id

    @property
    def name(self) -> str:
        return self.dto.name

    @property
    def run_id(self) -> int:
        return self.dto.run__id

    @property
    def data(self) -> dict[str, list[float] | list[int] | list[str]]:
        return self.dto.data

    @property
    def values(self) -> list[float]:
        return cast(list[float], self.dto.data.get("values", []))

    @property
    def units(self) -> list[str]:
        return cast(list[str], self.dto.data.get("units", []))

    @property
    def indexset_names(self) -> list[str] | None:
        return self.dto.indexset_names

    @property
    def column_names(self) -> list[str] | None:
        return self.dto.column_names

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

    def add_data(self, data: dict[str, Any] | pd.DataFrame) -> None:
        """Adds data to the Parameter."""
        self.run.require_lock()
        self.service.add_data(id=self._model.id, data=data)
        self._model = self.service.get(
            run_id=self._model.run__id, name=self._model.name
        )

    def remove_data(self, data: dict[str, Any] | pd.DataFrame | None = None) -> None:
        """Removes data from the Parameter.

        If `data` is `None` (the default), remove all data. Otherwise, data must specify
        all indexed columns. All other keys/columns are ignored.
        """
        self.run.require_lock()
        self.service.remove_data(id=self._model.id, data=data)
        self._model = self.service.get(
            run_id=self._model.run__id, name=self._model.name
        )

    def delete(self) -> None:
        self.service.delete_by_id(self.dto.id)

    def __str__(self) -> str:
        return f"<Parameter {self.id} name={self.name}>"


class ParameterServiceFacade(
    BaseOptimizationServiceFacade[Parameter | int | str, ParameterDto, ParameterService]
):
    def get_item_id(self, key: Parameter | int | str) -> int:
        if isinstance(key, Parameter):
            id = key.id
        elif isinstance(key, int):
            id = key
        elif isinstance(key, str):
            dto = self.service.get(self.run.id, key)
            id = dto.id
        else:
            raise TypeError("Invalid argument: Must be `Parameter`, `int` or `str`.")

        return id

    def create(
        self,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Parameter:
        self.run.require_lock()
        dto = self.service.create(
            self.run.id, name, constrained_to_indexsets, column_names
        )
        return Parameter(self.service, dto)

    def delete(self, x: Parameter | int | str) -> None:
        self.run.require_lock()
        id = self.get_item_id(x)
        self.service.delete_by_id(id)

    def get_by_name(self, name: str) -> Parameter:
        dto = self.service.get(self.run.id, name)
        return Parameter(self.service, dto)

    def list(self, **kwargs: Unpack[ParameterFilter]) -> list[Parameter]:
        parameters = self.service.list(**kwargs)
        return [Parameter(self.service, dto) for dto in parameters]

    def tabulate(self, **kwargs: Unpack[ParameterFilter]) -> pd.DataFrame:
        return self.service.tabulate(**kwargs)
