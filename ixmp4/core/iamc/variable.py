from datetime import datetime

import pandas as pd
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.core.base import BaseDocsServiceFacade, BaseFacadeObject
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.iamc.variable.dto import Variable as VariableDto
from ixmp4.data.iamc.variable.exceptions import (
    VariableDeletionPrevented,
    VariableNotFound,
    VariableNotUnique,
)
from ixmp4.data.iamc.variable.filter import VariableFilter
from ixmp4.data.iamc.variable.service import VariableService


class Variable(BaseFacadeObject[VariableService, VariableDto]):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    DeletionPrevented = VariableDeletionPrevented

    @property
    def id(self) -> int:
        return self.dto.id

    @property
    def name(self) -> str:
        return self.dto.name

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

    def delete(self) -> None:
        """Deletes the variable from the database."""
        self.service.delete_by_id(self.dto.id)

    def _get_service(self, backend: Backend) -> VariableService:
        return backend.iamc.variables

    def __str__(self) -> str:
        return f"<Variable {self.id} name='{self.name}'>"


class VariableServiceFacade(
    BaseDocsServiceFacade[Variable | int | str, Variable, VariableService]
):
    def _get_service(self, backend: Backend) -> VariableService:
        return backend.iamc.variables

    def _get_item_id(self, ref: Variable | int | str) -> int:
        if isinstance(ref, Variable):
            return ref.id
        elif isinstance(ref, int):
            return ref
        elif isinstance(ref, str):
            dto = self._service.get_by_name(ref)
            return dto.id
        else:
            raise ValueError(f"Invalid reference to variable: {ref}")

    def create(self, name: str) -> Variable:
        dto = self._service.create(name)
        return Variable(self._backend, dto)

    def delete(self, ref: Variable | int | str) -> None:
        id = self._get_item_id(ref)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Variable:
        dto = self._service.get_by_name(name)
        return Variable(self._backend, dto)

    def list(self, **kwargs: Unpack[VariableFilter]) -> list[Variable]:
        units = self._service.list(**kwargs)
        return [Variable(self._backend, dto) for dto in units]

    def tabulate(self, **kwargs: Unpack[VariableFilter]) -> pd.DataFrame:
        return self._service.tabulate(**kwargs)
