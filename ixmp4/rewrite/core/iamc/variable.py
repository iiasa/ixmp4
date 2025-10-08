from collections.abc import Iterable
from datetime import datetime

import pandas as pd

from ixmp4.rewrite.backend import Backend
from ixmp4.rewrite.core.base import BaseFacade
from ixmp4.rewrite.data.docs.dto import Docs as DocsModel
from ixmp4.rewrite.data.iamc.variable.dto import Variable as VariableModel


class Variable(BaseFacade):
    dto: VariableModel

    def __init__(self, backend: Backend, dto: VariableModel) -> None:
        super().__init__(backend)
        self.dto = dto

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
            return self._backend.iamc.variables.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self._backend.iamc.variables.docs.delete(self.id)
        else:
            self._backend.iamc.variables.docs.set(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self._backend.iamc.variables.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"


class VariableRepository(BaseFacade):
    def create(self, name: str) -> Variable:
        model = self._backend.iamc.variables.create(name)
        return Variable(backend=self._backend, dto=model)

    def get(self, name: str) -> Variable:
        model = self._backend.iamc.variables.get(name)
        return Variable(backend=self._backend, dto=model)

    def list(self, name: str | None = None) -> list[Variable]:
        variables = self._backend.iamc.variables.list(name=name)
        return [Variable(backend=self._backend, dto=v) for v in variables]

    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self._backend.iamc.variables.tabulate(name=name)

    def _get_variable_id(self, variable: str) -> int | None:
        # NOTE leaving this check for users without mypy
        if isinstance(variable, str):
            obj = self._backend.iamc.variables.get(variable)
            return obj.id
        else:
            raise ValueError(f"Invalid reference to variable: {variable}")

    def get_docs(self, name: str) -> str | None:
        variable_id = self._get_variable_id(name)
        if variable_id is None:
            return None
        try:
            return self._backend.iamc.variables.docs.get(
                dimension_id=variable_id
            ).description
        except DocsModel.NotFound:
            return None

    def set_docs(self, name: str, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(name=name)
            return None
        variable_id = self._get_variable_id(name)
        if variable_id is None:
            return None
        return self._backend.iamc.variables.docs.set(
            dimension_id=variable_id, description=description
        ).description

    def delete_docs(self, name: str) -> None:
        # TODO: this function is failing silently, which we should avoid
        variable_id = self._get_variable_id(name)
        if variable_id is None:
            return None
        try:
            self._backend.iamc.variables.docs.delete(dimension_id=variable_id)
            return None
        except DocsModel.NotFound:
            return None

    def list_docs(
        self, id: int | None = None, id__in: Iterable[int] | None = None
    ) -> Iterable[str]:
        return [
            item.description
            for item in self._backend.iamc.variables.docs.list(
                dimension_id=id, dimension_id__in=id__in
            )
        ]
