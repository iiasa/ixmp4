from datetime import datetime
from typing import ClassVar

import pandas as pd

from ixmp4.core.base import BaseFacade, BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import Variable as VariableModel


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
    def created_at(self) -> datetime | None:
        return self._model.created_at

    @property
    def created_by(self) -> str | None:
        return self._model.created_by

    @property
    def docs(self):
        try:
            return self.backend.iamc.variables.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description):
        if description is None:
            self.backend.iamc.variables.docs.delete(self.id)
        else:
            self.backend.iamc.variables.docs.set(self.id, description)

    @docs.deleter
    def docs(self):
        try:
            self.backend.iamc.variables.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"


class VariableRepository(BaseFacade):
    def create(
        self,
        name: str,
    ) -> Variable:
        model = self.backend.iamc.variables.create(name)
        return Variable(_backend=self.backend, _model=model)

    def get(self, name: str) -> Variable:
        model = self.backend.iamc.variables.get(name)
        return Variable(_backend=self.backend, _model=model)

    def list(self, name: str | None = None) -> list[Variable]:
        variables = self.backend.iamc.variables.list(name=name)
        return [Variable(_backend=self.backend, _model=v) for v in variables]

    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self.backend.iamc.variables.tabulate(name=name)

    def _get_variable_id(self, variable: str) -> int | None:
        if variable is None:
            return None
        elif isinstance(variable, str):
            obj = self.backend.iamc.variables.get(variable)
            return obj.id
        else:
            raise ValueError(f"Invalid reference to variable: {variable}")

    def get_docs(self, name: str) -> str | None:
        variable_id = self._get_variable_id(name)
        if variable_id is None:
            return None
        try:
            return self.backend.iamc.variables.docs.get(
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
        return self.backend.iamc.variables.docs.set(
            dimension_id=variable_id, description=description
        ).description

    def delete_docs(self, name: str) -> None:
        # TODO: this function is failing silently, which we should avoid
        variable_id = self._get_variable_id(name)
        if variable_id is None:
            return None
        try:
            self.backend.iamc.variables.docs.delete(dimension_id=variable_id)
            return None
        except DocsModel.NotFound:
            return None
