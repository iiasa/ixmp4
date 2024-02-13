from datetime import datetime
from typing import ClassVar

import pandas as pd

from ixmp4.core.base import BaseFacade, BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import Model as ModelModel


class Model(BaseModelFacade):
    _model: ModelModel  # smh
    NotFound: ClassVar = ModelModel.NotFound
    NotUnique: ClassVar = ModelModel.NotUnique

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
            return self.backend.models.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description):
        if description is None:
            self.backend.models.docs.delete(self.id)
        else:
            self.backend.models.docs.set(self.id, description)

    @docs.deleter
    def docs(self):
        try:
            self.backend.models.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Model {self.id} name={self.name}>"


class ModelRepository(BaseFacade):
    def create(
        self,
        name: str,
    ) -> Model:
        model = self.backend.models.create(name)
        return Model(_backend=self.backend, _model=model)

    def get(self, name: str) -> Model:
        model = self.backend.models.get(name)
        return Model(_backend=self.backend, _model=model)

    def list(self, name: str | None = None) -> list[Model]:
        models = self.backend.models.list(name=name)
        return [Model(_backend=self.backend, _model=m) for m in models]

    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self.backend.models.tabulate(name=name)

    def _get_model_id(self, model: str) -> int | None:
        if model is None:
            return None
        elif isinstance(model, str):
            obj = self.backend.models.get(model)
            return obj.id
        else:
            raise ValueError(f"Invalid reference to model: {model}")

    def get_docs(self, name: str) -> str | None:
        model_id = self._get_model_id(name)
        if model_id is None:
            return None
        try:
            return self.backend.models.docs.get(dimension_id=model_id).description
        except DocsModel.NotFound:
            return None

    def set_docs(self, name: str, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(name=name)
            return None
        model_id = self._get_model_id(name)
        if model_id is None:
            return None
        return self.backend.models.docs.set(
            dimension_id=model_id, description=description
        ).description

    def delete_docs(self, name: str) -> None:
        # TODO: this function is failing silently, which we should avoid
        model_id = self._get_model_id(name)
        if model_id is None:
            return None
        try:
            self.backend.models.docs.delete(dimension_id=model_id)
            return None
        except DocsModel.NotFound:
            return None
