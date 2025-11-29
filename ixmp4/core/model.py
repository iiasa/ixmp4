from collections.abc import Iterable
from datetime import datetime

import pandas as pd
from typing_extensions import Unpack

from ixmp4.core.base import BaseFacadeObject, BaseServiceFacade
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.model.dto import Model as ModelDto
from ixmp4.data.model.exceptions import (
    ModelDeletionPrevented,
    ModelNotFound,
    ModelNotUnique,
)
from ixmp4.data.model.filter import ModelFilter
from ixmp4.data.model.service import ModelService


class Model(BaseFacadeObject[ModelService, ModelDto]):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    DeletionPrevented = ModelDeletionPrevented

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

    def __str__(self) -> str:
        return f"<Model {self.id} name={self.name}>"


class ModelServiceFacade(BaseServiceFacade[ModelService]):
    def create(
        self,
        name: str,
    ) -> Model:
        dto = self.service.create(name)
        return Model(self.service, dto)

    def get(self, name: str) -> Model:
        dto = self.service.get_by_name(name)
        return Model(self.service, dto)

    def list(self, **kwargs: Unpack[ModelFilter]) -> list[Model]:
        models = self.service.list(**kwargs)
        return [Model(self.service, dto) for dto in models]

    def tabulate(self, **kwargs: Unpack[ModelFilter]) -> pd.DataFrame:
        return self.service.tabulate(**kwargs)

    def _get_model_id(self, model: str) -> int | None:
        # NOTE leaving this check for users without mypy
        if isinstance(model, str):
            obj = self.service.get_by_name(model)
            return obj.id
        else:
            raise ValueError(f"Invalid reference to model: {model}")

    def get_docs(self, name: str) -> str | None:
        model_id = self._get_model_id(name)
        if model_id is None:
            return None
        try:
            return self.service.get_docs(dimension__id=model_id).description
        except DocsNotFound:
            return None

    def set_docs(self, name: str, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(name=name)
            return None
        model_id = self._get_model_id(name)
        if model_id is None:
            return None
        return self.service.set_docs(
            dimension__id=model_id, description=description
        ).description

    def delete_docs(self, name: str) -> None:
        # TODO: this function is failing silently, which we should avoid
        model_id = self._get_model_id(name)
        if model_id is None:
            return None
        try:
            self.service.delete_docs(dimension__id=model_id)
            return None
        except DocsNotFound:
            return None

    def list_docs(
        self, id: int | None = None, id__in: Iterable[int] | None = None
    ) -> Iterable[str]:
        return [
            item.description
            for item in self.service.list_docs(
                dimension__id=id, dimension__id__in=id__in
            )
        ]
