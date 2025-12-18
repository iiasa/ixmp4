from datetime import datetime

import pandas as pd
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.core.base import BaseDocsServiceFacade, BaseFacadeObject
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
        return self._dto.id

    @property
    def name(self) -> str:
        return self._dto.name

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

    @property
    def docs(self) -> str | None:
        try:
            return self._service.get_docs(self.id).description
        except DocsNotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self._service.delete_docs(self.id)
        else:
            self._service.set_docs(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self._service.delete_docs(self.id)
        # TODO: silently failing
        except DocsNotFound:
            return None

    def delete(self) -> None:
        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> ModelService:
        return backend.models

    def __str__(self) -> str:
        return f"<Model {self.id} name='{self.name}'>"


class ModelServiceFacade(BaseDocsServiceFacade[Model | int | str, Model, ModelService]):
    def _get_service(self, backend: Backend) -> ModelService:
        return backend.models

    def _get_item_id(self, ref: Model | int | str) -> int:
        if isinstance(ref, Model):
            return ref.id
        elif isinstance(ref, int):
            return ref
        elif isinstance(ref, str):
            dto = self._service.get_by_name(ref)
            return dto.id
        else:
            raise ValueError(f"Invalid reference to model: {ref}")

    def create(
        self,
        name: str,
    ) -> Model:
        dto = self._service.create(name)
        return Model(self._backend, dto)

    def delete(self, ref: Model | int | str) -> None:
        id = self._get_item_id(ref)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Model:
        dto = self._service.get_by_name(name)
        return Model(self._backend, dto)

    def list(self, **kwargs: Unpack[ModelFilter]) -> list[Model]:
        models = self._service.list(**kwargs)
        return [Model(self._backend, dto) for dto in models]

    def tabulate(self, **kwargs: Unpack[ModelFilter]) -> pd.DataFrame:
        return self._service.tabulate(**kwargs)
