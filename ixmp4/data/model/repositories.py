from typing import Any

import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from ixmp4.data.base.repository import AuthRepository

from .db import Model, ModelVersion
from .exceptions import (
    ModelNotFound,
    ModelNotUnique,
)
from .filter import ModelFilter


class ModelAuthRepository(AuthRepository[Model | ModelVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        model_exc = self.select_permitted_model_ids(auth_ctx, platform)
        return exc.where(Model.id.in_(model_exc))


class ItemRepository(ModelAuthRepository, BaseItemRepository[Model]):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target = ModelTarget(Model)
    filter = Filter(ModelFilter, Model)


class PandasRepository(ModelAuthRepository, BasePandasRepository):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target: ModelTarget[Model | ModelVersion] = ModelTarget(Model)
    filter = Filter(ModelFilter, Model)


class VersionRepository(PandasRepository):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target = ModelTarget(ModelVersion)
    filter = Filter(ModelFilter, ModelVersion)
