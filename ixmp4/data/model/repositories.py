from typing import Any

import sqlalchemy as sa
from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol

from ixmp4.data.base.repository import AuthRepository

from .db import Model, ModelVersion
from .exceptions import (
    ModelNotFound,
    ModelNotUnique,
)
from .filter import ModelFilter


class ModelAuthRepository(AuthRepository[Model]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        model_exc = self.select_permitted_model_ids(auth_ctx, platform)
        return exc.where(Model.id.in_(model_exc))


class ItemRepository(ModelAuthRepository, db.r.ItemRepository[Model]):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target = db.r.ModelTarget(Model)
    filter = db.r.Filter(ModelFilter, Model)


class PandasRepository(ModelAuthRepository, db.r.PandasRepository):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target = db.r.ModelTarget(Model)
    filter = db.r.Filter(ModelFilter, Model)


class VersionRepository(db.r.PandasRepository):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target = db.r.ModelTarget(ModelVersion)
    filter = db.r.Filter(ModelFilter, ModelVersion)
