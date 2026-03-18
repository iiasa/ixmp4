from typing import Any

import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from ixmp4.data.base.repository import AuthRepository

from .db import Scalar, ScalarVersion
from .exceptions import (
    ScalarNotFound,
    ScalarNotUnique,
)
from .filter import ScalarFilter


class ParameterAuthRepository(AuthRepository[Scalar | ScalarVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        return exc.where(Scalar.run__id.in_(run_exc))


class ItemRepository(ParameterAuthRepository, BaseItemRepository[Scalar]):
    NotFound = ScalarNotFound
    NotUnique = ScalarNotUnique
    target = ModelTarget(Scalar)
    filter = Filter(ScalarFilter, Scalar)


class PandasRepository(ParameterAuthRepository, BasePandasRepository):
    NotFound = ScalarNotFound
    NotUnique = ScalarNotUnique
    target = ModelTarget(Scalar)
    filter = Filter(ScalarFilter, Scalar)


class VersionRepository(PandasRepository):
    NotFound = ScalarNotFound
    NotUnique = ScalarNotUnique
    target = ModelTarget(ScalarVersion)
    filter = Filter(ScalarFilter, ScalarVersion)
