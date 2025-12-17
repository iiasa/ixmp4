from typing import Any

import sqlalchemy as sa
from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol

from ixmp4.data.base.repository import AuthRepository

from .db import Scalar, ScalarVersion
from .exceptions import (
    ScalarNotFound,
    ScalarNotUnique,
)
from .filter import ScalarFilter


class ParameterAuthRepository(AuthRepository[Scalar]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        return exc.where(Scalar.run__id.in_(run_exc))


class ItemRepository(ParameterAuthRepository, db.r.ItemRepository[Scalar]):
    NotFound = ScalarNotFound
    NotUnique = ScalarNotUnique
    target = db.r.ModelTarget(Scalar)
    filter = db.r.Filter(ScalarFilter, Scalar)


class PandasRepository(ParameterAuthRepository, db.r.PandasRepository):
    NotFound = ScalarNotFound
    NotUnique = ScalarNotUnique
    target = db.r.ModelTarget(Scalar)
    filter = db.r.Filter(ScalarFilter, Scalar)


class VersionRepository(db.r.PandasRepository):
    NotFound = ScalarNotFound
    NotUnique = ScalarNotUnique
    target = db.r.ModelTarget(ScalarVersion)
    filter = db.r.Filter(ScalarFilter, ScalarVersion)
