from typing import Any

import sqlalchemy as sa
from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol

from ixmp4.data.base.repository import AuthRepository

from .db import Checkpoint
from .exceptions import CheckpointNotFound, CheckpointNotUnique
from .filter import CheckpointFilter


class CheckpointAuthRepository(AuthRepository[Checkpoint]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        return exc.where(Checkpoint.run__id.in_(run_exc))


class ItemRepository(CheckpointAuthRepository, db.r.ItemRepository[Checkpoint]):
    NotFound = CheckpointNotFound
    NotUnique = CheckpointNotUnique
    target = db.r.ModelTarget(Checkpoint)
    filter = db.r.Filter(CheckpointFilter, Checkpoint)


class PandasRepository(CheckpointAuthRepository, db.r.PandasRepository):
    NotFound = CheckpointNotFound
    NotUnique = CheckpointNotUnique
    target = db.r.ModelTarget(Checkpoint)
    filter = db.r.Filter(CheckpointFilter, Checkpoint)
