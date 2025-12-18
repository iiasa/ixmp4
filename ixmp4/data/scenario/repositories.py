from typing import Any

import sqlalchemy as sa
from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.run.db import Run

from .db import Scenario, ScenarioVersion
from .exceptions import ScenarioNotFound, ScenarioNotUnique
from .filter import ScenarioFilter


class ScenarioAuthRepository(AuthRepository[Scenario]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        return exc.where(Scenario.runs.any(Run.id.in_(run_exc)))


class ItemRepository(db.r.ItemRepository[Scenario], ScenarioAuthRepository):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = db.r.ModelTarget(Scenario)
    filter = db.r.Filter(ScenarioFilter, Scenario)


class PandasRepository(db.r.PandasRepository, ScenarioAuthRepository):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = db.r.ModelTarget(Scenario)
    filter = db.r.Filter(ScenarioFilter, Scenario)


class VersionRepository(db.r.PandasRepository):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = db.r.ModelTarget(ScenarioVersion)
    filter = db.r.Filter(ScenarioFilter, ScenarioVersion)
