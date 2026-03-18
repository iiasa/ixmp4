from typing import Any

import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.run.db import Run

from .db import Scenario, ScenarioVersion
from .exceptions import ScenarioNotFound, ScenarioNotUnique
from .filter import ScenarioFilter


class ScenarioAuthRepository(AuthRepository[Scenario | ScenarioVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        return exc.where(Scenario.runs.any(Run.id.in_(run_exc)))


class ItemRepository(
    BaseItemRepository[Scenario | ScenarioVersion], ScenarioAuthRepository
):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = ModelTarget(Scenario)
    filter = Filter(ScenarioFilter, Scenario)


class PandasRepository(BasePandasRepository, ScenarioAuthRepository):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = ModelTarget(Scenario)
    filter = Filter(ScenarioFilter, Scenario)


class VersionRepository(PandasRepository):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = ModelTarget(ScenarioVersion)
    filter = Filter(ScenarioFilter, ScenarioVersion)
