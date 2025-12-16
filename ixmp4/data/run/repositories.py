from typing import Any, Sequence, cast

import sqlalchemy as sa
from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.repository.base import Values

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.model.db import Model
from ixmp4.data.scenario.db import Scenario

from .db import Run, RunVersion
from .exceptions import RunNotFound, RunNotUnique
from .filter import RunFilter


class RunAuthRepository(AuthRepository[Run]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        model_exc = self.select_permitted_model_ids(auth_ctx, platform)
        return exc.where(Run.model__id.in_(model_exc))

    def list_model_names(self, run_ids: Sequence[int]) -> Sequence[str]:
        exc = (
            sa.select(Model.name)
            .distinct()
            .select_from(Run)
            .join(Run.model)
            .where(Run.id.in_(run_ids))
        )

        with self.executor.select(exc) as result:
            return result.scalars().all()


class ItemRepository(RunAuthRepository, db.r.ItemRepository[Run]):
    NotFound = RunNotFound
    NotUnique = RunNotUnique
    target = db.r.ModelTarget(Run)
    filter = db.r.Filter(RunFilter, Run)

    def create(
        self, model_id: int, scenario_id: int, values: Values | None = None
    ) -> int:
        if values is None:
            values = {}

        exc = self.target.insert_statement()
        version_query = (
            # run creation logic in a single query
            # to forego potential concurrency bugs
            sa.select(
                sa.literal(model_id),
                sa.literal(scenario_id),
                sa.func.coalesce(sa.func.max(Run.version), sa.literal(0))
                + sa.literal(1),
                *(sa.literal(v) for v in values.values()),
            )
            .where(Run.model__id == model_id)
            .where(Run.scenario__id == scenario_id)
        )
        exc = exc.from_select(
            ["model__id", "scenario__id", "version", *values.keys()],
            version_query,
        ).returning(Run.id)

        with self.wrap_executor_exception():
            with self.executor.insert_one(exc) as result:
                return cast(int, result.scalar_one())

    def set_as_default_version(self, id: int, values: Values | None = None) -> None:
        run = self.get_by_pk({"id": id})

        unset_exc = (
            self.target.update_statement()
            .where(
                Run.model__id == run.model__id,
                Run.scenario__id == run.scenario__id,
                Run.is_default,
            )
            .values(is_default=False)
        )

        with self.executor.update(unset_exc):
            pass

        exc = (
            self.target.update_statement()
            .where(
                Run.id == id,
            )
            .values(is_default=True)
        )
        if values is not None:
            exc = exc.values(**values)

        with self.executor.update(exc):
            return None

    def unset_as_default_version(self, id: int, values: Values | None = None) -> None:
        exc = (
            self.target.update_statement()
            .where(
                Run.id == id,
            )
            .values(is_default=False)
        )
        if values is not None:
            exc = exc.values(**values)

        with self.executor.update(exc):
            return None


class PandasRepository(RunAuthRepository, db.r.PandasRepository):
    NotFound = RunNotFound
    NotUnique = RunNotUnique
    filter = db.r.Filter(RunFilter, Run)
    target = db.r.ExtendedTarget(
        Run,
        {
            "model": (Run.model, Model.name),
            "scenario": (Run.scenario, Scenario.name),
        },
    )


class VersionRepository(db.r.PandasRepository):
    NotFound = RunNotFound
    NotUnique = RunNotUnique
    filter = db.r.Filter(RunFilter, Run)
    target = db.r.ModelTarget(RunVersion)
