from typing import cast

import sqlalchemy as sa
from toolkit import db
from toolkit.db.repository.base import Values

from ixmp4.data.model.db import Model
from ixmp4.data.scenario.db import Scenario
from ixmp4.exceptions import (
    BadRequest,
    ConstraintViolated,
    NotFound,
    NotUnique,
    registry,
)

from .db import Run, RunVersion
from .filter import RunFilter


@registry.register()
class RunNotFound(NotFound):
    pass


@registry.register()
class RunNotUnique(NotUnique):
    pass


@registry.register()
class RunDeletionPrevented(ConstraintViolated):
    pass


@registry.register()
class NoDefaultRunVersion(BadRequest):
    message = "No default version available for this run."


@registry.register()
class RunIsLocked(BadRequest):
    message = "This run is already locked."


@registry.register()
class RunLockRequired(BadRequest):
    http_error_name = "run_lock_required"


class ItemRepository(db.r.ItemRepository[Run]):
    NotFound = RunNotFound
    NotUnique = RunNotUnique
    target = db.r.ModelTarget(Run)
    filter = db.r.Filter(RunFilter, Run)

    def create(self, model_id: int, scenario_id: int, values: Values | None) -> int:
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

    def set_as_default_version(self, id: int) -> None:
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

        with self.executor.update(exc) as rowcount:
            if rowcount == 0:
                raise self.NotFound()

    def unset_as_default_version(self, id: int) -> None:
        exc = (
            self.target.update_statement()
            .where(
                Run.id == id,
            )
            .values(is_default=False)
        )

        with self.executor.update(exc) as rowcount:
            if rowcount == 0:
                raise self.NotFound()


class PandasRepository(db.r.PandasRepository):
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


class PandasVersionRepository(db.r.PandasRepository):
    NotFound = RunNotFound
    NotUnique = RunNotUnique
    filter = db.r.Filter(RunFilter, Run)
    target = db.r.ExtendedTarget(
        RunVersion,
        {
            "model": (Run.model, Model.name),
            "scenario": (Run.scenario, Scenario.name),
        },
    )
