from typing import cast

import sqlalchemy as sa
from toolkit import db

from ixmp4.rewrite.data.model.db import Model
from ixmp4.rewrite.data.scenario.db import Scenario
from ixmp4.rewrite.exceptions import (
    BadRequest,
    ConstraintViolated,
    NotFound,
    NotUnique,
    ProgrammingError,
    registry,
)

from .db import Run
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

    def create(self, model_id: int, scenario_id: int) -> int:
        exc = self.target.insert_statement()
        exc = exc.values(model__id=model_id, scenario__id=scenario_id)
        version_query = (
            sa.select(sa.func.max(Run.version) | 1)
            .where(Run.model__id == model_id)
            .where(Run.scenario__id == scenario_id)
        )
        exc = exc.from_select(["version"], version_query)

        with self.executor.insert(exc) as result:
            if isinstance(result, sa.CursorResult):
                return cast(int, result.lastrowid)
            else:
                raise ProgrammingError("Expected CursorResult")  # TODO

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
