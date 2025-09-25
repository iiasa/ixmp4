from typing import cast

import sqlalchemy as sa
from toolkit import db
from toolkit.exceptions import BadRequest, NotFound, NotUnique, ProgrammingError

from ixmp4.core.exceptions import DeletionPrevented

from .db import Run
from .filter import RunFilter


class RunNotFound(NotFound):
    pass


class RunNotUnique(NotUnique):
    pass


class RunDeletionPrevented(DeletionPrevented):
    pass


class NoDefaultRunVersion(BadRequest):
    message = "No default version available for this run."


class RunIsLocked(BadRequest):
    message = "This run is already locked."


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
                raise ProgrammingError("Expected CursorResult")

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
    target = db.r.ModelTarget(Run)
    filter = db.r.Filter(RunFilter, Run)
