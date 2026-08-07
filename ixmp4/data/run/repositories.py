from typing import Any, Sequence, cast

import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.executor import SessionExecutor
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.repositories.base import Values
from toolkit.db.target import ExtendedTarget, ModelTarget

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.checkpoint.db import Checkpoint
from ixmp4.data.iamc.datapoint.db import DataPoint
from ixmp4.data.iamc.timeseries.db import TimeSeries
from ixmp4.data.meta.db import RunMetaEntry
from ixmp4.data.model.db import Model
from ixmp4.data.optimization.equation.db import Equation
from ixmp4.data.optimization.indexset.db import IndexSet
from ixmp4.data.optimization.parameter.db import Parameter
from ixmp4.data.optimization.scalar.db import Scalar
from ixmp4.data.optimization.table.db import Table
from ixmp4.data.optimization.variable.db import Variable
from ixmp4.data.scenario.db import Scenario

from .db import Run, RunVersion
from .exceptions import RunNotFound, RunNotUnique
from .filter import RunFilter


class RunAuthRepository(AuthRepository[Run | RunVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        model_exc = self.select_permitted_model_ids(auth_ctx, platform)
        if model_exc is None:
            return exc
        return exc.where(Run.model__id.in_(model_exc))

    def list_model_names(self, run_ids: Sequence[int]) -> Sequence[str]:
        exc = sa.select(Model.name).distinct().select_from(Run).join(Run.model)

        model_names: list[str] = []
        for result in self.executor.select_in_chunks(Run.__table__.c.id, run_ids, exc):
            model_names.extend(result.scalars().all())
        return model_names


class ItemRepository(RunAuthRepository, BaseItemRepository[Run]):
    NotFound = RunNotFound
    NotUnique = RunNotUnique
    target = ModelTarget(Run)
    filter = Filter(RunFilter, Run)
    executor: SessionExecutor

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

    def delete_cascade(self, id: int) -> None:
        """Delete a run and all associated iamc, optimization and meta data.

        Dependent rows are deleted before the run itself to satisfy foreign-key
        constraints. All statements are executed atomically in a single
        transaction so that a failure rolls back the entire cascade.
        """
        self.get_by_pk({"id": id})

        # iamc: datapoints reference time series, time series reference the run
        data_point_exc = sa.delete(DataPoint).where(
            DataPoint.time_series__id.in_(
                sa.select(TimeSeries.id).where(TimeSeries.run__id == id)
            )
        )
        time_series_exc = sa.delete(TimeSeries).where(TimeSeries.run__id == id)
        # meta and checkpoints reference the run directly
        meta_exc = sa.delete(RunMetaEntry).where(RunMetaEntry.run__id == id)
        checkpoint_exc = sa.delete(Checkpoint).where(Checkpoint.run__id == id)
        # optimization: indexed items (and their `ondelete="CASCADE"` association
        # rows) must be removed before index sets, which they reference.
        var_exc = sa.delete(Variable).where(Variable.run__id == id)
        equ_exc = sa.delete(Equation).where(Equation.run__id == id)
        par_exc = sa.delete(Parameter).where(Parameter.run__id == id)
        tab_exc = sa.delete(Table).where(Table.run__id == id)
        sca_exc = sa.delete(Scalar).where(Scalar.run__id == id)
        idx_exc = sa.delete(IndexSet).where(IndexSet.run__id == id)

        statements = [
            data_point_exc,
            time_series_exc,
            meta_exc,
            checkpoint_exc,
            var_exc,
            equ_exc,
            par_exc,
            tab_exc,
            sca_exc,
            idx_exc,
            self.target.delete_statement().where(Run.id == id),
        ]

        with self.wrap_executor_exception():
            try:
                with self.executor.delete_many(statements) as _:
                    pass
                self.executor.session.commit()
            except Exception:
                self.executor.session.rollback()
                raise

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


class PandasRepository(RunAuthRepository, BasePandasRepository):
    NotFound = RunNotFound
    NotUnique = RunNotUnique
    filter = Filter(RunFilter, Run)
    target: ModelTarget[Run | RunVersion] = ExtendedTarget(
        Run,
        {
            "model": (Run.model, Model.name),
            "scenario": (Run.scenario, Scenario.name),
        },
    )


class VersionRepository(PandasRepository):
    NotFound = RunNotFound
    NotUnique = RunNotUnique
    filter = Filter(RunFilter, Run)
    target = ModelTarget(RunVersion)
