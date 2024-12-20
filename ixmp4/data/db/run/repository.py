from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy.exc import NoResultFound

# TODO Adapt import when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

from ixmp4 import db
from ixmp4.core.exceptions import Forbidden, IxmpError, NoDefaultRunVersion
from ixmp4.core.utils import substitute_type
from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db.iamc.utils import normalize_df
from ixmp4.db import utils

from .. import base
from ..model import Model, ModelRepository
from ..scenario import Scenario, ScenarioRepository
from .model import Run


class CreateKwargs(TypedDict, total=False):
    scenario_name: str


class RunRepository(
    base.Creator[Run],
    base.Retriever[Run],
    base.Enumerator[Run],
    abstract.RunRepository,
):
    model_class = Run

    models: ModelRepository
    scenarios: ScenarioRepository

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        self.models = ModelRepository(*args)
        self.scenarios = ScenarioRepository(*args)

        from .filter import RunFilter

        self.filter_class = RunFilter
        super().__init__(*args)

    def join_auth(self, exc: db.sql.Select[tuple[Run]]) -> db.sql.Select[tuple[Run]]:
        if not utils.is_joined(exc, Model):
            exc = exc.join(Model, Run.model)
        return exc

    def add(self, model_name: str, scenario_name: str) -> Run:
        # Get or create model
        try:
            exc_model = self.models.select(name=model_name)
            model = self.session.execute(exc_model).scalar_one()
        except NoResultFound:
            model = Model(name=model_name)
            self.session.add(model)

        # Get or create scenario
        try:
            exc_scenario = self.scenarios.select(name=scenario_name)
            scenario = self.session.execute(exc_scenario).scalar_one()
        except NoResultFound:
            scenario = Scenario(name=scenario_name)
            self.session.add(scenario)

        (version,) = (
            # Aggregate MAX over run.version where the run's
            # model and scenario match.
            self.session.query(db.func.max(Run.version))
            .filter(Run.model.has(name=model_name))
            .filter(Run.scenario.has(name=scenario_name))
            .one()
        )
        if version is None:
            version = 0
        version += 1

        run = Run(model=model, scenario=scenario, version=version)
        self.session.add(run)
        return run

    @guard("edit")
    def create(
        self, model_name: str, *args: str, **kwargs: Unpack[CreateKwargs]
    ) -> Run:
        if self.backend.auth_context is not None:
            if not self.backend.auth_context.check_access("edit", model_name):
                raise Forbidden(f"Access to model '{model_name}' denied.")
        return super().create(model_name, *args, **kwargs)

    @guard("view")
    def get(self, model_name: str, scenario_name: str, version: int) -> Run:
        exc = self.select(
            model={"name": model_name},
            scenario={"name": scenario_name},
            version=version,
            default_only=False,
        )

        try:
            return self.session.execute(exc).scalar_one()
        except NoResultFound:
            raise Run.NotFound(
                model=model_name,
                scenario=scenario_name,
                version=version,
            )

    @guard("view")
    def get_by_id(self, id: int) -> Run:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise Run.NotFound(id=id)

        return obj

    @guard("view")
    def get_default_version(self, model_name: str, scenario_name: str) -> Run:
        exc = self.select(
            model={"name": model_name},
            scenario={"name": scenario_name},
        )

        try:
            return self.session.execute(exc).scalar_one()
        except NoResultFound:
            raise NoDefaultRunVersion

    @guard("view")
    def tabulate(self, **kwargs: Unpack[abstract.run.EnumerateKwargs]) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    @guard("view")
    def list(self, **kwargs: Unpack[abstract.run.EnumerateKwargs]) -> list[Run]:
        return super().list(**kwargs)

    @guard("edit")
    def set_as_default_version(self, id: int) -> None:
        try:
            run = self.session.get(Run, id)
        except NoResultFound:
            raise Run.NotFound(id=id)

        if run is None:
            raise Run.NotFound

        exc = (
            db.update(Run)
            .where(
                Run.model__id == run.model__id,
                Run.scenario__id == run.scenario__id,
                Run.is_default,
            )
            .values(is_default=False)
        )

        with self.backend.event_handler.pause():
            # we dont want to trigger the
            # updated_at fields for this operation.
            self.session.execute(exc)
            self.session.commit()

        run.is_default = True
        self.session.commit()

    @guard("edit")
    def unset_as_default_version(self, id: int) -> None:
        try:
            run = self.session.get(Run, id)
        except NoResultFound:
            raise Run.NotFound(id=id)

        if run is None:
            raise Run.NotFound

        if not run.is_default:
            raise IxmpError(f"Run with id={id} is not set as the default version.")

        run.is_default = False
        self.session.commit()

    @guard("edit")
    def clone(
        self,
        run_id: int,
        model_name: str | None = None,
        scenario_name: str | None = None,
        keep_solution: bool = True,
    ) -> Run:
        base_run = self.get_by_id(id=run_id)
        run = self.create(
            model_name=model_name if model_name else base_run.model.name,
            scenario_name=scenario_name if scenario_name else base_run.scenario.name,
        )

        datapoints = normalize_df(
            df=self.backend.iamc.datapoints.tabulate(
                join_parameters=True,
                join_runs=False,
                run={"id": base_run.id, "default_only": False},
            ),
            raw=False,
            join_runs=False,
        )
        if not datapoints.empty:
            datapoints["run__id"] = run.id
            # TODO This is essentially duplicating core/iamc/data/_get_or_create_ts,
            # which we should probably avoid.
            id_cols = ["region", "variable", "unit", "run__id"]
            # create set of unqiue timeseries (if missing)
            ts_df = datapoints[id_cols].drop_duplicates()
            self.backend.iamc.timeseries.bulk_upsert(ts_df, create_related=True)

            # retrieve them again to get database ids
            ts_df = self.backend.iamc.timeseries.tabulate(
                join_parameters=True,
                run={"id": run.id, "default_only": False},
            )
            ts_df = ts_df.rename(columns={"id": "time_series__id"})

            # merge on the identity columns
            datapoints = pd.merge(
                datapoints, ts_df, how="left", on=id_cols, suffixes=(None, "_y")
            )
            substitute_type(df=datapoints)
            # TODO This function expects a pandera.DataFrame of a certain schema, even
            # though the abstract layer annotates it as pd.DataFrame and it works with
            # one. I'm not simply adjusting the type hint because the function might be
            # called directly, in which case we would like to reject unvalidated
            # pd.DataFrames, I think. However, in this case, the data were validated
            # before and need not be checked again.
            self.backend.iamc.datapoints.bulk_upsert(datapoints)  # type: ignore[arg-type]

        for scalar in base_run.scalars:
            self.backend.optimization.scalars.create(
                run_id=run.id,
                name=scalar.name,
                value=scalar.value,
                unit_name=scalar.unit.name,
            )

        for indexset in base_run.indexsets:
            new_indexset = self.backend.optimization.indexsets.create(
                run_id=run.id, name=indexset.name
            )
            self.backend.optimization.indexsets.add_data(
                indexset_id=new_indexset.id, data=indexset.data
            )

        for table in base_run.tables:
            new_table = self.backend.optimization.tables.create(
                run_id=run.id,
                name=table.name,
                constrained_to_indexsets=table.indexsets,
                column_names=table.column_names,
            )
            self.backend.optimization.tables.add_data(
                table_id=new_table.id, data=table.data
            )

        for parameter in base_run.parameters:
            new_parameter = self.backend.optimization.parameters.create(
                run_id=run.id,
                name=parameter.name,
                constrained_to_indexsets=[
                    column.indexset.name for column in parameter.columns
                ],
                column_names=[column.name for column in parameter.columns],
            )
            self.backend.optimization.parameters.add_data(
                parameter_id=new_parameter.id, data=parameter.data
            )

        for equation in base_run.equations:
            new_equation = self.backend.optimization.equations.create(
                run_id=run.id,
                name=equation.name,
                constrained_to_indexsets=[
                    column.indexset.name for column in equation.columns
                ],
                column_names=[column.name for column in equation.columns],
            )
            if keep_solution:
                self.backend.optimization.equations.add_data(
                    equation_id=new_equation.id, data=equation.data
                )

        for variable in base_run.variables:
            new_variable = self.backend.optimization.variables.create(
                run_id=run.id,
                name=variable.name,
                constrained_to_indexsets=[
                    column.indexset.name for column in variable.columns
                ],
                column_names=[column.name for column in variable.columns],
            )
            if keep_solution:
                self.backend.optimization.variables.add_data(
                    variable_id=new_variable.id, data=variable.data
                )

        return run
