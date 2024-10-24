import pandas as pd
from sqlalchemy.exc import NoResultFound

from ixmp4 import db
from ixmp4.core.exceptions import Forbidden, IxmpError, NoDefaultRunVersion
from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.db import utils

from .. import base
from ..model import Model, ModelRepository
from ..scenario import Scenario, ScenarioRepository
from .model import Run


class RunRepository(
    base.Creator[Run],
    base.Retriever[Run],
    base.Enumerator[Run],
    abstract.RunRepository,
):
    model_class = Run

    models: ModelRepository
    scenarios: ScenarioRepository

    def __init__(self, *args, **kwargs) -> None:
        self.models = ModelRepository(*args, **kwargs)
        self.scenarios = ScenarioRepository(*args, **kwargs)

        from .filter import RunFilter

        self.filter_class = RunFilter
        super().__init__(*args, **kwargs)

    def join_auth(self, exc: db.sql.Select):
        if not utils.is_joined(exc, Model):
            exc = exc.join(Model, Run.model)
        return exc

    def add(self, model_name: str, scenario_name: str) -> Run:
        # Get or create model
        try:
            exc: db.sql.Select = self.models.select(name=model_name)
            model = self.session.execute(exc).scalar_one()
        except NoResultFound:
            model = Model(name=model_name)
            self.session.add(model)

        # Get or create scenario
        try:
            exc = self.scenarios.select(name=scenario_name)
            scenario: Scenario = self.session.execute(exc).scalar_one()
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
    def create(self, model_name: str, *args, **kwargs) -> Run:
        if self.backend.auth_context is not None:
            if not self.backend.auth_context.check_access("edit", model_name):
                raise Forbidden(f"Access to model '{model_name}' denied.")
        return super().create(model_name, *args, **kwargs)

    @guard("view")
    def get(
        self,
        model_name: str,
        scenario_name: str,
        version: int,
    ) -> Run:
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
    def get_default_version(
        self,
        model_name: str,
        scenario_name: str,
    ) -> Run:
        exc = self.select(
            model={"name": model_name},
            scenario={"name": scenario_name},
        )

        try:
            return self.session.execute(exc).scalar_one()
        except NoResultFound:
            raise NoDefaultRunVersion

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    @guard("view")
    def list(self, *args, **kwargs) -> list[Run]:
        return super().list(*args, **kwargs)

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
