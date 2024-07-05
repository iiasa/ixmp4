import pandas as pd

from ixmp4 import db
from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db.model.model import Model

from .. import base
from .docs import ScenarioDocsRepository
from .model import Scenario


class ScenarioRepository(
    base.Creator[Scenario],
    base.Retriever[Scenario],
    base.Enumerator[Scenario],
    abstract.ScenarioRepository,
):
    model_class = Scenario

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = ScenarioDocsRepository(*args, **kwargs)

        from .filter import ScenarioFilter

        self.filter_class = ScenarioFilter

    def join_auth(self, exc: db.sql.Select) -> db.sql.Select:
        from ixmp4.data.db.run.model import Run

        if not db.utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=Run.scenario__id == Scenario.id)
        if not db.utils.is_joined(exc, Model):
            exc = exc.join(Model, Run.model)

        return exc

    def add(self, name: str) -> Scenario:
        scenario = Scenario(name=name)
        self.session.add(scenario)
        return scenario

    @guard("view")
    def get(self, name: str) -> Scenario:
        exc = db.select(Scenario).where(Scenario.name == name)
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Scenario.NotFound

    @guard("edit")
    def create(self, *args, **kwargs) -> Scenario:
        return super().create(*args, **kwargs)

    @guard("view")
    def list(self, *args, **kwargs) -> list[Scenario]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)
