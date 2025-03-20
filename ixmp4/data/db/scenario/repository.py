from typing import TYPE_CHECKING

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

from ixmp4 import db
from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db.model.model import Model
from ixmp4.db.filters import BaseFilter

from .. import base
from .docs import ScenarioDocsRepository
from .model import Scenario


class EnumerateKwargs(abstract.annotations.HasNameFilter, total=False):
    _filter: BaseFilter


class CreateKwargs(TypedDict, total=False):
    name: str


class ScenarioRepository(
    base.Creator[Scenario],
    base.Retriever[Scenario],
    base.Enumerator[Scenario],
    base.VersionManager[Scenario],
    abstract.ScenarioRepository,
):
    model_class = Scenario

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)
        self.docs = ScenarioDocsRepository(*args)

        from .filter import ScenarioFilter

        self.filter_class = ScenarioFilter

    def join_auth(
        self, exc: db.sql.Select[tuple[Scenario]]
    ) -> db.sql.Select[tuple[Scenario]]:
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
    def create(self, *args: str, **kwargs: Unpack[CreateKwargs]) -> Scenario:
        return super().create(*args, **kwargs)

    @guard("view")
    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[Scenario]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    @guard("view")
    def tabulate_versions(
        self, /, **kwargs: Unpack[base.TabulateVersionsKwargs]
    ) -> pd.DataFrame:
        return super().tabulate_versions(**kwargs)
