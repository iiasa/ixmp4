from typing import TYPE_CHECKING

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

from ixmp4 import db
from ixmp4.data import abstract
from ixmp4.data.abstract.annotations import HasNameFilter
from ixmp4.data.auth.decorators import guard
from ixmp4.db.filters import BaseFilter

from .. import base
from .docs import VariableDocsRepository
from .model import Variable

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend


class EnumerateKwargs(HasNameFilter, total=False):
    _filter: BaseFilter


class CreateKwargs(TypedDict, total=False):
    name: str


class VariableRepository(
    base.Creator[Variable],
    base.Retriever[Variable],
    base.Enumerator[Variable],
    base.VersionManager[Variable],
    abstract.VariableRepository,
):
    model_class = Variable

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)
        self.docs = VariableDocsRepository(*args)

        from .filter import VariableFilter

        self.filter_class = VariableFilter

    def add(self, name: str) -> Variable:
        variable = Variable(name=name)
        self.session.add(variable)
        return variable

    @guard("view")
    def get(self, name: str) -> Variable:
        exc = db.select(Variable).where(Variable.name == name)
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Variable.NotFound

    @guard("edit")
    def create(self, *args: str, **kwargs: Unpack[CreateKwargs]) -> Variable:
        return super().create(*args, **kwargs)

    @guard("view")
    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[Variable]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    @guard("view")
    def tabulate_versions(
        self, /, **kwargs: Unpack[base.TabulateVersionsKwargs]
    ) -> pd.DataFrame:
        return super().tabulate_versions(**kwargs)
