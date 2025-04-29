from typing import TYPE_CHECKING

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend


from ixmp4 import db
from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.db.filters import BaseFilter

from .. import base
from .docs import ModelDocsRepository
from .model import Model


class EnumerateKwargs(abstract.annotations.HasNameFilter, total=False):
    _filter: BaseFilter


class CreateKwargs(TypedDict, total=False):
    name: str


class ModelRepository(
    base.Creator[Model],
    base.Retriever[Model],
    base.Enumerator[Model],
    base.VersionManager[Model],
    abstract.ModelRepository,
):
    model_class = Model
    docs: ModelDocsRepository

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)
        self.docs = ModelDocsRepository(*args)

        from .filter import ModelFilter

        self.filter_class = ModelFilter

    def add(self, name: str) -> Model:
        model = Model(name=name)
        self.session.add(model)
        return model

    @guard("view")
    def get(self, name: str) -> Model:
        exc = self.select(name=name)
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Model.NotFound

    @guard("edit")
    def create(self, *args: str, **kwargs: Unpack[CreateKwargs]) -> Model:
        return super().create(*args, **kwargs)

    @guard("view")
    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[Model]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    @guard("view")
    def tabulate_versions(
        self, /, **kwargs: Unpack[base.TabulateVersionsKwargs]
    ) -> pd.DataFrame:
        return super().tabulate_versions()
