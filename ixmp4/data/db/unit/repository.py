from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy.exc import NoResultFound

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

from ixmp4 import db
from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.db.filters import BaseFilter

from .. import base
from .docs import UnitDocsRepository
from .model import Unit


class EnumerateKwargs(abstract.annotations.HasNameFilter, total=False):
    _filter: BaseFilter


class CreateKwargs(TypedDict, total=False):
    name: str


class UnitRepository(
    base.Creator[Unit],
    base.Deleter[Unit],
    base.Retriever[Unit],
    base.Enumerator[Unit],
    base.VersionManager[Unit],
    abstract.UnitRepository,
):
    model_class = Unit

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)

        from .filter import UnitFilter

        self.filter_class = UnitFilter
        self.docs = UnitDocsRepository(*args)

    def add(self, name: str) -> Unit:
        unit = Unit(name=name)
        self.session.add(unit)
        return unit

    @guard("manage")
    def create(self, /, *args: str, **kwargs: Unpack[CreateKwargs]) -> Unit:
        return super().create(*args, **kwargs)

    @guard("manage")
    def delete(self, /, *args: Unpack[tuple[int]]) -> None:
        return super().delete(*args)

    @guard("view")
    def get(self, name: str) -> Unit:
        exc = db.select(Unit).where(Unit.name == name)
        try:
            return self.session.execute(exc).scalar_one()
        except NoResultFound:
            raise Unit.NotFound

    @guard("view")
    def get_by_id(self, id: int) -> Unit:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise Unit.NotFound(id=id)

        return obj

    @guard("view")
    def list(self, /, **kwargs: Unpack[EnumerateKwargs]) -> list[Unit]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    @guard("view")
    def tabulate_versions(
        self, /, **kwargs: Unpack[base.TabulateVersionsKwargs]
    ) -> pd.DataFrame:
        return super().tabulate_versions(**kwargs)
