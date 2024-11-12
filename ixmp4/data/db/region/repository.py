from collections.abc import Iterable
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy.exc import NoResultFound

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.db.filters import BaseFilter

from .. import base
from .docs import RegionDocsRepository
from .model import Region


class EnumerateKwargs(TypedDict, total=False):
    name: str
    name__in: Iterable[str]
    name__like: str
    name__ilike: str
    name__notlike: str
    name__notilike: str
    hierarchy: str
    hierarchy__in: Iterable[str]
    hierarchy__like: str
    hierarchy__ilike: str
    hierarchy__notlike: str
    hierarchy__notilike: str
    _filter: BaseFilter


class CreateKwargs(TypedDict, total=False):
    name: str
    hierarchy: str


class RegionRepository(
    base.Creator[Region],
    base.Deleter[Region],
    base.Retriever[Region],
    base.Enumerator[Region],
    abstract.RegionRepository,
):
    model_class = Region

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)

        from .filter import RegionFilter

        self.filter_class = RegionFilter
        self.docs = RegionDocsRepository(*args)

    def add(self, name: str, hierarchy: str) -> Region:
        region = Region(name=name, hierarchy=hierarchy)
        self.session.add(region)
        return region

    @guard("manage")
    def create(self, *args: str, **kwargs: Unpack[CreateKwargs]) -> Region:
        return super().create(*args, **kwargs)

    @guard("manage")
    def delete(self, *args: int) -> None:
        super().delete(*args)

    @guard("view")
    def get(self, name: str) -> Region:
        exc = self.select().where(Region.name == name)
        try:
            region: Region = self.session.execute(exc).scalar_one()
            return region
        except NoResultFound:
            raise Region.NotFound

    @guard("view")
    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[Region]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        return super().tabulate(**kwargs)
