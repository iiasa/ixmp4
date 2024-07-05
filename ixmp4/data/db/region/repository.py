import pandas as pd
from sqlalchemy.exc import NoResultFound

from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import RegionDocsRepository
from .model import Region


class RegionRepository(
    base.Creator[Region],
    base.Deleter[Region],
    base.Retriever[Region],
    base.Enumerator[Region],
    abstract.RegionRepository,
):
    model_class = Region

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        from .filter import RegionFilter

        self.filter_class = RegionFilter
        self.docs = RegionDocsRepository(*args, **kwargs)

    def add(self, name: str, hierarchy: str) -> Region:
        region = Region(name=name, hierarchy=hierarchy)
        self.session.add(region)
        return region

    @guard("manage")
    def create(self, *args, **kwargs) -> Region:
        return super().create(*args, **kwargs)

    @guard("manage")
    def delete(self, *args, **kwargs):
        return super().delete(*args, **kwargs)

    @guard("view")
    def get(self, name: str) -> Region:
        exc = self.select().where(Region.name == name)
        try:
            return self.session.execute(exc).scalar_one()
        except NoResultFound:
            raise Region.NotFound

    @guard("view")
    def list(self, *args, **kwargs) -> list[Region]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)
