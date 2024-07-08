import pandas as pd
from sqlalchemy.exc import NoResultFound

from ixmp4 import db
from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import UnitDocsRepository
from .model import Unit


class UnitRepository(
    base.Creator[Unit],
    base.Deleter[Unit],
    base.Retriever[Unit],
    base.Enumerator[Unit],
    abstract.UnitRepository,
):
    model_class = Unit

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        from .filter import UnitFilter

        self.filter_class = UnitFilter
        self.docs = UnitDocsRepository(*args, **kwargs)

    def add(self, name: str) -> Unit:
        unit = Unit(name=name)
        self.session.add(unit)
        return unit

    @guard("manage")
    def create(self, *args, **kwargs) -> Unit:
        return super().create(*args, **kwargs)

    @guard("manage")
    def delete(self, *args, **kwargs):
        return super().delete(*args, **kwargs)

    @guard("view")
    def get(self, name: str) -> Unit:
        exc: db.sql.Select = db.select(Unit).where(Unit.name == name)
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
    def list(self, *args, **kwargs) -> list[Unit]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)
