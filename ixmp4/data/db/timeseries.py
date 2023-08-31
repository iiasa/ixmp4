from typing import Any, ClassVar, Generic, Iterable, Mapping, TypeVar

import pandas as pd
from sqlalchemy.ext.declarative import AbstractConcreteBase
from sqlalchemy.orm.decl_api import declared_attr
from sqlalchemy.orm.exc import NoResultFound

from ixmp4 import db
from ixmp4.data import abstract
from ixmp4.data.db.model import Model
from ixmp4.data.db.run import Run

from ..auth.decorators import guard
from . import base


class TimeSeries(AbstractConcreteBase, base.BaseModel):
    NotFound: ClassVar = abstract.TimeSeries.NotFound
    NotUnique: ClassVar = abstract.TimeSeries.NotUnique
    DeletionPrevented: ClassVar = abstract.TimeSeries.DeletionPrevented

    __abstract__ = True
    parameters: dict = {}

    @declared_attr
    def run__id(cls):
        return db.Column(
            "run__id",
            db.Integer,
            db.ForeignKey("run.id"),
            nullable=False,
            index=True,
        )

    @declared_attr
    def run(cls):
        return db.relationship("Run", backref="time_series", foreign_keys=[cls.run__id])

    @property
    def run_id(self) -> int:
        return self.run__id


ModelType = TypeVar("ModelType", bound=TimeSeries)


class TimeSeriesRepository(
    base.Creator[ModelType],
    base.Retriever[ModelType],
    base.Enumerator[ModelType],
    base.BulkUpserter[ModelType],
    Generic[ModelType],
):
    def join_auth(self, exc: db.sql.Select) -> db.sql.Select:
        if not db.utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=Run.id == TimeSeries.run__id)
        if not db.utils.is_joined(exc, Model):
            exc = exc.join(Model, Run.model)

        return exc

    def add(self, run_id: int, parameters: Mapping) -> ModelType:
        time_series = self.model_class(run_id=run_id, **parameters)
        self.session.add(time_series)
        return time_series

    @guard("edit")
    def create(self, *args, **kwargs) -> ModelType:
        return super().create(*args, **kwargs)

    @guard("view")
    def get(self, run_id: int, **kwargs: Any) -> ModelType:
        exc = self.select(run_ids=[run_id], **kwargs)

        try:
            return self.session.execute(exc).scalar_one()
        except NoResultFound:
            raise self.model_class.NotFound

    @guard("view")
    def get_by_id(self, id: int) -> ModelType:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise self.model_class.NotFound

        return obj

    def filter_by_parameters(
        self, exc: db.sql.Select, parameters: Mapping
    ) -> db.sql.Select:
        raise NotImplementedError

    def select_joined_parameters(self) -> db.sql.Select:
        raise NotImplementedError

    def select(
        self,
        *,
        id__in: Iterable[int] | None = None,
        run_ids: Iterable[int] | None = None,
        parameters: Mapping | None = None,
        join_parameters: bool | None = False,
        _exc: db.sql.Select | None = None,
        _access_type: str = "view",
    ) -> db.sql.Select:
        if _exc is not None:
            exc = _exc
        elif join_parameters:
            exc = self.select_joined_parameters()
        else:
            exc = db.select(self.model_class)

        exc = self.apply_auth(exc, _access_type)

        if id__in is not None:
            exc = exc.where(self.model_class.id.in_(id__in))

        if run_ids is not None:
            exc = exc.where(self.model_class.run__id.in_(run_ids))
        if parameters:
            exc = self.filter_by_parameters(exc, parameters)

        return exc

    @guard("view")
    def list(self, *args, **kwargs) -> Iterable[ModelType]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    @guard("edit")
    def bulk_upsert(self, df: pd.DataFrame) -> None:
        return super().bulk_upsert(df)
