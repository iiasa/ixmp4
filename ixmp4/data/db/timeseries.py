from collections.abc import Mapping
from typing import Any, ClassVar, Generic, TypeVar

import pandas as pd
from sqlalchemy.ext.declarative import AbstractConcreteBase
from sqlalchemy.orm.decl_api import declared_attr
from sqlalchemy.orm.exc import NoResultFound

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

from ixmp4 import db
from ixmp4.data import abstract
from ixmp4.data.db.model import Model
from ixmp4.data.db.run import Run
from ixmp4.db.filters import BaseFilter

from ..auth.decorators import guard
from . import base


class TimeSeries(AbstractConcreteBase, base.BaseModel):
    NotFound: ClassVar = abstract.TimeSeries.NotFound
    NotUnique: ClassVar = abstract.TimeSeries.NotUnique
    DeletionPrevented: ClassVar = abstract.TimeSeries.DeletionPrevented

    __abstract__ = True
    parameters: dict[str, Any] = {}

    @declared_attr
    def run__id(cls) -> db.MappedColumn[int]:
        return db.Column(
            "run__id",
            db.Integer,
            db.ForeignKey("run.id"),
            nullable=False,
            index=True,
        )

    @declared_attr
    def run(cls) -> db.Relationship["Run"]:
        # Mypy doesn't recognize cls.run__id as Mapped[int], even when type hinting as
        # such directly
        return db.relationship("Run", backref="time_series", foreign_keys=[cls.run__id])  # type: ignore[list-item]

    @property
    def run_id(self) -> int:
        return self.run__id


ModelType = TypeVar("ModelType", bound=TimeSeries)


class EnumerateTransactionsKwargs(abstract.annotations.HasPaginationArgs, total=False):
    pass


class EnumerateVersionsKwargs(abstract.annotations.HasPaginationArgs, total=False):
    transaction__id: int | None


class GetKwargs(TypedDict):
    _filter: BaseFilter


class SelectKwargs(TypedDict, total=False):
    _filter: BaseFilter
    run: dict[str, int]


class EnumerateKwargs(abstract.annotations.IamcTimeseriesFilter, total=False):
    _filter: BaseFilter
    join_parameters: bool | None


class CreateKwargs(TypedDict):
    run__id: int
    parameters: Mapping[str, Any]


class TimeSeriesRepository(
    base.Creator[ModelType],
    base.Retriever[ModelType],
    base.Enumerator[ModelType],
    base.BulkUpserter[ModelType],
    base.BulkDeleter[ModelType],
    base.VersionManager[ModelType],
    Generic[ModelType],
):
    def join_auth(
        self, exc: db.sql.Select[tuple[ModelType]]
    ) -> db.sql.Select[tuple[ModelType]]:
        if not db.utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=Run.id == self.model_class.run__id)
        if not db.utils.is_joined(exc, Model):
            exc = exc.join(Model, Run.model)

        return exc

    def add(self, run_id: int, parameters: Mapping[str, Any]) -> ModelType:
        time_series = self.model_class(run_id=run_id, **parameters)
        self.session.add(time_series)
        return time_series

    @guard("edit")
    def create(self, **kwargs: Unpack[CreateKwargs]) -> ModelType:
        return super().create(**kwargs)

    @guard("view")
    def get(self, run_id: int, **kwargs: Unpack[GetKwargs]) -> ModelType:
        exc = self.select(run={"id": run_id}, **kwargs)

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

    def select_joined_parameters(self) -> db.sql.Select[tuple[Any, ...]]:
        raise NotImplementedError

    def select(
        self,
        *,
        _exc: db.sql.Select[tuple[ModelType]] | None = None,
        join_parameters: bool | None = False,
        **kwargs: Unpack[SelectKwargs],
    ) -> db.sql.Select[tuple[ModelType]]:
        if _exc is not None:
            exc = _exc
        elif join_parameters:
            exc = self.select_joined_parameters()
        else:
            exc = db.select(self.model_class)

        return super().select(_exc=exc, **kwargs)

    @guard("view")
    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[ModelType]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    @guard("edit")
    def bulk_upsert(self, df: pd.DataFrame) -> None:
        return super().bulk_upsert(df)
