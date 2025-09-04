from typing import Any

from ixmp4 import db
from ixmp4.data.db import filters as base
from ixmp4.data.db.iamc.timeseries import TimeSeries
from ixmp4.data.db.run.model import Run
from ixmp4.db import Session, filters, typing_column, utils

from . import Model


class BaseIamcFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    def join_datapoints(
        self, exc: db.sql.Select[tuple[Model]], session: db.Session | None = None
    ) -> db.sql.Select[tuple[Model]]:
        if not utils.is_joined(exc, Run):
            # This looks like any Select coming in here must have Model in it
            exc = exc.join(Run, onclause=Run.model__id == Model.id)

        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=TimeSeries.run__id == Run.id)

        return exc


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    def join(
        self, exc: db.sql.Select[tuple[Model]], session: Session | None = None
    ) -> db.sql.Select[tuple[Model]]:
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, Run.model)
        return exc

    model: base.ModelFilter | None = filters.Field(default=None, exclude=True)


class IamcModelFilter(
    base.ModelFilter,
    BaseIamcFilter,
    metaclass=filters.FilterMeta,
):
    region: base.RegionFilter | None = filters.Field(None)
    variable: base.VariableFilter | None = filters.Field(None)
    unit: base.UnitFilter | None = filters.Field(None)
    run: RunFilter = filters.Field(
        default=RunFilter(id=None, version=None, is_default=True)
    )

    _remote_filters = {"run", "region", "variable", "unit"}
    _remote_path = [
        {
            "target_model": Run,
            "fk_attr": "model__id",
            "source_model": Model,
            "pk_attr": "id",
        },
        {
            "target_model": TimeSeries,
            "fk_attr": "run__id",
            "source_model": Run,
            "pk_attr": "id",
        },
    ]

    def join(
        self, exc: db.sql.Select[tuple[Model]], session: db.Session | None = None
    ) -> db.sql.Select[tuple[Model]]:
        if self._should_use_subquery_optimization():
            return exc

        return super().join_datapoints(exc, session)


class ModelFilter(base.ModelFilter, BaseIamcFilter, metaclass=filters.FilterMeta):
    iamc: IamcModelFilter | filters.Boolean

    def filter_iamc(
        self,
        exc: db.sql.Select[tuple[Model]],
        c: typing_column[Any],
        v: bool | None,
        session: db.Session | None = None,
    ) -> db.sql.Select[tuple[Model]]:
        if v is None:
            return exc

        if v is True:
            return self.join_datapoints(exc, session)
        else:
            ids = self.join_datapoints(db.select(Model.id), session)
            exc = exc.where(~Model.id.in_(ids))
            return exc

    def join(
        self, exc: db.sql.Select[tuple[Model]], session: Session | None = None
    ) -> db.sql.Select[tuple[Model]]:
        return exc
