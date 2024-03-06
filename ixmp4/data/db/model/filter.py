from ixmp4 import db
from ixmp4.data.db import filters as base
from ixmp4.data.db.iamc.timeseries import TimeSeries
from ixmp4.data.db.run.model import Run
from ixmp4.db import filters, utils

from . import Model


class BaseIamcFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    def join_datapoints(self, exc: db.sql.Select, session=None):
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=Run.model__id == Model.id)

        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=TimeSeries.run__id == Run.id)

        return exc


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, Run.model)
        return exc

    model: base.ModelFilter = filters.Field(default=None, exclude=True)


class IamcModelFilter(base.ModelFilter, BaseIamcFilter, metaclass=filters.FilterMeta):
    region: base.RegionFilter
    variable: base.VariableFilter
    unit: base.UnitFilter
    run: RunFilter = filters.Field(
        default=RunFilter(id=None, version=None, is_default=True)
    )

    def join(self, exc, session=None):
        return super().join_datapoints(exc, session)


class ModelFilter(base.ModelFilter, BaseIamcFilter, metaclass=filters.FilterMeta):
    iamc: IamcModelFilter | filters.Boolean

    def filter_iamc(self, exc, c, v, session=None):
        if v is None:
            return exc

        if v is True:
            return self.join_datapoints(exc, session)
        else:
            ids = self.join_datapoints(db.select(Model.id), session)
            exc = exc.where(~Model.id.in_(ids))
            return exc

    def join(self, exc, **kwargs):
        return exc
