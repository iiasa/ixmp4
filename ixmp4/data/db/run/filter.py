from ixmp4 import db
from ixmp4.data.db import filters as base
from ixmp4.data.db.iamc.timeseries import TimeSeries
from ixmp4.data.db.run.model import Run
from ixmp4.db import filters, utils


class IamcRunFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    region: base.RegionFilter
    variable: base.VariableFilter
    unit: base.UnitFilter

    def join(self, exc, session=None):
        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=TimeSeries.run__id == Run.id)
        return exc


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    iamc: IamcRunFilter | filters.Boolean | None = None

    def join_datapoints(self, exc: db.sql.Select, session=None):
        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=TimeSeries.run__id == Run.id)
        return exc

    def filter_iamc(self, exc, c, v, session=None):
        if v is None:
            return exc

        if v is True:
            return self.join_datapoints(exc, session)
        else:
            ids = self.join_datapoints(db.select(Run.id), session)
            exc = exc.where(~Run.id.in_(ids))
            return exc

    def join(self, exc, **kwargs):
        return exc
