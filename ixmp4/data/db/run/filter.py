from ixmp4.data.db import filters as base
from ixmp4.data.db.iamc.datapoint import get_datapoint_model
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

        model = get_datapoint_model(session)
        if not utils.is_joined(exc, model):
            exc = exc.join(model, onclause=model.time_series__id == TimeSeries.id)
        return exc


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    iamc: IamcRunFilter | filters.Boolean

    def join(self, exc, **kwargs):
        return exc
