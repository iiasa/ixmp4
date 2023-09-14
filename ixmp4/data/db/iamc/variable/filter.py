from ixmp4.data.db import filters as base
from ixmp4.data.db.iamc.datapoint import get_datapoint_model
from ixmp4.data.db.iamc.timeseries import TimeSeries
from ixmp4.db import filters, utils

from ..measurand import Measurand
from . import Variable


class VariableFilter(base.VariableFilter, metaclass=filters.FilterMeta):
    region: base.RegionFilter | None
    variable: base.VariableFilter | None
    unit: base.UnitFilter | None
    run: base.RunFilter | None = filters.Field(
        default=base.RunFilter(id=None, version=None)
    )

    def join(self, exc, session=None):
        if not utils.is_joined(exc, Measurand):
            exc = exc.join(Measurand, Measurand.variable__id == Variable.id)

        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(
                TimeSeries, onclause=TimeSeries.measurand__id == Measurand.id
            )

        model = get_datapoint_model(session)
        if not utils.is_joined(exc, model):
            exc = exc.join(model, onclause=model.time_series__id == TimeSeries.id)
        return exc
