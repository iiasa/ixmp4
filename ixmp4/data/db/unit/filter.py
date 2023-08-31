from typing import Optional, Union

from ixmp4 import db
from ixmp4.data.db import filters as base
from ixmp4.data.db.iamc.datapoint import get_datapoint_model
from ixmp4.data.db.iamc.measurand import Measurand
from ixmp4.data.db.iamc.timeseries import TimeSeries
from ixmp4.db import filters, utils

from . import Unit


class BaseIamcFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    def join_datapoints(self, exc, session=None):
        if not utils.is_joined(exc, Measurand):
            exc = exc.join(Measurand, Measurand.unit__id == Unit.id)

        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(
                TimeSeries, onclause=TimeSeries.measurand__id == Measurand.id
            )

        model = get_datapoint_model(session)
        if not utils.is_joined(exc, model):
            exc = exc.join(model, onclause=model.time_series__id == TimeSeries.id)
        return exc


class SimpleIamcUnitFilter(
    base.UnitFilter, BaseIamcFilter, metaclass=filters.FilterMeta
):
    def join(self, exc, session=None):
        return super().join_datapoints(exc, session)


class IamcUnitFilter(base.UnitFilter, BaseIamcFilter, metaclass=filters.FilterMeta):
    variable: base.VariableFilter | None
    region: base.RegionFilter | None
    run: base.RunFilter | None = filters.Field(
        default=base.RunFilter(id=None, version=None)
    )

    def join(self, exc, session=None):
        return super().join_datapoints(exc, session)


class UnitFilter(base.UnitFilter, BaseIamcFilter, metaclass=filters.FilterMeta):
    iamc: Optional[Union[IamcUnitFilter, filters.Boolean]]

    def filter_iamc(self, exc, c, v, session=None):
        if v is None:
            return exc

        if v is True:
            return self.join_datapoints(exc, session)
        else:
            ids = self.join_datapoints(db.select(Unit.id), session)
            exc = exc.where(~Unit.id.in_(ids))
            return exc

    def join(self, exc, **kwargs):
        return exc
