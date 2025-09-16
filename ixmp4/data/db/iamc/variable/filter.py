from ixmp4.data.db import filters as base
from ixmp4.data.db.base import SelectType
from ixmp4.data.db.iamc.timeseries import TimeSeries
from ixmp4.db import Session, filters, utils

from ..measurand import Measurand
from . import Variable


class VariableFilter(base.VariableFilter, metaclass=filters.FilterMeta):
    region: base.RegionFilter | None = filters.Field(None)
    unit: base.UnitFilter | None = filters.Field(None)
    run: base.RunFilter = filters.Field(
        default=base.RunFilter(id=None, version=None, is_default=True)
    )

    _remote_filters = {"run", "region", "unit"}
    _remote_path = [
        {
            "target_model": Measurand,
            "fk_attr": "variable__id",
            "source_model": Variable,
            "pk_attr": "id",
        },
        {
            "target_model": TimeSeries,
            "fk_attr": "measurand__id",
            "source_model": Measurand,
            "pk_attr": "id",
        },
    ]

    def join(self, exc: SelectType, session: Session | None = None) -> SelectType:
        if self._should_use_subquery_optimization():
            return exc

        if not utils.is_joined(exc, Measurand):
            exc = exc.join(Measurand, Measurand.variable__id == Variable.id)

        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(
                TimeSeries, onclause=TimeSeries.measurand__id == Measurand.id
            )
        return exc
