from collections.abc import Mapping
from typing import Any, ClassVar, cast

import pandas as pd

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import Unpack

from ixmp4.data import abstract

from .. import base


class TimeSeries(base.BaseModel):
    NotFound: ClassVar = abstract.TimeSeries.NotFound
    NotUnique: ClassVar = abstract.TimeSeries.NotUnique
    DeletionPrevented: ClassVar = abstract.TimeSeries.DeletionPrevented

    id: int
    run__id: int
    parameters: Mapping[str, Any]


class TimeSeriesRepository(
    base.Creator[TimeSeries],
    base.Retriever[TimeSeries],
    base.Enumerator[TimeSeries],
    base.BulkUpserter[TimeSeries],
    base.VersionManager[TimeSeries],
    abstract.TimeSeriesRepository[abstract.TimeSeries],
):
    model_class = TimeSeries
    prefix = "iamc/timeseries/"

    def create(self, run_id: int, parameters: Mapping[str, Any]) -> TimeSeries:
        return super().create(run_id=run_id, parameters=parameters)

    def get(self, run_id: int, parameters: Mapping[str, Any]) -> TimeSeries:
        return super().get(run_ids=[run_id], parameters=parameters)

    def enumerate(
        self, **kwargs: Unpack[abstract.iamc.timeseries.EnumerateKwargs]
    ) -> list[TimeSeries] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(
        self, **kwargs: Unpack[abstract.iamc.timeseries.EnumerateKwargs]
    ) -> list[TimeSeries]:
        json = cast(abstract.annotations.IamcFilterAlias, kwargs)
        return super()._list(json=json)

    def tabulate(
        self,
        join_parameters: bool | None = None,
        **kwargs: Unpack[abstract.iamc.timeseries.EnumerateKwargs],
    ) -> pd.DataFrame:
        json = cast(abstract.annotations.IamcFilterAlias, kwargs)
        return super()._tabulate(json=json, params={"join_parameters": join_parameters})

    def get_by_id(self, id: int) -> TimeSeries:
        res = self._get_by_id(id)
        return TimeSeries(**res)

    def bulk_upsert(self, df: pd.DataFrame, create_related: bool = False) -> None:
        super().bulk_upsert(df, create_related=create_related)
