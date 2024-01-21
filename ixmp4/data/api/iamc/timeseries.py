from typing import ClassVar, Mapping

import pandas as pd

from ixmp4.data import abstract

from .. import base


class TimeSeries(base.BaseModel):
    NotFound: ClassVar = abstract.TimeSeries.NotFound
    NotUnique: ClassVar = abstract.TimeSeries.NotUnique
    DeletionPrevented: ClassVar = abstract.TimeSeries.DeletionPrevented

    id: int
    run__id: int
    parameters: Mapping


class TimeSeriesRepository(
    base.Creator[TimeSeries],
    base.Retriever[TimeSeries],
    base.Enumerator[TimeSeries],
    base.BulkUpserter[TimeSeries],
    abstract.TimeSeriesRepository,
):
    model_class = TimeSeries
    prefix = "iamc/timeseries/"

    def create(self, run_id: int, parameters: Mapping) -> TimeSeries:
        return super().create(run_id=run_id, parameters=parameters)

    def get(self, run_id: int, parameters: Mapping) -> TimeSeries:
        return super().get(run_ids=[run_id], parameters=parameters)

    def enumerate(self, **kwargs) -> list[TimeSeries] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(self, **kwargs) -> list[TimeSeries]:
        return super()._list(json=kwargs)

    def tabulate(self, join_parameters: bool | None = None, **kwargs) -> pd.DataFrame:
        return super()._tabulate(
            json=kwargs, params={"join_parameters": join_parameters}
        )

    def get_by_id(self, id: int) -> TimeSeries:
        res = self._get_by_id(id)
        return TimeSeries(**res)

    def bulk_upsert(self, df: pd.DataFrame, create_related: bool = False) -> None:
        super().bulk_upsert(df, create_related=create_related)
