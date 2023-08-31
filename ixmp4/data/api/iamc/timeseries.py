from typing import ClassVar, Iterable, Mapping

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

    def list(self, *args, **kwargs) -> Iterable[TimeSeries]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[TimeSeries] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)

    def get_by_id(self, id: int) -> TimeSeries:
        res = self._get_by_id(id)
        return TimeSeries(**res)

    def bulk_upsert(self, df: pd.DataFrame, create_related: bool = False) -> None:
        super().bulk_upsert(df, create_related=create_related)
