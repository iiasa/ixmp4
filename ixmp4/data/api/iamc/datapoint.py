from datetime import datetime
from typing import ClassVar

import pandas as pd

from ixmp4.data import abstract

from .. import base


class DataPoint(base.BaseModel):
    NotFound: ClassVar = abstract.DataPoint.NotFound
    NotUnique: ClassVar = abstract.DataPoint.NotUnique
    DeletionPrevented: ClassVar = abstract.DataPoint.DeletionPrevented

    id: int
    time_series__id: int
    value: float
    type: str

    step_category: str | None
    step_year: int | None
    step_datetime: datetime | None


class DataPointRepository(
    base.Enumerator[DataPoint],
    base.BulkUpserter[DataPoint],
    base.BulkDeleter[DataPoint],
    abstract.DataPointRepository,
):
    model_class = DataPoint
    prefix = "iamc/datapoints/"

    def enumerate(self, **kwargs) -> list[DataPoint] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(
        self,
        join_parameters: bool | None = None,
        join_runs: bool | None = None,
        **kwargs,
    ) -> list[DataPoint]:
        return super()._list(
            json=kwargs,
            params={
                "join_parameters": join_parameters,
                "join_runs": join_runs,
            },
        )

    def tabulate(
        self,
        join_parameters: bool | None = None,
        join_runs: bool | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        return super()._tabulate(
            json=kwargs,
            params={
                "join_parameters": join_parameters,
                "join_runs": join_runs,
            },
        )

    def bulk_upsert(self, df: pd.DataFrame, **kwargs) -> None:
        super().bulk_upsert(df)

    def bulk_delete(self, df: pd.DataFrame, **kwargs) -> None:
        super().bulk_delete(df)
