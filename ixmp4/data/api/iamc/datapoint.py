from datetime import datetime
from typing import ClassVar, Iterable

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
    enumeration_method = "PATCH"

    def list(self, *args, **kwargs) -> Iterable[DataPoint]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[DataPoint] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)

    def bulk_upsert(self, df: pd.DataFrame) -> None:
        super().bulk_upsert(df)

    def bulk_delete(self, df: pd.DataFrame) -> None:
        super().bulk_delete(df)
