from datetime import datetime

# TODO Use `type` instead of TypeAlias when dropping Python 3.11
from typing import ClassVar, TypeAlias, cast

import pandas as pd

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import Unpack

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


JsonType: TypeAlias = dict[
    str,
    abstract.annotations.IntFilterAlias
    | dict[str, bool | abstract.annotations.DefaultFilterAlias]
    | None,
]


class DataPointRepository(
    base.Enumerator[DataPoint],
    base.BulkUpserter[DataPoint],
    base.BulkDeleter[DataPoint],
    base.VersionManager[DataPoint],
    abstract.DataPointRepository,
):
    model_class = DataPoint
    prefix = "iamc/datapoints/"

    def enumerate(
        self, **kwargs: Unpack[abstract.iamc.datapoint.EnumerateKwargs]
    ) -> list[DataPoint] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(
        self,
        join_parameters: bool | None = None,
        join_runs: bool = False,
        **kwargs: Unpack[abstract.iamc.datapoint.EnumerateKwargs],
    ) -> list[DataPoint]:
        return super()._list(
            json=cast(JsonType, kwargs),
            params={
                "join_parameters": join_parameters,
                "join_runs": join_runs,
            },
        )

    def tabulate(
        self,
        join_parameters: bool | None = None,
        join_runs: bool = False,
        **kwargs: Unpack[abstract.iamc.datapoint.EnumerateKwargs],
    ) -> pd.DataFrame:
        return super()._tabulate(
            json=cast(JsonType, kwargs),
            params={
                "join_parameters": join_parameters,
                "join_runs": join_runs,
            },
        )

    def bulk_upsert(self, df: pd.DataFrame) -> None:
        super().bulk_upsert(df)

    def bulk_delete(self, df: pd.DataFrame) -> None:
        super().bulk_delete(df)
