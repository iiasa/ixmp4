from typing import Optional

import pandas as pd
import pandera as pa
from pandera.typing import Series

from ixmp4.data.abstract import DataPoint as DataPointModel
from ixmp4.data.abstract import Run
from ixmp4.data.backend import Backend

from ..base import BaseFacade
from ..utils import substitute_type
from .variable import VariableRepository


class RemoveDataPointFrameSchema(pa.DataFrameModel):
    type: Optional[Series[pa.String]] = pa.Field(isin=[t for t in DataPointModel.Type])
    step_year: Optional[Series[pa.Int]] = pa.Field(coerce=True, nullable=True)
    step_datetime: Optional[Series[pa.DateTime]] = pa.Field(coerce=True, nullable=True)
    step_category: Optional[Series[pa.String]] = pa.Field(nullable=True)

    region: Optional[Series[pa.String]] = pa.Field(coerce=True)
    unit: Optional[Series[pa.String]] = pa.Field(coerce=True)
    variable: Optional[Series[pa.String]] = pa.Field(coerce=True)


class AddDataPointFrameSchema(RemoveDataPointFrameSchema):
    value: Series[pa.Float] = pa.Field(coerce=True)


MAP_STEP_COLUMN = {
    "ANNUAL": "step_year",
    "CATEGORICAL": "step_year",
    "DATETIME": "step_datetime",
}


def convert_to_std_format(df: pd.DataFrame, join_runs: bool) -> pd.DataFrame:
    df.rename(columns={"step_category": "subannual"}, inplace=True)

    if set(df.type.unique()).issubset(["ANNUAL", "CATEGORICAL"]):
        df.rename(columns={"step_year": "year"}, inplace=True)
        time_col = "year"
    else:

        def map_step_column(df: pd.Series):
            df["time"] = df[MAP_STEP_COLUMN[df.type]]
            return df

        df = df.apply(map_step_column, axis=1)
        time_col = "time"

    columns = ["model", "scenario", "version"] if join_runs else []
    columns += ["region", "variable", "unit"] + [time_col]
    if "subannual" in df.columns:
        columns += ["subannual"]
    return df[columns + ["value"]]


def normalize_df(df: pd.DataFrame, raw: bool, join_runs: bool) -> pd.DataFrame:
    if not df.empty:
        df = df.drop(columns=["time_series__id"])
        if raw is False:
            return convert_to_std_format(df, join_runs)
    return df


class RunIamcData(BaseFacade):
    """IAMC data.

    Parameters
    ----------
    backend : ixmp4.data.backend.Backend
        Data source backend.
    run : ixmp4.base.run.Run
        Model run.
    """

    run: Run

    def __init__(self, *args, run: Run, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.run = run

    def _get_or_create_ts(self, df: pd.DataFrame) -> pd.DataFrame:
        id_cols = ["region", "variable", "unit", "run__id"]
        # create set of unqiue timeseries (if missing)
        ts_df = df[id_cols].drop_duplicates()
        self.backend.iamc.timeseries.bulk_upsert(ts_df, create_related=True)

        # retrieve them again to get database ids
        ts_df = self.backend.iamc.timeseries.tabulate(
            join_parameters=True,
            run={"id": self.run.id, "default_only": False},
        )
        ts_df = ts_df.rename(columns={"id": "time_series__id"})

        # merge on the identity columns
        return pd.merge(
            df,
            ts_df,
            how="left",
            on=id_cols,
            suffixes=(None, "_y"),
        )  # tada, df with 'time_series__id' added from the database.

    def add(
        self,
        df: pd.DataFrame,
        type: Optional[DataPointModel.Type] = None,
    ):
        df = AddDataPointFrameSchema.validate(df)  # type:ignore
        df["run__id"] = self.run.id
        df = self._get_or_create_ts(df)
        substitute_type(df, type)
        self.backend.iamc.datapoints.bulk_upsert(df)

    def remove(
        self,
        df: pd.DataFrame,
        type: Optional[DataPointModel.Type] = None,
    ):
        df = RemoveDataPointFrameSchema.validate(df)  # type:ignore
        df["run__id"] = self.run.id
        df = self._get_or_create_ts(df)
        substitute_type(df, type)
        df = df.drop(columns=["unit", "variable", "region"])
        self.backend.iamc.datapoints.bulk_delete(df)

    def tabulate(
        self,
        *,
        variable: dict | None = None,
        region: dict | None = None,
        unit: dict | None = None,
        raw: bool = False,
    ) -> pd.DataFrame:
        df = self.backend.iamc.datapoints.tabulate(
            join_parameters=True,
            join_runs=False,
            run={"id": self.run.id, "default_only": False},
            variable=variable,
            region=region,
            unit=unit,
        ).dropna(how="all", axis="columns")

        return normalize_df(df, raw, False)


class PlatformIamcData(BaseFacade):
    variables: VariableRepository

    def __init__(self, _backend: Backend | None = None) -> None:
        self.variables = VariableRepository(_backend=_backend)
        super().__init__(_backend=_backend)

    def tabulate(self, *, join_runs: bool = True, raw: bool = False, **kwargs):
        df = self.backend.iamc.datapoints.tabulate(
            join_parameters=True, join_runs=join_runs, **kwargs
        ).dropna(how="all", axis="columns")

        return normalize_df(df, raw, join_runs)
