from typing import TypeVar

import pandas as pd
import pandera as pa
from pandera.engines import pandas_engine
from pandera.pandas import DataFrameModel
from pandera.typing import Series

from ixmp4.data.abstract import DataPoint as DataPointModel


class RemoveDataPointFrameSchema(DataFrameModel):
    type: Series[pa.String] | None = pa.Field(isin=[t for t in DataPointModel.Type])
    step_year: Series[pa.Int] | None = pa.Field(coerce=True, nullable=True)
    step_datetime: Series[pandas_engine.DateTime] | None = pa.Field(
        coerce=True, nullable=True
    )
    step_category: Series[pa.String] | None = pa.Field(nullable=True)

    region: Series[pa.String] | None = pa.Field(coerce=True)
    unit: Series[pa.String] | None = pa.Field(coerce=True)
    variable: Series[pa.String] | None = pa.Field(coerce=True)


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
        T = TypeVar("T", bool, float, int, str)

        def map_step_column(df: "pd.Series[T]") -> "pd.Series[T]":
            df["time"] = df[MAP_STEP_COLUMN[str(df.type)]]
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
