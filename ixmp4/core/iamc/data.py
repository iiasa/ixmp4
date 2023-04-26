from typing import Optional

import pandas as pd
import pandera as pa
from pandera.typing import Series

from ixmp4.data.abstract import DataPoint as DataPointModel

from ..base import BaseFacade
from ..run import RunModel as Run
from ..utils import substitute_type


def to_dimensionless(df: pd.DataFrame) -> pd.DataFrame:
    if "dimensionless" in df.unit:
        raise ValueError(
            "Unit name 'dimensionless' is reserved, use an empty string '' instead."
        )
    df.unit.replace({"": "dimensionless"}, inplace=True)
    return df


class RemoveDataPointFrameSchema(pa.SchemaModel):
    type: Optional[Series[pa.String]] = pa.Field(isin=[t for t in DataPointModel.Type])
    step_year: Optional[Series[pa.Int]] = pa.Field(coerce=True, nullable=True)
    step_datetime: Optional[Series[pa.DateTime]] = pa.Field(coerce=True, nullable=True)
    step_category: Optional[Series[pa.String]] = pa.Field(nullable=True)

    region: Optional[Series[pa.String]] = pa.Field(coerce=True)
    unit: Optional[Series[pa.String]] = pa.Field(coerce=True)
    variable: Optional[Series[pa.String]] = pa.Field(coerce=True)


class AddDataPointFrameSchema(RemoveDataPointFrameSchema):
    value: Series[pa.Float] = pa.Field(coerce=True)


class IamcData(BaseFacade):
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

    def _contract_parameters(self, df: pd.DataFrame) -> pd.DataFrame:
        ts_df = df[["region", "variable", "unit", "run__id"]].drop_duplicates()
        self.backend.iamc.timeseries.bulk_upsert(ts_df, create_related=True)

        ts_df = self.backend.iamc.timeseries.tabulate(
            run_ids=[self.run.id], join_parameters=True
        )
        ts_df = ts_df.rename(columns={"id": "time_series__id"})

        return pd.merge(
            df,
            ts_df,
            how="left",
            on=["run__id", "region", "unit", "variable"],
            suffixes=(None, "_y"),
        )

    def add(
        self,
        df: pd.DataFrame,
        type: Optional[DataPointModel.Type] = None,
    ):
        df = AddDataPointFrameSchema.validate(df)  # type:ignore
        df = to_dimensionless(df.copy())
        df["run__id"] = self.run.id
        df = self._contract_parameters(df)
        substitute_type(df, type)
        self.backend.iamc.datapoints.bulk_upsert(df)

    def remove(
        self,
        df: pd.DataFrame,
        type: Optional[DataPointModel.Type] = None,
    ):
        df = RemoveDataPointFrameSchema.validate(df)  # type:ignore
        df = to_dimensionless(df.copy())
        df["run__id"] = self.run.id
        df = self._contract_parameters(df)
        substitute_type(df, type)
        df = df.drop(columns=["unit", "variable", "region"])
        self.backend.iamc.datapoints.bulk_delete(df)

    def tabulate(self, **filters) -> pd.DataFrame:
        df = self.backend.iamc.datapoints.tabulate(
            run={"id": self.run.id}, join_parameters=True, **filters
        ).dropna(how="all", axis="columns")

        if not df.empty:
            df = df.drop(columns=["time_series__id"])
            df.unit = df.unit.replace({"dimensionless": ""})

        return df
