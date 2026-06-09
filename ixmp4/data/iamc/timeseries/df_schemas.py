import pandas as pd
import pandera.pandas as pa
import pandera.typing as pat


class UpsertTimeSeriesFrameSchema(pa.DataFrameModel):
    run__id: pat.Series[pa.Int] = pa.Field(coerce=True)

    region__id: pat.Series[pa.Int] | None = pa.Field(coerce=True)
    measurand__id: pat.Series[pa.Int] | None = pa.Field(coerce=True)
    unit__id: pat.Series[pa.Int] | None = pa.Field(coerce=True)
    variable__id: pat.Series[pa.Int] | None = pa.Field(coerce=True)

    region: pat.Series[pa.String] | None = pa.Field(coerce=True)
    unit: pat.Series[pa.String] | None = pa.Field(coerce=True)
    variable: pat.Series[pa.String] | None = pa.Field(coerce=True)

    @pa.dataframe_check
    @classmethod
    def check_has_region(cls, df: pd.DataFrame) -> bool:
        return "region" in df.columns or "region__id" in df.columns

    @pa.dataframe_check
    @classmethod
    def check_has_unit(cls, df: pd.DataFrame) -> bool:
        return (
            "unit" in df.columns
            or "unit__id" in df.columns
            or "measurand__id" in df.columns
        )

    @pa.dataframe_check
    @classmethod
    def check_has_variable(cls, df: pd.DataFrame) -> bool:
        return (
            "variable" in df.columns
            or "variable__id" in df.columns
            or "measurand__id" in df.columns
        )


class TabulateTimeSeriesFrameSchema(pa.DataFrameModel):
    run__id: pat.Series[pa.Int] = pa.Field(coerce=True)

    region: pat.Series[pa.String] = pa.Field(coerce=True)
    unit: pat.Series[pa.String] = pa.Field(coerce=True)
    variable: pat.Series[pa.String] = pa.Field(coerce=True)
