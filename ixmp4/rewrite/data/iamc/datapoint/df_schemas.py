import pandas as pd
import pandera.pandas as pa
import pandera.typing as pat

from ixmp4.rewrite.exceptions import InconsistentIamcType

from .type import Type, TypeColumnsDict


class BaseDataPointFrameSchema(pa.DataFrameModel):
    time_series__id: pat.Series[pa.Int] = pa.Field(coerce=True)
    type: pat.Series[pa.String] | None = pa.Field(isin=[str(t) for t in Type])
    step_year: pat.Series[pd.Int64Dtype] | None = pa.Field(coerce=True, nullable=True)
    step_datetime: pat.Series[pa.Timestamp] | None = pa.Field(
        coerce=True, nullable=True
    )
    step_category: pat.Series[pa.String] | None = pa.Field(nullable=True)

    @pa.dataframe_parser
    @classmethod
    def parse_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        if "type" not in df.columns:
            df = cls.infer_type(df)
        else:
            df = cls.validate_type(df)
        return df

    @classmethod
    def build_column_condition(
        cls, df: pd.DataFrame, dict_: TypeColumnsDict
    ) -> pd.Series | bool:
        cond = pd.Series(True, index=df.index)
        for col, val in dict_.items():
            if col in df.columns:
                cond &= pd.notnull(df[col]) if val else pd.isnull(df[col])
            else:
                if val:
                    return pd.Series(False, index=df.index)
        return cond

    @classmethod
    def infer_type(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(type=None)

        for type_ in Type:
            cols = Type.columns_for_type(type_)
            df.loc[cls.build_column_condition(df, cols), "type"] = str(type_)

        invalid_df = df[pd.isnull(df["type"])]
        if not invalid_df.empty:
            raise InconsistentIamcType(
                f"Could not infer iamc datapoint type for {len(invalid_df)} rows."
            )
        return df

    @classmethod
    def validate_type(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(valid=False)
        for type_ in Type:
            cols = Type.columns_for_type(type_)
            df.loc[cls.build_column_condition(df, cols), "valid"] = df["type"] == str(
                type_
            )

        invalid_df = df[~df["valid"]]
        if not invalid_df.empty:
            raise InconsistentIamcType(
                f"Invalid iamc datapoint type in {len(invalid_df)} rows."
            )

        return df.drop(columns=["valid"])


class DeleteDataPointFrameSchema(BaseDataPointFrameSchema):
    pass


class UpsertDataPointFrameSchema(BaseDataPointFrameSchema):
    value: pat.Series[pa.Float] = pa.Field(coerce=True)
