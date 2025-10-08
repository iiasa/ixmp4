from datetime import datetime
from typing import Annotated, Any, TypedDict

import pandas as pd
import pydantic as pyd
from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema


class DataFrameDict(TypedDict):
    index: list[int] | list[str]
    columns: list[str]
    dtypes: list[str]
    data: list[
        list[
            bool
            | datetime
            | int
            | float
            | str
            | datetime
            | dict[str, Any]
            | list[float]
            | list[int]
            | list[str]
            | None
        ]
    ]


class DataFrameTypeAdapter(pyd.BaseModel):
    index: list[int] | list[str] | None = pyd.Field(None)
    columns: list[str] | None
    dtypes: list[str] | None
    data: (
        list[
            list[
                bool
                | datetime
                | int
                | float
                | str
                | dict[str, Any]
                | list[float]
                | list[int]
                | list[str]
                | None
            ]
        ]
        | None
    )

    model_config = pyd.ConfigDict(json_encoders={pd.Timestamp: lambda x: x.isoformat()})

    @pyd.model_validator(mode="before")
    @classmethod
    def validate(cls, df: pd.DataFrame | DataFrameDict) -> DataFrameDict:
        return cls.df_to_dict(df) if isinstance(df, pd.DataFrame) else df

    @classmethod
    def df_to_dict(cls, df: pd.DataFrame) -> DataFrameDict:
        columns = []
        dtypes = []
        for c in df.columns:
            columns.append(c)
            dtypes.append(df[c].dtype.name)

        return DataFrameDict(
            index=df.index.to_list(),
            columns=columns,
            dtypes=dtypes,
            data=df.values.tolist(),
        )

    def to_pandas(self) -> pd.DataFrame:
        df = pd.DataFrame(
            index=self.index or None,
            columns=self.columns,
            data=self.data,
        )
        if self.columns and self.dtypes:
            for c, dt in zip(self.columns, self.dtypes):
                # there seems to be a type incompatbility between StrDtypeArg and str
                df[c] = df[c].astype(dt)  # type: ignore[call-overload]
        return df


class _SerializableDataFrameAnnotation:
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        _handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        return DataFrameTypeAdapter.__get_pydantic_core_schema__(_source_type, _handler)

    @classmethod
    def __get_pydantic_json_schema__(
        cls, _core_schema: core_schema.CoreSchema, _handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        # Use the same schema that would be used for `int`
        return DataFrameTypeAdapter.__get_pydantic_json_schema__(_core_schema, _handler)


SerializableDataFrame = Annotated[pd.DataFrame, _SerializableDataFrameAnnotation]
