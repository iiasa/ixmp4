from datetime import datetime
from typing import Any, Generic, TypedDict, TypeVar

import pandas as pd
import pydantic as pyd


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


class DataFrame(pyd.BaseModel):
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


ResultsT = TypeVar("ResultsT")


class Pagination(pyd.BaseModel):
    limit: int
    offset: int


class EnumerationOutput(pyd.BaseModel, Generic[ResultsT]):
    results: ResultsT
    total: int
    pagination: Pagination
    model_config = pyd.ConfigDict(from_attributes=True, arbitrary_types_allowed=True)
