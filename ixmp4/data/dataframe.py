from datetime import datetime
from typing import Annotated, Any

import pandas as pd
import pydantic as pyd
from pydantic import PlainSerializer, PlainValidator, WithJsonSchema


def parse_ts(v: Any) -> pd.Timestamp:
    if isinstance(v, pd.Timestamp):
        return v

    s = str(v)
    return pd.Timestamp(s)


def serialize_ts(v: pd.Timestamp) -> str:
    return v.isoformat()


SerializablePandasTimestamp = Annotated[
    pd.Timestamp,
    PlainValidator(parse_ts),
    PlainSerializer(serialize_ts, return_type=dict, when_used="json"),
    WithJsonSchema(
        {"type": "string"},
        mode="serialization",
    ),
]


class DataFrameTypeAdapter(pyd.BaseModel):
    index: list[int] | list[str] | None = None
    columns: list[str] | None = None
    dtypes: list[str] | None = None
    data: (
        list[
            list[
                bool
                | int
                | float
                | str
                | dict[str, Any]
                | list[float]
                | list[int]
                | list[str]
                | SerializablePandasTimestamp
                | datetime
                | None
            ]
        ]
        | None
    )


def serialize_df(df: pd.DataFrame) -> dict[str, Any]:
    columns = []
    dtypes = []
    for c in df.columns:
        columns.append(c)
        dtypes.append(df[c].dtype.name)

    adapter = DataFrameTypeAdapter(
        index=df.index.to_list(),
        columns=columns,
        dtypes=dtypes,
        data=df.replace({pd.NA: None}).values.tolist(),
    )
    return adapter.model_dump()


def parse_df(val: Any, *args: Any, **kwargs: Any) -> pd.DataFrame:
    if isinstance(val, pd.DataFrame):
        return val
    if isinstance(val, dict):
        dtypes = val.pop("dtypes", None)
        columns = val.get("columns", None)
        try:
            df = pd.DataFrame(
                data=val["data"],
                columns=columns,
                index=val.pop("index", None),
            )
        except (TypeError, KeyError) as e:
            raise ValueError(f"Not a valid dataframe dict: {str(e)}")

        if dtypes and columns:
            for c, dt in zip(columns, dtypes):
                df[c] = df[c].astype(dt)

        return df
    raise ValueError(f"Cannot create `DataFrame` from `{str(type(val))}`.")


SerializableDataFrame = Annotated[
    pd.DataFrame,
    PlainValidator(parse_df),
    PlainSerializer(serialize_df, return_type=dict, when_used="json"),
    WithJsonSchema(
        DataFrameTypeAdapter.model_json_schema(mode="serialization"),
        mode="serialization",
    ),
]
