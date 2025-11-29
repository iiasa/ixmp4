from datetime import datetime
from typing import Any

import pandas as pd
import pydantic as pyd
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema, core_schema
from typing_extensions import NotRequired, TypedDict


class SerializablePandasTimestamp(pd.Timestamp):
    @pyd.model_validator(mode="before")
    @classmethod
    def validate_timestamp(cls, v: Any) -> pd.Timestamp:
        if isinstance(v, pd.Timestamp):
            return v

        s = str(v)
        return SerializablePandasTimestamp(s)

    @pyd.model_serializer(mode="plain")
    def serialize_model(self) -> str:
        return self.isoformat()

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type, handler: pyd.GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_before_validator_function(
            cls.validate_timestamp,
            core_schema.str_schema(),
            serialization=core_schema.plain_serializer_function_ser_schema(
                cls.serialize_model, return_schema=core_schema.str_schema()
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, _core_schema: CoreSchema, handler: pyd.GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        return handler(core_schema.str_schema())


class DataFrameDict(TypedDict):
    index: NotRequired[list[int] | list[str] | None]
    columns: NotRequired[list[str] | None]
    dtypes: NotRequired[list[str] | None]
    data: list[
        list[
            bool
            | int
            | float
            | str
            | dict[str, Any]
            | list[float]
            | list[int]
            | list[str]
            | pd.Timestamp
            | datetime
            | None
        ]
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


class SerializableDataFrame(pd.DataFrame):
    @pyd.model_validator(mode="wrap")
    @classmethod
    def validate_model(
        cls, v: pd.DataFrame | DataFrameDict, handler: pyd.ModelWrapValidatorHandler
    ) -> pd.DataFrame:
        if isinstance(v, pd.DataFrame):
            return v

        if isinstance(v, dict):
            dtypes = v.pop("dtypes", None)
            columns = v.get("columns", None)
            try:
                df = pd.DataFrame(**v)
            except TypeError as e:
                raise ValueError(f"Not a valid dataframe dict: {str(e)}")

            if dtypes and columns:
                for c, dt in zip(columns, dtypes):
                    # there seems to be a type incompatbility between StrDtypeArg and str
                    df[c] = df[c].astype(dt)  # type: ignore[call-overload]

            return df

    @pyd.model_serializer(mode="wrap")
    def serialize_model(
        self, handler: pyd.SerializerFunctionWrapHandler
    ) -> DataFrameDict:
        columns = []
        dtypes = []
        for c in self.columns:
            columns.append(c)
            dtypes.append(self[c].dtype.name)

        dict_ = DataFrameDict(
            index=self.index.to_list(),
            columns=columns,
            dtypes=dtypes,
            data=self.replace({pd.NA: None}).values.tolist(),
        )
        return DataFrameTypeAdapter(**dict_).model_dump()

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type, handler: pyd.GetCoreSchemaHandler
    ) -> CoreSchema:
        adapter_schema = handler(DataFrameTypeAdapter)
        return core_schema.no_info_wrap_validator_function(
            cls.validate_model,
            adapter_schema,
            serialization=core_schema.wrap_serializer_function_ser_schema(
                cls.serialize_model, schema=core_schema.any_schema()
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, _core_schema: CoreSchema, handler: pyd.GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        return handler(_core_schema)
