from typing import Any

import pandera.pandas as pa
import pandera.typing as pat


class UpsertRunMetaFrameSchema(pa.DataFrameModel):
    run__id: pat.Series[pa.Int] = pa.Field(coerce=True)
    key: pat.Series[pa.String] = pa.Field(coerce=True)
    value: pat.Series[Any] = pa.Field()


class DeleteRunMetaFrameSchema(pa.DataFrameModel):
    run__id: pat.Series[pa.Int] = pa.Field(coerce=True)
    key: pat.Series[pa.String] = pa.Field(coerce=True)
