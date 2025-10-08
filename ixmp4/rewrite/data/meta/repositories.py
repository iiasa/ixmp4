from typing import Any, Collection, Mapping, Sequence, cast

import pandas as pd
import sqlalchemy as sa
from toolkit import db

from ixmp4.rewrite.data.model.db import Model
from ixmp4.rewrite.data.run.db import Run
from ixmp4.rewrite.data.scenario.db import Scenario
from ixmp4.rewrite.exceptions import (
    BadRequest,
    DeletionPrevented,
    NotFound,
    NotUnique,
    registry,
)

from .db import RunMetaEntry
from .dto import MetaValueType
from .filter import RunMetaEntryFilter
from .type import Type


@registry.register()
class RunMetaEntryNotFound(NotFound):
    pass


@registry.register()
class RunMetaEntryNotUnique(NotUnique):
    pass


@registry.register()
class RunMetaEntryDeletionPrevented(DeletionPrevented):
    pass


@registry.register()
class InvalidRunMeta(BadRequest):
    message = "Invalid run meta entry."


ILLEGAL_META_KEYS = {"model", "scenario", "id", "version", "is_default"}


class ItemRepository(db.r.ItemRepository[RunMetaEntry]):
    NotFound = RunMetaEntryNotFound
    NotUnique = RunMetaEntryNotUnique
    target = db.r.ModelTarget(RunMetaEntry)
    filter = db.r.Filter(RunMetaEntryFilter, RunMetaEntry)

    def create(self, run__id: int, key: str, value: MetaValueType) -> sa.Result[Any]:
        if key in ILLEGAL_META_KEYS:
            raise InvalidRunMeta("Illegal meta key: " + key)

        value_type = type(value)
        try:
            dtype = Type.from_pytype(value_type)
            col = Type.column_for_type(dtype)
        except KeyError:
            raise InvalidRunMeta(
                f"Invalid type `{value_type}` for value of `RunMetaEntry`."
            )
        values = {"run__id": run__id, "key": key, "dtype": dtype.value, col: value}
        return super().create(values)


class PandasRepository(db.r.PandasRepository):
    NotFound = RunMetaEntryNotFound
    NotUnique = RunMetaEntryNotUnique
    target = db.r.ExtendedTarget(
        RunMetaEntry,
        {
            "model": ((RunMetaEntry.run, Run.model), Model.name),
            "scenario": ((RunMetaEntry.run, Run.scenario), Scenario.name),
            "version": ((RunMetaEntry.run), Run.version),
        },
    )
    filter = db.r.Filter(RunMetaEntryFilter, RunMetaEntry)

    def merge_value_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        def map_value_column(df: pd.DataFrame) -> pd.DataFrame:
            type_str = df.name
            type_ = Type(type_str)
            col = Type.column_for_type(type_)
            df["value"] = df[col]
            df["dtype"] = type_str
            return df.drop(columns=Type.columns())

        type_df = df.groupby("dtype", group_keys=False)

        # ensure compatibility with pandas y 2.2
        # TODO remove legacy-handling when dropping support for pandas < 2.2

        if pd.__version__[0:3] in ["2.0", "2.1"]:
            df_with_value = type_df.apply(map_value_column)
        else:
            df_with_value = type_df.apply(map_value_column, include_groups=True)  # type: ignore[call-overload]

        return df_with_value.drop(
            columns=["value_int", "value_str", "value_float", "value_bool"]
        )

    def tabulate(
        self,
        values: Mapping[str, Any] | None = None,
        columns: Sequence[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> pd.DataFrame:
        df = super().tabulate(values, columns, limit, offset)
        return self.merge_value_columns(df)

    def upsert(
        self,
        df: pd.DataFrame,
        key: Collection[str] | None = None,
    ) -> Any:
        unique_keys = df["key"].unique()
        if illegal_keys := (set(unique_keys) & ILLEGAL_META_KEYS):
            raise InvalidRunMeta("Illegal meta key(s): " + ", ".join(illegal_keys))

        df["dtype"] = df["value"].map(type).map(Type.from_pytype)

        for type_, type_df in df.groupby("dtype"):
            col = Type.column_for_type(cast(Type, type_))
            type_df["dtype"] = type_df["dtype"].map(lambda x: x.value)
            type_df = type_df.rename(columns={"value": col})

            null_cols = set(Type.columns()) - set([col])
            # ensure all other columns are overwritten
            for nc in null_cols:
                type_df[nc] = None

            super().upsert(df, key)
