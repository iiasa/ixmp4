from typing import Any, Collection, Mapping, Sequence, cast

import pandas as pd
import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ExtendedTarget, ModelTarget

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.model.db import Model
from ixmp4.data.run.db import Run
from ixmp4.data.scenario.db import Scenario

from .db import RunMetaEntry, RunMetaEntryVersion
from .dto import MetaValueType
from .exceptions import InvalidRunMeta, RunMetaEntryNotFound, RunMetaEntryNotUnique
from .filter import RunMetaEntryFilter, RunMetaEntryVersionFilter
from .type import Type

ILLEGAL_META_KEYS = {"model", "scenario", "id", "version", "is_default"}


class RunMetaAuthRepository(AuthRepository[RunMetaEntry | RunMetaEntryVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_id_exc = self.select_permitted_run_ids(auth_ctx, platform)
        if run_id_exc is None:
            return exc
        return exc.where(RunMetaEntry.run__id.in_(run_id_exc))


class RunMetaVersionAuthRepository(AuthRepository[RunMetaEntryVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_id_exc = self.select_permitted_run_ids(auth_ctx, platform)
        if run_id_exc is None:
            return exc
        return exc.where(RunMetaEntryVersion.run__id.in_(run_id_exc))


class ItemRepository(RunMetaAuthRepository, BaseItemRepository[RunMetaEntry]):
    NotFound = RunMetaEntryNotFound
    NotUnique = RunMetaEntryNotUnique
    target = ModelTarget(RunMetaEntry)
    filter = Filter(RunMetaEntryFilter, RunMetaEntry)

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


class PandasRepository(RunMetaAuthRepository, BasePandasRepository):
    NotFound = RunMetaEntryNotFound
    NotUnique = RunMetaEntryNotUnique
    target: ModelTarget[RunMetaEntry | RunMetaEntryVersion] = ExtendedTarget(
        RunMetaEntry,
        {
            "model": ((RunMetaEntry.run, Run.model), Model.name),
            "scenario": ((RunMetaEntry.run, Run.scenario), Scenario.name),
            "version": ((RunMetaEntry.run), Run.version),
        },
    )
    filter = Filter(RunMetaEntryFilter, RunMetaEntry)

    def default_order_by(self, exc: sa.Select[Any]) -> sa.Select[Any]:
        return exc.order_by(RunMetaEntry.run__id, RunMetaEntry.key)

    def merge_value_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        def map_value_column(df: pd.DataFrame) -> pd.DataFrame:
            type_str = df.name
            type_ = Type(type_str)
            col = Type.column_for_type(type_)
            df["value"] = df[col].astype(Type.pd_dtype_for_type(type_))
            df["dtype"] = type_str
            return df.drop(columns=[col])

        type_df = df.groupby("dtype", group_keys=True)

        # ensure compatibility with pandas v2.2
        # TODO remove legacy-handling when dropping support for pandas < 2.2

        if pd.__version__[0:3] in ["2.0", "2.1"]:
            df_with_value = type_df.apply(map_value_column)
        else:
            df_with_value = type_df.apply(map_value_column, include_groups=False)  # type: ignore[call-overload]

        if df_with_value.empty:
            df_with_value["value"] = None
            df_with_value["dtype"] = None

        df_with_value.drop(
            columns=set(Type.columns()) & set(df_with_value.columns.to_list()),
            inplace=True,
        )
        return df_with_value.reset_index(drop=True)

    def tabulate(
        self,
        values: Mapping[str, Any] | None = None,
        columns: Sequence[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> SerializableDataFrame:
        df = super().tabulate(values, columns, limit, offset)
        return self.merge_value_columns(df)

    def upsert(
        self, df: SerializableDataFrame, key: Collection[str] | None = None
    ) -> Any:
        unique_keys = df["key"].unique()
        if illegal_keys := (set(unique_keys) & ILLEGAL_META_KEYS):
            raise InvalidRunMeta("Illegal meta key(s): " + ", ".join(illegal_keys))

        df["dtype"] = df["value"].map(type).map(Type.from_pytype)

        for type_, type_df in df.groupby("dtype"):
            col = Type.column_for_type(cast(Type, type_))
            type_df["dtype"] = type_df["dtype"].map(lambda x: str(x))
            type_df = type_df.rename(columns={"value": col})

            null_cols = set(Type.columns()) - set([col])
            # ensure all other columns are overwritten
            for nc in null_cols:
                type_df[nc] = None

            super().upsert(type_df, key)


class VersionRepository(RunMetaVersionAuthRepository, PandasRepository):
    NotFound = RunMetaEntryNotFound
    NotUnique = RunMetaEntryNotUnique
    target: ModelTarget[RunMetaEntry | RunMetaEntryVersion] = ModelTarget(
        RunMetaEntryVersion
    )
    filter = Filter(RunMetaEntryVersionFilter, RunMetaEntryVersion)

    def default_order_by(self, exc: sa.Select[Any]) -> sa.Select[Any]:
        return exc.order_by(RunMetaEntryVersion.run__id, RunMetaEntryVersion.key)
