from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

import pandas as pd
import pandera as pa
from pandera.pandas import DataFrameModel
from pandera.typing import DataFrame, Series
from sqlalchemy.exc import NoResultFound

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

from ixmp4 import db
from ixmp4.core.decorators import check_types
from ixmp4.core.exceptions import InvalidRunMeta
from ixmp4.data import abstract
from ixmp4.data.abstract.meta import EnumerateKwargs as AbstractEnumerateKwargs
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db.model import Model
from ixmp4.data.db.run import Run
from ixmp4.data.db.scenario import Scenario
from ixmp4.db.filters import BaseFilter

from .. import base
from .model import RunMetaEntry

ILLEGAL_META_KEYS = {"model", "scenario", "id", "version", "is_default"}


class RemoveRunMetaEntryFrameSchema(DataFrameModel):
    key: Series[pa.String] = pa.Field(coerce=True)
    run__id: Series[pa.Int] = pa.Field(coerce=True)

    class Config:
        strict = True
        coerce = True


class AddRunMetaEntryFrameSchema(RemoveRunMetaEntryFrameSchema):
    value: Series[pa.Object] = pa.Field(coerce=True)


class UpdateRunMetaEntryFrameSchema(AddRunMetaEntryFrameSchema):
    id: Series[pa.Int] = pa.Field(coerce=True)


class EnumerateKwargs(AbstractEnumerateKwargs, total=False):
    _filter: BaseFilter


class CreateKwargs(TypedDict, total=False):
    run__id: int
    key: str
    value: abstract.annotations.PrimitiveTypes


class RunMetaEntryRepository(
    base.Creator[RunMetaEntry],
    base.Enumerator[RunMetaEntry],
    base.BulkUpserter[RunMetaEntry],
    base.BulkDeleter[RunMetaEntry],
    base.VersionManager[RunMetaEntry],
    abstract.RunMetaEntryRepository,
):
    model_class = RunMetaEntry

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)

        from .filter import RunMetaEntryFilter

        self.filter_class = RunMetaEntryFilter

    def add(
        self, run__id: int, key: str, value: abstract.annotations.PrimitiveTypes
    ) -> RunMetaEntry:
        if self.backend.auth_context is not None:
            self.backend.runs.check_access(
                {run__id},
                access_type="edit",
                is_default=None,
                default_only=False,
            )

        if key in ILLEGAL_META_KEYS:
            raise InvalidRunMeta("Illegal meta key: " + key)
        entry = RunMetaEntry(run__id=run__id, key=key, value=value)
        self.session.add(entry)
        return entry

    def check_df_access(self, df: pd.DataFrame) -> None:
        if self.backend.auth_context is not None:
            ts_ids = cast(set[int], set(df["run__id"].unique().tolist()))
            self.backend.runs.check_access(
                ts_ids,
                access_type="edit",
                is_default=None,
                default_only=False,
            )

    @guard("edit")
    def create(
        self, *args: abstract.annotations.PrimitiveTypes, **kwargs: Unpack[CreateKwargs]
    ) -> RunMetaEntry:
        return super().create(*args, **kwargs)

    @guard("view")
    def get(self, run__id: int, key: str) -> RunMetaEntry:
        if self.backend.auth_context is not None:
            self.backend.runs.check_access(
                {run__id},
                access_type="view",
                is_default=None,
                default_only=False,
            )

        exc = self.select(run_id=run__id, key=key)

        try:
            return self.session.execute(exc).scalar_one()
        except NoResultFound:
            raise RunMetaEntry.NotFound(run__id=run__id, key=key)

    @guard("edit")
    def delete(self, id: int) -> None:
        exc = db.select(RunMetaEntry).where(RunMetaEntry.id == id)

        try:
            meta = self.session.execute(exc).scalar_one()
        except NoResultFound:
            raise RunMetaEntry.NotFound(id=id)

        if self.backend.auth_context is not None:
            self.backend.runs.check_access(
                {meta.run__id},
                access_type="edit",
                is_default=None,
                default_only=False,
            )

        self.session.delete(meta)
        self.session.commit()

    def join_auth(
        self, exc: db.sql.Select[tuple[RunMetaEntry]]
    ) -> db.sql.Select[tuple[RunMetaEntry]]:
        if not db.utils.is_joined(exc, Run):
            exc = exc.join(Run, RunMetaEntry.run)
        if not db.utils.is_joined(exc, Model):
            exc = exc.join(Model, Run.model)

        return super().join_auth(exc)

    def select_with_run_index(self) -> db.sql.Select[tuple[str, str, int, Any]]:
        _exc = db.select(
            Model.name.label("model_name"),
            Scenario.name.label("scenario_name"),
            Run.version.label("version"),
            self.bundle,
        ).select_from(self.model_class)

        return (
            _exc.join(Run, Run.id == RunMetaEntry.run__id)
            .join(Model, onclause=Model.id == Run.model__id)
            .join(Scenario, onclause=Scenario.id == Run.scenario__id)
        )

    @guard("view")
    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[RunMetaEntry]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(
        self,
        join_run_index: bool = False,
        _raw: bool | None = False,
        **kwargs: Unpack[EnumerateKwargs],
    ) -> pd.DataFrame:
        if _raw:
            return super().tabulate(**kwargs)

        if join_run_index:
            _exc = self.select_with_run_index()
            df = super().tabulate(_exc=_exc, **kwargs)
            df.drop(columns="run__id", inplace=True)
            df.rename(
                columns={"model_name": "model", "scenario_name": "scenario"},
                inplace=True,
            )
            index_columns = ["model", "scenario", "version"]
        else:
            df = super().tabulate(**kwargs)
            index_columns = ["run__id"]

        if df.empty:
            return pd.DataFrame(
                [], columns=index_columns + ["id", "dtype", "key", "value"]
            )

        return self.merge_value_columns(df)

    def merge_value_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        def map_value_column(df: pd.DataFrame) -> pd.DataFrame:
            type_str = df.name
            type_ = RunMetaEntry.Type(type_str)
            col = RunMetaEntry._column_map[type_]
            df["value"] = df[col]
            df["dtype"] = type_str
            return df.drop(columns=RunMetaEntry._column_map.values())

        # ensure compatibility with pandas y 2.2
        # TODO remove legacy-handling when dropping support for pandas < 2.2
        apply_args = (
            dict()
            if pd.__version__[0:3] in ["2.0", "2.1"]
            else dict(include_groups=False)
        )

        return df.groupby("dtype", group_keys=False).apply(
            map_value_column, **apply_args
        )

    @check_types
    @guard("edit")
    def bulk_upsert(self, df: DataFrame[AddRunMetaEntryFrameSchema]) -> None:
        if illegal_keys := (set(df.key.values) & ILLEGAL_META_KEYS):
            raise InvalidRunMeta("Illegal meta key(s): " + ", ".join(illegal_keys))

        self.check_df_access(df)
        df["dtype"] = df["value"].map(type).map(RunMetaEntry.Type.from_pytype)

        for type_, type_df in df.groupby("dtype"):
            # This cast should always be a no-op
            col = RunMetaEntry._column_map[cast(str, type_)]
            null_cols = set(RunMetaEntry._column_map.values()) - set([col])
            type_df["dtype"] = type_df["dtype"].map(lambda x: x.value)  # type: ignore[union-attr]
            type_df = type_df.rename(columns={"value": col})

            # ensure all other columns are overwritten
            for nc in null_cols:
                type_df[nc] = None

            super().bulk_upsert(type_df)

    @check_types
    @guard("edit")
    def bulk_delete(self, df: DataFrame[RemoveRunMetaEntryFrameSchema]) -> None:
        self.check_df_access(df)
        super().bulk_delete(df)

    @guard("view")
    def tabulate_versions(
        self, /, **kwargs: Unpack[abstract.annotations.TabulateRunMetaVersionsKwargs]
    ) -> pd.DataFrame:
        return super().tabulate_versions(**kwargs)
