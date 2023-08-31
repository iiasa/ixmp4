from typing import ClassVar, Iterable, Optional, Union, cast

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series
from sqlalchemy.exc import NoResultFound

from ixmp4 import db
from ixmp4.core.decorators import check_types
from ixmp4.core.exceptions import InvalidRunMeta
from ixmp4.data import abstract, types
from ixmp4.data.db.model import Model
from ixmp4.data.db.run import Run

from ..auth.decorators import guard
from . import base


class RemoveRunMetaEntryFrameSchema(pa.SchemaModel):
    key: Series[pa.String] = pa.Field(coerce=True)
    run__id: Series[pa.Int] = pa.Field(coerce=True)

    class Config:
        strict = True
        coerce = True


class AddRunMetaEntryFrameSchema(RemoveRunMetaEntryFrameSchema):
    value: Series[pa.Object] = pa.Field(coerce=True)


class UpdateRunMetaEntryFrameSchema(AddRunMetaEntryFrameSchema):
    id: Series[pa.Int] = pa.Field(coerce=True)


class RunMetaEntry(base.BaseModel):
    NotFound: ClassVar = abstract.RunMetaEntry.NotFound
    NotUnique: ClassVar = abstract.RunMetaEntry.NotUnique
    DeletionPrevented: ClassVar = abstract.RunMetaEntry.DeletionPrevented

    Type: ClassVar = abstract.RunMetaEntry.Type

    _column_map = {
        abstract.RunMetaEntry.Type.INT: "value_int",
        abstract.RunMetaEntry.Type.STR: "value_str",
        abstract.RunMetaEntry.Type.FLOAT: "value_float",
        abstract.RunMetaEntry.Type.BOOL: "value_bool",
    }

    __table_args__ = (
        db.UniqueConstraint(
            "run__id",
            "key",
        ),
    )
    updateable_columns = [
        "type",
        "value_int",
        "value_str",
        "value_float",
        "value_bool",
    ]

    run__id: types.Integer = db.Column(
        db.Integer,
        db.ForeignKey("run.id"),
        nullable=False,
        index=True,
    )
    run = db.relationship(
        "Run",
        backref="meta",
        foreign_keys=[run__id],
    )

    key: types.String = db.Column(db.String(1023), nullable=False)
    type: types.String = db.Column(db.String(20), nullable=False)

    value_int: types.Integer = db.Column(db.Integer, nullable=True)
    value_str: types.String = db.Column(db.String(1023), nullable=True)
    value_float: types.Float = db.Column(db.Float, nullable=True)
    value_bool: types.Boolean = db.Column(db.Boolean, nullable=True)

    @property
    def value(self) -> abstract.MetaValue:
        type_ = RunMetaEntry.Type(self.type)
        col = self._column_map[type_]
        return getattr(self, col)

    def __init__(self, *args, **kwargs) -> None:
        value = kwargs.pop("value")
        value_type = type(value)
        try:
            type_ = RunMetaEntry.Type.from_pytype(value_type)
            col = self._column_map[type_]
        except KeyError:
            raise InvalidRunMeta(
                f"Invalid type `{value_type}` for value of `RunMetaEntry`."
            )
        kwargs["type"] = type_
        kwargs[col] = value
        super().__init__(*args, **kwargs)


class RunMetaEntryRepository(
    base.Creator[RunMetaEntry],
    base.Enumerator[RunMetaEntry],
    base.BulkUpserter[RunMetaEntry],
    base.BulkDeleter[RunMetaEntry],
    abstract.RunMetaEntryRepository,
):
    model_class = RunMetaEntry

    def add(
        self, run__id: int, key: str, value: Union[str, int, bool, float]
    ) -> RunMetaEntry:
        if self.backend.auth_context is not None:
            self.backend.runs.check_access(
                {run__id},
                access_type="edit",
                is_default=None,
                default_only=False,
            )

        entry = RunMetaEntry(run__id=run__id, key=key, value=value)
        self.session.add(entry)
        return entry

    def check_df_access(self, df: pd.DataFrame):
        if self.backend.auth_context is not None:
            ts_ids = set(df["run__id"].unique().tolist())
            self.backend.runs.check_access(
                ts_ids,
                access_type="edit",
                is_default=None,
                default_only=False,
            )

    @guard("edit")
    def create(self, *args, **kwargs) -> RunMetaEntry:
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

        exc = self.select(
            run_ids=[run__id],
            keys=[key],
        )

        try:
            return self.session.execute(exc).scalar_one()
        except NoResultFound:
            raise RunMetaEntry.NotFound(
                run__id=run__id,
                key=key,
            )

    @guard("edit")
    def delete(self, id: int) -> None:
        if self.backend.auth_context is not None:
            try:
                pre_exc = db.select(RunMetaEntry).where(RunMetaEntry.id == id)
                meta = self.session.execute(pre_exc).scalar_one()
            except NoResultFound:
                raise RunMetaEntry.NotFound(
                    id=id,
                )
            self.backend.runs.check_access(
                {meta.run__id},
                access_type="edit",
                is_default=None,
                default_only=False,
            )

        exc = db.delete(RunMetaEntry).where(RunMetaEntry.id == id)

        try:
            self.session.execute(exc)
            self.session.commit()
        except NoResultFound:
            raise RunMetaEntry.NotFound(
                id=id,
            )

    def join_auth(self, exc: db.sql.Select) -> db.sql.Select:
        if not db.utils.is_joined(exc, Run):
            exc = exc.join(Run, RunMetaEntry.run)
        if not db.utils.is_joined(exc, Model):
            exc = exc.join(Model, Run.model)

        return super().join_auth(exc)

    def select(
        self,
        *,
        run_ids: Optional[Iterable[int]] = None,
        keys: Optional[Iterable[str]] = None,
        _access_type: str = "view",
    ) -> db.sql.Select:
        exc: db.sql.Select = db.select(RunMetaEntry)
        exc = self.apply_auth(exc, _access_type)

        if run_ids is not None:
            exc = exc.where(RunMetaEntry.run__id.in_(run_ids))

        if keys is not None:
            exc = exc.where(RunMetaEntry.key.in_(keys))

        return exc

    @guard("view")
    def list(self, *args, **kwargs) -> Iterable[RunMetaEntry]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(
        self,
        *args,
        _raw: bool | None = False,
        **kwargs,
    ) -> pd.DataFrame:
        if _raw:
            return super().tabulate(*args, **kwargs)
        df = super().tabulate(*args, **kwargs)

        if df.empty:
            return pd.DataFrame([], columns=["id", "run__id", "type", "key", "value"])

        def map_value_column(df: pd.DataFrame):
            type_str = df.name
            type_ = RunMetaEntry.Type(type_str)
            col = RunMetaEntry._column_map[type_]
            df["value"] = df[col]
            return df.drop(columns=RunMetaEntry._column_map.values())

        return df.groupby("type", group_keys=False).apply(map_value_column)

    @check_types
    @guard("edit")
    def bulk_upsert(self, df: DataFrame[AddRunMetaEntryFrameSchema]) -> None:
        self.check_df_access(df)
        df["type"] = df["value"].map(type).map(RunMetaEntry.Type.from_pytype)

        for type_, type_df in df.groupby("type"):
            type_ = cast(RunMetaEntry.Type, type_)
            col = RunMetaEntry._column_map[type_]
            null_cols = set(RunMetaEntry._column_map.values()) - set([col])
            type_df["type"] = type_df["type"].map(lambda x: x.value)
            type_df = type_df.rename(columns={"value": col})

            # ensure all other columns are overwritten
            for nc in null_cols:
                type_df[nc] = None

            super().bulk_upsert(type_df)

    @check_types
    @guard("edit")
    def bulk_delete(self, df: DataFrame[RemoveRunMetaEntryFrameSchema]) -> None:
        self.check_df_access(df)
        return super().bulk_delete(df)
