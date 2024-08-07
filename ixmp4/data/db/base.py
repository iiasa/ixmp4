from __future__ import annotations

import logging
import sqlite3
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterable,
    Iterator,
    Tuple,
    TypeVar,
    cast,
)

import numpy as np
import pandas as pd
from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Bundle, DeclarativeBase, declared_attr
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.schema import Identity, MetaData

from ixmp4 import db
from ixmp4.core.exceptions import Forbidden, IxmpError, ProgrammingError
from ixmp4.data import abstract, types
from ixmp4.db import filters

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

logger = logging.getLogger(__name__)


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


@compiles(Identity, "sqlite")
def visit_identity(element, compiler, **kwargs):
    return text("")


class BaseModel(DeclarativeBase):
    NotFound: ClassVar[type[IxmpError]]
    NotUnique: ClassVar[type[IxmpError]]
    DeletionPrevented: ClassVar[type[IxmpError]]

    table_prefix: str = ""
    updateable_columns: ClassVar[list[str]] = []

    @declared_attr.directive
    def __tablename__(cls: "BaseModel") -> str:
        return str(cls.table_prefix + cls.__name__.lower())

    id: types.Integer = db.Column(
        db.Integer,
        Identity(always=False, on_null=True, start=1, increment=1),
        primary_key=True,
        info={"skip_autogenerate": True},
    )

    def __str__(self):
        return self.__class__.__name__


BaseModel.metadata = MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_N_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)


ModelType = TypeVar("ModelType", bound=BaseModel)


class BaseRepository(Generic[ModelType]):
    backend: "SqlAlchemyBackend"
    session: Session
    dialect: Dialect
    bundle: Bundle
    model_class: type[ModelType]

    def __init__(self, backend: "SqlAlchemyBackend", *args, **kwargs) -> None:
        self.backend = backend
        self.session = backend.session
        self.engine = backend.engine

        if self.session.bind is not None:
            self.dialect = self.session.bind.dialect
        else:
            raise ProgrammingError("Database session is closed.")

        self.bundle: Bundle = Bundle(
            self.model_class.__name__, *db.utils.get_columns(self.model_class).values()
        )
        super().__init__(*args, **kwargs)


class Retriever(BaseRepository[ModelType], abstract.Retriever):
    def get(self, *args, **kwargs) -> ModelType:
        raise NotImplementedError


class Creator(BaseRepository[ModelType], abstract.Creator):
    def add(self, *args, **kwargs) -> ModelType:
        raise NotImplementedError

    def create(self, *args, **kwargs) -> ModelType:
        model = self.add(*args, **kwargs)
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            raise self.model_class.NotUnique(*args)

        return model


class Deleter(BaseRepository[ModelType]):
    def delete(self, id: int):
        exc: db.sql.Delete = db.delete(self.model_class).where(
            self.model_class.id == id
        )

        try:
            self.session.execute(
                exc, execution_options={"synchronize_session": "fetch"}
            )
            self.session.commit()
        except NoResultFound:
            raise self.model_class.NotFound
        except IntegrityError:
            raise self.model_class.DeletionPrevented


class Selecter(BaseRepository[ModelType]):
    filter_class: type[filters.BaseFilter]

    def check_access(self, ids: set[int], access_type: str = "view", **kwargs):
        exc = self.select(
            _exc=db.select(db.func.count()).select_from(self.model_class),
            id__in=ids,
            _access_type=access_type,
            **kwargs,
        )
        num_permitted_ids = self.session.execute(exc).scalar()
        num_ids = len(ids)
        if not num_permitted_ids == num_ids:
            logger.debug(
                f"Permission check failed {num_permitted_ids}/{num_ids} objects "
                "permitted."
            )
            raise Forbidden(f"Permission check failed for access type '{access_type}'.")

    def join_auth(self, exc: db.sql.Select) -> db.sql.Select:
        return exc

    def apply_auth(self, exc: db.sql.Select, access_type: str) -> db.sql.Select:
        if self.backend.auth_context is not None:
            if not self.backend.auth_context.is_managed:
                exc = self.join_auth(exc)
            exc = self.backend.auth_context.apply(access_type, exc)
        return exc

    def select(
        self,
        _filter: filters.BaseFilter | None = None,
        _exc: db.sql.Select | None = None,
        _access_type: str = "view",
        _post_filter: Callable[[db.sql.Select], db.sql.Select] | None = None,
        _skip_filter: bool = False,
        **kwargs,
    ) -> db.sql.Select:
        if self.filter_class is None:
            cls_name = self.__class__.__name__
            raise NotImplementedError(
                f"Provide `{cls_name}.filter_class` or reimplement `{cls_name}.select`."
            )

        if _exc is None:
            _exc = db.select(self.model_class)

        _exc = self.apply_auth(_exc, _access_type)

        if _filter is not None and not _skip_filter:
            # for some reason checkers resolve the type of `_filter` to `Unknown`
            filter_instance: filters.BaseFilter = _filter
            _exc = filter_instance.join(_exc, session=self.session)
            _exc = filter_instance.apply(_exc, self.model_class, self.session)
        elif not _skip_filter:
            kwarg_filter = self.filter_class(**kwargs)
            _exc = kwarg_filter.join(_exc, session=self.session)
            _exc = kwarg_filter.apply(_exc, self.model_class, self.session)

        if _post_filter is not None:
            _exc = _post_filter(_exc)
        return _exc


class Lister(Selecter[ModelType]):
    def list(self, *args, **kwargs) -> list[ModelType]:
        _exc = self.select(*args, **kwargs)
        _exc = _exc.order_by(self.model_class.id.asc())
        result = self.session.execute(_exc).scalars().all()
        return list(result)


class Tabulator(Selecter[ModelType]):
    def tabulate(
        self,
        *args,
        _raw: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        _exc = self.select(*args, **kwargs)
        _exc = _exc.order_by(self.model_class.id.asc())

        if self.session.bind is not None:
            with self.engine.connect() as con:
                return pd.read_sql(_exc, con=con).replace([np.nan], [None])
        else:
            raise ProgrammingError("Database session is closed.")


class Enumerator(Lister[ModelType], Tabulator[ModelType]):
    def enumerate(
        self, *args, table: bool = False, **kwargs
    ) -> list[ModelType] | pd.DataFrame:
        if table:
            return self.tabulate(*args, **kwargs)
        else:
            return self.list(*args, **kwargs)

    def paginate(
        self,
        *args,
        limit: int = 1000,
        offset: int = 0,
        **kwargs,
    ) -> list[ModelType] | pd.DataFrame:
        return self.enumerate(
            *args, **kwargs, _post_filter=lambda e: e.offset(offset).limit(limit)
        )

    def count(
        self,
        **kwargs,
    ) -> int:
        _exc = self.select(
            _exc=db.select(db.func.count(self.model_class.id.distinct())),
            **kwargs,
        )
        return self.session.execute(_exc).scalar_one()


class BulkOperator(Tabulator[ModelType]):
    merge_suffix: str = "_y"

    @property
    def max_list_length(self) -> int:
        return 50_000

    def merge_existing(
        self, df: pd.DataFrame, existing_df: pd.DataFrame
    ) -> pd.DataFrame:
        columns = db.utils.get_columns(self.model_class)
        primary_key_columns = db.utils.get_pk_columns(self.model_class)
        on = (
            (
                set(existing_df.columns) & set(df.columns) & set(columns.keys())
            )  # all cols which exist in both dfs and the db model
            - set(self.model_class.updateable_columns)  # no updateable columns
            - set(primary_key_columns)  # no pk columns
        )  # = all columns that are constant and provided during creation

        return df.merge(
            existing_df,
            how="left",
            on=list(on),
            suffixes=(None, self.merge_suffix),
        )

    def drop_merge_artifacts(
        self, df: pd.DataFrame, extra_columns: list[str] | None = None
    ) -> pd.DataFrame:
        if extra_columns is None:
            extra_columns = []

        existing_columns = [
            str(col) for col in df.columns if col.endswith(self.merge_suffix)
        ]

        df = df.drop(columns=existing_columns + extra_columns)
        df = df.dropna(axis="columns", how="all")
        df = df.dropna()
        return df

    def split_by_max_unique_values(
        self, df: pd.DataFrame, columns: Iterable[str], mu: int
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df_len = len(df.index)
        chunk_size = df_len
        remaining_df = pd.DataFrame()

        if chunk_size <= mu:
            return df, remaining_df

        max_ = chunk_size
        chunk_df = df
        while True:
            max_ = max(chunk_df[c].nunique() for c in columns)
            if max_ <= mu:
                break
            chunk_size = int(np.floor((mu / max_) * chunk_size))
            chunk_df = df.iloc[:chunk_size, :]
            remaining_df = df.iloc[chunk_size:, :]

        return chunk_df, remaining_df

    def tabulate_existing(self, df: pd.DataFrame) -> pd.DataFrame:
        exc = db.select(self.model_class)
        foreign_columns = db.utils.get_foreign_columns(self.model_class)

        for col in foreign_columns:
            foreign_pks = df[col.name].unique().tolist()
            exc = exc.where(col.in_(foreign_pks))
        return self.tabulate(_exc=exc, _raw=True, _skip_filter=True)

    def yield_chunks(self, df: pd.DataFrame) -> Iterator[pd.DataFrame]:
        foreign_columns = db.utils.get_foreign_columns(self.model_class)
        foreign_names = [c.name for c in foreign_columns]
        remaining_df = df.sort_values(foreign_names)

        while not remaining_df.empty:
            chunk_df, remaining_df = self.split_by_max_unique_values(
                pd.DataFrame(remaining_df), foreign_names, self.max_list_length
            )
            yield pd.DataFrame(chunk_df)


class BulkUpserter(BulkOperator[ModelType]):
    def bulk_upsert(self, df: pd.DataFrame) -> None:
        if len(df.index) < self.max_list_length:
            self.bulk_upsert_chunk(df)
        else:
            for chunk_df in self.yield_chunks(df):
                self.bulk_upsert_chunk(pd.DataFrame(chunk_df))

    def bulk_upsert_chunk(self, df: pd.DataFrame) -> None:
        logger.debug(f"Starting `bulk_upsert_chunk` for {len(df)} rows.")
        columns = db.utils.get_columns(self.model_class)
        df = df[list(set(columns.keys()) & set(df.columns))]
        existing_df = self.tabulate_existing(df)
        if existing_df.empty:
            logger.debug(f"Inserting {len(df)} rows.")
            self.bulk_insert(df, skip_validation=True)
        else:
            df = self.merge_existing(df, existing_df)
            df["exists"] = np.where(pd.notnull(df["id"]), True, False)
            cond = []
            for col in self.model_class.updateable_columns:
                updated_col = col + self.merge_suffix
                if updated_col in df.columns:
                    # coerce to same type so the inequality
                    # operation works with pyarrow installed
                    df[updated_col] = df[updated_col].astype(df[col].dtype)
                    are_not_equal = df[col] != df[updated_col]
                    # extra check if both values are NA because NA == NA = NA
                    # in pandas with pyarrow
                    both_are_na = pd.isna(df[col]) & pd.isna(df[updated_col])
                    cond.append(~both_are_na | are_not_equal)

            df["differs"] = np.where(np.logical_or.reduce(cond), True, False)

            insert_df = self.drop_merge_artifacts(
                df.where(~df["exists"]), extra_columns=["id", "differs", "exists"]
            )
            update_df = self.drop_merge_artifacts(
                df.where(df["exists"] & df["differs"]),
                extra_columns=["differs", "exists"],
            )

            if not insert_df.empty:
                logger.debug(f"Inserting {len(insert_df)} rows.")
                self.bulk_insert(insert_df, skip_validation=True)
            if not update_df.empty:
                logger.debug(f"Updating {len(update_df)} rows.")
                self.bulk_update(update_df, skip_validation=True)

        self.session.commit()

    def bulk_insert(self, df: pd.DataFrame, **kwargs) -> None:
        # to_dict returns a more general list[Mapping[Hashable, Unknown]]
        if "id" in df.columns:
            raise ProgrammingError("You may not insert the 'id' column.")
        m = cast(list[dict[str, Any]], df.to_dict("records"))

        try:
            self.session.execute(
                db.insert(self.model_class),
                m,
                execution_options={"synchronize_session": False},
            )
        except IntegrityError as e:
            raise self.model_class.NotUnique(*e.args)

    def bulk_update(self, df: pd.DataFrame, **kwargs) -> None:
        # to_dict returns a more general list[Mapping[Hashable, Unknown]]
        m = cast(list[dict[str, Any]], df.to_dict("records"))
        self.session.execute(
            db.update(self.model_class),
            m,
            execution_options={"synchronize_session": False},
        )


class BulkDeleter(BulkOperator[ModelType]):
    def bulk_delete(self, df: pd.DataFrame) -> None:
        for chunk_df in self.yield_chunks(df):
            self.bulk_delete_chunk(chunk_df)

    def bulk_delete_chunk(self, df: pd.DataFrame) -> None:
        existing_df = self.tabulate_existing(df)
        if existing_df.empty:
            return

        df = self.merge_existing(df, existing_df)
        df["exists"] = np.where(pd.notnull(df["id"]), True, False)
        delete_df = df.where(df["exists"])
        delete_df = df[["id"]]
        excs = []
        for _, sdf in delete_df.groupby(delete_df.index // self.max_list_length):
            exc = db.delete(self.model_class).where(self.model_class.id.in_(sdf["id"]))
            excs.append(exc)

        for exc in excs:
            self.session.execute(exc, execution_options={"synchronize_session": False})

        self.session.commit()
