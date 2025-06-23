from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Protocol, TypeVar, cast

import numpy as np
import pandas as pd
from sqlalchemy import TextClause, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Bundle, DeclarativeBase, declared_attr
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.schema import Identity, MetaData
from sqlalchemy_continuum import make_versioned, transaction_class, version_class
from sqlalchemy_continuum.operation import Operation
from sqlalchemy_continuum.utils import (
    end_tx_column_name,
    get_versioning_manager,
    is_versioned,
    tx_column_name,
)

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import NotRequired, TypedDict, Unpack

from ixmp4 import db
from ixmp4.core.exceptions import Forbidden, IxmpError, ProgrammingError
from ixmp4.data import abstract, types
from ixmp4.data.abstract.annotations import (
    TabulateVersionsKwargs as TabulateVersionsKwargs,
)
from ixmp4.db import filters

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

logger = logging.getLogger(__name__)


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(
    dbapi_connection: sqlite3.Connection, connection_record: Any
) -> None:
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


@compiles(Identity, "sqlite")
def visit_identity(element: Any, compiler: Any, **kwargs: Any) -> TextClause:
    return text("")


make_versioned(user_cls=None)


class BaseModel(DeclarativeBase):
    __versioned__: dict[str, object]

    NotFound: ClassVar[type[IxmpError]]
    NotUnique: ClassVar[type[IxmpError]]
    DeletionPrevented: ClassVar[type[IxmpError]]

    table_prefix: str = ""
    updateable_columns: ClassVar[list[str]] = []

    @declared_attr.directive
    def __tablename__(cls: "BaseModel") -> str:
        return str(cls.table_prefix + cls.__name__.lower())

    id: types.Mapped[int] = db.Column(
        db.Integer,
        Identity(always=False, on_null=True, start=1, increment=1),
        primary_key=True,
        info={"skip_autogenerate": True},
    )

    def __str__(self) -> str:
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

SelectType = TypeVar("SelectType", bound=db.sql.Select[tuple[BaseModel, ...]])

ModelType = TypeVar("ModelType", bound=BaseModel)


class BaseRepository(Generic[ModelType]):
    backend: "SqlAlchemyBackend"
    session: Session
    dialect: Dialect
    bundle: Bundle[Any]
    model_class: type[ModelType]

    @cached_property
    def is_versioned(self) -> Any:
        return is_versioned(self.model_class)

    @cached_property
    def version_class(self) -> Any:
        return version_class(self.model_class)

    @cached_property
    def transaction_class(self) -> Any:
        return transaction_class(self.model_class)

    @cached_property
    def tx_column_name(self) -> Any:
        return tx_column_name(self.model_class)

    @cached_property
    def end_tx_column_name(self) -> Any:
        return end_tx_column_name(self.model_class)

    def __init__(self, backend: "SqlAlchemyBackend") -> None:
        self.backend = backend
        self.session = backend.session
        self.engine = backend.engine

        if self.session.bind is not None:
            self.dialect = self.session.bind.dialect
        else:
            raise ProgrammingError("Database session is closed.")

        self.bundle: Bundle[Any] = Bundle(
            self.model_class.__name__, *db.utils.get_columns(self.model_class).values()
        )

        super().__init__()


class Retriever(BaseRepository[ModelType], abstract.Retriever):
    def get(self, *args: Any, **kwargs: Any) -> ModelType:
        raise NotImplementedError


class CreateKwargs(TypedDict, total=False):
    dimension_id: int
    description: str
    name: str
    model_name: str
    scenario_name: str
    hierarchy: str
    run__id: int
    key: str
    value: abstract.annotations.PrimitiveTypes
    parameters: Mapping[str, Any]
    run_id: int
    unit_name: str | None
    column_names: list[str] | None
    constrained_to_indexsets: list[str] | None


class Creator(BaseRepository[ModelType], abstract.Creator):
    def add(self, *args: Any, **kwargs: Any) -> ModelType:
        raise NotImplementedError

    def create(self, *args: Any, **kwargs: Unpack[CreateKwargs]) -> ModelType:
        model = self.add(*args, **kwargs)
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            raise self.model_class.NotUnique(*args)

        return model


class Deleter(BaseRepository[ModelType], abstract.Deleter):
    def delete(self, id: int) -> None:
        exc = db.select(self.model_class).where(self.model_class.id == id)

        try:
            obj = self.session.execute(exc).scalar_one()
        except NoResultFound:
            raise self.model_class.NotFound(id=id)

        try:
            self.session.delete(obj)
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            raise self.model_class.DeletionPrevented


class CheckAccessKwargs(TypedDict, total=False):
    run: abstract.annotations.HasRunFilter
    is_default: bool | None
    default_only: bool | None


class SelectCountKwargs(abstract.HasNameFilter, total=False):
    default_only: bool | None
    dimension_id: int | None
    dimension_id__in: Iterable[int] | None
    id__in: set[int]
    is_default: bool | None
    join_parameters: bool | None
    join_runs: bool | None
    iamc: abstract.annotations.IamcFilterAlias
    model: abstract.annotations.HasModelFilter
    region: abstract.annotations.HasRegionFilter
    run: abstract.annotations.HasRunFilter
    scenario: abstract.annotations.HasScenarioFilter
    unit: abstract.annotations.HasUnitFilter
    variable: abstract.annotations.HasVariableFilter


class Selecter(BaseRepository[ModelType]):
    filter_class: type[filters.BaseFilter] | None

    def check_access(
        self,
        ids: set[int],
        access_type: str = "view",
        **kwargs: Unpack[CheckAccessKwargs],
    ) -> None:
        exc = self.select_for_count(
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

    SelectAuthType = TypeVar("SelectAuthType", bound=db.sql.Select[tuple[Any]])

    def join_auth(self, exc: SelectAuthType) -> SelectAuthType:
        return exc

    def apply_auth(self, exc: SelectAuthType, access_type: str) -> SelectAuthType:
        if self.backend.auth_context is not None:
            if not self.backend.auth_context.is_managed:
                exc = self.join_auth(exc)
            exc = self.backend.auth_context.apply(access_type, exc)
        return exc

    def select_for_count(
        self,
        _exc: db.sql.Select[tuple[int]],
        _filter: filters.BaseFilter | None = None,
        _skip_filter: bool = False,
        _access_type: str = "view",
        **kwargs: Unpack[SelectCountKwargs],
    ) -> db.sql.Select[tuple[int]]:
        if self.filter_class is None:
            cls_name = self.__class__.__name__
            raise NotImplementedError(
                f"Provide `{cls_name}.filter_class` or reimplement `{cls_name}.select`."
            )

        _exc = self.apply_auth(_exc, _access_type)
        if _filter is not None and not _skip_filter:
            filter_instance = _filter
            _exc = filter_instance.join(_exc, session=self.session)
            _exc = filter_instance.apply(_exc, self.model_class, self.session)
        elif not _skip_filter:
            kwarg_filter: filters.BaseFilter = self.filter_class(**kwargs)
            _exc = kwarg_filter.join(_exc, session=self.session)
            _exc = kwarg_filter.apply(_exc, self.model_class, self.session)
        return _exc

    def select(
        self,
        _filter: filters.BaseFilter | None = None,
        _exc: db.sql.Select[tuple[ModelType]] | None = None,
        _access_type: str = "view",
        _post_filter: Callable[
            [db.sql.Select[tuple[ModelType]]], db.sql.Select[tuple[ModelType]]
        ]
        | None = None,
        _skip_filter: bool = False,
        **kwargs: Any,
    ) -> db.sql.Select[tuple[ModelType]]:
        if self.filter_class is None:
            cls_name = self.__class__.__name__
            raise NotImplementedError(
                f"Provide `{cls_name}.filter_class` or reimplement `{cls_name}.select`."
            )

        if _exc is None:
            _exc = db.select(self.model_class)

        _exc = self.apply_auth(_exc, _access_type)

        if _filter is not None and not _skip_filter:
            filter_instance = _filter
            _exc = filter_instance.join(_exc, session=self.session)
            _exc = filter_instance.apply(_exc, self.model_class, self.session)
        elif not _skip_filter:
            kwarg_filter: filters.BaseFilter = self.filter_class(**kwargs)
            _exc = kwarg_filter.join(_exc, session=self.session)
            _exc = kwarg_filter.apply(_exc, self.model_class, self.session)

        if _post_filter is not None:
            _exc = _post_filter(_exc)
        return _exc


class QueryMixin(BaseRepository[ModelType]):
    def tabulate_query(self, exc: db.sql.Select[Any]) -> pd.DataFrame:
        if self.session.bind is not None:
            with self.engine.connect() as con:
                return pd.read_sql(exc, con=con).replace({np.nan: None})
        else:
            raise ProgrammingError("Database session is closed.")

    def list_query(self, exc: db.sql.Select[Any]) -> list[Any]:
        result = self.session.execute(exc).scalars().all()
        return list(result)


class Lister(QueryMixin[ModelType], Selecter[ModelType], abstract.Lister):
    def list(self, *args: Any, **kwargs: Any) -> list[ModelType]:
        _exc = self.select(*args, **kwargs)
        _exc = _exc.order_by(self.model_class.id.asc())
        return self.list_query(_exc)


class Tabulator(QueryMixin[ModelType], Selecter[ModelType], abstract.Tabulator):
    def tabulate(self, *args: Any, _raw: bool = False, **kwargs: Any) -> pd.DataFrame:
        _exc = self.select(*args, **kwargs)
        _exc = _exc.order_by(self.model_class.id.asc())

        return self.tabulate_query(_exc)


class PaginateKwargs(TypedDict):
    _filter: filters.BaseFilter
    join_parameters: NotRequired[bool | None]
    join_runs: NotRequired[bool | None]
    join_run_index: NotRequired[bool | None]
    table: bool


class EnumerateKwargs(TypedDict):
    _filter: filters.BaseFilter
    join_parameters: NotRequired[bool | None]
    join_runs: NotRequired[bool | None]
    join_run_index: NotRequired[bool | None]
    _post_filter: Callable[
        [db.sql.Select[tuple[ModelType]]], db.sql.Select[tuple[ModelType]]
    ]


class CountKwargs(abstract.HasNameFilter, total=False):
    dimension_id: int | None
    dimension_id__in: Iterable[int] | None
    _filter: filters.BaseFilter
    join_parameters: bool | None
    join_runs: bool | None
    iamc: abstract.annotations.IamcFilterAlias
    model: abstract.annotations.HasModelFilter
    region: abstract.annotations.HasRegionFilter
    run: abstract.annotations.HasRunFilter
    scenario: abstract.annotations.HasScenarioFilter
    unit: abstract.annotations.HasUnitFilter
    variable: abstract.annotations.HasVariableFilter


class Enumerator(Lister[ModelType], Tabulator[ModelType], abstract.Enumerator):
    def enumerate(
        self, table: bool = False, **kwargs: Unpack[EnumerateKwargs]
    ) -> list[ModelType] | pd.DataFrame:
        return self.tabulate(**kwargs) if table else self.list(**kwargs)

    def paginate(
        self, limit: int = 1000, offset: int = 0, **kwargs: Unpack[PaginateKwargs]
    ) -> list[ModelType] | pd.DataFrame:
        return self.enumerate(
            **kwargs, _post_filter=lambda e: e.offset(offset).limit(limit)
        )

    def count(self, **kwargs: Unpack[CountKwargs]) -> int:
        _exc = self.select_for_count(
            _exc=db.select(db.func.count(self.model_class.id.distinct())),
            **kwargs,
        )
        return self.session.execute(_exc).scalar_one()


class TransactionProtocol(Protocol):
    """Protocol class to mock the type of sqlalchemy_continuums"""

    id: int
    issued_at: datetime


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
            - set(primary_key_columns.keys())  # no pk columns
        )  # = all columns that are constant and provided during creation

        for x in existing_df.select_dtypes(include=["datetime64"]).columns.tolist():
            existing_df.loc[:, x] = existing_df[x].astype(object)

        for x in df.select_dtypes(include=["datetime64"]).columns.tolist():
            df.loc[:, x] = df[x].astype(object)

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
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Splits a DataFrame into two parts based on the maximum allowed unique
        values in specified columns.

        The function iteratively adjusts the chunk size starting from
        the entire DataFrame, reducing it until the maximum number of unique
        values in any of the specified columns within the chunk is ≤ `mu`.
        The remaining rows form the second part.

        Args:
            df (pd.DataFrame): The input DataFrame to split.
            columns (Iterable[str]): Columns to check for the maximum unique values.
            mu (int): Maximum allowed unique values in any
                of the specified columns for the first chunk.

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]:
                - First element (chunk_df): Largest initial chunk where
                    columns have ≤ `mu` unique values.
                - Second element (remaining_df): Remaining rows of the
                    DataFrame after `chunk_df`.

        Example:
            If `df` has 100 rows and a column initially has 10 unique values
            (with `mu=5`), the chunk is reduced to 50 rows.
            If the chunk then has 6 unique values, it's further
            reduced to ~41 rows, and so on until all columns in the chunk
            meet the condition.
        """

        chunk_size = len(df.index)
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

    def get_transaction(self) -> TransactionProtocol:
        mgr = get_versioning_manager(self.model_class)
        uow = mgr.unit_of_work(self.session)
        return cast(TransactionProtocol, uow.create_transaction(self.session))


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
        df = df[list(set(columns.keys()) & set(df.columns))].copy()
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

    def bulk_insert_versions(self, df: pd.DataFrame) -> None:
        transaction = self.get_transaction()

        df.loc[:, self.tx_column_name] = transaction.id
        df.loc[:, "operation_type"] = Operation.INSERT

        vclass = version_class(self.model_class)
        vm = cast(list[dict[str, Any]], df.to_dict("records"))
        self.session.execute(db.insert(vclass), vm)

    def bulk_insert(self, df: pd.DataFrame, **kwargs: Any) -> None:
        if "id" in df.columns:
            raise ProgrammingError("You may not insert the 'id' column.")

        # to_dict returns a more general list[Mapping[Hashable, Unknown]]
        m = cast(list[dict[str, Any]], df.to_dict("records"))

        exc = db.insert(self.model_class).execution_options(synchronize_session=False)
        if self.is_versioned:
            if self.dialect.name == "sqlite":
                # https://docs.sqlalchemy.org/en/20/core/connections.html#correlating-returning-rows-to-parameter-sets
                logger.warning(
                    "Dispatching a versioned insert statement on an 'sqlite' backend. "
                    "This might be very slow!"
                )

            exc = exc.returning(self.model_class.id, sort_by_parameter_order=True)

        try:
            result = self.session.execute(exc, m).scalars()
        except IntegrityError as e:
            raise self.model_class.NotUnique(*e.args)

        if self.is_versioned:
            ids = list(result)
            df.loc[:, "id"] = ids
            self.bulk_insert_versions(df)

    def bulk_update_versions(self, df: pd.DataFrame) -> None:
        transaction = self.get_transaction()
        df[self.tx_column_name] = transaction.id
        df["operation_type"] = Operation.UPDATE

        exc = (
            db.update(self.version_class)
            .where(
                db.and_(
                    self.version_class.id.in_(df["id"]),
                    self.version_class.end_transaction_id == db.null(),
                )
            )
            .values(end_transaction_id=transaction.id)
            .execution_options(synchronize_session=False)
        )

        self.session.execute(exc)

        vm = cast(list[dict[str, Any]], df.to_dict("records"))
        self.session.execute(db.insert(self.version_class), vm)

    def bulk_update(self, df: pd.DataFrame, **kwargs: Any) -> None:
        # to_dict returns a more general list[Mapping[Hashable, Unknown]]
        m = cast(list[dict[str, Any]], df.to_dict("records"))

        self.session.execute(
            db.update(self.model_class).execution_options(synchronize_session=False), m
        )

        if is_versioned(self.model_class):
            self.bulk_update_versions(df)


class BulkDeleter(BulkOperator[ModelType]):
    def bulk_delete(self, df: pd.DataFrame) -> None:
        for chunk_df in self.yield_chunks(df):
            self.bulk_delete_chunk(chunk_df)

    def bulk_delete_versions(self, df: pd.DataFrame) -> None:
        transaction = self.get_transaction()
        vclass = version_class(self.model_class)

        df[self.tx_column_name] = transaction.id
        df["operation_type"] = Operation.DELETE

        for _, sdf in df.groupby(df.index // self.max_list_length):
            uexc = (
                db.update(vclass)
                .where(
                    db.and_(
                        vclass.id.in_(sdf["id"]),
                        vclass.end_transaction_id == db.null(),
                    )
                )
                .values(end_transaction_id=transaction.id)
            )

        self.session.execute(uexc, execution_options={"synchronize_session": False})

        vm = cast(list[dict[str, Any]], df.to_dict("records"))
        self.session.execute(db.insert(vclass), vm)

    def bulk_delete_chunk(self, df: pd.DataFrame) -> None:
        existing_df = self.tabulate_existing(df)
        if existing_df.empty:
            return

        df = self.merge_existing(df, existing_df)
        df["exists"] = np.where(pd.notnull(df["id"]), True, False)
        delete_df = df.where(df["exists"])
        id_df = delete_df[["id"]]

        excs = []
        for _, sdf in id_df.groupby(id_df.index // self.max_list_length):
            dexc = db.delete(self.model_class).where(self.model_class.id.in_(sdf["id"]))
            excs.append(dexc)

        for exc in excs:
            self.session.execute(exc, execution_options={"synchronize_session": False})

        if is_versioned(self.model_class):
            self.bulk_delete_versions(df)

        self.session.commit()


class TabulateTransactionsKwargs(abstract.annotations.HasPaginationArgs, total=False):
    pass


class VersionManager(QueryMixin[ModelType], BaseRepository[ModelType]):
    def __init__(self, backend: "SqlAlchemyBackend") -> None:
        super().__init__(backend)

        self.version_bundle: Bundle[Any] = Bundle(
            self.version_class.__name__,
            *db.utils.get_columns(self.version_class).values(),
        )

    def select_transactions(self) -> db.sql.Select[Any]:
        return db.select(self.transaction_class)

    def select_versions(
        self, transaction__id: int | None = None, **kwargs: Any
    ) -> db.sql.Select[Any]:
        exc = db.select(self.version_class)

        if transaction__id is not None:
            exc = exc.where(
                db.and_(
                    self.version_class.transaction_id <= transaction__id,
                    self.version_class.operation_type != Operation.DELETE,
                    db.or_(
                        self.version_class.end_transaction_id > transaction__id,
                        self.version_class.end_transaction_id == db.null(),
                    ),
                )
            )

        for key in kwargs:
            columns = db.utils.get_columns(self.version_class)
            if key in columns:
                exc = exc.where(columns[key] == kwargs[key])

        return exc

    def apply_pagination(
        self, exc: db.sql.Select[Any], limit: int | None, offset: int | None
    ) -> db.sql.Select[Any]:
        if limit is not None:
            exc = exc.limit(limit)
        if offset is not None:
            exc = exc.offset(offset)
        return exc

    def tabulate_versions(
        self,
        limit: int | None = None,
        offset: int | None = None,
        transaction__id: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        exc = self.select_versions(transaction__id=transaction__id, **kwargs)
        exc = exc.order_by(self.version_class.transaction_id.asc())
        exc = self.apply_pagination(exc, limit, offset)

        return self.tabulate_query(exc)

    def tabulate_transactions(
        self,
        limit: int | None = None,
        offset: int | None = None,
    ) -> pd.DataFrame:
        exc = self.select_transactions()
        exc = exc.order_by(self.transaction_class.issued_at.desc())
        exc = self.apply_pagination(exc, limit, offset)

        return self.tabulate_query(exc)
