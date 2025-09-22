from collections.abc import Iterable
from contextlib import suppress
from functools import reduce
from typing import TYPE_CHECKING, Any, TypeVar

from sqlalchemy import inspect, sql
from sqlalchemy.sql import ColumnCollection, ColumnElement
from sqlalchemy.sql.base import ReadOnlyColumnCollection

from ixmp4.core.exceptions import ProgrammingError

if TYPE_CHECKING:
    from ixmp4.data.db.base import BaseModel

JoinType = TypeVar("JoinType", bound=sql.Select[tuple["BaseModel", ...]])


# This should not need to be Any, I think, but if I put "BaseModel" instead, various
# things like RunMetaEntry are not recognized -> this sounds like covariance again,
# but covariant typevars are not allowed as type hints.
def is_joined(exc: sql.Select[tuple[Any, ...]], model: type["BaseModel"]) -> bool:
    """Returns `True` if `model` has been joined in `exc`."""
    for visitor in sql.visitors.iterate(exc):
        # Checking for `.join(Child)` clauses
        if visitor.__visit_name__ == "table":
            # Visitor might be of ColumnCollection or so,
            # which cannot be compared to model
            with suppress(TypeError):
                if model == visitor.entity_namespace:  # type: ignore[attr-defined]
                    return True
    return False


def get_columns(
    model_class: type["BaseModel"],
) -> ReadOnlyColumnCollection[str, ColumnElement[Any]]:
    mapper = inspect(model_class)
    if mapper is not None:
        return mapper.selectable.columns
    else:
        raise ProgrammingError(f"Model class `{model_class.__name__}` is not mapped.")


def _maybe_add_pk_column_to_collection(
    columns: ColumnCollection[str, ColumnElement[int]], column: ColumnElement[Any]
) -> ColumnCollection[str, ColumnElement[int]]:
    if column.primary_key:
        columns.add(column)

    return columns


def get_pk_columns(
    model_class: type["BaseModel"],
) -> ReadOnlyColumnCollection[str, ColumnElement[int]]:
    columns: ColumnCollection[str, ColumnElement[int]] = reduce(
        _maybe_add_pk_column_to_collection, get_columns(model_class), ColumnCollection()
    )

    return columns.as_readonly()


def _maybe_add_fk_column_to_collection(
    columns: ColumnCollection[str, ColumnElement[int]], column: ColumnElement[Any]
) -> ColumnCollection[str, ColumnElement[int]]:
    if len(column.foreign_keys) > 0:
        columns.add(column)

    return columns


def get_foreign_columns(
    model_class: type["BaseModel"],
) -> ReadOnlyColumnCollection[str, ColumnElement[int]]:
    columns: ColumnCollection[str, ColumnElement[int]] = reduce(
        _maybe_add_fk_column_to_collection, get_columns(model_class), ColumnCollection()
    )

    return columns.as_readonly()


def create_id_map_subquery(
    old_exc: sql.Subquery, new_exc: sql.Subquery
) -> sql.Subquery:
    return (
        sql.select(old_exc.c.id.label("old_id"), new_exc.c.id.label("new_id"))
        .join(new_exc, old_exc.c.name == new_exc.c.name)
        .subquery()
    )


def collect_columns_to_select(
    columns: ColumnCollection[str, ColumnElement[Any]], exclude: Iterable[str]
) -> ColumnCollection[str, ColumnElement[Any]]:
    # NOTE we only get ReadOnlyCollections, so can't just remove() items
    columns_to_collect: ColumnCollection[str, ColumnElement[Any]] = ColumnCollection()

    for name, column in columns.items():
        if name not in exclude:
            columns_to_collect.add(column=column, key=name)

    return columns_to_collect
