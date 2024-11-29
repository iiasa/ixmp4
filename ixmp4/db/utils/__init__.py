from contextlib import suppress
from typing import TYPE_CHECKING, Any, TypeVar

from sqlalchemy import inspect, sql
from sqlalchemy.orm import Mapper
from sqlalchemy.sql import ColumnCollection, ColumnElement
from sqlalchemy.sql.base import ReadOnlyColumnCollection

from ixmp4.core.exceptions import ProgrammingError

if TYPE_CHECKING:
    from ixmp4.data.db.base import BaseModel

JoinType = TypeVar("JoinType", bound=sql.Select[tuple["BaseModel", ...]])


# This should not need to be Any, I think, but if I put "BaseModel" instead, various
# things like RunMetaEntry are not recognized -> this sounds like covarianve again,
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


def get_columns(model_class: type) -> ColumnCollection[str, ColumnElement[Any]]:
    mapper: Mapper[Any] | None = inspect(model_class)
    if mapper is not None:
        return mapper.selectable.columns
    else:
        raise ProgrammingError(f"Model class `{model_class.__name__}` is not mapped.")


def get_pk_columns(
    model_class: type,
) -> ReadOnlyColumnCollection[str, ColumnElement[int]]:
    columns: ColumnCollection[str, ColumnElement[int]] = ColumnCollection()
    for col in get_columns(model_class):
        if col.primary_key:
            columns.add(col)

    return columns.as_readonly()


def get_foreign_columns(
    model_class: type,
) -> ReadOnlyColumnCollection[str, ColumnElement[int]]:
    columns: ColumnCollection[str, ColumnElement[int]] = ColumnCollection()
    for col in get_columns(model_class):
        if len(col.foreign_keys) > 0:
            columns.add(col)

    return columns.as_readonly()
