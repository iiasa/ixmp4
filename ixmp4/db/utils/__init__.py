from contextlib import suppress

from sqlalchemy import inspect, sql
from sqlalchemy.orm import Mapper
from sqlalchemy.sql import ColumnCollection

from ixmp4.core.exceptions import ProgrammingError


def is_joined(exc: sql.Select, model):
    """Returns `True` if `model` has been joined in `exc`."""
    for visitor in sql.visitors.iterate(exc):
        # Checking for `.join(Child)` clauses
        if visitor.__visit_name__ == "table":
            # Visitor might be of ColumnCollection or so,
            # which cannot be compared to model
            with suppress(TypeError):
                if model == visitor.entity_namespace:  # type: ignore
                    return True
    return False


def get_columns(model_class: type) -> ColumnCollection:
    mapper: Mapper | None = inspect(model_class)
    if mapper is not None:
        return mapper.selectable.columns
    else:
        raise ProgrammingError(f"Model class `{model_class.__name__}` is not mapped.")


def get_pk_columns(model_class: type) -> ColumnCollection:
    columns: ColumnCollection = ColumnCollection()
    for col in get_columns(model_class):
        if col.primary_key:
            columns.add(col)

    return columns.as_readonly()


def get_foreign_columns(model_class: type) -> ColumnCollection:
    columns: ColumnCollection = ColumnCollection()
    for col in get_columns(model_class):
        if len(col.foreign_keys) > 0:
            columns.add(col)

    return columns.as_readonly()
