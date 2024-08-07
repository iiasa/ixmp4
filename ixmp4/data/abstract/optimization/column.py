from typing import Protocol

from ixmp4.data import types

from .. import base
from .indexset import IndexSet


# TODO: standardize docstrings (run/Run)
class Column(base.BaseModel, Protocol):
    """Column data model."""

    name: types.String
    """Unique name of the Column."""
    dtype: types.String
    """Type of the Column's data."""
    equation__id: types.Mapped[int | None]
    """Foreign unique integer id of a Equation."""
    parameter__id: types.Mapped[int | None]
    """Foreign unique integer id of a Parameter."""
    table__id: types.Mapped[int | None]
    """Foreign unique integer id of a Table."""
    variable__id: types.Mapped[int | None]
    """Foreign unique integer id of a Variable."""
    indexset: types.Mapped[IndexSet]
    """Associated IndexSet."""
    constrained_to_indexset: types.Integer
    """Foreign unique integer id of an IndexSet."""

    unique: types.Boolean
    """Boolean to determine whether data in this Column must contribute to Uniqueness
    of Keys."""

    def __str__(self) -> str:
        return f"<Column {self.id} name={self.name}>"
