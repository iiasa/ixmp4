from typing import Protocol

from ixmp4.data import types

from .. import base
from ..docs import DocsRepository


# TODO: standardize docstrings (run/Run/`Run`)
class Column(base.BaseModel, Protocol):
    """Column data model."""

    name: types.String
    """Unique name of the Table."""
    dtype: types.String
    """Type of the Column's data."""
    table__id: types.Integer
    """Foreign unique integer id of a Table."""
    indexset: types.Mapped
    """Associated IndexSet."""
    constrained_to_indexset: types.Integer
    """Foreign unique integer id of an IndexSet."""

    unique: types.Boolean
    """Boolean to determine whether data in this Column must contribute to Uniqueness
    of Keys."""

    created_at: types.DateTime
    "Creation date/time. TODO"
    created_by: types.String
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Column {self.id} name={self.name}>"


class ColumnRepository(base.Creator, base.Retriever, Protocol):
    docs: DocsRepository

    # TODO Why does this need kwargs?
    def create(
        self,
        table_id: int,
        name: str,
        dtype: str,
        constrained_to_indexset: int,
        unique: bool,
        **kwargs,
    ) -> Column:
        """Creates a Column.

        Parameters
        ----------
        table_id : int
            The unique integer id of the :class:`ixmp4.data.abstract.optimization.Table`
            this Column belongs to.
        name : str
            The unique name of the Column.
        dtype : str
            The pandas-inferred type of the Column's data.
        constrained_to_indexset : int
            The id of an :class:`ixmp4.data.abstract.optimization.IndexSet`, which must
            contain all values used as entries in this Column.
        unique : bool
            A bool to determine whether entries in this Column should be considered for
            evaluating uniqueness of keys. Defaults to True.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Column.NotUnique`:
            If the Column with `name` already exists for the related
            :class:`ixmp4.data.abstract.optimization.Table`.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Column`:
            The created Column.
        """
        ...
