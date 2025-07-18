from collections.abc import Iterable
from typing import Protocol

from . import base


class Docs(base.BaseModel, Protocol):
    """Abstract documentation model for one object of any dimension."""

    description: str
    "Description of the dimension object."
    dimension__id: int
    "Foreign unique integer id of the object in the dimension's table."
    dimension: base.BaseModel
    "The documented object."

    # This doesn't work since each dimension has a different self.dimension object as
    # of now
    # def __str__(self) -> str:
    #    return (
    #        f"<Documentation for {self.dimension} {self.dimension.name}>"
    #    )


class DocsRepository(base.Retriever, base.Deleter, base.Enumerator, Protocol):
    def get(self, dimension_id: int) -> Docs:
        """Retrieve the documentation of an object of any dimension.

        Parameters
        ----------
        dimension_id : int
            The unique integer id of the object.

        Raises
        ------
        :class:`ixmp4.data.abstract.Docs.NotFound`:
            If the documentation for the object with `dimension_id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.Docs`
            The object's documentation.
        """
        ...

    def set(self, dimension_id: int, description: str) -> Docs:
        """Sets the documentation for an object of any dimension.

        Parameters
        ----------
        dimension_id : int
            The id of the object in its dimension's table.
        description : str
            Description of the object.

        Returns
        -------
        :class:`ixmp4.data.abstract.Docs`
            The object's documentation.
        """
        ...

    def delete(self, dimension_id: int) -> None:
        """Deletes a dimension object's documentation.
        Warning: No recovery of deleted data shall be possible via ixmp
        after the execution of this function.

        Parameters
        ----------
        dimension_id : int
            The unique id of the object whose documentation should be deleted in its
            dimension's table.

        Raises
        ------
        :class:`ixmp4.data.abstract.Docs.NotFound`:
            If the documentation for the object with `dimension_id` does not exist.
        """
        ...

    def list(
        self,
        *,
        dimension_id: int | None = None,
        dimension_id__in: Iterable[int] | None = None,
    ) -> list[Docs]:
        """Lists documentations.

        Parameters
        ----------
        dimension_id : int or None
            The id of an object in any dimension. If supplied only one result will be
            returned.
        dimension_id__in : list[int] or None
            A list of ids of objects in any one dimension. If supplied, only these
            objects will be returned.

        Returns
        -------
            Iterable[:class:`ixmp4.data.abstract.Docs`] : List of documentations.
        """
        ...
