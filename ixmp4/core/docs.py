from typing import Any, Generic

from ixmp4.data.docs.repository import DocsNotFound

from .base import BaseFacadeObject, DocsServiceT, DtoT


class DocsDescriptor(Generic[DocsServiceT, DtoT]):
    """Handles documentation strings of various objects on a platform."""

    def __set__(
        self, obj: BaseFacadeObject[DocsServiceT, DtoT], value: str | None
    ) -> None:
        """
        Sets the documentation string for a facade object.
        If the supplied value is as ``string`` the saved
        entry will be updated or created.
        If the value is ``None`` the entry will be deleted.

        .. code:: python

            model.docs = "Model documentation string."
            model.docs

            #> "Model documentation string.

            scenario.docs = None
            #> Entry deleted.

        """
        if value is None:
            obj._service.delete_docs(obj._dto.id)
        else:
            obj._service.set_docs(obj._dto.id, value)

    def __get__(
        self, obj: BaseFacadeObject[DocsServiceT, DtoT], objtype: type[Any]
    ) -> str | None:
        """
        Retrieves the documentation string for a facade object.
        If the entry does not exist, the
        :class:`~ixmp4.data.docs.repository.DocsNotFound` exception
        will be caught and ``None`` will be returned instead.

        .. code:: python

            region.docs
            #> "Region documentation string.

        """
        try:
            return obj._service.get_docs(obj._dto.id).description
        except DocsNotFound:
            return None

    def __delete__(self, obj: BaseFacadeObject[DocsServiceT, DtoT]) -> None:
        """
        Deletes the documentation string for a facade object.
        If the entry does not exist, the
        :class:`~ixmp4.data.docs.repository.DocsNotFound` exception
        will be caught.

        .. code:: python

            del unit.docs
            #> Entry deleted.

            unit.docs
            #> `None`
        """
        try:
            obj._service.delete_docs(obj._dto.id)
        # TODO: silently failing
        except DocsNotFound:
            return None
