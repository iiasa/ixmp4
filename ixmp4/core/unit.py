from datetime import datetime

import pandas as pd
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.core.base import BaseDocsServiceFacade, BaseFacadeObject
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.unit.dto import Unit as UnitDto
from ixmp4.data.unit.exceptions import (
    UnitDeletionPrevented,
    UnitNotFound,
    UnitNotUnique,
)
from ixmp4.data.unit.filter import UnitFilter
from ixmp4.data.unit.service import UnitService


class Unit(BaseFacadeObject[UnitService, UnitDto]):
    NotUnique = UnitNotUnique
    NotFound = UnitNotFound
    DeletionPrevented = UnitDeletionPrevented

    @property
    def id(self) -> int:
        """Unique id."""
        return self._dto.id

    @property
    def name(self) -> str:
        """Unit name."""
        return self._dto.name

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

    @property
    def docs(self) -> str | None:
        try:
            return self._service.get_docs(self.id).description
        except DocsNotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self._service.delete_docs(self.id)
        else:
            self._service.set_docs(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self._service.delete_docs(self.id)
        # TODO: silently failing
        except DocsNotFound:
            return None

    def delete(self) -> None:
        """Deletes this unit."""
        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> UnitService:
        return backend.units

    def __str__(self) -> str:
        return f"<Unit {self.id} name={self.name}>"

    def __repr__(self) -> str:
        return str(self)


class UnitServiceFacade(BaseDocsServiceFacade[Unit | int | str, Unit, UnitService]):
    def _get_service(self, backend: Backend) -> UnitService:
        return backend.units

    def _get_item_id(self, ref: Unit | int | str) -> int:
        if isinstance(ref, Unit):
            return ref.id
        elif isinstance(ref, int):
            return ref
        elif isinstance(ref, str):
            dto = self._service.get_by_name(ref)
            return dto.id
        else:
            raise ValueError(f"Invalid reference to unit: {ref}")

    def create(self, name: str) -> Unit:
        """Creates a unit.

        .. code:: python

            platform.units.create("MtCO2/yr")
            #> <Unit 1 name='MtCO2/yr'>

        Parameters
        ----------
        name : str
            The name of the model.

        Raises
        ------
        :class:`UnitNotUnique`:
            If the unit with `name` is not unique.

        Returns
        -------
        :class:`ixmp4.core.unit.Unit`:
            The created unit.
        """
        # TODO: Refactor name checks to data layer

        if name != "" and name.strip() == "":
            raise ValueError("Using a space-only unit name is not allowed.")
        if name == "dimensionless":
            raise ValueError(
                "Unit name 'dimensionless' is reserved, use an empty string '' instead."
            )
        dto = self._service.create(name)
        return Unit(self._backend, dto)

    def delete(self, ref: Unit | int | str) -> None:
        """Deletes a unit.

        .. code:: python

            platform.units.delete("MtCO2/yr")

        Parameters
        ----------
        ref : :class:`ixmp4.core.unit.Unit` | int | str
            Unit object, unit id or unit name.

        Raises
        ------
        :class:`UnitNotFound`:
            If no region matching ``ref`` exists.
        :class:`UnitDeletionPrevented`:
            If the region matching ``ref`` is used in the database,
            preventing it's deletion.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        """
        id = self._get_item_id(ref)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Unit:
        """Retrieves a unit by its name.

        .. code:: python

            platform.units.get_by_name("MtCO2/yr")
            #> <Unit 1 name='MtCO2/yr'>

        Parameters
        ----------
        name : str
            The unique name of the unit.

        Raises
        ------
        :class:`UnitNotFound`:
            If the unit with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.core.unit.Unit`:
            The retrieved unit.
        """

        dto = self._service.get_by_name(name)
        return Unit(self._backend, dto)

    def list(self, **kwargs: Unpack[UnitFilter]) -> list[Unit]:
        r"""Lists units by specified criteria.

        .. code:: python

            platform.units.list()
            #> [<Unit 1 name='MtCO2/yr'>]

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `UnitFilter`.

        Returns
        -------
        list[:class:`ixmp4.core.unit.Unit`]:
            List of units.
        """

        units = self._service.list(**kwargs)
        return [Unit(self._backend, dto) for dto in units]

    def tabulate(self, **kwargs: Unpack[UnitFilter]) -> pd.DataFrame:
        r"""Tabulates units by specified criteria.

        .. code:: python

            platform.units.tabulate()
            #>     name      id
            # 0  MtCO2/yr   1

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `UnitFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """

        return self._service.tabulate(**kwargs)
