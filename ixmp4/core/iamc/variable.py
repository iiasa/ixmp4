from datetime import datetime
from typing import List

import pandas as pd
from typing_extensions import Unpack

from ixmp4.core.base import BaseDocsServiceFacade, BaseFacadeObject
from ixmp4.core.docs import DocsDescriptor
from ixmp4.data.backend import Backend
from ixmp4.data.iamc.variable.dto import Variable as VariableDto
from ixmp4.data.iamc.variable.exceptions import (
    VariableDeletionPrevented,
    VariableNotFound,
    VariableNotUnique,
)
from ixmp4.data.iamc.variable.filter import VariableFilter
from ixmp4.data.iamc.variable.service import VariableService


class Variable(BaseFacadeObject[VariableService, VariableDto]):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    DeletionPrevented = VariableDeletionPrevented

    docs: DocsDescriptor[VariableService, VariableDto] = DocsDescriptor()
    """IAMC Variable docs."""

    @property
    def id(self) -> int:
        """Unique id."""
        return self._dto.id

    @property
    def name(self) -> str:
        """Variable name."""
        return self._dto.name

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

    def delete(self) -> None:
        """Deletes the variable from the database."""
        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> VariableService:
        return backend.iamc.variables

    def __str__(self) -> str:
        return f"<Variable {self.id} name='{self.name}'>"


class VariableServiceFacade(
    BaseDocsServiceFacade[Variable | int | str, Variable, VariableService]
):
    def _get_service(self, backend: Backend) -> VariableService:
        return backend.iamc.variables

    def _get_item_id(self, ref: Variable | int | str) -> int:
        if isinstance(ref, Variable):
            return ref.id
        elif isinstance(ref, int):
            return ref
        elif isinstance(ref, str):
            dto = self._service.get_by_name(ref)
            return dto.id
        else:
            raise ValueError(f"Invalid reference to variable: {ref}")

    def create(self, name: str) -> Variable:
        """Creates a variable.

        .. code:: python

            platform.iamc.variables.create("Emissions|CO2")
            #> <Variable 1 name='Emissions|CO2'>

        Parameters
        ----------
        name : str
            The name of the variable.

        Raises
        ------
        :class:`~ixmp4.data.iamc.variable.exceptions.VariableNotUnique`:
            If the variable with `name` is not unique.

        Returns
        -------
        :class:`ixmp4.core.iamc.variable.Variable`:
            The created variable.
        """
        dto = self._service.create(name)
        return Variable(self._backend, dto)

    def delete(self, ref: Variable | int | str) -> None:
        """Deletes a variable.

        .. code:: python

            platform.iamc.variables.delete("Emissions|CO2")

        Parameters
        ----------
        ref : :class:`~ixmp4.core.iamc.variable.Variable` | int | str
            Variable object, variable id or variable name.

        Raises
        ------
        :class:`~ixmp4.data.iamc.variable.exceptions.VariableNotFound`:
            If no variable matching ``ref`` exists.
        :class:`~ixmp4.data.iamc.variable.exceptions.VariableDeletionPrevented`:
            If the variable matching ``ref`` is used in the database,
            preventing its deletion.
        """

        id = self._get_item_id(ref)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Variable:
        """Retrieves a variable by its name.

        .. code:: python

            platform.iamc.variables.get_by_name("Emissions|CO2")
            #> <Variable 1 name='Emissions|CO2'>

        Parameters
        ----------
        name : str
            The unique name of the variable.

        Raises
        ------
        :class:`~ixmp4.data.iamc.variable.exceptions.VariableNotFound`:
            If the variable with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.core.iamc.variable.Variable`:
            The retrieved variable.
        """

        dto = self._service.get_by_name(name)
        return Variable(self._backend, dto)

    def list(self, **kwargs: Unpack[VariableFilter]) -> List[Variable]:
        r"""Lists variables by specified criteria.

        .. code:: python

            platform.iamc.variables.list()
            #> [<Variable 1 name='Emissions|CO2'>]

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `VariableFilter`.

        Returns
        -------
        list[:class:`ixmp4.data.iamc.variable.dto.Variable`]:
            List of variables.
        """

        variables = self._service.list(**kwargs)
        return [Variable(self._backend, dto) for dto in variables]

    def tabulate(self, **kwargs: Unpack[VariableFilter]) -> pd.DataFrame:
        r"""Tabulates variables by specified criteria.

        .. code:: python

            platform.iamc.variables.tabulate()
            #>     name  id
            # 0  Emissions|CO2   1

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `VariableFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """

        return self._service.tabulate(**kwargs)
