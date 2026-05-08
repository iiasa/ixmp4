from datetime import datetime
from typing import List

import pandas as pd
from typing_extensions import Unpack

from ixmp4.core.base import BaseDocsServiceFacade, BaseFacadeObject
from ixmp4.core.docs import DocsDescriptor
from ixmp4.data.backend import Backend
from ixmp4.data.scenario.dto import Scenario as ScenarioDto
from ixmp4.data.scenario.exceptions import (
    ScenarioDeletionPrevented,
    ScenarioNotFound,
    ScenarioNotUnique,
)
from ixmp4.data.scenario.filter import (
    FacadeScenarioFilter,
    facade_to_data_filter,
)
from ixmp4.data.scenario.service import ScenarioService


class Scenario(BaseFacadeObject[ScenarioService, ScenarioDto]):
    Filter = FacadeScenarioFilter
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    DeletionPrevented = ScenarioDeletionPrevented

    docs: DocsDescriptor[ScenarioService, ScenarioDto] = DocsDescriptor()
    """Scenario docs."""

    @property
    def id(self) -> int:
        return self._dto.id

    @property
    def name(self) -> str:
        return self._dto.name

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

    def delete(self) -> None:
        """Deletes this scenario."""

        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> ScenarioService:
        return backend.scenarios

    def __str__(self) -> str:
        return f"<Scenario {self.id} name='{self.name}'>"

    def __repr__(self) -> str:
        return str(self)


class ScenarioServiceFacade(
    BaseDocsServiceFacade[Scenario | int | str, Scenario, ScenarioService]
):
    def _get_service(self, backend: Backend) -> ScenarioService:
        return backend.scenarios

    def _get_item_id(self, ref: Scenario | int | str) -> int:
        if isinstance(ref, Scenario):
            return ref.id
        elif isinstance(ref, int):
            return ref
        elif isinstance(ref, str):
            dto = self._service.get_by_name(ref)
            return dto.id
        else:
            raise ValueError(f"Invalid reference to scenario: {ref}")

    def create(self, name: str) -> Scenario:
        """Creates a scenario.

        .. code:: python

            platform.scenarios.create("Scenario")
            #> <Scenario 1 name='Scenario'>

        Parameters
        ----------
        name : str
            The name of the scenario.

        Raises
        ------
        :class:`ScenarioNotUnique`:
            If the scenario with `name` is not unique.

        Returns
        -------
        :class:`ixmp4.core.scenario.Scenario`:
            The created scenario.
        """

        scen = self._service.create(name)
        return Scenario(backend=self._backend, dto=scen)

    def delete(self, ref: Scenario | int | str) -> None:
        """Deletes a scenario.

        .. code:: python

            platform.scenarios.delete("Scenario")

        Parameters
        ----------
        ref : :class:`ixmp4.core.scenario.Scenario` | int | str
            Unit object, unit id or unit name.

        Raises
        ------
        :class:`ScenarioNotFound`:
            If no scenario matching ``ref`` exists.
        :class:`ScenarioDeletionPrevented`:
            If the scenario matching ``ref`` is used in the database,
            preventing it deletion.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.
        """
        id = self._get_item_id(ref)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Scenario:
        """Retrieves a scenario by its name.

        .. code:: python

            platform.scenarios.get_by_name("Scenario")
            #> <Scenario 1 name='Scenario'>

        Parameters
        ----------
        name : str
            The unique name of the scenario.

        Raises
        ------
        :class:`ScenarioNotFound`:
            If the scenario with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.core.scenario.Scenario`:
            The retrieved scenario.
        """

        scen = self._service.get_by_name(name)
        return Scenario(self._backend, scen)

    def list(self, **kwargs: Unpack[FacadeScenarioFilter]) -> List[Scenario]:
        r"""Lists scenarios by specified criteria.

        .. code:: python

            platform.scenarios.list()
            #> [<Scenario 1 name='Scenario'>]

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in :class:`ScenarioFilter`.

        Returns
        -------
        list[:class:`ixmp4.core.scenario.Scenario`]:
            List of scenarios.
        """
        scenarios = self._service.list(**facade_to_data_filter(kwargs))
        return [Scenario(self._backend, dto) for dto in scenarios]

    def tabulate(self, **kwargs: Unpack[FacadeScenarioFilter]) -> pd.DataFrame:
        r"""Tabulates scenarios by specified criteria.

        .. code:: python

            platform.scenarios.tabulate()
            #>     name  id
            # 0  Scenario   1

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in :class:`ScenarioFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """
        return self._service.tabulate(**facade_to_data_filter(kwargs))
