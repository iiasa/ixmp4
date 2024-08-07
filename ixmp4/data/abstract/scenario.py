from typing import Protocol

import pandas as pd

from ixmp4.data import types

from . import base
from .docs import DocsRepository


class Scenario(base.BaseModel, Protocol):
    """Modeling scenario data model."""

    name: types.String
    "Unique name of the scenario."

    created_at: types.DateTime
    "Creation date/time. TODO"
    created_by: types.String
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Scenario {self.id} name={self.name}>"


class ScenarioRepository(base.Creator, base.Retriever, base.Enumerator, Protocol):
    docs: DocsRepository

    def create(self, name: str) -> Scenario:
        """Creates a scenario.

        Parameters
        ----------
        name : str
            The name of the scenario.

        Raises
        ------
        :class:`ixmp4.core.exceptions.ScenarioNotUnique`:
            If the scenario with `name` is not unique.

        Returns
        -------
        :class:`ixmp4.data.abstract.Scenario`:
            The created scenario.
        """
        ...

    def get(self, name: str) -> Scenario:
        """Retrieves a scenario.

        Parameters
        ----------
        name : str
            The unique name of the scenario.

        Raises
        ------
        :class:`ixmp4.data.abstract.Scenario.NotFound`:
            If the scenario with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.Scenario`:
            The retrieved scenario.
        """
        ...

    def list(
        self,
        *,
        name: str | None = None,
    ) -> list[Scenario]:
        """Lists scenarios by specified criteria.

        Parameters
        ----------
        name : str
            The name of a scenario. If supplied only one result will be returned.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.Scenario`]:
            List of scenarios.
        """
        ...

    def tabulate(self, *, name: str | None = None, **kwargs) -> pd.DataFrame:
        """Tabulate scenarios by specified criteria.

        Parameters
        ----------
        name : str
            The name of a scenario. If supplied only one result will be returned.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """
        ...

    def map(self, *args, **kwargs) -> dict:
        """Return a mapping of scenario-id to scenario-name.

        Returns
        -------
        :class:`dict`
            A dictionary `id` -> `name`
        """
        return dict([(s.id, s.name) for s in self.list(*args, **kwargs)])
