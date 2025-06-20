from datetime import datetime
from typing import Protocol

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from . import base
from .annotations import HasIdFilter, HasNameFilter, IamcScenarioFilter
from .docs import DocsRepository


class Scenario(base.BaseModel, Protocol):
    """Modeling scenario data model."""

    name: str
    "Unique name of the scenario."

    created_at: datetime
    "Creation date/time."
    created_by: str
    "Creator."

    def __str__(self) -> str:
        return f"<Scenario {self.id} name={self.name}>"


class EnumerateKwargs(HasIdFilter, HasNameFilter, total=False):
    iamc: IamcScenarioFilter | bool


class ScenarioRepository(
    base.Creator,
    base.Retriever,
    base.Enumerator,
    base.VersionManager,
    Protocol,
):
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

    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[Scenario]:
        r"""Lists scenarios by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.scenario.filter.ScenarioFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.Scenario`]:
            List of scenarios.
        """
        ...

    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        r"""Tabulate scenarios by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.scenario.filter.ScenarioFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """
        ...

    def map(self, **kwargs: Unpack[EnumerateKwargs]) -> dict[int, str]:
        """Return a mapping of scenario-id to scenario-name.

        Returns
        -------
        :class:`dict`
            A dictionary `id` -> `name`
        """
        return dict([(s.id, s.name) for s in self.list(**kwargs)])
