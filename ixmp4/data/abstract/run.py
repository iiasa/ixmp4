from typing import TYPE_CHECKING, ClassVar, Protocol

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.core.exceptions import IxmpError, NoDefaultRunVersion
from ixmp4.data import types

from . import base
from .annotations import HasRunFilter, IamcRunFilter

if TYPE_CHECKING:
    from . import Model, Scenario


class Run(base.BaseModel, Protocol):
    """Model run data model."""

    NoDefaultVersion: ClassVar[type[IxmpError]] = NoDefaultRunVersion

    model__id: types.Integer
    "Foreign unique integer id of the model."
    model: types.Mapped["Model"]
    "Associated model."

    scenario__id: types.Integer
    "Foreign unique integer id of the scenario."
    scenario: types.Mapped["Scenario"]
    "Associated scenario."

    version: types.Integer
    "Run version."
    is_default: types.Boolean
    "`True` if this is the default run version."

    def __str__(self) -> str:
        return f"<Run {self.id} model={self.model.name} \
            scenario={self.scenario.name} version={self.version} \
            is_default={self.is_default}>"


class EnumerateKwargs(HasRunFilter, total=False):
    iamc: IamcRunFilter | bool | None


class RunRepository(
    base.Creator,
    base.Retriever,
    base.Enumerator,
    Protocol,
):
    def create(self, model_name: str, scenario_name: str) -> Run:
        """Creates a run with an incremented version number or version=1 if no versions
        exist. Will automatically create the models and scenarios if they don't exist
        yet.

        Parameters
        ----------
        model_name : str
            The name of a model.
        scenario_name : str
            The name of a scenario.

        Returns
        -------
        :class:`ixmp4.data.abstract.Run`:
            The created run.
        """
        ...

    def get(self, model_name: str, scenario_name: str, version: int) -> Run:
        """Retrieves a run.

        Parameters
        ----------
        model_name : str
            The name of a model.
        scenario_name : str
            The name of a scenario.
        version : int
            The version number of this run.

        Raises
        ------
        :class:`ixmp4.data.abstract.Run.NotFound`:
            If the run with `model_name`, `scenario_name` and `version` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.Run`:
            The retrieved run.
        """
        ...

    def get_or_create(self, model_name: str, scenario_name: str) -> Run:
        """Tries to retrieve a run's default version
        and creates it if it was not found.

        Parameters
        ----------
        model_name : str
            The name of a model.
        scenario_name : str
            The name of a scenario.

        Returns
        -------
        :class:`ixmp4.data.abstract.Run`:
            The retrieved or created run.
        """
        try:
            return self.get_default_version(model_name, scenario_name)
        except Run.NoDefaultVersion:
            return self.create(model_name, scenario_name)

    def get_default_version(self, model_name: str, scenario_name: str) -> Run:
        """Retrieves a run's default version.

        Parameters
        ----------
        model_name : str
            The name of a model.
        scenario_name : str
            The name of a scenario.

        Raises
        ------
        :class:`ixmp4.core.exceptions.NoDefaultRunVersion`:
            If no runs with `model_name`, `scenario_name` and `is_default=True` exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.Run`:
            The retrieved run.
        """
        ...

    def get_by_id(self, id: int) -> Run:
        """Retrieves a Run by its id.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`ixmp4.data.abstract.Run.NotFound`.
            If the Run with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.Run`:
            The retrieved Run.
        """
        ...

    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[Run]:
        r"""Lists runs by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.run.filter.RunFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.Run`]:
            List of runs.
        """
        ...

    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        r"""Tabulate runs by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.run.filter.RunFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - model__id
                - scenario__id
        """
        ...

    def set_as_default_version(self, id: int) -> None:
        """Sets a run as the default version for a (model, scenario) combination.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`ixmp4.data.abstract.Run.NotFound`:
            If no run with the `id` exists.

        """
        ...

    def unset_as_default_version(self, id: int) -> None:
        """Unsets a run as the default version leaving no
        default version for a (model, scenario) combination.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`ixmp4.data.abstract.Run.NotFound`:
            If no run with the `id` exists.
        :class:`ixmp4.core.exceptions.IxmpError`:
            If the run is not set as a default version.

        """
        ...
