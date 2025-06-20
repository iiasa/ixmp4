from datetime import datetime
from typing import Protocol

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from . import base
from .annotations import HasIdFilter, HasNameFilter, IamcModelFilter
from .docs import DocsRepository


class Model(base.BaseModel, Protocol):
    """Data model of an assement modeling "model".
    Unfortunately two naming conventions clash here.
    """

    name: str
    "Unique name of the model."

    created_at: datetime
    "Creation date/time."
    created_by: str
    "Creator."

    def __str__(self) -> str:
        return f"<Model {self.id} name={self.name}>"


class EnumerateKwargs(HasIdFilter, HasNameFilter, total=False):
    iamc: IamcModelFilter | bool


class ModelRepository(
    base.Creator,
    base.Retriever,
    base.Enumerator,
    base.VersionManager,
    Protocol,
):
    docs: DocsRepository

    def create(self, name: str) -> Model:
        """Creates a model.

        Parameters
        ----------
        name : str
            The name of the model.

        Raises
        ------
        :class:`ixmp4.core.exceptions.ModelNotUnique`:
            If the model with `name` is not unique.

        Returns
        -------
        :class:`ixmp4.data.abstract.Model`:
            The created model.
        """
        ...

    def get(self, name: str) -> Model:
        """Retrieves a model.

        Parameters
        ----------
        name : str
            The unique name of the model.

        Raises
        ------
        :class:`ixmp4.data.abstract.Model.NotFound`:
            If the model with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.Model`:
            The retrieved model.
        """
        ...

    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[Model]:
        r"""Lists models by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.model.filter.ModelFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.Model`]:
            List of Model.
        """
        ...

    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        r"""Tabulate models by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.model.filter.ModelFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name

        """
        ...

    def map(self, **kwargs: Unpack[EnumerateKwargs]) -> dict[int, str]:
        """Return a mapping of model-id to model-name.

        Returns
        -------
        :class:`dict`:
            A dictionary `id` -> `name`
        """
        return dict([(m.id, m.name) for m in self.list(**kwargs)])
