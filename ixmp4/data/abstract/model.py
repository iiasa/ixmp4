from typing import Protocol

import pandas as pd

from ixmp4.data import types

from . import base
from .docs import DocsRepository


class Model(base.BaseModel, Protocol):
    """Data model of an assement modeling "model".
    Unfortunately two naming conventions clash here.
    """

    name: types.String
    "Unique name of the model."

    created_at: types.DateTime
    "Creation date/time. TODO"
    created_by: types.String
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Model {self.id} name={self.name}>"


class ModelRepository(
    base.Creator,
    base.Retriever,
    base.Enumerator,
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

    def list(
        self,
        *,
        name: str | None = None,
    ) -> list[Model]:
        """Lists models by specified criteria.

        Parameters
        ----------
        name : str
            The name of a model. If supplied only one result will be returned.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.Model`]:
            List of Model.
        """
        ...

    def tabulate(
        self,
        *,
        name: str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Tabulate models by specified criteria.

        Parameters
        ----------
        name : str
            The name of a model. If supplied only one result will be returned.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name

        """
        ...

    def map(self, *args, **kwargs) -> dict:
        """Return a mapping of model-id to model-name.

        Returns
        -------
        :class:`dict`:
            A dictionary `id` -> `name`
        """
        return dict([(m.id, m.name) for m in self.list(*args, **kwargs)])
