from datetime import datetime
from typing import List

import pandas as pd
from typing_extensions import Unpack

from ixmp4.core.base import BaseDocsServiceFacade, BaseFacadeObject
from ixmp4.core.docs import DocsDescriptor
from ixmp4.data.backend import Backend
from ixmp4.data.model.dto import Model as ModelDto
from ixmp4.data.model.exceptions import (
    ModelDeletionPrevented,
    ModelNotFound,
    ModelNotUnique,
)
from ixmp4.data.model.filter import (
    FacadeModelFilter,
    facade_to_data_filter,
)
from ixmp4.data.model.service import ModelService


class Model(BaseFacadeObject[ModelService, ModelDto]):
    Filter = FacadeModelFilter
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    DeletionPrevented = ModelDeletionPrevented

    docs: DocsDescriptor[ModelService, ModelDto] = DocsDescriptor()
    """Model docs."""

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
        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> ModelService:
        return backend.models

    def __str__(self) -> str:
        return f"<Model name='{self.name}' id={self.id}>"

    def __repr__(self) -> str:
        return str(self)


class ModelServiceFacade(BaseDocsServiceFacade[Model | int | str, Model, ModelService]):
    def _get_service(self, backend: Backend) -> ModelService:
        return backend.models

    def _get_item_id(self, ref: Model | int | str) -> int:
        if isinstance(ref, Model):
            return ref.id
        elif isinstance(ref, int):
            return ref
        elif isinstance(ref, str):
            dto = self._service.get_by_name(ref)
            return dto.id
        else:
            raise ValueError(f"Invalid reference to model: {ref}")

    def create(self, name: str) -> Model:
        """Creates a model.

        .. code:: python

            platform.models.create("Model")
            #> <Model 1 name='Model'>

        Parameters
        ----------
        name : str
            The name of the model.

        Raises
        ------
        :class:`ModelNotUnique`:
            If the model with `name` is not unique.


        Returns
        -------
        :class:`ixmp4.core.model.Model`:
            The created model.
        """

        dto = self._service.create(name)
        return Model(self._backend, dto)

    def delete(self, ref: Model | int | str) -> None:
        """Deletes a model.

        .. code:: python

            platform.models.delete("Model")

        Parameters
        ----------
        ref : :class:`ixmp4.core.model.Model` | int | str
            Model object, model id or model name.

        Raises
        ------
        :class:`ModelNotFound`:
            If no model matching ``ref`` exists.
        :class:`ModelDeletionPrevented`:
            If the model matching ``ref`` is used in the database,
            preventing its deletion.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        """

        id = self._get_item_id(ref)
        self._service.delete_by_id(id)

    def get_by_name(self, name: str) -> Model:
        """Retrieves a model by its name.

        .. code:: python

            platform.models.get_by_name("Model")
            #> <Model 1 name='Model'>

        Parameters
        ----------
        name : str
            The unique name of the model.

        Raises
        ------
        :class:`ModelNotFound`:
            If the model with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.core.model.Model`:
            The retrieved model.
        """

        dto = self._service.get_by_name(name)
        return Model(self._backend, dto)

    def list(self, **kwargs: Unpack[FacadeModelFilter]) -> List[Model]:
        r"""Lists models by specified criteria.

        .. code:: python

            platform.models.list()
            #> [<Model 1 name='Model'>]

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in :class:`ModelFilter`.

        Returns
        -------
        list[:class:`ixmp4.core.model.Model`]:
            List of models.
        """

        models = self._service.list(**facade_to_data_filter(kwargs))
        return [Model(self._backend, dto) for dto in models]

    def tabulate(self, **kwargs: Unpack[FacadeModelFilter]) -> pd.DataFrame:
        r"""Tabulates models by specified criteria.

        .. code:: python

            platform.models.tabulate()
            #>     name  id
            # 0  Model   1

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in :class:`ModelFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """

        return self._service.tabulate(**facade_to_data_filter(kwargs))
