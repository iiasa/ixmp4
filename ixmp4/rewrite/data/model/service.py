from typing import List

from toolkit import db
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance
from typing_extensions import Unpack

from ixmp4.rewrite.data.dataframe import SerializableDataFrame
from ixmp4.rewrite.data.docs.service import DocsService
from ixmp4.rewrite.data.pagination import PaginatedResult, Pagination
from ixmp4.rewrite.exceptions import Forbidden
from ixmp4.rewrite.services import (
    DirectTransport,
    Service,
    paginated_procedure,
    procedure,
)

from .db import ModelDocs
from .dto import Model
from .filter import ModelFilter
from .repositories import ItemRepository, PandasRepository


class ModelService(DocsService, Service):
    router_prefix = "/models"
    router_tags = ["models"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        DocsService.__init_direct__(self, transport, docs_model=ModelDocs)

    @procedure(methods=["POST"])
    def create(self, name: str) -> Model:
        """Creates a model.

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
        :class:`Model`:
            The created model.
        """

        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_management_permission(platform, raise_exc=Forbidden)

        self.items.create({"name": name})
        return Model.model_validate(self.items.get({"name": name}))

    @procedure(methods=["POST"])
    def get_by_name(self, name: str) -> Model:
        """Retrieves a model by its name.

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
        :class:`ixmp4.data.base.iamc.Model`:
            The retrieved model.
        """

        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return Model.model_validate(self.items.get({"name": name}))

    @procedure(path="/{id}/", methods=["GET"])
    def get_by_id(self, id: int) -> Model:
        """Retrieves a model by its id.

        Parameters
        ----------
        id : int
            The integer id of the model.

        Raises
        ------
        :class:`ixmp4.data.abstract.Model.NotFound`:
            If the model with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.base.iamc.Model`:
            The retrieved model.
        """

        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return Model.model_validate(self.items.get_by_pk({"id": id}))

    @paginated_procedure(methods=["PATCH"])
    def list(self, **kwargs: Unpack[ModelFilter]) -> list[Model]:
        r"""Lists models by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `ModelFilter`.

        Returns
        -------
        Iterable[:class:`Model`]:
            List of models.
        """

        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return [Model.model_validate(i) for i in self.items.list(values=kwargs)]

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[ModelFilter]
    ) -> PaginatedResult[List[Model]]:
        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return PaginatedResult(
            results=[
                Model.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    @paginated_procedure(methods=["PATCH"])
    def tabulate(self, **kwargs: Unpack[ModelFilter]) -> SerializableDataFrame:
        r"""Tabulates models by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `ModelFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """

        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return self.pandas.tabulate(values=kwargs)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[ModelFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
