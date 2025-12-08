from typing import Any, List

from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.docs.service import DocsService
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.services import DirectTransport, GetByIdService, Http, procedure

from .db import ModelDocs
from .dto import Model
from .filter import ModelFilter
from .repositories import ItemRepository, PandasRepository, VersionRepository


class ModelService(DocsService, GetByIdService):
    router_prefix = "/models"
    router_tags = ["models"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository
    versions: VersionRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        self.versions = VersionRepository(self.executor)

        DocsService.__init_direct__(self, transport, docs_model=ModelDocs)

    @procedure(Http(path="/", methods=["POST"]))
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
        self.items.create({"name": name, **self.get_creation_info()})
        return Model.model_validate(self.items.get({"name": name}))

    @create.auth_check()
    def create_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_management_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id}/", methods=["DELETE"]))
    def delete_by_id(self, id: int) -> None:
        """Deletes a model.

        Parameters
        ----------
        id : int
            The unique integer id of the model.

        Raises
        ------
        :class:`ModelNotFound`:
            If the model with `id` does not exist.
        :class:`ModelDeletionPrevented`:
            If the model with `id` is used in the database, preventing it's deletion.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        """
        self.items.delete_by_pk({"id": id})

    @delete_by_id.auth_check()
    def delete_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_management_permission(platform, raise_exc=Forbidden)

    @procedure(Http(methods=["POST"]))
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
        return Model.model_validate(self.items.get({"name": name}))

    @get_by_name.auth_check()
    def get_by_name_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id}/", methods=["GET"]))
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

        return Model.model_validate(self.items.get_by_pk({"id": id}))

    @get_by_id.auth_check()
    def get_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(Http(methods=["PATCH"]))
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

        return [Model.model_validate(i) for i in self.items.list(values=kwargs)]

    @list.auth_check()
    def list_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[ModelFilter]
    ) -> PaginatedResult[List[Model]]:
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

    @procedure(Http(methods=["PATCH"]))
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

        return self.pandas.tabulate(values=kwargs)

    @tabulate.auth_check()
    def tabulate_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[ModelFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
