from typing import Any, List

from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.docs.service import DocsService
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.services import DirectTransport, GetByIdService, Http, procedure

from .db import RegionDocs
from .dto import Region
from .exceptions import (
    RegionNotFound,
    RegionNotUnique,
)
from .filter import RegionFilter
from .repositories import ItemRepository, PandasRepository, VersionRepository


class RegionService(DocsService, GetByIdService):
    router_prefix = "/regions"
    router_tags = ["regions"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository
    versions: VersionRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        self.versions = VersionRepository(self.executor)

        DocsService.__init_direct__(self, transport, docs_model=RegionDocs)

    @procedure(Http(path="/", methods=["POST"]))
    def create(self, name: str, hierarchy: str) -> Region:
        """Creates a region.

        Parameters
        ----------
        name : str
            The name of the region.
        hierarchy : str
            The hierarchy this region is assigned to.

        Raises
        ------
        :class:`RegionNotUnique`:
            If the region with `name` is not unique.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Region`:
            The created region.
        """
        self.items.create(
            {"name": name, "hierarchy": hierarchy, **self.get_creation_info()}
        )
        return Region.model_validate(self.items.get({"name": name}))

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
        """Deletes a region.

        Parameters
        ----------
        id : int
            The unique integer id of the region.

        Raises
        ------
        :class:`RegionNotFound`:
            If the region with `id` does not exist.
        :class:`RegionDeletionPrevented`:
            If the region with `id` is used in the database, preventing it's deletion.
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
    def get_by_name(self, name: str) -> Region:
        """Retrieves a region by its name.

        Parameters
        ----------
        name : str
            The unique name of the region.

        Raises
        ------
        :class:`RegionNotFound`:
            If the region with `name` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Region`:
            The retrieved region.
        """
        return Region.model_validate(self.items.get({"name": name}))

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
    def get_by_id(self, id: int) -> Region:
        """Retrieves a region by its id.

        Parameters
        ----------
        id : int
            The integer id of the region.

        Raises
        ------
        :class:`RegionNotFound`:
            If the region with `id` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Region`:
            The retrieved region.
        """

        return Region.model_validate(self.items.get_by_pk({"id": id}))

    @get_by_id.auth_check()
    def get_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    def get_or_create(self, name: str, hierarchy: str | None = None) -> Region:
        try:
            region = self.get_by_name(name)
        except RegionNotFound:
            if hierarchy is None:
                raise TypeError(
                    "Argument `hierarchy` is required if `Region` with `name` does not "
                    "exist."
                )
            return self.create(name, hierarchy)

        if hierarchy is not None and region.hierarchy != hierarchy:
            raise RegionNotUnique(name)
        else:
            return region

    @procedure(Http(methods=["PATCH"]))
    def list(self, **kwargs: Unpack[RegionFilter]) -> list[Region]:
        r"""Lists regions by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `RegionFilter`.

        Raises
        ------
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        list[:class:`Region`]:
            List of regions.
        """
        return [Region.model_validate(i) for i in self.items.list(values=kwargs)]

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
        self, pagination: Pagination, **kwargs: Unpack[RegionFilter]
    ) -> PaginatedResult[List[Region]]:
        return PaginatedResult(
            results=[
                Region.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    @procedure(Http(methods=["PATCH"]))
    def tabulate(self, **kwargs: Unpack[RegionFilter]) -> SerializableDataFrame:
        r"""Tabulates regions by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `RegionFilter`.

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
        self, pagination: Pagination, **kwargs: Unpack[RegionFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
