from typing import List

import pandas as pd
from toolkit import db
from toolkit.exceptions import Unauthorized
from typing_extensions import Unpack

from ixmp4.rewrite.data.pagination import PaginatedResult, Pagination
from ixmp4.rewrite.services import (
    DirectTransport,
    Service,
    paginated_procedure,
    procedure,
)

from .dto import Region
from .filter import RegionFilter
from .repositories import (
    ItemRepository,
    PandasRepository,
    RegionNotFound,
    RegionNotUnique,
)


class RegionService(Service):
    router_prefix = "/regions"
    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)

    @procedure(methods=["POST"])
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
        self.auth_ctx.has_management_permission(self.platform, raise_exc=Unauthorized)

        self.items.create({"name": name, "hierarchy": hierarchy})
        return Region.model_validate(self.items.get({"name": name}))

    @procedure(methods=["DELETE"])
    def delete(self, id: int) -> None:
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
        self.auth_ctx.has_management_permission(self.platform, raise_exc=Unauthorized)

        self.items.delete_by_pk({"id": id})

    @procedure(methods=["POST"])
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
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return Region.model_validate(self.items.get({"name": name}))

    @procedure(methods=["POST"])
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
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return Region.model_validate(self.items.get_by_pk({"id": id}))

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

    @paginated_procedure(methods=["PATCH"])
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
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return [Region.model_validate(i) for i in self.items.list(values=kwargs)]

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[RegionFilter]
    ) -> PaginatedResult[List[Region]]:
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

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

    @paginated_procedure(methods=["PATCH"])
    def tabulate(self, **kwargs: Unpack[RegionFilter]) -> pd.DataFrame:
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
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return self.pandas.tabulate(values=kwargs)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[RegionFilter]
    ) -> PaginatedResult[pd.DataFrame]:
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
