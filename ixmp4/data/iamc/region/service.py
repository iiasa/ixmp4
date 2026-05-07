from typing import List

from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.executor import SessionExecutor
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.compat_controller import EnumerationCompatibilityController
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.data.region.dto import Region
from ixmp4.data.region.filter import IamcRegionFilter
from ixmp4.data.services import DirectTransport, Http, Service, procedure

from .repositories import ItemRepository, PandasRepository


class IamcRegionService(Service):
    router_prefix = "/iamc/regions"
    router_tags = ["iamc-regions"]

    http_controller = EnumerationCompatibilityController
    executor: SessionExecutor
    items: ItemRepository
    pandas: PandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor, **self.get_auth_kwargs(transport))
        self.pandas = PandasRepository(self.executor, **self.get_auth_kwargs(transport))

    @procedure(Http(methods=("PATCH",)))
    def list(self, **kwargs: Unpack[IamcRegionFilter]) -> List[Region]:
        r"""Lists regions **with iamc data** by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in :class:`IamcRegionFilter`.

        Returns
        -------
        list[:class:`ixmp4.data.region.dto.Region`]:
            List of regions.
        """

        return [
            Region.model_validate(i)
            for i in self.items.list(values=self.apply_filter_defaults(kwargs))
        ]

    @list.auth_check()
    def list_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[IamcRegionFilter]
    ) -> PaginatedResult[List[Region]]:
        return PaginatedResult(
            results=[
                Region.model_validate(i)
                for i in self.items.list(
                    values=self.apply_filter_defaults(kwargs),
                    limit=pagination.limit,
                    offset=pagination.offset,
                )
            ],
            total=self.items.count(values=self.apply_filter_defaults(kwargs)),
            pagination=pagination,
        )

    @procedure(Http(methods=("PATCH",)))
    def tabulate(self, **kwargs: Unpack[IamcRegionFilter]) -> SerializableDataFrame:
        r"""Tabulates regions **with iamc data** by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in :class:`IamcRegionFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
                - hierarchy
        """

        return self.pandas.tabulate(values=self.apply_filter_defaults(kwargs))

    @tabulate.auth_check()
    def tabulate_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[IamcRegionFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult[SerializableDataFrame](
            results=self.pandas.tabulate(
                values=self.apply_filter_defaults(kwargs),
                limit=pagination.limit,
                offset=pagination.offset,
            ),
            total=self.pandas.count(values=self.apply_filter_defaults(kwargs)),
            pagination=pagination,
        )
