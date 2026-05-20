from typing import List

from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.executor import SessionExecutor
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.compat_controller import EnumerationCompatibilityController
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.data.services import Http, Service, procedure
from ixmp4.data.unit.dto import Unit
from ixmp4.data.unit.filter import IamcUnitFilter
from ixmp4.transport import DirectTransport

from .repositories import ItemRepository, PandasRepository


class IamcUnitService(Service):
    router_prefix = "/iamc/units"
    router_tags = ["iamc-units"]

    http_controller = EnumerationCompatibilityController
    executor: SessionExecutor
    items: ItemRepository
    pandas: PandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor, **self.get_auth_kwargs(transport))
        self.pandas = PandasRepository(self.executor, **self.get_auth_kwargs(transport))

    @procedure(Http(methods=("PATCH",)))
    def list(self, **kwargs: Unpack[IamcUnitFilter]) -> List[Unit]:
        r"""Lists units **with iamc data** by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in :class:`IamcUnitFilter`.

        Returns
        -------
        list[:class:`ixmp4.data.unit.dto.Unit`]:
            List of units.
        """

        return [
            Unit.model_validate(i)
            for i in self.items.list(values=self.apply_filter_defaults(kwargs))
        ]

    @list.auth_check()
    def list_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[IamcUnitFilter]
    ) -> PaginatedResult[List[Unit]]:
        return PaginatedResult(
            results=[
                Unit.model_validate(i)
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
    def tabulate(self, **kwargs: Unpack[IamcUnitFilter]) -> SerializableDataFrame:
        r"""Tabulates units **with iamc data** by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in :class:`IamcUnitFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """

        return self.pandas.tabulate(values=self.apply_filter_defaults(kwargs))

    @tabulate.auth_check()
    def tabulate_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[IamcUnitFilter]
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
