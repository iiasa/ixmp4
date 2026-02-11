from typing import List

from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.compat_controller import EnumerationCompatibilityController
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.data.scenario.dto import Scenario
from ixmp4.data.scenario.filter import IamcScenarioFilter
from ixmp4.data.services import DirectTransport, Http, Service, procedure

from .repositories import ItemRepository, PandasRepository


class IamcScenarioService(Service):
    router_prefix = "/iamc/scenarios"
    router_tags = ["iamc-scenarios"]

    http_controller = EnumerationCompatibilityController
    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor, **self.get_auth_kwargs(transport))
        self.pandas = PandasRepository(self.executor, **self.get_auth_kwargs(transport))

    @procedure(Http(methods=("PATCH",)))
    def list(self, **kwargs: Unpack[IamcScenarioFilter]) -> List[Scenario]:
        r"""Lists scenarios **with iamc data** by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `IamcScenarioFilter`.

        Returns
        -------
        list[:class:`ixmp4.data.scenario.dto.Scenario`]:
            List of scenarios.
        """

        return [
            Scenario.model_validate(i)
            for i in self.items.list(values=self.apply_filter_defaults(kwargs))
        ]

    @list.auth_check()
    def list_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[IamcScenarioFilter]
    ) -> PaginatedResult[List[Scenario]]:
        return PaginatedResult(
            results=[
                Scenario.model_validate(i)
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
    def tabulate(self, **kwargs: Unpack[IamcScenarioFilter]) -> SerializableDataFrame:
        r"""Tabulates scenarios **with iamc data** by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `IamcScenarioFilter`.

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
        self, pagination: Pagination, **kwargs: Unpack[IamcScenarioFilter]
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
