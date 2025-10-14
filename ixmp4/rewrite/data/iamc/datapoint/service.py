from toolkit import db
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance
from typing_extensions import Unpack

from ixmp4.rewrite.data.dataframe import SerializableDataFrame
from ixmp4.rewrite.data.iamc.timeseries.repositories import (
    PandasRepository as TimeSeriesPandasRepository,
)
from ixmp4.rewrite.data.pagination import PaginatedResult, Pagination
from ixmp4.rewrite.exceptions import Forbidden
from ixmp4.rewrite.services import (
    DirectTransport,
    Service,
    paginated_procedure,
    procedure,
)

from .filter import DataPointFilter
from .repositories import PandasRepository


class DataPointService(Service):
    router_prefix = "/iamc/datapoints"
    router_tags = ["iamc", "datapoints"]

    executor: db.r.SessionExecutor
    pandas: PandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.pandas = PandasRepository(self.executor)
        self.timeseries = TimeSeriesPandasRepository(self.executor)

    @paginated_procedure(methods=["PATCH"])
    def tabulate(self, **kwargs: Unpack[DataPointFilter]) -> SerializableDataFrame:
        r"""Tabulates datapoints by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `DataPointFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - TODO
        """

        # TODO: get list of models from list of timeseries__ids
        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return self.pandas.tabulate(values=kwargs)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[DataPointFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        # TODO: get list of models from list of timeseries__ids
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

    @procedure(methods=["POST"])
    def bulk_upsert(self, df: SerializableDataFrame) -> None:
        # TODO: get list of models from list of timeseries__ids
        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

        self.pandas.upsert(df)

    @procedure(methods=["DELETE"])
    def bulk_delete(self, df: SerializableDataFrame) -> None:
        # TODO: get list of models from list of timeseries__ids
        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

        self.pandas.delete(df)
        self.timeseries.delete_orphans()
