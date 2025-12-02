from typing import Any

from toolkit import db
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.iamc.timeseries.repositories import (
    PandasRepository as TimeSeriesPandasRepository,
)
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.services import (
    DirectTransport,
    Service,
    paginated_procedure,
    procedure,
)

from .df_schemas import DeleteDataPointFrameSchema, UpsertDataPointFrameSchema
from .filter import DataPointFilter
from .repositories import PandasRepository


class DataPointService(Service):
    router_prefix = "/iamc/datapoints"
    router_tags = ["iamc", "datapoints"]

    executor: db.r.SessionExecutor
    pandas: PandasRepository

    full_key = {
        "time_series__id",
        "type",
        "step_year",
        "step_category",
        "step_datetime",
    }
    base_columns = {
        "id",
        "time_series__id",
        "type",
        "step_year",
        "step_category",
        "step_datetime",
        "value",
    }
    ts_columns = {"region", "unit", "variable"}
    run_columns = {"model", "scenario", "version"}

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.pandas = PandasRepository(self.executor)
        self.timeseries = TimeSeriesPandasRepository(self.executor)

    def get_columns(
        self,
        join_parameters: bool,
        join_runs: bool,
        join_run_id: bool,
    ) -> set[str] | None:
        if not any([join_run_id, join_parameters, join_runs]):
            return None
        columns = set(self.base_columns)
        if join_parameters:
            columns |= self.ts_columns
        if join_runs:
            columns |= self.run_columns
        if join_run_id:
            columns |= {"run__id"}
        return columns

    @paginated_procedure(methods=["PATCH"])
    def tabulate(
        self,
        join_parameters: bool | None = False,
        join_runs: bool = False,
        join_run_id: bool = False,
        **kwargs: Unpack[DataPointFilter],
    ) -> SerializableDataFrame:
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

        return self.pandas.tabulate(
            values=kwargs,
            columns=self.get_columns(join_parameters, join_runs, join_run_id),
        )

    @tabulate.auth_check()
    def tabulate_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @tabulate.paginated()
    def paginated_tabulate(
        self,
        pagination: Pagination,
        join_parameters: bool | None = False,
        join_runs: bool = False,
        join_run_id: bool = False,
        **kwargs: Unpack[DataPointFilter],
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs,
                limit=pagination.limit,
                offset=pagination.offset,
                columns=self.get_columns(join_parameters, join_runs, join_run_id),
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )

    @procedure(methods=["POST"])
    def bulk_upsert(self, df: SerializableDataFrame) -> None:
        df = self.validate_df_or_raise(df, UpsertDataPointFrameSchema)
        self.pandas.upsert(df, key=self.full_key & set(df.columns))

    @bulk_upsert.auth_check()
    def bulk_upsert_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        # TODO: get list of models from list of timeseries__ids
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(methods=["DELETE"])
    def bulk_delete(self, df: SerializableDataFrame) -> None:
        df = self.validate_df_or_raise(df, DeleteDataPointFrameSchema)
        self.pandas.delete(df, key=self.full_key & set(df.columns))
        self.timeseries.delete_orphans()

    @bulk_delete.auth_check()
    def bulk_delete_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        # TODO: get list of models from list of timeseries__ids
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)
