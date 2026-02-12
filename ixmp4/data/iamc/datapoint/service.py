from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.compat_controller import EnumerationCompatibilityController
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.iamc.timeseries.repositories import (
    PandasRepository as TimeSeriesPandasRepository,
)
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.data.services import DirectTransport, Http, Service, procedure

from .df_schemas import DeleteDataPointFrameSchema, UpsertDataPointFrameSchema
from .filter import DataPointFilter
from .repositories import PandasRepository, VersionRepository


class DataPointService(Service):
    router_prefix = "/iamc/datapoints"
    router_tags = ["iamc-datapoints"]

    http_controller = EnumerationCompatibilityController
    executor: db.r.SessionExecutor
    pandas: PandasRepository
    versions: VersionRepository

    default_filter: DataPointFilter = {"run": {"default_only": True}}

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
        self.pandas = PandasRepository(self.executor, **self.get_auth_kwargs(transport))
        self.timeseries = TimeSeriesPandasRepository(self.executor)
        self.versions = VersionRepository(self.executor)

    def get_columns(
        self, *, join_parameters: bool, join_runs: bool, join_run_id: bool
    ) -> tuple[str, ...] | None:
        if not any([join_run_id, join_parameters, join_runs]):
            return None

        columns = set(self.base_columns)
        if join_parameters:
            columns |= self.ts_columns
        if join_runs:
            columns |= self.run_columns
        if join_run_id:
            columns |= {"run__id"}
        return tuple(columns)

    @procedure(Http(methods=("PATCH",)))
    def tabulate(
        self,
        join_parameters: bool = False,
        join_runs: bool = False,
        join_run_id: bool = False,
        **kwargs: Unpack[DataPointFilter],
    ) -> SerializableDataFrame:
        r"""Tabulates datapoints by specified criteria.

        Parameters
        ----------
        join_parameters: bool, optional
            Whether to include region, unit and variable in the data frame.
            Default: ``False``
        join_runs: bool, optional
            Whether to include model, scenario and version in the data frame.
            Default: ``False``
        join_run_id: bool, optional
            Whether to include run__id in the data frame.
            Default: ``False``
        \*\*kwargs: any
            Filter parameters as specified in `DataPointFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - step_year
                - step_category
                - step_datetime
                - type
                - values
            if `join_parameters` is ``True``:
                - region
                - unit
                - variable
            if `join_runs` is ``True``:
                - model
                - scenario
                - version
            if `join_run_id` is ``True``:
                - run__id

        """

        return self.pandas.tabulate(
            values=self.apply_filter_defaults(kwargs),
            columns=self.get_columns(
                join_parameters=join_parameters,
                join_runs=join_runs,
                join_run_id=join_run_id,
            ),
        )

    @tabulate.auth_check()
    def tabulate_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @tabulate.paginated()
    def paginated_tabulate(
        self,
        pagination: Pagination,
        join_parameters: bool = False,
        join_runs: bool = False,
        join_run_id: bool = False,
        **kwargs: Unpack[DataPointFilter],
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult[SerializableDataFrame](
            results=self.pandas.tabulate(
                values=self.apply_filter_defaults(kwargs),
                limit=pagination.limit,
                offset=pagination.offset,
                columns=self.get_columns(
                    join_parameters=join_parameters,
                    join_runs=join_runs,
                    join_run_id=join_run_id,
                ),
            ),
            total=self.pandas.count(values=self.apply_filter_defaults(kwargs)),
            pagination=pagination,
        )

    @procedure(Http(methods=("POST",)))
    def bulk_upsert(self, df: SerializableDataFrame) -> None:
        """Bulk inserts or updates datapoints from a supplied dataframe.

        This method accepts a dataframe containing datapoint data and
        validates it against the upsert schema before inserting or updating
        records in the database. The upsert operation is keyed on the
        subset of full key columns present in the dataframe.

        Parameters
        ----------
        df: :class:`pandas.DataFrame`
            DataFrame containing rows of datapoint data to upsert.
            Must conform to `UpsertDataPointFrameSchema` structure.
            Key columns include:
                - time_series__id
                - step_category and/or step_year or step_datetime
                - type, optional
                - value

        Raises
        ------
        :class:`InvalidDataFrame`
            If the dataframe does not conform to `UpsertDataPointFrameSchema`.
        """
        df = self.validate_df_or_raise(df, UpsertDataPointFrameSchema)
        self.pandas.upsert(df, key=self.full_key & set(df.columns))

    @bulk_upsert.auth_check()
    def bulk_upsert_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        /,
        df: SerializableDataFrame,
    ) -> None:
        timeseries_ids = df["time_series__id"].unique().tolist()
        model_names = self.timeseries.list_model_names(timeseries_ids)
        auth_ctx.has_edit_permission(platform, models=model_names, raise_exc=Forbidden)

    @procedure(Http(methods=("DELETE",)))
    def bulk_delete(self, df: SerializableDataFrame) -> None:
        """Bulk deletes datapoints from a supplied dataframe.

        This method accepts a dataframe containing datapoint identifiers and
        deletes the matching records from the database. After deletion, orphaned
        timeseries (those with no remaining datapoints) are also removed.

        Parameters
        ----------
        df: :class:`pandas.DataFrame`
            DataFrame containing rows of datapoint identifiers to delete.
            Must conform to `DeleteDataPointFrameSchema` structure.
            Key columns include:
                - time_series__id
                - step_category and/or step_year or step_datetime
                - type, optional

        Raises
        ------
        :class:`InvalidDataFrame`
            If the dataframe does not conform to `DeleteDataPointFrameSchema`.
        """
        df = self.validate_df_or_raise(df, DeleteDataPointFrameSchema)
        self.pandas.delete(df, key=self.full_key & set(df.columns))
        self.timeseries.delete_orphans()

    @bulk_delete.auth_check()
    def bulk_delete_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        df: SerializableDataFrame,
    ) -> None:
        timeseries_ids = df["time_series__id"].unique().tolist()
        model_names = self.timeseries.list_model_names(timeseries_ids)
        auth_ctx.has_edit_permission(platform, models=model_names, raise_exc=Forbidden)
