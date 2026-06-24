import pandas as pd
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.executor import SessionExecutor
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.compat_controller import EnumerationCompatibilityController
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.iamc.measurand.repositories import (
    PandasRepository as MeasurandPandasRepository,
)
from ixmp4.data.iamc.variable.repositories import (
    PandasRepository as VariablePandasRepository,
)
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.data.region.exceptions import RegionNotFound
from ixmp4.data.region.repositories import (
    PandasRepository as RegionPandasRepository,
)
from ixmp4.data.run.repositories import ItemRepository as RunRepository
from ixmp4.data.services import Http, Service, procedure
from ixmp4.data.unit.exceptions import UnitNotFound
from ixmp4.data.unit.repositories import (
    PandasRepository as UnitPandasRepository,
)
from ixmp4.transport import DirectTransport

from .df_schemas import TabulateTimeSeriesFrameSchema, UpsertTimeSeriesFrameSchema
from .filter import TimeSeriesFilter
from .repositories import PandasRepository, VersionRepository


class TimeSeriesService(Service):
    router_prefix = "/iamc/timeseries"
    router_tags = ["iamc", "timeseries"]

    http_controller = EnumerationCompatibilityController
    executor: SessionExecutor
    pandas: PandasRepository
    versions: VersionRepository

    measurands: MeasurandPandasRepository
    regions: RegionPandasRepository
    units: UnitPandasRepository
    variables: VariablePandasRepository
    runs: RunRepository

    default_filter: TimeSeriesFilter = {"run": {"default_only": True}}

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = SessionExecutor(transport.session)
        self.pandas = PandasRepository(self.executor, **self.get_auth_kwargs(transport))
        self.versions = VersionRepository(self.executor)

        self.measurands = MeasurandPandasRepository(self.executor)
        self.regions = RegionPandasRepository(self.executor)
        self.units = UnitPandasRepository(self.executor)
        self.variables = VariablePandasRepository(self.executor)
        self.runs = RunRepository(self.executor)

    @procedure(Http(methods=("PATCH",)))
    def tabulate_by_df(self, df: SerializableDataFrame) -> SerializableDataFrame:
        """Tabulates timeseries by values in a supplied dataframe.

        Parameters
        ----------
        df: :class:`pandas.DataFrame`
            DataFrame containing rows of timeseries keys to tabulate.
                - run__id
                - region
                - variable
                - unit

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - run__id
                - region
                - variable
                - unit
        """
        df = self.validate_df_or_raise(df, TabulateTimeSeriesFrameSchema)

        return self.pandas.tabulate_by_df(
            df,
            key=["run__id", "region", "variable", "unit"],
            columns=["id", "run__id", "region", "variable", "unit"],
        )

    @tabulate_by_df.auth_check()
    def tabulate_by_df_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(Http(methods=("PATCH",)))
    def tabulate(
        self, join_parameters: bool = False, **kwargs: Unpack[TimeSeriesFilter]
    ) -> SerializableDataFrame:
        r"""Tabulates timeseries by specified criteria.

        Parameters
        ----------
        join_parameters: bool, optional
            Include names of related region, variable and unit rows.
        \*\*kwargs: any
            Filter parameters as specified in :class:`TimeSeriesFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - run__id
            if ``join_parameters`` is ``False`` (default):
                - region__id
                - measurand__id
            if ``join_parameters`` is ``True``:
                - region
                - variable
                - unit
        """
        if join_parameters:
            columns = ["id", "run__id", "region", "variable", "unit"]
        else:
            columns = None

        return self.pandas.tabulate(
            values=self.apply_filter_defaults(kwargs), columns=columns
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
        **kwargs: Unpack[TimeSeriesFilter],
    ) -> PaginatedResult[SerializableDataFrame]:
        if join_parameters:
            columns = ["id", "run__id", "region", "variable", "unit"]
        else:
            columns = None

        return PaginatedResult(
            results=self.pandas.tabulate(
                values=self.apply_filter_defaults(kwargs),
                columns=columns,
                limit=pagination.limit,
                offset=pagination.offset,
            ),
            total=self.pandas.count(values=self.apply_filter_defaults(kwargs)),
            pagination=pagination,
        )

    def merge_regions(self, df: pd.DataFrame) -> pd.DataFrame:
        region_names = (
            df[["region"]].drop_duplicates().rename(columns={"region": "name"})
        )
        regions = self.regions.tabulate_by_df(region_names, columns=["id", "name"])
        regions = regions.rename(columns={"name": "region", "id": "region__id"})
        merged_df = df.merge(
            regions,
            how="left",
            on=["region"],
        )
        missing_regions = merged_df[pd.isna(merged_df["region__id"])]
        if not missing_regions.empty:
            missing_region_names = missing_regions["region"].unique()
            raise RegionNotFound(", ".join(missing_region_names))

        return merged_df.drop(columns=["region"])

    def merge_units(self, df: pd.DataFrame) -> pd.DataFrame:
        unit_names = df[["unit"]].drop_duplicates().rename(columns={"unit": "name"})
        units = self.units.tabulate_by_df(unit_names, columns=["id", "name"])
        units = units.rename(columns={"name": "unit", "id": "unit__id"})
        merged_df = df.merge(
            units,
            how="left",
            on=["unit"],
        )
        missing_units = merged_df[pd.isna(merged_df["unit__id"])]
        if not missing_units.empty:
            missing_unit_names = missing_units["unit"].unique()
            raise UnitNotFound(", ".join(missing_unit_names))

        return merged_df.drop(columns=["unit"])

    def merge_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        variable_df = df[["variable"]].rename(columns={"variable": "name"})
        variable_df = variable_df.drop_duplicates()
        variable_names = variable_df["name"].to_list()
        self.variables.upsert(variable_df, insert_values=self.get_creation_info())

        variables = self.variables.tabulate(
            values={"name__in": variable_names}, columns=["id", "name"]
        )
        variables = variables.rename(columns={"name": "variable", "id": "variable__id"})
        merged_df = df.merge(
            variables,
            how="left",
            on=["variable"],
        )
        return merged_df.drop(columns=["variable"])

    def merge_measurands(self, df: pd.DataFrame) -> pd.DataFrame:
        measurand_df = df[["variable__id", "unit__id"]].drop_duplicates()
        self.measurands.upsert(measurand_df, insert_values=self.get_creation_info())

        measurand_df = self.measurands.tabulate_by_df(
            measurand_df, columns=["id", "variable__id", "unit__id"]
        )
        measurand_df = measurand_df.rename(columns={"id": "measurand__id"})
        merged_df = df.merge(
            measurand_df,
            how="left",
            on=["variable__id", "unit__id"],
        )
        return merged_df.drop(columns=["variable__id", "unit__id"])

    @procedure(Http(methods=("POST",)))
    def bulk_upsert(self, df: SerializableDataFrame) -> None:
        r"""Bulk inserts or updates timeseries from a supplied dataframe.

        This method accepts a dataframe containing timeseries data and
        automatically resolves region, unit, and variable references,
        upserting them as needed. Measurands (variable/unit pairs) are also
        created automatically. The method performs validation and merging
        operations before inserting or updating the timeseries records.

        Parameters
        ----------
        df: :class:`pandas.DataFrame`
            DataFrame containing rows of timeseries data to upsert.

            Minimum required columns:
              - run__id
            Optional columns that will be resolved automatically:
              - region: region name; will be resolved to region__id
              - unit: unit name; will be resolved to unit__id
              - variable: variable name; will be instantiated if missing
            To skip resolving related rows, supply ids directly:
              - region__id
              - measurand__id or unit__id and variable__id

        Raises
        ------
        :class:`RegionNotFound`:
            If one or more region names in the dataframe do not exist.
        :class:`UnitNotFound`:
            If one or more unit names in the dataframe do not exist.
        """
        df = self.validate_df_or_raise(df, UpsertTimeSeriesFrameSchema)
        if df.empty:
            return None

        if "region" in df.columns:
            df = self.merge_regions(df)
        if "unit" in df.columns:
            df = self.merge_units(df)
        if "variable" in df.columns:
            df = self.merge_variables(df)
        if "variable__id" in df.columns and "unit__id" in df.columns:
            df = self.merge_measurands(df)

        self.pandas.upsert(df)

    @bulk_upsert.auth_check()
    def bulk_upsert_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        /,
        df: SerializableDataFrame,
    ) -> None:
        run_ids = df["run__id"].unique().tolist()
        model_names = self.runs.list_model_names(run_ids)
        auth_ctx.has_edit_permission(platform, models=model_names, raise_exc=Forbidden)
