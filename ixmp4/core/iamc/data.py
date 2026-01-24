from typing import TYPE_CHECKING

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

# from ixmp4.data.db.iamc.utils import (
#     AddDataPointFrameSchema,
#     RemoveDataPointFrameSchema,
#     normalize_df,
# )
from ixmp4.data.backend import Backend
from ixmp4.data.iamc.datapoint.filter import DataPointFilter
from ixmp4.data.iamc.datapoint.type import Type

from ..base import BaseBackendFacade
from .variable import VariableServiceFacade

if TYPE_CHECKING:
    from ixmp4.core import run


class RunIamcData(BaseBackendFacade):
    """IAMC data linked to a :class:`ixmp4.core.run.Run`.

    .. code:: python

        import pandas as pd

        input_df = pd.read_csv("my_iamc_data.csv")
        run.iamc.add(input_df)

        returned_df = run.iamc.tabulate()
        print(returned_df)

        run.iamc.remove(input_df.drop(columns=["value"]))

    """

    _run: "run.Run"

    def __init__(self, backend: Backend, run: "run.Run") -> None:
        super().__init__(backend)
        self._run = run

    def _get_or_create_ts(self, df: pd.DataFrame) -> pd.DataFrame:
        id_cols = ["region", "variable", "unit", "run__id"]
        # upsert set of unqiue timeseries
        ts_df = df[id_cols].drop_duplicates()
        self._backend.iamc.timeseries.bulk_upsert(ts_df)

        # retrieve them again to get database ids
        ts_df = self._backend.iamc.timeseries.tabulate_by_df(ts_df)
        ts_df = ts_df.rename(columns={"id": "time_series__id"})

        # merge on the identity columns
        return pd.merge(
            df, ts_df, how="left", on=id_cols, suffixes=(None, "_y")
        )  # tada, df with 'time_series__id' added from the database.

    def _rename_arg_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "year": "step_year",
                "category": "step_category",
                "datetime": "step_datetime",
            }
        )

    def _rename_ret_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "step_year": "year",
                "step_category": "category",
                "step_datetime": "datetime",
            }
        ).drop(columns=["id", "time_series__id"])

    def add(self, df: pd.DataFrame, type: Type | None = None) -> None:
        """Adds IAMC data from a data frame to a run.

        .. code:: python

            import pandas as pd

            input_df = pd.DataFrame({
                "region": ["World"],
                "variable": ["Emissions|CO2"],
                "unit": ["MtCO2/yr"],
                "year": [2020],
                "value": [36.5],
            })

            run.iamc.add(input_df)

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            A data frame with the columns:
                - region
                - variable
                - unit
                - value

            Any combination of:
                - step_year for ANNUAL data points
                - step_year and step_category for CATEGORICAL data points
                - step_datetime for DATETIME data points

            You may optionally supply the type column for mixed data points:
                - type

        type: :class:`ixmp4.data.iamc.datapoint.type.Type`, optional
            Will be set as the type for all provided data points.

        """

        self._run.require_lock()
        # df = AddDataPointFrameSchema.validate(df) TODO
        df = self._rename_arg_cols(df)
        df["run__id"] = self._run.id
        df = self._get_or_create_ts(df)

        if type is not None:
            df["type"] = type

        self._backend.iamc.datapoints.bulk_upsert(df)

    def remove(self, df: pd.DataFrame, type: Type | None = None) -> None:
        """Removes IAMC data matchin a data frame from a run.

        .. code:: python

            import pandas as pd

            remove_df = pd.DataFrame({
                "region": ["World"],
                "variable": ["Emissions|CO2"],
                "unit": ["MtCO2/yr"],
                "year": [2020],
            })

            run.iamc.remove(remove_df)

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            A data frame with the columns:
                - region
                - variable
                - unit

            Any combination of:
                - step_year for ANNUAL data points
                - step_year and step_category for CATEGORICAL data points
                - step_datetime for DATETIME data points

            You may optionally supply the type column for mixed data points:
                - type

        type: :class:`ixmp4.data.iamc.datapoint.type.Type`, optional
            Will be set as the type for all provided data points.
        """

        self._run.require_lock()
        # df = RemoveDataPointFrameSchema.validate(df) TODO
        df = self._rename_arg_cols(df)
        df["run__id"] = self._run.id
        # TODO: This creates ts and deletes them right after
        df = self._get_or_create_ts(df)
        if type is not None:
            df["type"] = type

        df = df.drop(columns=["unit", "variable", "region"])
        self._backend.iamc.datapoints.bulk_delete(df)

    def tabulate(
        self,
        raw: bool = False,
        **kwargs: Unpack[DataPointFilter],
    ) -> pd.DataFrame:
        r"""Tabulates datapoints by specified criteria.

        .. code:: python

            df = run.iamc.tabulate()
            #>    region    unit       variable        year  value
            # 0  World    MtCO2/yr   Emissions|CO2     2020  36.5

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `DataPointFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - region
                - unit
                - variable
                - year
                - category
                - datetime
                - type
        """

        kwargs["run"] = {"id": self._run.id, "default_only": False}
        df = self._backend.iamc.datapoints.tabulate(
            join_parameters=True,
            join_runs=False,
            **kwargs,
        )
        # return normalize_df(df, raw, False, False)
        return self._rename_ret_cols(df)


class PlatformIamcData(BaseBackendFacade):
    """IAMC data on a platform."""

    variables: VariableServiceFacade

    def __init__(self, backend: Backend) -> None:
        super().__init__(backend)
        self.variables = VariableServiceFacade(backend)

    def _rename_ret_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "step_year": "year",
                "step_category": "category",
                "step_datetime": "datetime",
            }
        ).drop(columns=["id", "time_series__id"])

    def tabulate(
        self,
        *,
        join_runs: bool = True,
        join_run_id: bool = False,
        raw: bool = False,
        **kwargs: Unpack[DataPointFilter],
    ) -> pd.DataFrame:
        r"""Tabulates datapoints by specified criteria.

        .. code:: python

            df = platform.iamc.tabulate()
            #>   model   scenario  version  region  ...   year  value
            # 0  Model  Scenario   1         World  ...  2020   36.5

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `DataPointFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - model
                - scenario
                - version
                - region
                - unit
                - variable
                - year
                - category
                - datetime
                - type
        """

        df = self._backend.iamc.datapoints.tabulate(
            join_parameters=True,
            join_runs=join_runs,
            join_run_id=join_run_id,
            **kwargs,
        )

        return self._rename_ret_cols(df)
        # return normalize_df(df, raw, join_runs, join_run_id)
