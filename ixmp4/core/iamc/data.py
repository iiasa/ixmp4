from typing import TYPE_CHECKING

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

# from ixmp4.data.db.iamc.utils import (
#     AddDataPointFrameSchema,
#     RemoveDataPointFrameSchema,
#     normalize_df,
# )
from ixmp4.backend import Backend
from ixmp4.data.iamc.datapoint.filter import DataPointFilter
from ixmp4.data.iamc.datapoint.type import Type

from ..base import BaseBackendFacade
from .variable import VariableServiceFacade

if TYPE_CHECKING:
    from ..run import Run


class RunIamcData(BaseBackendFacade):
    """IAMC data.

    Parameters
    ----------
    backend : ixmp4.data.backend.Backend
        Data source backend.
    run : ixmp4.base.run.Run
        Model run.
    """

    _run: "Run"

    def __init__(self, backend: Backend, run: "Run") -> None:
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
        self._run.require_lock()
        # df = AddDataPointFrameSchema.validate(df) TODO
        df = self._rename_arg_cols(df)
        df["run__id"] = self._run.id
        df = self._get_or_create_ts(df)

        if type is not None:
            df["type"] = type

        self._backend.iamc.datapoints.bulk_upsert(df)

    def remove(self, df: pd.DataFrame, type: Type | None = None) -> None:
        self._run.require_lock()
        # df = RemoveDataPointFrameSchema.validate(df) TODO
        df = self._rename_arg_cols(df)
        df["run__id"] = self._run.id
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
        df = self._backend.iamc.datapoints.tabulate(
            join_parameters=True,
            join_runs=False,
            run={"id": self._run.id, "default_only": False},
            **kwargs,
        )
        # return normalize_df(df, raw, False, False)
        return self._rename_ret_cols(df)


class PlatformIamcData(BaseBackendFacade):
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
        df = self._backend.iamc.datapoints.tabulate(
            join_parameters=True,
            join_runs=join_runs,
            join_run_id=join_run_id,
            **kwargs,
        )

        return self._rename_ret_cols(df)
        # return normalize_df(df, raw, join_runs, join_run_id)
