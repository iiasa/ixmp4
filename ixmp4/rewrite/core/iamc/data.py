from collections.abc import Iterable
from typing import TYPE_CHECKING

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

# from ixmp4.data.db.iamc.utils import (
#     AddDataPointFrameSchema,
#     RemoveDataPointFrameSchema,
#     normalize_df,
# )
from ixmp4.rewrite.backend import Backend
from ixmp4.rewrite.data.iamc.datapoint.filter import DataPointFilter
from ixmp4.rewrite.data.iamc.datapoint.type import Type

from ..base import BaseFacade
from ..utils import substitute_type
from .variable import VariableRepository

if TYPE_CHECKING:
    from ..run import Run


class RunIamcData(BaseFacade):
    """IAMC data.

    Parameters
    ----------
    backend : ixmp4.data.backend.Backend
        Data source backend.
    run : ixmp4.base.run.Run
        Model run.
    """

    run: "Run"

    def __init__(self, backend: Backend, run: "Run") -> None:
        super().__init__(backend)
        self.run = run

    def _get_or_create_ts(self, df: pd.DataFrame) -> pd.DataFrame:
        id_cols = ["region", "variable", "unit", "run__id"]
        # create set of unqiue timeseries (if missing)
        ts_df = df[id_cols].drop_duplicates()
        self._backend.iamc.timeseries.bulk_upsert(ts_df, create_related=True)

        # retrieve them again to get database ids
        ts_df = self._backend.iamc.timeseries.tabulate(
            join_parameters=True,
            run={"id": self.run.id, "default_only": False},
        )
        ts_df = ts_df.rename(columns={"id": "time_series__id"})

        # merge on the identity columns
        return pd.merge(
            df, ts_df, how="left", on=id_cols, suffixes=(None, "_y")
        )  # tada, df with 'time_series__id' added from the database.

    def add(self, df: pd.DataFrame, type: Type | None = None) -> None:
        self.run.require_lock()
        # df = AddDataPointFrameSchema.validate(df) TODO
        df["run__id"] = self.run.id
        df = self._get_or_create_ts(df)
        substitute_type(df, type)
        self._backend.iamc.datapoints.bulk_upsert(df)

    def remove(self, df: pd.DataFrame, type: Type | None = None) -> None:
        self.run.require_lock()
        # df = RemoveDataPointFrameSchema.validate(df) TODO
        df["run__id"] = self.run.id
        df = self._get_or_create_ts(df)
        substitute_type(df, type)
        df = df.drop(columns=["unit", "variable", "region"])
        self._backend.iamc.datapoints.bulk_delete(df)

    def tabulate(
        self,
        *,
        variable: dict[str, str | Iterable[str]] | None = None,
        region: dict[str, str | Iterable[str]] | None = None,
        unit: dict[str, str | Iterable[str]] | None = None,
        raw: bool = False,
    ) -> pd.DataFrame:
        df = self._backend.iamc.datapoints.tabulate(
            join_parameters=True,
            join_runs=False,
            run={"id": self.run.id, "default_only": False},
            variable=variable,
            region=region,
            unit=unit,
        ).dropna(how="all", axis="columns")
        # return normalize_df(df, raw, False, False)
        return df


class PlatformIamcData(BaseFacade):
    variables: VariableRepository

    def __init__(self, backend: Backend) -> None:
        self.variables = VariableRepository(backend=backend)
        super().__init__(backend)

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
        ).dropna(how="all", axis="columns")

        return df
        # return normalize_df(df, raw, join_runs, join_run_id)
