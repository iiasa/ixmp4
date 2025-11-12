import enum
from collections.abc import Iterable
from datetime import datetime
from typing import Protocol

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

from .. import base
from ..annotations import (
    HasModelFilter,
    HasRegionFilter,
    HasRunFilter,
    HasScenarioFilter,
    HasUnitFilter,
    HasVariableFilter,
)


class DataPoint(base.BaseModel, Protocol):
    """Data point data model."""

    time_series__id: int
    "Foreign unique integer id of the associated time series."
    value: float
    "Value of the data point."
    type: str
    "Type of data point either `ANNUAL`, `CATEGORICAL` or `DATETIME`."

    step_year: int | None
    "An integer time step required by data points of type `ANNUAL` or `CATEGORICAL`."
    step_category: str | None
    "A string category required by data points of type `CATEGORICAL`."
    step_datetime: datetime | None
    "A datetime object required by data points of type `DATETIME`."

    is_input: bool
    "Whether the datapoint is input (as opposed to solution, e.g. for remove_solution)."

    class Type(str, enum.Enum):
        BASE = "BASE"
        ANNUAL = "ANNUAL"
        CATEGORICAL = "CATEGORICAL"
        DATETIME = "DATETIME"

    def __str__(self) -> str:
        return f"<Datapoint {self.id} type={self.type}>"


class EnumerateKwargs(TypedDict, total=False):
    step_year: int | None
    step_year__in: Iterable[int]
    step_year__gt: int | None
    step_year__lt: int | None
    step_year__gte: int | None
    step_year__lte: int | None
    year: int | None
    year__in: Iterable[int]
    year__gt: int | None
    year__lt: int | None
    year__gte: int | None
    year__lte: int | None
    time_series_id: int | None
    time_series_id__in: Iterable[int]
    time_series_id__gt: int | None
    time_series_id__lt: int | None
    time_series_id__gte: int | None
    time_series_id__lte: int | None
    time_series__id: int | None
    time_series__id__in: Iterable[int]
    time_series__id__gt: int | None
    time_series__id__lt: int | None
    time_series__id__gte: int | None
    time_series__id__lte: int | None
    is_input: bool | None
    region: HasRegionFilter | None
    unit: HasUnitFilter | None
    variable: HasVariableFilter | None
    model: HasModelFilter | None
    scenario: HasScenarioFilter | None
    run: HasRunFilter | None


class DataPointRepository(
    base.Enumerator,
    base.BulkUpserter,
    base.BulkDeleter,
    Protocol,
):
    def list(
        self,
        *,
        join_parameters: bool | None = False,
        join_runs: bool = False,
        join_run_id: bool = False,
        **kwargs: Unpack[EnumerateKwargs],
    ) -> list[DataPoint]:
        """Lists data points by specified criteria.
        This method incurrs mentionable overhead compared to :meth:`tabulate`.

        Parameters
        ----------
        join_parameters : bool | None
            If set to `True` the resulting list will include parameter columns
            from the associated :class:`ixmp4.data.abstract.iamc.timeseries.TimeSeries`.
        join_runs : bool
            If set to `True` the resulting list will include model & scenario name
            and version id of the associated Run.
        join_run_id : bool
            If set to `True` the resulting list will include the id of the associated
            Run as `run__id`.
        kwargs : Any
            Additional key word arguments. Any left over kwargs will be used as filters.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.iamc.datapoint.DataPoint`]:
            List of data points.
        """
        ...

    def tabulate(
        self,
        *,
        join_parameters: bool | None = False,
        join_runs: bool = False,
        join_run_id: bool = False,
        **kwargs: Unpack[EnumerateKwargs],
    ) -> pd.DataFrame:
        """Tabulates data points by specified criteria.

        Parameters
        ----------
        join_parameters : bool | None
            If set to `True` the resulting data frame will include parameter columns
            from the associated :class:`ixmp4.data.abstract.iamc.timeseries.TimeSeries`.
        join_runs : bool
            If set to `True` the resulting data frame will include model & scenario name
            and version id of the associated Run.
        join_run_id : bool
            If set to `True` the resulting data frame will include the id of the
            associated Run as `run__id`.
        kwargs : Any
            Additional key word arguments. Any left over kwargs will be used as filters.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - time_series__id
                - value
                - type
                - step_year
                    if it contains data points of type `ANNUAL` or `CATEGORICAL`
                - step_category
                    if it contains data points of type `CATEGORICAL`
                - step_datetime
                    if it contains data points of type `DATETIME`
                - run__id (if `join_run_id` is set to `True`)
                - is_input
                - ... misc parameter columns if `join_parameters` is set to `True`
        """
        ...

    def bulk_upsert(self, df: pd.DataFrame) -> None:
        """Looks which data points in the supplied data frame already exists,
        updates those that have changed and inserts new ones.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            A data frame with the columns:
                - time_series__id
                - value
                - type
                - step_year
                    if it contains data points of type `ANNUAL` or `CATEGORICAL`
                - step_category
                    if it contains data points of type `CATEGORICAL`
                - step_datetime
                    if it contains data points of type `DATETIME`
                - is_input
        """
        ...

    def bulk_delete(self, df: pd.DataFrame) -> None:
        """Deletes data points which match criteria in the supplied data frame.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            A data frame with the columns:
                - time_series__id
                - type
                - step_year
                    if it contains data points of type `ANNUAL` or `CATEGORICAL`
                - step_category
                    if it contains data points of type `CATEGORICAL`
                - step_datetime
                    if it contains data points of type `DATETIME`
        """
        ...
