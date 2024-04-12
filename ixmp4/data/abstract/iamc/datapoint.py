import enum
from typing import Protocol

import pandas as pd

from ixmp4.data import types

from .. import base


class DataPoint(base.BaseModel, Protocol):
    """Data point data model."""

    time_series__id: types.Integer
    "Foreign unique integer id of the associated time series."
    value: types.Float
    "Value of the data point."
    type: types.String
    "Type of data point either `ANNUAL`, `CATEGORICAL` or `DATETIME`."

    step_year: types.Integer | None
    "An integer time step required by data points of type `ANNUAL` or `CATEGORICAL`."
    step_category: types.String | None
    "A string category required by data points of type `CATEGORICAL`."
    step_datetime: types.DateTime | None
    "A datetime object required by data points of type `DATETIME`."

    class Type(str, enum.Enum):
        BASE = "BASE"
        ANNUAL = "ANNUAL"
        CATEGORICAL = "CATEGORICAL"
        DATETIME = "DATETIME"

    def __str__(self) -> str:
        return f"<Datapoint {self.id} type={self.type}>"


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
        **kwargs,
    ) -> list[DataPoint]:
        """Lists data points by specified criteria.
        This method incurrs mentionable overhead compared to :meth:`tabulate`.

        Parameters
        ----------
        join_parameters : bool | None
            If set to `True` the resulting data frame will include parameter columns
            from the associated :class:`ixmp4.data.base.TimeSeries`.
        kwargs : Any
            Additional key word arguments. Any left over kwargs will be used as filters.

        Returns
        -------
        Iterable[:class:`ixmp4.data.base.DataPoint`]:
            List of data points.
        """
        ...

    def tabulate(
        self,
        *,
        join_parameters: bool | None = False,
        join_runs: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """Tabulates data points by specified criteria.

        Parameters
        ----------
        join_parameters : bool | None
            If set to `True` the resulting data frame will include parameter columns
            from the associated :class:`ixmp4.data.abstract.TimeSeries`.
        join_runs : bool
            If set to `True` the resulting data frame will include model & scenario name
            and version id of the associated Run.
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
                - ... misc parameter columns if `join_parameters` is set to `True`
        """
        ...

    def bulk_upsert(self, df: pd.DataFrame, **kwargs) -> None:
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
        """
        ...

    def bulk_delete(self, df: pd.DataFrame, **kwargs) -> None:
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
