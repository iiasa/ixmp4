from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

if TYPE_CHECKING:
    from ixmp4.data.db.timeseries import CreateKwargs

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from .. import base
from ..annotations import (
    HasIdFilter,
    HasRegionFilter,
    HasRunFilter,
    HasUnitFilter,
    HasVariableFilter,
)


class TimeSeries(base.BaseModel, Protocol):
    """Time series data model."""

    run__id: int
    "Unique run id."
    parameters: Mapping[str, Any]
    "A set of parameter values for the time series."

    def __str__(self) -> str:
        return f"<TimeSeries {self.id} parameters={self.parameters}>"


ModelType = TypeVar("ModelType", bound=TimeSeries)


class EnumerateKwargs(HasIdFilter, total=False):
    region: HasRegionFilter
    unit: HasUnitFilter
    variable: HasVariableFilter
    run: HasRunFilter


class TimeSeriesRepository(
    base.Creator,
    base.Retriever,
    base.Enumerator,
    base.BulkUpserter,
    base.VersionManager,
    Protocol,
    Generic[ModelType],
):
    def create(self, **kwargs: Unpack["CreateKwargs"]) -> ModelType:
        """Retrieves a time series.

        Parameters
        ----------
        run__id : int
            Unique run id.
        parameters : Mapping[str, Any]
            A set of parameter values for the time series.

        Raises
        ------
        :class:`ixmp4.data.abstract.iamc.timeseries.TimeSeries.NotUnique`.

        Returns
        -------
        :class:`ixmp4.data.abstract.iamc.timeseries.TimeSeries`:
            The retrieved time series.
        """
        ...

    # NOTE this seems unused, so I'm guessing at the parameters type
    def get(self, run_id: int, parameters: Mapping[str, Any]) -> ModelType:
        """Retrieves a time series.

        Parameters
        ----------
        run_id : int
            Unique run id.
        parameters : Mapping[str, Any]
            A set of parameter values for the time series.

        Raises
        ------
        :class:`ixmp4.data.abstract.TimeSeries.NotFound`.
            If the time series with `run_id` and `parameters` does not exist.

        Returns
        -------
        :class:`ixmp4.data.base.TimeSeries`:
            The retrieved time series.
        """
        ...

    def get_by_id(self, id: int) -> ModelType:
        """Retrieves a time series by it's id.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`ixmp4.data.abstract.TimeSeries.NotFound`.
            If the time series with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.base.TimeSeries`:
            The retrieved time series.
        """
        ...

    def get_or_create(self, run_id: int, parameters: Mapping[str, Any]) -> ModelType:
        """Tries to retrieve a time series and creates it if it was not found.

        Parameters
        ----------
        run_id : int
            Unique run id.
        parameters : Mapping[str, Any]
            A set of parameter values for the time series.

        Returns
        -------
        :class:`ixmp4.data.base.TimeSeries`:
            The retrieved or created time series.
        """
        try:
            return self.get(run_id, parameters)
        except TimeSeries.NotFound:
            return self.create(run__id=run_id, parameters=parameters)

    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[ModelType]:
        r"""Lists time series by specified criteria.

        Parameters
        ----------
            \*\*kwargs: any
            Filter parameters as specified in
            :class:`ixmp4.data.db.iamc.timeseries.filter.TimeSeriesFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.base.TimeSeries`]:
            List of time series.
        """
        ...

    def tabulate(
        self, *, join_parameters: bool | None = False, **kwargs: Unpack[EnumerateKwargs]
    ) -> pd.DataFrame:
        r"""Tabulate time series by specified criteria.

        Parameters
        ----------
        join_parameters : bool | None
            If set to `True` the resulting data frame will include
            parameter columns as values instead of foreign key id's.
         \*\*kwargs: any
            Filter parameters as specified in
            :class:`ixmp4.data.db.iamc.timeseries.filter.TimeSeriesFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - run__id
                - ... parameter id columns
                  Or:
                - ... parameter value columns
        """
        ...

    def bulk_upsert(
        self, df: pd.DataFrame, create_related: bool | None = False
    ) -> None:
        """Looks which time series in the supplied data frame already exist, and
        inserts new ones.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            A data frame with the columns:
                - id
                - run__id

                if `create_related` = False:
                    - ... parameter id columns

                else:
                    - ... parameter value columns (i.e `region` column with value
                      "World" instead of `region__id` column with value "1".)

        create_related : bool
            Creates related database entries with value data supplied in `df`.
        """
        ...
