import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.base_exceptions import VersioningNotSupported
from ixmp4.data.iamc.datapoint.filter import (
    DataPointVersionFilter,
    FacadeDataPointFilter,
    facade_to_data_filter,
)

from ..base import BaseCheckpointView
from .data import IamcDataFacade

_VERSIONING_NOT_SUPPORTED_MSG = (
    "Checkpoint data views require PostgreSQL versioning support. "
    "This feature is not available on the current backend."
)


class CheckpointIamcData(BaseCheckpointView, IamcDataFacade):
    """Read-only view of IAMC data for a run at a checkpoint."""

    def tabulate(self, **kwargs: Unpack[FacadeDataPointFilter]) -> pd.DataFrame:
        """Tabulate IAMC data at this checkpoint, in standard IAMC format.

        Returns a DataFrame with columns: region, variable, unit, year
        (or time), and value. Data reflects the state of the run at the
        checkpoint's transaction.

        Returns
        -------
        :class:`pandas.DataFrame`
            IAMC-formatted data at checkpoint time.

        Raises
        ------
        :class:`ixmp4.base_exceptions.OperationNotSupported`
            If versioning is not supported on this backend (e.g. SQLite).
        """

        if self._checkpoint.transaction__id is None:
            raise VersioningNotSupported()

        # TODO: facade_to_version_filter
        filter: DataPointVersionFilter = facade_to_data_filter(kwargs)  # type: ignore[assignment]
        filter["valid_at_transaction"] = self._checkpoint.transaction__id
        filter["run"] = {"id": self._run.id}
        df = self._backend.iamc.datapoints.tabulate_versions(
            join_parameters=True, **filter
        )
        return self._convert_to_std_format(
            df, join_runs=False, join_run_id=False, extra_columns=self._version_columns
        )

    def difference(self) -> pd.DataFrame:
        """Tabulate the changes made to IAMC data in this checkpoint.

        Returns all IAMC datapoint version records created since the
        previous checkpoint (exclusive) up to and including this
        checkpoint's transaction.

        Returns
        -------
        :class:`pandas.DataFrame`
            IAMC datapoint version records (including version columns).

        Raises
        ------
        :class:`ixmp4.base_exceptions.OperationNotSupported`
            If versioning is not supported on this backend (e.g. SQLite).
        """

        if self._checkpoint.transaction__id is None:
            raise VersioningNotSupported()

        current_tx_id = self._checkpoint.transaction__id
        previous = self._checkpoint.previous
        filter: DataPointVersionFilter = {
            "run": {"id": self._run.id},
            "transaction_id__lte": current_tx_id,
        }

        if previous is not None:
            if previous.transaction__id is None:
                raise VersioningNotSupported()
            filter["transaction_id__gt"] = previous.transaction__id

        df = self._backend.iamc.datapoints.tabulate_versions(
            join_parameters=True,
            **filter,
        )
        df = self._map_op_type(df)
        return self._convert_to_std_format(
            df, join_runs=False, join_run_id=False, extra_columns=self._version_columns
        )
