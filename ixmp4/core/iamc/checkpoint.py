from typing import TYPE_CHECKING

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.base_exceptions import OperationNotSupported
from ixmp4.data.backend import Backend
from ixmp4.data.checkpoint.dto import Checkpoint
from ixmp4.data.iamc.datapoint.filter import (
    DataPointVersionFilter,
    FacadeDataPointFilter,
    facade_to_data_filter,
)

from ..base import BaseBackendFacade
from .data import _convert_to_std_format

if TYPE_CHECKING:
    from ixmp4.core.run import Run

_VERSIONING_NOT_SUPPORTED_MSG = (
    "Checkpoint data views require PostgreSQL versioning support. "
    "This feature is not available on the current backend."
)


class CheckpointIamcData(BaseBackendFacade):
    """Read-only view of IAMC data for a run at a checkpoint."""

    def __init__(self, backend: Backend, run: "Run", checkpoint: Checkpoint) -> None:
        super().__init__(backend)
        self._run = run
        self._checkpoint = checkpoint

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
            raise OperationNotSupported(_VERSIONING_NOT_SUPPORTED_MSG)

        # TODO: facade_to_version_filter
        filter: DataPointVersionFilter = facade_to_data_filter(kwargs)  # type: ignore[assignment]
        filter["valid_at_transaction"] = self._checkpoint.transaction__id
        filter["run"] = {"id": self._run.id}
        df = self._backend.iamc.datapoints.tabulate_versions(
            join_parameters=True, **filter
        )
        return _convert_to_std_format(df, join_runs=False, join_run_id=False)
