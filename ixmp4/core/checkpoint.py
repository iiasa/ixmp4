from typing import TYPE_CHECKING, Any

import pandas as pd

from ixmp4.base_exceptions import OperationNotSupported
from ixmp4.data.backend import Backend
from ixmp4.data.checkpoint.dto import Checkpoint as CheckpointDto
from ixmp4.data.checkpoint.exceptions import (
    CheckpointDeletionPrevented,
    CheckpointNotFound,
    CheckpointNotUnique,
)
from ixmp4.data.checkpoint.service import CheckpointService

from .base import BaseFacadeObject, BaseServiceFacade
from .iamc.checkpoint import CheckpointIamcData
from .optimization.checkpoint import CheckpointOptimizationData

if TYPE_CHECKING:
    from .run import Run

_VERSIONING_NOT_SUPPORTED_MSG = (
    "Checkpoint data views require PostgreSQL versioning support. "
    "This feature is not available on the current backend."
)


class Checkpoint(BaseFacadeObject[CheckpointService, CheckpointDto]):
    """Read-only view of a run's state at a specific checkpoint.

    Provides access to IAMC data, optimization data, and run metadata
    as they existed when the checkpoint was created. Also allows reverting
    the run to this checkpoint state.

    .. code:: python

        # Create a checkpoint and access it later
        run.checkpoints.create("after scenario setup")
        checkpoint = run.checkpoints.tabulate().iloc[0]

        view = run.checkpoints[checkpoint.id]
        df = view.iamc.tabulate()
        meta = view.meta
        scalars = view.optimization.scalars.tabulate()

        # Revert run to this checkpoint state
        view.revert()
    """

    iamc: CheckpointIamcData
    optimization: CheckpointOptimizationData

    def __init__(self, backend: Backend, run: "Run", checkpoint: CheckpointDto) -> None:
        super().__init__(backend, checkpoint)
        self._run = run
        self.iamc = CheckpointIamcData(backend, run, checkpoint)
        self.optimization = CheckpointOptimizationData(backend, run, checkpoint)

    def _get_service(self, backend: Backend) -> CheckpointService:
        return backend.checkpoints

    @property
    def id(self) -> int:
        return self._dto.id

    @property
    def run__id(self) -> int:
        return self._dto.run__id

    @property
    def transaction__id(self) -> int | None:
        return self._dto.transaction__id

    @property
    def meta(self) -> dict[str, Any]:
        """Run metadata at checkpoint time.

        Returns
        -------
        dict[str, Any]
            Dictionary mapping meta keys to their values at checkpoint time.

        Raises
        ------
        :class:`ixmp4.base_exceptions.OperationNotSupported`
            If versioning is not supported on this backend (e.g. SQLite).
        """
        if self._dto.transaction__id is None:
            raise OperationNotSupported(_VERSIONING_NOT_SUPPORTED_MSG)
        df = self._backend.meta.tabulate_versions(
            run__id=self._run.id,
            valid_at_transaction=self._dto.transaction__id,
        )
        if df.empty:
            return {}
        return dict(zip(df["key"], df["value"]))

    def revert(self) -> None:
        """Revert the run to the state captured at this checkpoint.

        Restores all run data (meta, IAMC datapoints, optimization data) to the
        values they had at the time this checkpoint was created.

        Raises
        ------
        :class:`ixmp4.base_exceptions.OperationNotSupported`
            If versioning is not supported on this backend (e.g. SQLite).
        """
        if self._dto.transaction__id is None:
            raise OperationNotSupported(_VERSIONING_NOT_SUPPORTED_MSG)
        self._backend.runs.revert(self._run.id, self._dto.transaction__id)


class RunCheckpoints(BaseServiceFacade[CheckpointService]):
    run: "Run"

    NotFound = CheckpointNotFound
    NotUnique = CheckpointNotUnique
    DeletionPrevented = CheckpointDeletionPrevented

    def __init__(self, backend: Backend, run: "Run") -> None:
        super().__init__(backend)
        self.run = run

    def _get_service(self, backend: Backend) -> CheckpointService:
        return backend.checkpoints

    def __getitem__(self, checkpoint_id: int) -> Checkpoint:
        """Retrieve a read-only view of the run at a specific checkpoint.

        Parameters
        ----------
        checkpoint_id : int
            The integer id of the checkpoint.

        Returns
        -------
        :class:`Checkpoint`
            A read-only view of the run state at the checkpoint.

        Raises
        ------
        :class:`~ixmp4.data.checkpoint.exceptions.CheckpointNotFound`
            If no checkpoint with the given id exists or it belongs to a
            different run.

        .. code:: python

            view = run.checkpoints[1]
            df = view.iamc.tabulate()
        """
        checkpoint = self._service.get_by_id(checkpoint_id)
        if checkpoint.run__id != self.run.id:
            raise CheckpointNotFound(
                f"Checkpoint {checkpoint_id} not found for run {self.run.id}"
            )
        return Checkpoint(self._backend, self.run, checkpoint)

    def tabulate(self) -> pd.DataFrame:
        """Tabulates checkpoints for this run.

        .. code:: python

            run.checkpoints.tabulate()
            #>    id  message  transaction__id
            # 0   1  "message"  123

        Returns
        -------
        :class:`pandas.DataFrame`:
            Data frame with checkpoint information.
        """
        return self._service.tabulate(run__id=self.run.id)

    def create(self, message: str) -> Checkpoint:
        """Creates a checkpoint for this run.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        .. code:: python

            run.checkpoints.create("My message")
            #> <Checkpoint 1 message='My message'>

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self.run.require_lock()
        dto = self._service.create(run__id=self.run.id, message=message)
        return Checkpoint(self._backend, self.run, dto)
