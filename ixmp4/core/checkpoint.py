from typing import TYPE_CHECKING, Any, List

import pandas as pd

from ixmp4.base_exceptions import VersioningNotSupported
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
        self.iamc = CheckpointIamcData(backend, run, self)
        self.optimization = CheckpointOptimizationData(backend, run, self)

    def _get_service(self, backend: Backend) -> CheckpointService:
        return backend.checkpoints

    @property
    def id(self) -> int:
        return self._dto.id

    @property
    def message(self) -> str:
        return self._dto.message

    @property
    def run__id(self) -> int:
        return self._dto.run__id

    @property
    def transaction__id(self) -> int | None:
        return self._dto.transaction__id

    @property
    def previous(self) -> "Checkpoint | None":
        all_prev = self._service.list(id__lt=self.id, run__id=self._run.id)
        if len(all_prev) == 0:
            return None
        else:
            return Checkpoint(self._backend, self._run, all_prev[-1])

    @property
    def next(self) -> "Checkpoint | None":
        all_next = self._service.list(id__gt=self.id, run__id=self._run.id)
        if len(all_next) == 0:
            return None
        else:
            return Checkpoint(self._backend, self._run, all_next[0])

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
            raise VersioningNotSupported()
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
            raise VersioningNotSupported()
        self._backend.runs.revert(self._run.id, self._dto.transaction__id)

    def delete(self) -> None:
        """Deletes this checkpoint.
        **Warning**: Deleted checkpoints cannot be recovered."""
        self._service.delete_by_id(self._dto.id)

    def __str__(self) -> str:
        return (
            f"<Checkpoint message='{self.message}' "
            f"transaction__id={self.transaction__id} "
            f"run__id={self.run__id} id={self.id}>"
        )

    def __repr__(self) -> str:
        return str(self)


class RunCheckpoints(BaseServiceFacade[CheckpointService]):
    _run: "Run"

    NotFound = CheckpointNotFound
    NotUnique = CheckpointNotUnique
    DeletionPrevented = CheckpointDeletionPrevented

    def __init__(self, backend: Backend, run: "Run") -> None:
        super().__init__(backend)
        self._run = run

    def _get_service(self, backend: Backend) -> CheckpointService:
        return backend.checkpoints

    def _get_item_id(self, ref: Checkpoint | int) -> int:
        if isinstance(ref, Checkpoint):
            return ref.id
        elif isinstance(ref, int):
            return ref
        else:
            raise ValueError(f"Invalid reference to checkpoint: {ref}")

    def create(self, message: str) -> Checkpoint:
        """Creates a checkpoint for this run.

        Requires an active run lock — use ``with run.transact("message"):``
        before calling this method.

        .. code:: python

            run.checkpoints.create("My message")
            #> <Checkpoint message='My message' ... >

        Raises
        ------
        :class:`ixmp4.data.run.exceptions.RunLockRequired`
            If no run lock is held.
        """
        self._run.require_lock()
        dto = self._service.create(run__id=self._run.id, message=message)
        return Checkpoint(self._backend, self._run, dto)

    def delete(self, ref: Checkpoint | int) -> None:
        r"""Deletes a checkpoint.

        .. code:: python

            run.checkpoints.delete(42)

        Parameters
        ----------
        ref : :class:`ixmp4.core.checkpoint.Checkpoint` | int
            Checkpoint object or id.

        Raises
        ------
        :class:`CheckpointNotFound`:
            If no checkpoint matching ``ref`` exists or it
            belongs to another run.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        """

        id = self._get_item_id(ref)
        dto = self._service.get_by_id(id)

        if dto.run__id != self._run.id:
            raise CheckpointNotFound()

        self._service.delete_by_id(id)

    def list(self) -> List[Checkpoint]:
        """Lists checkpoints for this run.

        .. code:: python

            run.checkpoints.tabulate()
            #> [<Checkpoint message='My message' ... >, ...]

        Returns
        -------
        :class:`pandas.DataFrame`:
            Data frame with checkpoint information.
        """
        checkpoints = self._service.list(run__id=self._run.id)
        return [Checkpoint(self._backend, self._run, dto) for dto in checkpoints]

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
        return self._service.tabulate(run__id=self._run.id).drop(columns=["run__id"])
