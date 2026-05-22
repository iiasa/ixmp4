from typing import TYPE_CHECKING, Any

import pandas as pd

from ixmp4.base_exceptions import OperationNotSupported
from ixmp4.data.backend import Backend
from ixmp4.data.checkpoint.dto import Checkpoint
from ixmp4.data.checkpoint.exceptions import (
    CheckpointDeletionPrevented,
    CheckpointNotFound,
    CheckpointNotUnique,
)
from ixmp4.data.checkpoint.service import CheckpointService

from .base import BaseBackendFacade, BaseServiceFacade

if TYPE_CHECKING:
    from .run import Run

_VERSIONING_NOT_SUPPORTED_MSG = (
    "Checkpoint data views require PostgreSQL versioning support. "
    "This feature is not available on the current backend."
)


class CheckpointScalarView(BaseBackendFacade):
    """Read-only view of optimization scalars at a checkpoint."""

    def __init__(self, backend: Backend, run: "Run", checkpoint: Checkpoint) -> None:
        super().__init__(backend)
        self._run = run
        self._checkpoint = checkpoint

    def tabulate(self, **kwargs: Any) -> pd.DataFrame:
        """Tabulate scalar versions at this checkpoint.

        Returns
        -------
        :class:`pandas.DataFrame`
            DataFrame with the scalar version columns.
        """
        if self._checkpoint.transaction__id is None:
            raise OperationNotSupported(_VERSIONING_NOT_SUPPORTED_MSG)
        return self._backend.optimization.scalars.tabulate_versions(
            run__id=self._run.id,
            valid_at_transaction=self._checkpoint.transaction__id,
            **kwargs,
        )


class CheckpointTableView(BaseBackendFacade):
    """Read-only view of optimization tables at a checkpoint."""

    def __init__(self, backend: Backend, run: "Run", checkpoint: Checkpoint) -> None:
        super().__init__(backend)
        self._run = run
        self._checkpoint = checkpoint

    def tabulate(self, **kwargs: Any) -> pd.DataFrame:
        """Tabulate table versions at this checkpoint.

        Returns
        -------
        :class:`pandas.DataFrame`
            DataFrame with the table version columns.
        """
        if self._checkpoint.transaction__id is None:
            raise OperationNotSupported(_VERSIONING_NOT_SUPPORTED_MSG)
        return self._backend.optimization.tables.tabulate_versions(
            run__id=self._run.id,
            valid_at_transaction=self._checkpoint.transaction__id,
            **kwargs,
        )


class CheckpointParameterView(BaseBackendFacade):
    """Read-only view of optimization parameters at a checkpoint."""

    def __init__(self, backend: Backend, run: "Run", checkpoint: Checkpoint) -> None:
        super().__init__(backend)
        self._run = run
        self._checkpoint = checkpoint

    def tabulate(self, **kwargs: Any) -> pd.DataFrame:
        """Tabulate parameter versions at this checkpoint.

        Returns
        -------
        :class:`pandas.DataFrame`
            DataFrame with the parameter version columns.
        """
        if self._checkpoint.transaction__id is None:
            raise OperationNotSupported(_VERSIONING_NOT_SUPPORTED_MSG)
        return self._backend.optimization.parameters.tabulate_versions(
            run__id=self._run.id,
            valid_at_transaction=self._checkpoint.transaction__id,
            **kwargs,
        )


class CheckpointEquationView(BaseBackendFacade):
    """Read-only view of optimization equations at a checkpoint."""

    def __init__(self, backend: Backend, run: "Run", checkpoint: Checkpoint) -> None:
        super().__init__(backend)
        self._run = run
        self._checkpoint = checkpoint

    def tabulate(self, **kwargs: Any) -> pd.DataFrame:
        """Tabulate equation versions at this checkpoint.

        Returns
        -------
        :class:`pandas.DataFrame`
            DataFrame with the equation version columns.
        """
        if self._checkpoint.transaction__id is None:
            raise OperationNotSupported(_VERSIONING_NOT_SUPPORTED_MSG)
        return self._backend.optimization.equations.tabulate_versions(
            run__id=self._run.id,
            valid_at_transaction=self._checkpoint.transaction__id,
            **kwargs,
        )


class CheckpointVariableView(BaseBackendFacade):
    """Read-only view of optimization variables at a checkpoint."""

    def __init__(self, backend: Backend, run: "Run", checkpoint: Checkpoint) -> None:
        super().__init__(backend)
        self._run = run
        self._checkpoint = checkpoint

    def tabulate(self, **kwargs: Any) -> pd.DataFrame:
        """Tabulate variable versions at this checkpoint.

        Returns
        -------
        :class:`pandas.DataFrame`
            DataFrame with the variable version columns.
        """
        if self._checkpoint.transaction__id is None:
            raise OperationNotSupported(_VERSIONING_NOT_SUPPORTED_MSG)
        return self._backend.optimization.variables.tabulate_versions(
            run__id=self._run.id,
            valid_at_transaction=self._checkpoint.transaction__id,
            **kwargs,
        )


class CheckpointIndexSetView(BaseBackendFacade):
    """Read-only view of optimization indexsets at a checkpoint."""

    def __init__(self, backend: Backend, run: "Run", checkpoint: Checkpoint) -> None:
        super().__init__(backend)
        self._run = run
        self._checkpoint = checkpoint

    def tabulate(self, **kwargs: Any) -> pd.DataFrame:
        """Tabulate indexset versions at this checkpoint.

        Returns
        -------
        :class:`pandas.DataFrame`
            DataFrame with the indexset version columns.
        """
        if self._checkpoint.transaction__id is None:
            raise OperationNotSupported(_VERSIONING_NOT_SUPPORTED_MSG)
        return self._backend.optimization.indexsets.tabulate_versions(
            run__id=self._run.id,
            valid_at_transaction=self._checkpoint.transaction__id,
            **kwargs,
        )


class CheckpointOptimizationData(BaseBackendFacade):
    """Read-only view of all optimization data for a run at a checkpoint."""

    scalars: CheckpointScalarView
    tables: CheckpointTableView
    parameters: CheckpointParameterView
    equations: CheckpointEquationView
    variables: CheckpointVariableView
    indexsets: CheckpointIndexSetView

    def __init__(self, backend: Backend, run: "Run", checkpoint: Checkpoint) -> None:
        super().__init__(backend)
        self.scalars = CheckpointScalarView(backend, run, checkpoint)
        self.tables = CheckpointTableView(backend, run, checkpoint)
        self.parameters = CheckpointParameterView(backend, run, checkpoint)
        self.equations = CheckpointEquationView(backend, run, checkpoint)
        self.variables = CheckpointVariableView(backend, run, checkpoint)
        self.indexsets = CheckpointIndexSetView(backend, run, checkpoint)


class CheckpointIamcData(BaseBackendFacade):
    """Read-only view of IAMC data for a run at a checkpoint."""

    def __init__(self, backend: Backend, run: "Run", checkpoint: Checkpoint) -> None:
        super().__init__(backend)
        self._run = run
        self._checkpoint = checkpoint

    def tabulate(self, **kwargs: Any) -> pd.DataFrame:
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
        from ixmp4.core.iamc.data import _convert_to_std_format

        if self._checkpoint.transaction__id is None:
            raise OperationNotSupported(_VERSIONING_NOT_SUPPORTED_MSG)

        dp_df = self._backend.iamc.datapoints.tabulate_versions(
            timeseries={"run__id": self._run.id},
            valid_at_transaction=self._checkpoint.transaction__id,
        )

        if dp_df.empty:
            return pd.DataFrame(columns=["region", "variable", "unit", "year", "value"])

        # Enrich version datapoints with region/variable/unit from live timeseries
        ts_df = self._backend.iamc.timeseries.tabulate(
            run__id=self._run.id,
            run={"default_only": False},
        )
        ts_df = ts_df.rename(columns={"id": "time_series__id"})[
            ["time_series__id", "region", "variable", "unit"]
        ]
        merged = dp_df.merge(ts_df, on="time_series__id", how="inner")
        return _convert_to_std_format(merged, join_runs=False, join_run_id=False)


class CheckpointView(BaseBackendFacade):
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

    def __init__(self, backend: Backend, run: "Run", checkpoint: Checkpoint) -> None:
        super().__init__(backend)
        self._run = run
        self._checkpoint = checkpoint
        self.iamc = CheckpointIamcData(backend, run, checkpoint)
        self.optimization = CheckpointOptimizationData(backend, run, checkpoint)

    @property
    def checkpoint(self) -> Checkpoint:
        """The checkpoint this view is associated with."""
        return self._checkpoint

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
        if self._checkpoint.transaction__id is None:
            raise OperationNotSupported(_VERSIONING_NOT_SUPPORTED_MSG)
        df = self._backend.meta.tabulate_versions(
            run__id=self._run.id,
            valid_at_transaction=self._checkpoint.transaction__id,
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
        if self._checkpoint.transaction__id is None:
            raise OperationNotSupported(_VERSIONING_NOT_SUPPORTED_MSG)
        self._backend.runs.revert(self._run.id, self._checkpoint.transaction__id)


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

    def __getitem__(self, checkpoint_id: int) -> CheckpointView:
        """Retrieve a read-only view of the run at a specific checkpoint.

        Parameters
        ----------
        checkpoint_id : int
            The integer id of the checkpoint.

        Returns
        -------
        :class:`CheckpointView`
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
        return CheckpointView(self._backend, self.run, checkpoint)

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
        return self._service.create(run__id=self.run.id, message=message)
