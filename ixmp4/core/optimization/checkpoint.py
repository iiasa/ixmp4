from typing import TYPE_CHECKING, Any

import pandas as pd

from ixmp4.base_exceptions import OperationNotSupported
from ixmp4.data.backend import Backend
from ixmp4.data.checkpoint.dto import Checkpoint

from ..base import BaseBackendFacade

if TYPE_CHECKING:
    from ..run import Run

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
