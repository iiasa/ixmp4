import logging
import time
import warnings
from contextlib import contextmanager
from datetime import datetime
from typing import Generator, List

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.base_exceptions import OperationNotSupported
from ixmp4.data.backend import Backend
from ixmp4.data.model.dto import Model as ModelDto
from ixmp4.data.run.dto import Run as RunDto
from ixmp4.data.run.exceptions import (
    NoDefaultRunVersion,
    RunDeletionPrevented,
    RunIsLocked,
    RunLockRequired,
    RunNotFound,
    RunNotUnique,
)
from ixmp4.data.run.filter import (
    FacadeRunFilter,
    facade_to_data_filter,
)
from ixmp4.data.run.service import RunService
from ixmp4.data.scenario.dto import Scenario as ScenarioDto

from .base import BaseFacadeObject, BaseServiceFacade
from .checkpoint import RunCheckpoints
from .iamc import RunIamcData
from .meta import RunMetaDescriptor
from .optimization.data import RunOptimizationData

logger = logging.getLogger(__name__)


class RunCloner:
    def clone(self, src_run: "Run", dst_run: "Run", keep_solution: bool) -> None:
        with dst_run.transact("Clone run " + str(src_run)):
            self.clone_meta(src_run, dst_run)
            self.clone_iamc(src_run, dst_run)
            self.clone_optimization(src_run, dst_run, keep_solution)

    def clone_meta(self, src_run: "Run", dst_run: "Run") -> None:
        dst_run.meta = dict(src_run.meta)

    def clone_iamc(self, src_run: "Run", dst_run: "Run") -> None:
        df = src_run.iamc.tabulate()
        if not df.empty:
            dst_run.iamc.add(df)

    def clone_optimization(
        self, src_run: "Run", dst_run: "Run", keep_solution: bool
    ) -> None:
        for src_scalar in src_run.optimization.scalars.list():
            dst_run.optimization.scalars.create(
                src_scalar.name, src_scalar.value, src_scalar.unit.name
            )

        for src_idxset in src_run.optimization.indexsets.list():
            dst_idxset = dst_run.optimization.indexsets.create(src_idxset.name)
            dst_idxset.add_data(src_idxset.data)

        for src_table in src_run.optimization.tables.list():
            dst_table = dst_run.optimization.tables.create(
                src_table.name,
                constrained_to_indexsets=src_table.indexset_names,
                column_names=src_table.column_names,
            )
            dst_table.add_data(src_table.data)

        for src_parameter in src_run.optimization.parameters.list():
            dst_parameter = dst_run.optimization.parameters.create(
                src_parameter.name,
                constrained_to_indexsets=src_parameter.indexset_names,
                column_names=src_parameter.column_names,
            )
            dst_parameter.add_data(src_parameter.data)

        for src_equation in src_run.optimization.equations.list():
            dst_equation = dst_run.optimization.equations.create(
                src_equation.name,
                constrained_to_indexsets=src_equation.indexset_names,
                column_names=src_equation.column_names,
            )
            if keep_solution:
                dst_equation.add_data(src_equation.data)

        for src_variable in src_run.optimization.variables.list():
            dst_variable = dst_run.optimization.variables.create(
                src_variable.name,
                constrained_to_indexsets=src_variable.indexset_names,
                column_names=src_variable.column_names,
            )
            if keep_solution:
                dst_variable.add_data(src_variable.data)


class Run(BaseFacadeObject[RunService, RunDto]):
    """As a central class to organize data on a platform
    the ``Run`` provides methods and access to ``Facade`` instances.

    .. list-table::
        :header-rows: 1

        * - Attribute
          - Facade Class

        * - :py:attr:`~.meta`
          - :class:`~ixmp4.core.meta.RunMetaDescriptor`
            :class:`~ixmp4.core.meta.RunMetaDictFacade`

        * - :py:attr:`~.checkpoints`
          - :class:`~ixmp4.core.checkpoints.RunCheckpoints`

        * - :py:attr:`~.iamc`
          - :class:`~ixmp4.core.iamc.data.RunIamcData`

        * - :py:attr:`~.optimization`
          - :class:`~ixmp4.core.optimization.data.RunOptimizationData`

    """

    Filter = FacadeRunFilter
    NotFound = RunNotFound
    NotUnique = RunNotUnique
    DeletionPrevented = RunDeletionPrevented
    IsLocked = RunIsLocked
    LockRequired = RunLockRequired
    NoDefaultVersion = NoDefaultRunVersion

    meta: RunMetaDescriptor = RunMetaDescriptor()
    """Facade instance to query run meta indicators
    for a run."""

    checkpoints: RunCheckpoints
    """Facade instance to manage :class:`~ixmp4.core.checkpoint.Checkpoint`
    instances for a run."""

    iamc: RunIamcData
    """Facade instance to manager IAMC data for a run."""

    optimization: RunOptimizationData
    """Facade instance to manager optimization data for a run."""

    owns_lock: bool = False
    """Indicated whether this run object has acquired the run's lock."""
    minimum_lock_timeout: float = 0.1
    maximum_lock_timeout: float = 5

    _cloner: RunCloner = RunCloner()

    def __init__(self, backend: Backend, dto: RunDto) -> None:
        super().__init__(backend, dto)
        self.iamc = RunIamcData(backend, run=self)
        self.optimization = RunOptimizationData(backend, run=self)
        self.checkpoints = RunCheckpoints(backend, run=self)

    @property
    def id(self) -> int:
        """Unique id."""
        return self._dto.id

    @property
    def model(self) -> ModelDto:
        """Associated model."""
        return self._dto.model

    @property
    def scenario(self) -> ScenarioDto:
        """Associated scenario."""
        return self._dto.scenario

    @property
    def version(self) -> int:
        """Run version."""
        return self._dto.version

    @property
    def is_default(self) -> bool:
        return self._dto.is_default

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

    @property
    def is_locked(self) -> bool:
        """``True`` if this run is locked. **Warning**: If a lock was acquired by
        another user while this run was already fetched, this property might be out
        of sync."""
        return self._dto.lock_transaction is not None

    def set_as_default(self) -> None:
        """Sets this run as the default version for its `model` + `scenario`
        combination.

        .. code:: python

            run.set_as_default()
            run.is_default
            #> True
        """
        self._service.set_as_default_version(self._dto.id)
        self._refresh()

    def unset_as_default(self) -> None:
        """Unsets this run as the default version.

        .. code:: python

            run.unset_as_default()
            run.is_default
            #> False
        """
        self._service.unset_as_default_version(self._dto.id)
        self._refresh()

    @contextmanager
    def transact(
        self,
        message: str,
        timeout: float | None = None,
        revert_platform_on_error: bool = False,
    ) -> Generator[None, None, None]:
        """
        Context manager to lock the run before yielding control
        back to the caller. The run is unlocked and a checkpoint
        with the provided `message` is created after the context
        manager exits.
        If an exception occurs, the run is reverted to the last
        checkpoint or if no checkpoint exists, to the transaction
        the run was locked at.

        If the run is already locked, the context manager will
        throw `Run.IsLocked` or if `timeout` is provided retry until
        the timeout has passed (and then throw the original
        `Run.IsLocked` exception).

        .. code:: python

            with run.transact("My message"):
                # perform operations that require locking the run
                run.meta["new-key"] = 1

            #> Checkpoint created and run unlocked

        Parameters
        ----------
        message : str
            The message for the checkpoint created after
            conclusion of the context manager.
        timeout : int, optional
            Timeout in seconds.
        revert_platform_on_error : bool, optional
            Whether to revert units when encountering an error; default `False`.

        Raises
        ------
        :class:`ixmp4.core.exceptions.RunIsLocked`
            If the run is already locked and no timeout is provided
            or the provided timeout is exceeded.
        """
        try:
            self.lock(timeout=timeout, check=False)
        except RunIsLocked:
            if self.owns_lock:
                raise RunIsLocked(
                    "Nested `run.transact()` calls are not supported. "
                    "This run is already locked by the enclosing "
                    "`transact()` context manager or `lock()` call "
                    "on the same object."
                )
            else:
                raise

        try:
            yield
        except Exception as e:
            try:
                self.revert(revert_platform=revert_platform_on_error)
            except OperationNotSupported as ons_exc:
                warnings.warn(
                    "An exception occurred but the `Run` "
                    "was not reverted because versioning "
                    "is not supported by this platform: " + str(ons_exc.message)
                )

            self._unlock()
            raise e

        self.checkpoints.create(message)
        self._unlock()

    def delete(self) -> None:
        """Delete this run.
        Tries to acquire a lock in the background.

        .. code:: python

            run.delete()

        Raises
        ------
        :class:`ixmp4.core.exceptions.RunIsLocked`:
            If the run is already locked by this or another object.
        """
        self._service.delete_by_id(self._dto.id)

    def require_lock(self) -> None:
        """Raises :class:`~ixmp4.data.run.exceptions.RunLockRequired`
        if this :class:`~ixmp4.core.run.Run` object does not currently
        "own" a lock on the run.
        """
        if not self.owns_lock:
            raise RunLockRequired()

    def lock(self, *, timeout: float | None = None, check: bool = True) -> None:
        """Attempts to lock the run with this :class:`~ixmp4.core.run.Run`
        object as the lock "owner".
        **Warning**: Once a run is locked, it must be unlocked with
        :meth:`~ixmp4.core.run.Run.unlock` once changes have been made.
        If it remains locked other users will not be able to edit the run
        without forcibly unlocking it.

        Prefer the :meth:`~ixmp4.core.run.Run.transact` context manager to
        automatically acquire and release a lock for a code block wherever possible.

        .. code:: python
            run.lock()
            # or
            run.lock(timeout=2.5)
            # fails:
            run.lock(check=False)

        Parameters
        ----------
        timeout : float, optional
            If supplied, this function will attempt to lock
            the run until ``timeout`` seconds have passed.
        check : boolean, optional
            If ``True`` (the default), this function will check
            if the lock is already held by this run object and
            skip acquiring the lock again.

        Raises
        ------
        :class:`~ixmp4.data.run.exceptions.RunIsLocked`
            If this run is already locked.

        """
        if check and self.owns_lock:
            logger.debug(
                "Skipping lock acquisition because this object already owns it: %s",
                str(self),
            )
            return

        if timeout is not None:
            return self._lock_with_timeout(timeout)
        else:
            return self._lock()

    def _lock(self) -> None:
        self._dto = self._service.lock(self._dto.id)
        self.owns_lock = True
        logger.debug(f"Acquired lock on {self}.")

    def _lock_with_timeout(self, timeout: float) -> None:
        """Try locking the run until a timeout passes."""
        start_time = time.time()
        while True:
            try:
                self._lock()
                break
            except RunIsLocked as e:
                elapsed_time = time.time() - start_time
                if elapsed_time > timeout:
                    raise e
                remaining_time = timeout - elapsed_time
                sleep_time = elapsed_time * 2
                sleep_time = min(sleep_time, self.maximum_lock_timeout)
                sleep_time = max(sleep_time, self.minimum_lock_timeout)
                sleep_time = min(sleep_time, remaining_time)
                time.sleep(sleep_time)

    def _unlock(self) -> None:
        self._dto = self._service.unlock(self._dto.id)
        self.owns_lock = False
        logger.debug(f"Released lock on {self}.")

    def unlock(self, *, check: bool = True, force: bool = False) -> None:
        """Attempts to unlock the run, allowing it to be locked again
        by any run object afterwards.

        .. code:: python
            run.unlock()
            # or
            run.unlock(force=True)

        Parameters
        ----------
        check : boolean, optional
            If ``True`` (the default), this function will check
            if the run is already unlocked and skip the rest of
            the function if so.
        force : boolean, optional
            If ``True`` the run will be unlocked even if this
            run object does not own the lock. Default: ``False``.
            **Warning**: This may interfere with the operations of
            other users. Use with care!

        Raises
        ------
        :class:`~ixmp4.data.run.exceptions.RunLockRequired`
            If ``force=False`` (the default) and this run object
            does not own the lock.
        """
        if check:
            self._refresh()
            if not self.is_locked:
                return

        if not force and not self.owns_lock:
            raise RunLockRequired(
                "Trying to unlock a run that was not locked by this object. "
                "Use `force=True` if you want to unlock it anyway "
                "(potentially interfering with other users)."
            )

        return self._unlock()

    def revert(
        self, transaction_id: int | None = None, *, revert_platform: bool = False
    ) -> None:
        """Reverts the run to the state of a previous transaction.
        If no ``transaction_id`` is supplied the transcation at which the run's
        lock was acquired or the last checkpoint created since then will be used.

        .. code:: python
            with run.transact("Add and Revert"):
                run.iamc.add(...)
                run.checkpoints.create("Add IAMC Data")
                run.iamc.delete(...)
                run.revert() # Reverts to "Add IAMC Data"

            run.iamc.tabulate()
            # > IAMC Data from before deletion

        When supplying a ``transaction_id`` runs can be reverted across
        :meth:`~ixmp4.core.run.Run.transact` blocks:

        .. code:: python
            with run.transact("Add IAMC Data"):
                run.iamc.add(...)
                cp1 = run.checkpoints.create("Add IAMC Data")

            with run.transact("Delete and Revert IAMC Data"):
                run.iamc.delete(...)
                run.revert(cp1.transaction__id) # Reverts to "Add IAMC Data"

            run.iamc.tabulate()

            with run.transact("Revert to origin"):
                run.revert(1) # Initial platform transaction

            run.iamc.tabulate()
            # > Empty DataFrame

        Parameters
        ----------
        transaction_id : int, optional
            The id of a previous transaction on the platform.
            The run will be reverted to the state at that transaction.
        revert_platform : boolean, optional
            If ``True`` the deleted units and regions will be restored.
            Default: ``False``.

        Raises
        ------
        :class:`~ixmp4.data.run.exceptions.RunLockRequired`
            If this run does not own the lock.
        """

        self.require_lock()
        assert self._dto.lock_transaction is not None

        if transaction_id is not None:
            target_transaction = transaction_id
        else:
            checkpoint_df = self.checkpoints.tabulate()
            if checkpoint_df.empty:
                checkpoint_transaction = -1
            else:
                max_tx_id = checkpoint_df["transaction__id"].max()
                if pd.isnull(max_tx_id):
                    checkpoint_transaction = -1
                else:
                    checkpoint_transaction = int(max_tx_id)

            target_transaction = max(checkpoint_transaction, self._dto.lock_transaction)

        self._service.revert(
            self._dto.id,
            target_transaction,
            revert_platform=revert_platform,
        )
        self.meta._refresh()

    def clone(
        self,
        model: str | None = None,
        scenario: str | None = None,
        keep_solution: bool = True,
    ) -> "Run":
        """Create a copy of this run.

        .. code:: python

            new_run = run.clone(
                model="OtherModel",
                scenario="OtherScenario",
                keep_solution=False
            )
            #> <Run 2 model='OtherModel' scenario='OtherScenario' version=1>

        Parameters
        ----------
        model : str | None
            Optional model name for the cloned run.
        scenario : str | None
            Optional scenario name for the cloned run.
        keep_solution : bool
            Whether to keep the solution data in the clone.

        Returns
        -------
        :class:`ixmp4.core.run.Run`:
            The cloned run.
        """
        dst_run = Run(
            backend=self._backend,
            dto=self._service.create(
                model_name=model or self.model.name,
                scenario_name=scenario or self.scenario.name,
            ),
        )

        self._cloner.clone(self, dst_run, keep_solution)
        return dst_run

    def _get_service(self, backend: Backend) -> RunService:
        return backend.runs

    def __str__(self) -> str:
        return (
            f"<Run model='{self.model.name}' "
            f"scenario='{self.scenario.name}' version={self.version} id={self.id}>"
        )

    def __repr__(self) -> str:
        return str(self)


class RunServiceFacade(BaseServiceFacade[RunService]):
    """Used to manipulate runs on a platform."""

    def _get_service(self, backend: Backend) -> RunService:
        return backend.runs

    def create(self, model: str, scenario: str) -> Run:
        """Creates a run with an incremented version number or version=1 if no versions
        exist. Will automatically create the models and scenarios if they don't exist
        yet.

        .. code:: python

            run = platform.runs.create("Model", "Scenario")
            #> <Run 1 model='Model' scenario='Scenario' version=1>

        Parameters
        ----------
        model_name : str
            The name of a model.
        scenario_name : str
            The name of a scenario.

        Returns
        -------
        :class:`ixmp4.core.run.Run`:
            The created run.
        """

        return Run(self._backend, self._service.create(model, scenario))

    def delete(self, ref: Run | int) -> None:
        """Deletes a run and **all associated iamc, optimization and meta data**.

        .. code:: python

            platform.runs.delete(1)

        Parameters
        ----------
        ref : :class:`ixmp4.core.run.Run` | int
            Run object or id.

        Raises
        ------
        :class:`RunNotFound`:
            If the ref does not exist.
        """

        if isinstance(ref, Run):
            id = ref.id
        elif isinstance(ref, int):
            id = ref
        else:
            raise TypeError("Invalid argument: Must be `Run` or `int`.")

        self._service.delete_by_id(id)

    def get(self, model: str, scenario: str, version: int | None = None) -> Run:
        """Retrieves a run.

        .. code:: python

            default_run = platform.runs.get("Model", "Scenario")
            specific_run = platform.runs.get("Model", "Scenario", version=2)

        Parameters
        ----------
        model_name : str
            The name of a model.
        scenario_name : str
            The name of a scenario.
        version : int | None
            The version number of a run or ``None``
            to get the default version.

        Raises
        ------
        :class:`RunNotFound`:
            If the run with `model_name`, `scenario_name` and
            optional `version` does not exist.

        Returns
        -------
        :class:`ixmp4.core.run.Run`:
            The retrieved run.
        """

        dto = (
            self._service.get_default_version(model, scenario)
            if version is None
            else self._service.get(model, scenario, version)
        )
        return Run(self._backend, dto)

    def list(self, **kwargs: Unpack[FacadeRunFilter]) -> List[Run]:
        r"""Lists runs by specified criteria.

        .. code:: python

            platform.runs.list()
            #> [<Run 1 model='Model' scenario='Scenario' version=1>]

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `RunFilter`.

        Returns
        -------
        list[:class:`ixmp4.core.run.Run`]:
            List of runs.
        """

        return [
            Run(self._backend, dto)
            for dto in self._service.list(**facade_to_data_filter(kwargs))
        ]

    def tabulate(
        self,
        include_audit_info: bool = False,
        include_internal_columns: bool = False,
        audit_info: bool | None = None,
        **kwargs: Unpack[FacadeRunFilter],
    ) -> pd.DataFrame:
        r"""Tabulate runs by specified criteria.

        .. code:: python

            platform.runs.tabulate()
            #>    id  model  scenario  version  is_default
            # 0   1   Model  Scenario  1        True

        Parameters
        ----------
        include_audit_info: bool
            Whether or not to include audit info columns in the data frame.
            Default: False
        include_internal_columns: bool
            Whether or not to include internal database columns
            (``model__id``, ``scenario__id``, ``lock_transaction``).
            Default: False
        \*\*kwargs: any
            Any filter parameters as specified in
            `RunFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - model
                - scenario
                - version
                - is_default
            and if ``include_internal_columns`` is ``True``:
                - model__id
                - scenario__id
                - lock_transaction
            and if ``include_audit_info`` is ``True``:
                - created_by
                - created_at
                - updated_by
                - updated_at

        """
        if audit_info is not None:
            include_audit_info = audit_info

        return self._service.tabulate(
            include_audit_info=include_audit_info,
            include_internal_columns=include_internal_columns,
            **facade_to_data_filter(kwargs),
        )
