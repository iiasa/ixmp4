import time
import warnings
from contextlib import contextmanager
from typing import Generator, List

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.base_exceptions import OperationNotSupported
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
from ixmp4.data.run.filter import RunFilter
from ixmp4.data.run.service import RunService
from ixmp4.data.scenario.dto import Scenario as ScenarioDto

from .base import BaseFacadeObject, BaseServiceFacade
from .checkpoints import RunCheckpoints
from .iamc import RunIamcData
from .meta import RunMetaFacade
from .optimization.data import RunOptimizationData


class Run(BaseFacadeObject[RunService, RunDto]):
    NotFound = RunNotFound
    NotUnique = RunNotUnique
    DeletionPrevented = RunDeletionPrevented
    IsLocked = RunIsLocked
    LockRequired = RunLockRequired
    NoDefaultVersion = NoDefaultRunVersion

    meta: RunMetaFacade

    checkpoints: RunCheckpoints
    iamc: RunIamcData
    optimization: RunOptimizationData

    owns_lock: bool = False
    minimum_lock_timeout: float = 0.1
    maximum_lock_timeout: float = 5

    def __init__(self, backend: Backend, dto: RunDto) -> None:
        super().__init__(backend, dto)
        self.iamc = RunIamcData(backend, run=self)
        self.meta = RunMetaFacade(backend, run=self)
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

    def require_lock(self) -> None:
        if not self.owns_lock:
            raise RunLockRequired()

    def _lock(self) -> None:
        self._dto = self._service.lock(self._dto.id)
        self.owns_lock = True

    def _unlock(self) -> None:
        self._dto = self._service.unlock(self._dto.id)
        self.owns_lock = False

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
        messsage : str
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

        if timeout is None:
            self._lock()
        else:
            self._lock_with_timeout(timeout)

        try:
            yield
        except Exception as e:
            checkpoint_df = self.checkpoints.tabulate()
            if checkpoint_df.empty:
                checkpoint_transaction = -1
            else:
                max_tx_id = checkpoint_df["transaction__id"].max()
                if pd.isnull(max_tx_id):
                    checkpoint_transaction = -1
                else:
                    checkpoint_transaction = int(max_tx_id)

            assert self._dto.lock_transaction is not None

            target_transaction = max(checkpoint_transaction, self._dto.lock_transaction)
            try:
                self._service.revert(
                    self._dto.id,
                    target_transaction,
                    revert_platform=revert_platform_on_error,
                )
            except OperationNotSupported as ons_exc:
                warnings.warn(
                    "An exception occurred but the `Run` "
                    "was not reverted because versioning "
                    "is not supported by this platform: " + str(ons_exc.message)
                )

            self.meta._refresh()
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
        return Run(
            backend=self._backend,
            dto=self._service.clone(
                run_id=self.id,
                model_name=model,
                scenario_name=scenario,
                keep_solution=keep_solution,
            ),
        )

    def _get_service(self, backend: Backend) -> RunService:
        return backend.runs

    def __str__(self) -> str:
        return (
            f"<Run {self.id} model='{self.model.name}' "
            f"scenario='{self.scenario.name}' version='{self.version}'>"
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

    def list(self, **kwargs: Unpack[RunFilter]) -> List[Run]:
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

        return [Run(self._backend, dto) for dto in self._service.list(**kwargs)]

    def tabulate(
        self,
        include_audit_info: bool = False,
        audit_info: bool | None = None,
        **kwargs: Unpack[RunFilter],
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
                - model__id
                - scenario__id
                - version
                - lock_transaction
                - is_default
            and if ``include_audit_info`` is ``True``:
                - created_by
                - created_at
                - updated_by
                - updated_at

        """
        if audit_info is not None:
            include_audit_info = audit_info

        return self._service.tabulate(include_audit_info=include_audit_info, **kwargs)
