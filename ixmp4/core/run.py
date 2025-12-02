import time
import warnings
from collections import UserDict
from contextlib import contextmanager
from typing import Generator, cast

import numpy as np
import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.base_exceptions import OperationNotSupported
from ixmp4.data.meta.dto import MetaValueType
from ixmp4.data.meta.service import RunMetaEntryService
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
from .optimization.data import RunOptimizationData


class Run(BaseFacadeObject[RunService, RunDto]):
    NotFound = RunNotFound
    NotUnique = RunNotUnique
    DeletionPrevented = RunDeletionPrevented
    IsLocked = RunIsLocked
    LockRequired = RunLockRequired
    NoDefaultVersion = NoDefaultRunVersion

    dto: RunDto
    _meta: "RunMetaFacade"

    checkpoints: RunCheckpoints
    iamc: RunIamcData
    optimization: RunOptimizationData

    owns_lock: bool = False
    minimum_lock_timeout: float = 0.1
    maximum_lock_timeout: float = 5

    def __init__(self, backend: Backend, dto: RunDto) -> None:
        super().__init__(backend, dto)
        self.iamc = RunIamcData(backend, run=self)
        self._meta = RunMetaFacade(backend, run=self)
        self.optimization = RunOptimizationData(backend, run=self)
        self.checkpoints = RunCheckpoints(backend, run=self)

    @property
    def id(self) -> int:
        """Unique id."""
        return self.dto.id

    @property
    def model(self) -> ModelDto:
        """Associated model."""
        return self.dto.model

    @property
    def scenario(self) -> ScenarioDto:
        """Associated scenario."""
        return self.dto.scenario

    @property
    def version(self) -> int:
        """Run version."""
        return self.dto.version

    @property
    def meta(self) -> "RunMetaFacade":
        "Meta indicator data (`dict`-like)."
        return self._meta

    @meta.setter
    def meta(self, meta: dict[str, MetaValueType | np.generic | None]) -> None:
        self._meta._set(meta)

    @property
    def is_default(self) -> bool:
        return self.dto.is_default

    def set_as_default(self) -> None:
        """Sets this run as the default version for its `model` + `scenario`
        combination."""
        self._service.set_as_default_version(self.dto.id)
        self.refresh()

    def unset_as_default(self) -> None:
        """Unsets this run as the default version."""
        self._service.unset_as_default_version(self.dto.id)
        self.refresh()

    def require_lock(self) -> None:
        if not self.owns_lock:
            raise RunLockRequired()

    def _lock(self) -> None:
        self.dto = self._service.lock(self.dto.id)
        self.owns_lock = True

    def _unlock(self) -> None:
        self.dto = self._service.unlock(self.dto.id)
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

            assert self.dto.lock_transaction is not None

            target_transaction = max(checkpoint_transaction, self.dto.lock_transaction)
            try:
                self._service.revert(
                    self.dto.id,
                    target_transaction,
                    revert_platform=revert_platform_on_error,
                )
            except OperationNotSupported as ons_exc:
                warnings.warn(
                    "An exception occurred but the `Run` "
                    "was not reverted because versioning "
                    "is not supported by this platform: " + str(ons_exc.message)
                )

            self._meta.refresh()
            self._unlock()
            raise e

        self.checkpoints.create(message)
        self._unlock()

    def delete(self) -> None:
        """Delete this run.
        Tries to acquire a lock in the background.

        Raises
        ------
        :class:`ixmp4.core.exceptions.RunIsLocked`:
            If the run is already locked by this or another object.
        """
        self._service.delete_by_id(self.dto.id)

    def clone(
        self,
        model: str | None = None,
        scenario: str | None = None,
        keep_solution: bool = True,
    ) -> "Run":
        return Run(
            service=self._service,
            dto=self._service.clone(
                run_id=self.id,
                model_name=model,
                scenario_name=scenario,
                keep_solution=keep_solution,
            ),
        )

    def _get_service(self, backend: Backend) -> RunService:
        return backend.runs


class RunServiceFacade(BaseServiceFacade[RunService]):
    def _get_service(self, backend: Backend) -> RunService:
        return backend.runs

    def create(self, model: str, scenario: str) -> Run:
        return Run(self._backend, self._service.create(model, scenario))

    def delete(self, x: Run | int) -> None:
        if isinstance(x, Run):
            id = x.id
        elif isinstance(x, int):
            id = x
        else:
            raise TypeError("Invalid argument: Must be `Run` or `int`.")

        self._service.delete_by_id(id)

    def get(self, model: str, scenario: str, version: int | None = None) -> Run:
        dto = (
            self._service.get_default_version(model, scenario)
            if version is None
            else self._service.get(model, scenario, version)
        )
        return Run(self._backend, dto)

    def list(self, **kwargs: Unpack[RunFilter]) -> list[Run]:
        return [Run(self._backend, dto) for dto in self._service.list(**kwargs)]

    def tabulate(
        self, audit_info: bool = False, **kwargs: Unpack[RunFilter]
    ) -> pd.DataFrame:
        runs = self._service.tabulate(**kwargs)
        columns = ["model", "scenario", "version", "is_default"]
        if audit_info:
            columns += ["updated_at", "updated_by", "created_at", "created_by", "id"]
        return runs[columns]


class RunMetaFacade(
    BaseServiceFacade[RunMetaEntryService], UserDict[str, MetaValueType | None]
):
    run: Run

    def _get_service(self, backend: Backend) -> RunMetaEntryService:
        return backend.meta

    def __init__(self, backend: Backend, run: Run) -> None:
        super().__init__(backend)
        self.run = run
        self.refresh()

    def refresh(self) -> None:
        self.df, self.data = self._get()

    def _get(self) -> tuple[pd.DataFrame, dict[str, MetaValueType | None]]:
        df = self._service.tabulate(run__id=self.run.id, run={"default_only": False})
        if df.empty:
            return df, {}
        return df, dict(zip(df["key"], df["value"]))

    def _set(self, meta: dict[str, MetaValueType | np.generic | None]) -> None:
        self.run.require_lock()

        df = pd.DataFrame({"key": self.data.keys()})
        df["run__id"] = self.run.id
        self._service.bulk_delete(df)
        df = pd.DataFrame(
            {"key": meta.keys(), "value": [numpy_to_pytype(v) for v in meta.values()]}
        )
        df.dropna(axis=0, inplace=True)
        df["run__id"] = self.run.id
        self._service.bulk_upsert(df)
        self.df, self.data = self._get()

    def __setitem__(self, key: str, value: MetaValueType | np.generic | None) -> None:
        self.run.require_lock()

        try:
            del self[key]
        except KeyError:
            pass

        py_value = numpy_to_pytype(value)
        if py_value is not None:
            self._service.create(self.run.id, key, py_value)
        self.df, self.data = self._get()

    def __delitem__(self, key: str) -> None:
        self.run.require_lock()
        id = dict(zip(self.df["key"], self.df["id"]))[key]
        self._service.delete_by_id(id)
        self.df, self.data = self._get()


def numpy_to_pytype(
    value: MetaValueType | np.generic | None,
) -> MetaValueType | None:
    """Cast numpy-types to basic Python types"""
    if value is np.nan:  # np.nan is cast to 'float', not None
        return None
    elif isinstance(value, np.generic):
        return cast(MetaValueType, value.item())
    else:
        return value
