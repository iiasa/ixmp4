import time
from collections import UserDict
from contextlib import contextmanager
from typing import ClassVar, Generator, cast

import numpy as np
import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

from ixmp4.data.abstract import Model as ModelModel
from ixmp4.data.abstract import Run as RunModel
from ixmp4.data.abstract import Scenario as ScenarioModel
from ixmp4.data.abstract.annotations import PrimitiveTypes
from ixmp4.data.abstract.run import EnumerateKwargs
from ixmp4.data.backend import Backend

from .base import BaseFacade, BaseModelFacade
from .checkpoints import RunCheckpoints
from .exceptions import RunLockRequired
from .iamc import RunIamcData
from .optimization import OptimizationData


class RunKwargs(TypedDict):
    _backend: Backend
    _model: RunModel


class Run(BaseModelFacade):
    _model: RunModel
    _meta: "RunMetaFacade"
    NoDefaultVersion: ClassVar = RunModel.NoDefaultVersion
    NotFound: ClassVar = RunModel.NotFound
    NotUnique: ClassVar = RunModel.NotUnique

    checkpoints: RunCheckpoints

    owns_lock: bool = False
    minimum_lock_timeout: float = 0.1
    maximum_lock_timeout: float = 5

    def __init__(self, **kwargs: Unpack[RunKwargs]) -> None:
        super().__init__(**kwargs)

        self.version = self._model.version

        self.iamc = RunIamcData(_backend=self.backend, run=self)
        self._meta = RunMetaFacade(_backend=self.backend, run=self)
        self.optimization = OptimizationData(_backend=self.backend, run=self._model)
        self.checkpoints = RunCheckpoints(_backend=self.backend, run=self)

    @property
    def model(self) -> ModelModel:
        """Associated model."""
        return self._model.model

    @property
    def scenario(self) -> ScenarioModel:
        """Associated scenario."""
        return self._model.scenario

    @property
    def id(self) -> int:
        """Unique id."""
        return self._model.id

    @property
    def meta(self) -> "RunMetaFacade":
        "Meta indicator data (`dict`-like)."
        return self._meta

    @meta.setter
    def meta(self, meta: dict[str, PrimitiveTypes | np.generic | None]) -> None:
        self._meta._set(meta)

    @property
    def is_default(self) -> bool:
        return self._model.is_default

    def set_as_default(self) -> None:
        """Sets this run as the default version for its `model` + `scenario`
        combination."""
        self.backend.runs.set_as_default_version(self._model.id)
        self._model = self.backend.runs.get_by_id(self._model.id)

    def unset_as_default(self) -> None:
        """Unsets this run as the default version."""
        self.backend.runs.unset_as_default_version(self._model.id)
        self._model = self.backend.runs.get_by_id(self._model.id)

    def require_lock(self) -> None:
        if not self.owns_lock:
            raise RunLockRequired()

    def _lock(self) -> None:
        self._model = self.backend.runs.lock(self._model.id)
        self.owns_lock = True

    def _unlock(self) -> None:
        self._model = self.backend.runs.unlock(self._model.id)
        self.owns_lock = False

    def _lock_with_timeout(self, timeout: float) -> None:
        """Try locking the run until a timeout passes."""
        start_time = time.time()
        while True:
            try:
                self._lock()
                break
            except RunModel.IsLocked as e:
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
        self, message: str, timeout: float | None = None
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
                checkpoint_transaction = int(checkpoint_df["transaction__id"].max())

            assert self._model.lock_transaction is not None

            if checkpoint_transaction > self._model.lock_transaction:
                self.backend.runs.revert(self._model.id, checkpoint_transaction)
            else:
                self.backend.runs.revert(self._model.id, self._model.lock_transaction)

            self._meta.refetch_data()
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
        self.backend.runs.delete(self._model.id)

    def clone(
        self,
        model: str | None = None,
        scenario: str | None = None,
        keep_solution: bool = True,
    ) -> "Run":
        return Run(
            _backend=self.backend,
            _model=self.backend.runs.clone(
                run_id=self.id,
                model_name=model,
                scenario_name=scenario,
                keep_solution=keep_solution,
            ),
        )


class RunRepository(BaseFacade):
    def create(self, model: str, scenario: str) -> Run:
        return Run(
            _backend=self.backend, _model=self.backend.runs.create(model, scenario)
        )

    def delete(self, x: Run | int) -> None:
        if isinstance(x, Run):
            id = x.id
        elif isinstance(x, int):
            id = x
        else:
            raise TypeError("Invalid argument: Must be `Run` or `int`.")

        self.backend.runs.delete(id)

    def get(self, model: str, scenario: str, version: int | None = None) -> Run:
        _model = (
            self.backend.runs.get_default_version(model, scenario)
            if version is None
            else self.backend.runs.get(model, scenario, version)
        )
        return Run(_backend=self.backend, _model=_model)

    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[Run]:
        return [
            Run(_backend=self.backend, _model=r)
            for r in self.backend.runs.list(**kwargs)
        ]

    def tabulate(
        self, audit_info: bool = False, **kwargs: Unpack[EnumerateKwargs]
    ) -> pd.DataFrame:
        runs = self.backend.runs.tabulate(**kwargs)
        runs["model"] = runs["model__id"].map(self.backend.models.map())
        runs["scenario"] = runs["scenario__id"].map(self.backend.scenarios.map())
        columns = ["model", "scenario", "version", "is_default"]
        if audit_info:
            columns += ["updated_at", "updated_by", "created_at", "created_by", "id"]
        return runs[columns]


class RunMetaFacade(BaseFacade, UserDict[str, PrimitiveTypes | None]):
    run: Run

    def __init__(self, run: Run, **kwargs: Backend) -> None:
        super().__init__(**kwargs)
        self.run = run
        self.refetch_data()

    def refetch_data(self) -> None:
        self.df, self.data = self._get()

    def _get(self) -> tuple[pd.DataFrame, dict[str, PrimitiveTypes | None]]:
        df = self.backend.meta.tabulate(run_id=self.run.id, run={"default_only": False})
        if df.empty:
            return df, {}
        return df, dict(zip(df["key"], df["value"]))

    def _set(self, meta: dict[str, PrimitiveTypes | np.generic | None]) -> None:
        self.run.require_lock()

        df = pd.DataFrame({"key": self.data.keys()})
        df["run__id"] = self.run.id
        self.backend.meta.bulk_delete(df)
        df = pd.DataFrame(
            {"key": meta.keys(), "value": [numpy_to_pytype(v) for v in meta.values()]}
        )
        df.dropna(axis=0, inplace=True)
        df["run__id"] = self.run.id
        self.backend.meta.bulk_upsert(df)
        self.df, self.data = self._get()

    def __setitem__(self, key: str, value: PrimitiveTypes | np.generic | None) -> None:
        self.run.require_lock()

        try:
            del self[key]
        except KeyError:
            pass

        py_value = numpy_to_pytype(value)
        if py_value is not None:
            self.backend.meta.create(self.run.id, key, py_value)
        self.df, self.data = self._get()

    def __delitem__(self, key: str) -> None:
        self.run.require_lock()
        id = dict(zip(self.df["key"], self.df["id"]))[key]
        self.backend.meta.delete(id)
        self.df, self.data = self._get()


def numpy_to_pytype(
    value: PrimitiveTypes | np.generic | None,
) -> PrimitiveTypes | None:
    """Cast numpy-types to basic Python types"""
    if value is np.nan:  # np.nan is cast to 'float', not None
        return None
    elif isinstance(value, np.generic):
        return cast(PrimitiveTypes, value.item())
    else:
        return value
