from collections import UserDict
from typing import ClassVar, cast

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

    def __init__(self, **kwargs: Unpack[RunKwargs]) -> None:
        super().__init__(**kwargs)

        self.version = self._model.version

        self.iamc = RunIamcData(_backend=self.backend, run=self._model)
        self._meta = RunMetaFacade(_backend=self.backend, run=self._model)
        self.optimization = OptimizationData(_backend=self.backend, run=self._model)

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

    def set_as_default(self) -> None:
        """Sets this run as the default version for its `model` + `scenario`
        combination."""
        self.backend.runs.set_as_default_version(self._model.id)

    def unset_as_default(self) -> None:
        """Unsets this run as the default version."""
        self.backend.runs.unset_as_default_version(self._model.id)

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
    run: RunModel

    def __init__(self, run: RunModel, **kwargs: Backend) -> None:
        super().__init__(**kwargs)
        self.run = run
        self.df, self.data = self._get()

    def _get(self) -> tuple[pd.DataFrame, dict[str, PrimitiveTypes | None]]:
        df = self.backend.meta.tabulate(run_id=self.run.id, run={"default_only": False})
        if df.empty:
            return df, {}
        return df, dict(zip(df["key"], df["value"]))

    def _set(self, meta: dict[str, PrimitiveTypes | np.generic | None]) -> None:
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
        try:
            del self[key]
        except KeyError:
            pass

        py_value = numpy_to_pytype(value)
        if py_value is not None:
            self.backend.meta.create(self.run.id, key, py_value)
        self.df, self.data = self._get()

    def __delitem__(self, key: str) -> None:
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
