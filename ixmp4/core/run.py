from collections import UserDict
from typing import ClassVar

import numpy as np
import pandas as pd

from ixmp4.data.abstract import Run as RunModel

from .base import BaseFacade, BaseModelFacade
from .iamc import RunIamcData
from .optimization import OptimizationData


class Run(BaseModelFacade):
    _model: RunModel
    _meta: "RunMetaFacade"
    NoDefaultVersion: ClassVar = RunModel.NoDefaultVersion
    NotFound: ClassVar = RunModel.NotFound
    NotUnique: ClassVar = RunModel.NotUnique

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self.version = self._model.version

        self.iamc = RunIamcData(_backend=self.backend, run=self._model)
        self._meta = RunMetaFacade(_backend=self.backend, run=self._model)
        self.optimization = OptimizationData(_backend=self.backend, run=self._model)

    @property
    def model(self):
        """Associated model."""
        return self._model.model

    @property
    def scenario(self):
        """Associated scenario."""
        return self._model.scenario

    @property
    def id(self):
        """Unique id."""
        return self._model.id

    @property
    def meta(self):
        "Meta indicator data (`dict`-like)."
        return self._meta

    @meta.setter
    def meta(self, meta):
        self._meta._set(meta)

    def set_as_default(self):
        """Sets this run as the default version for its `model` + `scenario`
        combination."""
        self.backend.runs.set_as_default_version(self._model.id)

    def unset_as_default(self):
        """Unsets this run as the default version."""
        self.backend.runs.unset_as_default_version(self._model.id)


class RunRepository(BaseFacade):
    def create(
        self,
        model: str,
        scenario: str,
    ) -> Run:
        return Run(
            _backend=self.backend, _model=self.backend.runs.create(model, scenario)
        )

    def get(
        self,
        model: str,
        scenario: str,
        version: int | None = None,
    ) -> Run:
        if version is None:
            _model = self.backend.runs.get_default_version(model, scenario)
        else:
            _model = self.backend.runs.get(model, scenario, version)
        return Run(_backend=self.backend, _model=_model)

    def list(self, default_only: bool = True, **kwargs) -> list[Run]:
        return [
            Run(_backend=self.backend, _model=r)
            for r in self.backend.runs.list(default_only=default_only, **kwargs)
        ]

    def tabulate(
        self, default_only: bool = True, audit_info: bool = False, **kwargs
    ) -> pd.DataFrame:
        runs = self.backend.runs.tabulate(default_only=default_only, **kwargs)
        runs["model"] = runs["model__id"].map(self.backend.models.map())
        runs["scenario"] = runs["scenario__id"].map(self.backend.scenarios.map())
        columns = ["model", "scenario", "version", "is_default"]
        if audit_info:
            columns += ["updated_at", "updated_by", "created_at", "created_by", "id"]
        return runs[columns]


class RunMetaFacade(BaseFacade, UserDict):
    run: RunModel

    def __init__(self, run: RunModel, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.run = run
        self.df, self.data = self._get()

    def _get(self) -> tuple[pd.DataFrame, dict]:
        df = self.backend.meta.tabulate(run_id=self.run.id, run={"default_only": False})
        if df.empty:
            return df, {}
        return df, dict(zip(df["key"], df["value"]))

    def _set(self, meta: dict):
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

    def __setitem__(self, key, value: int | float | str | bool):
        try:
            del self[key]
        except KeyError:
            pass

        value = numpy_to_pytype(value)
        if value is not None:
            self.backend.meta.create(self.run.id, key, value)
        self.df, self.data = self._get()

    def __delitem__(self, key):
        id = dict(zip(self.df["key"], self.df["id"]))[key]
        self.backend.meta.delete(id)
        self.df, self.data = self._get()


def numpy_to_pytype(value):
    """Cast numpy-types to basic Python types"""
    if value is np.nan:  # np.nan is cast to 'float', not None
        return None
    elif isinstance(value, np.generic):
        return value.item()
    else:
        return value
