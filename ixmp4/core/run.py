from collections import UserDict
from typing import ClassVar, Iterable

import pandas as pd

from ixmp4.data.abstract import Run as RunModel

from .base import BaseFacade, BaseModelFacade
from .iamc import IamcData
from .optimization import OptimizationData


class Run(BaseModelFacade):
    _model: RunModel
    _meta: "RunMetaFacade"
    NoDefaultVersion: ClassVar = RunModel.NoDefaultVersion
    NotFound: ClassVar = RunModel.NotFound
    NotUnique: ClassVar = RunModel.NotUnique

    def __init__(
        self,
        model: str | None = None,
        scenario: str | None = None,
        version: int | str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if getattr(self, "_model", None) is None:
            if (model is None) or (scenario is None):
                raise TypeError("`Run` requires `model` and `scenario`")

            if version is None:
                self._model = self.backend.runs.get_default_version(model, scenario)
            elif version == "new":
                self._model = self.backend.runs.create(model, scenario)
            elif isinstance(version, int):
                self._model = self.backend.runs.get(model, scenario, version)
            else:
                raise ValueError(
                    "Invalid value for `version`, must be 'new' or integer."
                )
            self.version = self._model.version

        self.iamc = IamcData(_backend=self.backend, run=self._model)
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
        """Sets this run as the default version for its `model` + `scenario` combination."""
        self.backend.runs.set_as_default_version(self._model.id)

    def unset_as_default(self):
        """Unsets this run as the default version."""
        self.backend.runs.unset_as_default_version(self._model.id)


class RunRepository(BaseFacade):
    def list(self, default_only: bool = True) -> Iterable[Run]:
        return [
            Run(_backend=self.backend, _model=r)
            for r in self.backend.runs.list(default_only=default_only)
        ]

    def tabulate(self, default_only: bool = True) -> pd.DataFrame:
        runs = self.backend.runs.tabulate(default_only=default_only)
        runs["model"] = runs["model__id"].map(
            dict([(m.id, m.name) for m in self.backend.models.list()])
        )
        runs["scenario"] = runs["scenario__id"].map(
            dict([(s.id, s.name) for s in self.backend.scenarios.list()])
        )
        return runs[["id", "model", "scenario", "version", "is_default"]]


class RunMetaFacade(BaseFacade, UserDict):
    run: RunModel

    def __init__(self, run: RunModel, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.run = run
        self.df, self.data = self._get()

    def _get(self) -> tuple[pd.DataFrame, dict]:
        df = self.backend.meta.tabulate(run_ids=[self.run.id])
        if df.empty:
            return df, {}
        return df, dict(zip(df["key"], df["value"]))

    def _set(self, meta: dict):
        df = pd.DataFrame({"key": self.data.keys()})
        df["run__id"] = self.run.id
        self.backend.meta.bulk_delete(df)
        df = pd.DataFrame({"key": meta.keys(), "value": meta.values()})
        df["run__id"] = self.run.id
        self.backend.meta.bulk_upsert(df)
        self.df, self.data = self._get()

    def __setitem__(self, key, value: int | float | str | bool):
        try:
            del self[key]
        except KeyError:
            pass

        self.backend.meta.create(self.run.id, key, value)
        self.df, self.data = self._get()

    def __delitem__(self, key):
        id = dict(zip(self.df["key"], self.df["id"]))[key]
        self.backend.meta.delete(id)
        self.df, self.data = self._get()
