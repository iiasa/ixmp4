from typing import Generic, TypeVar

import pandas as pd

from ixmp4.core.base import BaseFacade, BaseModelFacade
from ixmp4.data import abstract

OptimizationModelType = TypeVar("OptimizationModelType", bound=BaseModelFacade)


class OptimizationBaseRepository(BaseFacade, Generic[OptimizationModelType]):
    _run: abstract.Run
    _backend_repository: abstract.BackendBaseRepository
    _model_type: type[OptimizationModelType]

    def __init__(self, _run: abstract.Run, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._run = _run


class Creator(OptimizationBaseRepository[OptimizationModelType], abstract.Creator):
    def create(
        self,
        name: str,
        # TODO But how do we now show in core layer that e.g. Table needs these?
        # constrained_to_indexsets: list[str],
        # column_names: list[str] | None = None,
        *args,
        **kwargs,
    ) -> OptimizationModelType:
        model = self._backend_repository.create(
            *args,
            **dict(kwargs, name=name, run_id=self._run.id),
        )
        return self._model_type(_backend=self.backend, _model=model)


class Retriever(OptimizationBaseRepository[OptimizationModelType], abstract.Retriever):
    def get(self, name: str, *args, **kwargs) -> OptimizationModelType:
        model = self._backend_repository.get(run_id=self._run.id, name=name)
        return self._model_type(_backend=self.backend, _model=model)


class Lister(OptimizationBaseRepository[OptimizationModelType], abstract.Lister):
    def list(self, name: str | None = None) -> list[OptimizationModelType]:
        models = self._backend_repository.list(run_id=self._run.id, name=name)
        return [self._model_type(_backend=self.backend, _model=m) for m in models]


class Tabulator(OptimizationBaseRepository[OptimizationModelType], abstract.Tabulator):
    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self._backend_repository.tabulate(run_id=self._run.id, name=name)


class Enumerator(
    Lister[OptimizationModelType], Tabulator[OptimizationModelType], abstract.Enumerator
):
    def enumerate(
        self, *args, table: bool = False, **kwargs
    ) -> list[OptimizationModelType] | pd.DataFrame:
        return self.tabulate(*args, **kwargs) if table else self.list(*args, **kwargs)
