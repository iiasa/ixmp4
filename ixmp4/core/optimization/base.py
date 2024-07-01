# from typing import Any, ClassVar, Generic, TypeVar
from typing import Generic, TypeVar

import pandas as pd

from ixmp4.core.base import BaseFacade, BaseModelFacade

# from ixmp4.core.exceptions import IxmpError
from ixmp4.data import abstract

# class OptimizationBaseModel(BaseModelFacade):
#     _model: abstract.OptimizationBaseModel
#     NotFound: ClassVar[type[IxmpError]]
#     NotUnique: ClassVar[type[IxmpError]]

#     @property
#     def id(self) -> int:
#         return self._model.id

#     @property
#     def name(self) -> str:
#         return self._model.name

#     @property
#     def run_id(self) -> int:
#         return self._model.run__id

#     @property
#     def data(self) -> dict[str, Any]:
#         return self._model.data

#     # TODO Stopping here since I'm not convinced of the usefulness. The only benefit I
#     # see is that we'd separate functional code from placeholders so that if we need
#     # to change functionality for e.g. getting 'data', we only need to do so in one
#     # place. Still this feels like a lot of repetition.


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
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> OptimizationModelType:
        model = self._backend_repository.create(
            name=name,
            run_id=self._run.id,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
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
        return self._backend_repository.tabulate(name=name)


class Enumerator(
    Lister[OptimizationModelType], Tabulator[OptimizationModelType], abstract.Enumerator
):
    def enumerate(
        self, *args, table: bool = False, **kwargs
    ) -> list[OptimizationModelType] | pd.DataFrame:
        if table:
            return self.tabulate(*args, **kwargs)
        else:
            return self.list(*args, **kwargs)
