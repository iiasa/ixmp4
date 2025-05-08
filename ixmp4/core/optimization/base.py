from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from . import InitKwargs


import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

from ixmp4.core.base import BaseFacade, BaseModelFacade
from ixmp4.data import abstract

FacadeOptimizationModelType = TypeVar(
    "FacadeOptimizationModelType", bound=BaseModelFacade
)
AbstractOptimizationModelType = TypeVar(
    "AbstractOptimizationModelType", bound=abstract.BaseModel
)


class OptimizationBaseRepository(
    BaseFacade, Generic[FacadeOptimizationModelType, AbstractOptimizationModelType]
):
    _run: abstract.Run
    _backend_repository: abstract.BackendBaseRepository[AbstractOptimizationModelType]
    _model_type: type[FacadeOptimizationModelType]

    def __init__(self, _run: abstract.Run, **kwargs: Unpack["InitKwargs"]) -> None:
        super().__init__(**kwargs)
        self._run = _run


class Creator(
    OptimizationBaseRepository[
        FacadeOptimizationModelType, AbstractOptimizationModelType
    ],
    abstract.Creator,
):
    def create(
        self, name: str, **kwargs: Unpack["abstract.optimization.base.CreateKwargs"]
    ) -> FacadeOptimizationModelType:
        model = self._backend_repository.create(
            run_id=self._run.id, name=name, **kwargs
        )
        return self._model_type(_backend=self.backend, _model=model)


class Deleter(
    OptimizationBaseRepository[
        FacadeOptimizationModelType, AbstractOptimizationModelType
    ],
    abstract.Deleter,
):
    def delete(self, item: int | str) -> None:
        if isinstance(item, int):
            id = item
        elif isinstance(item, str):
            model = self._backend_repository.get(run_id=self._run.id, name=item)
            id = model.id
        else:
            raise TypeError("Invalid argument: `id` must be `int` or `str`.")

        self._backend_repository.delete(id=id)


class Retriever(
    OptimizationBaseRepository[
        FacadeOptimizationModelType, AbstractOptimizationModelType
    ],
    abstract.Retriever,
):
    def get(self, name: str) -> FacadeOptimizationModelType:
        model = self._backend_repository.get(run_id=self._run.id, name=name)
        return self._model_type(_backend=self.backend, _model=model)


class Lister(
    OptimizationBaseRepository[
        FacadeOptimizationModelType, AbstractOptimizationModelType
    ],
    abstract.Lister,
):
    def list(self, name: str | None = None) -> list[FacadeOptimizationModelType]:
        models = self._backend_repository.list(run_id=self._run.id, name=name)
        return [self._model_type(_backend=self.backend, _model=m) for m in models]


class Tabulator(
    OptimizationBaseRepository[
        FacadeOptimizationModelType, AbstractOptimizationModelType
    ],
    abstract.Tabulator,
):
    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self._backend_repository.tabulate(run_id=self._run.id, name=name)


class EnumerateKwargs(TypedDict, total=False):
    name: str | None


class Enumerator(
    Lister[FacadeOptimizationModelType, AbstractOptimizationModelType],
    Tabulator[FacadeOptimizationModelType, AbstractOptimizationModelType],
    abstract.Enumerator,
):
    def enumerate(
        self, table: bool = False, **kwargs: Unpack[EnumerateKwargs]
    ) -> list[FacadeOptimizationModelType] | pd.DataFrame:
        return self.tabulate(**kwargs) if table else self.list(**kwargs)
