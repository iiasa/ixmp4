from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.optimization.indexset.filter import OptimizationIndexSetFilter
from ixmp4.data.db.optimization.indexset.model import IndexSet

from .. import deps
from ..base import BaseModel, EnumerationOutput, Pagination
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/indexsets",
    tags=["optimization", "indexsets"],
)


class IndexSetInput(BaseModel):
    run_id: int
    name: str


class DataInput(BaseModel):
    data: float | int | str | list[float] | list[int] | list[str]


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.IndexSet])
def query(
    filter: OptimizationIndexSetFilter = Body(OptimizationIndexSetFilter()),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[IndexSet]:
    return EnumerationOutput(
        results=backend.optimization.indexsets.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.optimization.indexsets.count(_filter=filter),
        pagination=pagination,
    )


@autodoc
@router.post("/", response_model=api.IndexSet)
def create(
    indexset: IndexSetInput,
    backend: Backend = Depends(deps.get_backend),
) -> IndexSet:
    return backend.optimization.indexsets.create(**indexset.model_dump())


@autodoc
@router.delete("/{id}/")
def delete(
    id: int,
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.indexsets.delete(id=id)


@autodoc
@router.patch("/{id}/data/")
def add_data(
    id: int,
    data: DataInput,
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.indexsets.add_data(id=id, **data.model_dump())


@autodoc
@router.delete("/{id}/data/")
def remove_data(
    id: int,
    data: DataInput,
    remove_dependent_data: bool = Query(True),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.indexsets.remove_data(
        id=id, remove_dependent_data=remove_dependent_data, **data.model_dump()
    )
