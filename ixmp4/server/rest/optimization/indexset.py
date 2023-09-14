from fastapi import APIRouter, Body, Depends, Query
from pydantic import StrictInt, StrictStr

from ixmp4.data import api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.filters.optimizationindexset import OptimizationIndexSetFilter

from .. import deps
from ..base import BaseModel
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/indexsets",
    tags=["optimization", "indexsets"],
)


class IndexSetInput(BaseModel):
    run_id: int
    name: str


class EnumerationOutput(BaseModel):
    __root__: list[api.IndexSet] | api.DataFrame


class ElementsInput(BaseModel):
    elements: StrictInt | list[StrictInt | StrictStr] | StrictStr


@autodoc
@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: OptimizationIndexSetFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.indexsets.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: OptimizationIndexSetFilter = Body(OptimizationIndexSetFilter()),
    table: bool = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.indexsets.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.post("/", response_model=api.IndexSet)
def create(
    indexset: IndexSetInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.indexsets.create(**indexset.dict())


@autodoc
@router.patch("/{indexset_id}/")
def add_elements(
    indexset_id: int,
    elements: ElementsInput,
    backend: Backend = Depends(deps.get_backend),
):
    backend.optimization.indexsets.add_elements(
        indexset_id=indexset_id, **elements.dict()
    )
