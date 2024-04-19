from fastapi import APIRouter, Body, Depends, Query
from pydantic import StrictFloat, StrictInt, StrictStr

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.optimization.indexset.filter import OptimizationIndexSetFilter

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


class ElementsInput(BaseModel):
    elements: (
        StrictFloat | StrictInt | StrictStr | list[StrictFloat | StrictInt | StrictStr]
    )


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.IndexSet])
def query(
    filter: OptimizationIndexSetFilter = Body(OptimizationIndexSetFilter()),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
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
):
    return backend.optimization.indexsets.create(**indexset.model_dump())


@autodoc
@router.patch("/{indexset_id}/")
def add_elements(
    indexset_id: int,
    elements: ElementsInput,
    backend: Backend = Depends(deps.get_backend),
):
    backend.optimization.indexsets.add_elements(
        indexset_id=indexset_id, **elements.model_dump()
    )
