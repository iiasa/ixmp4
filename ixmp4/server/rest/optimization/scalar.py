from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.optimization.scalar.filter import OptimizationScalarFilter

from .. import deps
from ..base import BaseModel, EnumerationOutput, Pagination
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/scalars",
    tags=["optimization", "scalars"],
)


class ScalarCreateInput(BaseModel):
    name: str
    value: float
    unit_name: str
    run_id: int


class ScalarUpdateInput(BaseModel):
    value: float | None
    unit_id: int | None


@autodoc
@router.get("/{id}/", response_model=api.Scalar)
def get_by_id(
    id: int,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.scalars.get_by_id(id)


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.Scalar])
def query(
    filter: OptimizationScalarFilter = Body(
        OptimizationScalarFilter(id=None, name=None, unit__id=None)
    ),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.optimization.scalars.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.optimization.scalars.count(_filter=filter),
        pagination=pagination,
    )


@autodoc
@router.patch("/{id}/", response_model=api.Scalar)
def update(
    id: int,
    scalar: ScalarUpdateInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.scalars.update(id, **scalar.model_dump())


@autodoc
@router.post("/", response_model=api.Scalar)
def create(
    scalar: ScalarCreateInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.scalars.create(**scalar.model_dump())
