from fastapi import APIRouter, Body, Depends, Query
from pydantic import RootModel

from ixmp4.data import api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.filters.optimizationscalar import OptimizationScalarFilter

from .. import deps
from ..base import BaseModel
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/scalars",
    tags=["optimization", "scalars"],
)


class ScalarInput(BaseModel):
    name: str
    value: float
    unit_id: int
    run_id: int


class EnumerationOutput(BaseModel, RootModel):
    root: list[api.Scalar] | api.DataFrame


@autodoc
@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: OptimizationScalarFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.scalars.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.get("/{id}/", response_model=api.Scalar)
def get_by_id(
    id: int,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.scalars.get_by_id(id)


@autodoc
@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: OptimizationScalarFilter = Body(
        OptimizationScalarFilter(id=None, name=None, unit__id=None)
    ),
    table: bool = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.scalars.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.patch("/{id}/", response_model=api.Scalar)
def update(
    scalar: ScalarInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.scalars.update(**scalar.model_dump())


@autodoc
@router.post("/", response_model=api.Scalar)
def create(
    scalar: ScalarInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.scalars.create(**scalar.model_dump())
