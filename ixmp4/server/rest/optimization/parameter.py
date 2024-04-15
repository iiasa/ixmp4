from typing import Any

from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.optimization.parameter.filter import OptimizationParameterFilter

from .. import deps
from ..base import BaseModel, EnumerationOutput, Pagination
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/parameters",
    tags=["optimization", "parameters"],
)


class ParameterCreateInput(BaseModel):
    run_id: int
    name: str
    constrained_to_indexsets: list[str]
    column_names: list[str] | None


class DataInput(BaseModel):
    data: dict[str, Any]


@autodoc
@router.get("/{id}/", response_model=api.Parameter)
def get_by_id(
    id: int,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.parameters.get_by_id(id)


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.Parameter])
def query(
    filter: OptimizationParameterFilter = Body(
        OptimizationParameterFilter(id=None, name=None)
    ),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.optimization.parameters.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.optimization.parameters.count(_filter=filter),
        pagination=pagination,
    )


@autodoc
@router.patch("/{parameter_id}/data/")
def add_data(
    parameter_id: int,
    data: DataInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.parameters.add_data(
        parameter_id=parameter_id, **data.model_dump()
    )


@autodoc
@router.post("/", response_model=api.Parameter)
def create(
    parameter: ParameterCreateInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.parameters.create(**parameter.model_dump())
