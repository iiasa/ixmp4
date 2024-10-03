from typing import Any

from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.optimization.variable.filter import OptimizationVariableFilter

from .. import deps
from ..base import BaseModel, EnumerationOutput, Pagination
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/variables",
    tags=["optimization", "variables"],
)


class VariableCreateInput(BaseModel):
    run_id: int
    name: str
    constrained_to_indexsets: str | list[str] | None
    column_names: list[str] | None


class DataInput(BaseModel):
    data: dict[str, Any]


@autodoc
@router.get("/{id}/", response_model=api.OptimizationVariable)
def get_by_id(
    id: int,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.variables.get_by_id(id)


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.OptimizationVariable])
def query(
    filter: OptimizationVariableFilter = Body(
        OptimizationVariableFilter(id=None, name=None)
    ),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.optimization.variables.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.optimization.variables.count(_filter=filter),
        pagination=pagination,
    )


@autodoc
@router.patch("/{variable_id}/data/")
def add_data(
    variable_id: int,
    data: DataInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.variables.add_data(
        variable_id=variable_id, **data.model_dump()
    )


@autodoc
@router.delete("/{variable_id}/data/")
def remove_data(
    variable_id: int,
    backend: Backend = Depends(deps.get_backend),
):
    backend.optimization.variables.remove_data(variable_id=variable_id)


@autodoc
@router.post("/", response_model=api.OptimizationVariable)
def create(
    variable: VariableCreateInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.variables.create(**variable.model_dump())
