from typing import Any

from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.optimization.equation.filter import EquationFilter

from .. import deps
from ..base import BaseModel, EnumerationOutput, Pagination
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/equations",
    tags=["optimization", "equations"],
)


class EquationCreateInput(BaseModel):
    run_id: int
    name: str
    constrained_to_indexsets: list[str]
    column_names: list[str] | None


class DataInput(BaseModel):
    data: dict[str, Any]


@autodoc
@router.get("/{id}/", response_model=api.Equation)
def get_by_id(
    id: int,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.equations.get_by_id(id)


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.Equation])
def query(
    filter: EquationFilter = Body(EquationFilter(id=None, name=None)),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.optimization.equations.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.optimization.equations.count(_filter=filter),
        pagination=pagination,
    )


@autodoc
@router.patch("/{equation_id}/data/")
def add_data(
    equation_id: int,
    data: DataInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.equations.add_data(
        equation_id=equation_id, **data.model_dump()
    )


@autodoc
@router.delete("/{equation_id}/data/")
def remove_data(
    equation_id: int,
    backend: Backend = Depends(deps.get_backend),
):
    backend.optimization.equations.remove_data(equation_id == equation_id)


@autodoc
@router.post("/", response_model=api.Equation)
def create(
    equation: EquationCreateInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.equations.create(**equation.model_dump())
