from typing import Any

from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.optimization.equation.filter import EquationFilter
from ixmp4.data.db.optimization.equation.model import Equation

from .. import deps
from ..base import BaseModel, EnumerationOutput, Pagination
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/equations",
    tags=["optimization", "equations"],
)


class EquationCreateInput(BaseModel):
    name: str
    run_id: int
    constrained_to_indexsets: list[str] | None
    column_names: list[str] | None


class DataInput(BaseModel):
    data: dict[str, Any] | None


@autodoc
@router.get("/{id}/", response_model=api.Equation)
def get_by_id(
    id: int,
    backend: Backend = Depends(deps.get_backend),
) -> Equation:
    return backend.optimization.equations.get_by_id(id)


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.Equation])
def query(
    filter: EquationFilter = Body(EquationFilter(id=None, name=None)),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[Equation]:
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
@router.patch("/{id}/data/")
def add_data(
    id: int,
    data: DataInput,
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.equations.add_data(id=id, **data.model_dump())


@autodoc
@router.delete("/{id}/data/")
def remove_data(
    id: int,
    data: DataInput,
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.equations.remove_data(id=id, **data.model_dump())


@autodoc
@router.post("/", response_model=api.Equation)
def create(
    equation: EquationCreateInput,
    backend: Backend = Depends(deps.get_backend),
) -> Equation:
    return backend.optimization.equations.create(**equation.model_dump())


@autodoc
@router.delete("/{id}/")
def delete(
    id: int,
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.equations.delete(id=id)
