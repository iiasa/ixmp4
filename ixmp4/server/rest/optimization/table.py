from typing import Any

from fastapi import APIRouter, Body, Depends, Query
from pydantic import RootModel

from ixmp4.data import api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.filters.optimizationtable import OptimizationTableFilter

from .. import deps
from ..base import BaseModel
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/tables",
    tags=["optimization", "tables"],
)


class TableCreateInput(BaseModel):
    run_id: int
    name: str
    constrained_to_indexsets: list[str]
    column_names: list[str] | None


class DataInput(BaseModel):
    data: dict[str, Any]


class EnumerationOutput(BaseModel, RootModel):
    root: list[api.Table] | api.DataFrame


@autodoc
@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: OptimizationTableFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.tables.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.get("/{id}/", response_model=api.Table)
def get_by_id(
    id: int,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.tables.get_by_id(id)


@autodoc
@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: OptimizationTableFilter = Body(OptimizationTableFilter(id=None, name=None)),
    table: bool = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.tables.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.patch("/{table_id}/data/")
def add_data(
    table_id: int,
    data: DataInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.tables.add_data(table_id=table_id, **data.model_dump())


@autodoc
@router.post("/", response_model=api.Table)
def create(
    table: TableCreateInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.tables.create(**table.model_dump())
