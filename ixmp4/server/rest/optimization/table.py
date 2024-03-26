from typing import Any

from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.optimization.table.filter import OptimizationTableFilter

from .. import deps
from ..base import BaseModel, EnumerationOutput, Pagination
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


@autodoc
@router.get("/{id}/", response_model=api.Table)
def get_by_id(
    id: int,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.tables.get_by_id(id)


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.Table])
def query(
    filter: OptimizationTableFilter = Body(OptimizationTableFilter(id=None, name=None)),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.optimization.tables.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.optimization.tables.count(_filter=filter),
        pagination=pagination,
    )


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
