from typing import Any

from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.optimization.table.filter import OptimizationTableFilter
from ixmp4.data.db.optimization.table.model import Table

from .. import deps
from ..base import BaseModel, EnumerationOutput, Pagination
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/tables",
    tags=["optimization", "tables"],
)


class TableCreateInput(BaseModel):
    name: str
    run_id: int
    constrained_to_indexsets: list[str]
    column_names: list[str] | None


class DataInput(BaseModel):
    data: dict[str, Any]


@autodoc
@router.get("/{id}/", response_model=api.Table)
def get_by_id(
    id: int,
    backend: Backend = Depends(deps.get_backend),
) -> Table:
    return backend.optimization.tables.get_by_id(id)


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.Table])
def query(
    filter: OptimizationTableFilter = Body(OptimizationTableFilter(id=None, name=None)),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[Table]:
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
@router.patch("/{id}/data/")
def add_data(
    id: int,
    data: DataInput,
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.tables.add_data(id=id, **data.model_dump())


@autodoc
@router.delete("/{id}/data/")
def remove_data(
    id: int,
    data: DataInput,
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.tables.remove_data(id=id, **data.model_dump())


@autodoc
@router.post("/", response_model=api.Table)
def create(
    table: TableCreateInput,
    backend: Backend = Depends(deps.get_backend),
) -> Table:
    return backend.optimization.tables.create(**table.model_dump())


@autodoc
@router.delete("/{id}/")
def delete(
    id: int,
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.tables.delete(id=id)
