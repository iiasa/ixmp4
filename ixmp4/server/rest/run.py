from fastapi import APIRouter, Body, Depends, Path, Query
from pydantic import Field

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.run.filter import RunFilter

from . import deps
from .base import BaseModel, EnumerationOutput, Pagination

router: APIRouter = APIRouter(
    prefix="/runs",
    tags=["runs"],
)


class RunInput(BaseModel):
    name_of_model: str = Field(..., alias="model_name")
    scenario_name: str


@router.patch("/", response_model=EnumerationOutput[api.Run])
def query(
    filter: RunFilter = Body(RunFilter()),
    table: bool | None = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.runs.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.runs.count(_filter=filter),
        pagination=pagination,
    )


@router.post("/", response_model=api.Run)
def create(
    run: RunInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.runs.create(**run.model_dump(by_alias=True))


@router.post("/{id}/set-as-default-version/")
def set_as_default_version(
    id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
):
    backend.runs.set_as_default_version(id)


@router.post("/{id}/unset-as-default-version/")
def unset_as_default_version(
    id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
):
    backend.runs.unset_as_default_version(id)
