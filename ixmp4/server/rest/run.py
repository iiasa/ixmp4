from fastapi import APIRouter, Body, Depends, Path, Query
from pydantic import Field, RootModel

from ixmp4.data import api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.run.filter import RunFilter

from . import deps
from .base import BaseModel

router: APIRouter = APIRouter(
    prefix="/runs",
    tags=["runs"],
)


class RunInput(BaseModel):
    name_of_model: str = Field(..., alias="model_name")
    scenario_name: str


class EnumerationOutput(BaseModel, RootModel):
    root: list[api.Run] | api.DataFrame


@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: RunFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.runs.enumerate(table=bool(table), _filter=filter)


@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: RunFilter = Body(RunFilter(id=None, version=None, is_default=False)),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.runs.enumerate(table=bool(table), _filter=filter)


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
