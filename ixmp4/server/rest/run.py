from fastapi import APIRouter, Depends, Path, Query, Body

from ixmp4.data.backend import Backend
from ixmp4.data import api
from ixmp4.data.db.run.filter import RunFilter

from .base import BaseModel
from . import deps


router: APIRouter = APIRouter(
    prefix="/runs",
    tags=["runs"],
)


class RunInput(BaseModel):
    model_name: str
    scenario_name: str


class EnumerationOutput(BaseModel):
    __root__: list[api.Run] | api.DataFrame


@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: RunFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.runs.enumerate(table=bool(table), _filter=filter)


@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: RunFilter = Body(RunFilter()),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.runs.enumerate(table=bool(table), _filter=filter)


@router.post("/", response_model=api.Run)
def create(
    run: RunInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.runs.create(**run.dict())


@router.post("/{id}/set-as-default-version/")
def set_as_default_version(
    id: int = Path(None),
    backend: Backend = Depends(deps.get_backend),
):
    backend.runs.set_as_default_version(id)


@router.post("/{id}/unset-as-default-version/")
def unset_as_default_version(
    id: int = Path(None),
    backend: Backend = Depends(deps.get_backend),
):
    backend.runs.unset_as_default_version(id)
