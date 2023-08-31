from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.scenario.filter import ScenarioFilter

from . import deps
from .base import BaseModel
from .decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/scenarios",
    tags=["scenarios"],
)


class ScenarioInput(BaseModel):
    name: str


class EnumerationOutput(BaseModel):
    __root__: list[api.Scenario] | api.DataFrame


@autodoc
@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: ScenarioFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.scenarios.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: ScenarioFilter = Body(None),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.scenarios.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.post("/", response_model=api.Scenario)
def create(
    scenario: ScenarioInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.scenarios.create(**scenario.dict())
