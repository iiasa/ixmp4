from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.scenario.filter import ScenarioFilter

from . import deps
from .base import BaseModel, EnumerationOutput, Pagination
from .decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/scenarios",
    tags=["scenarios"],
)


class ScenarioInput(BaseModel):
    name: str


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.Scenario])
def query(
    filter: ScenarioFilter = Body(None),
    table: bool | None = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.scenarios.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.scenarios.count(_filter=filter),
        pagination=pagination,
    )


@autodoc
@router.post("/", response_model=api.Scenario)
def create(
    scenario: ScenarioInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.scenarios.create(**scenario.model_dump())
