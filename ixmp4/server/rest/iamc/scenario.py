from fastapi import APIRouter, Depends, Query, Body

from ixmp4.data.backend import Backend
from ixmp4.data.db.scenario.filter import IamcScenarioFilter
from ixmp4.data import api

from ..base import BaseModel
from .. import deps

router: APIRouter = APIRouter(
    prefix="/scenarios",
    tags=["iamc", "scenarios"],
)


class EnumerationOutput(BaseModel):
    __root__: list[api.Scenario] | api.DataFrame


@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: IamcScenarioFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.scenarios.enumerate(table=bool(table), _filter=filter)


@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: IamcScenarioFilter = Body(IamcScenarioFilter()),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.scenarios.enumerate(table=bool(table), _filter=filter)
