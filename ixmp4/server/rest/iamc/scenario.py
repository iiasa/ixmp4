from fastapi import APIRouter, Body, Depends, Query
from pydantic import RootModel

from ixmp4.data import api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.scenario.filter import IamcScenarioFilter

from .. import deps
from ..base import BaseModel

router: APIRouter = APIRouter(
    prefix="/scenarios",
    tags=["iamc", "scenarios"],
)


class EnumerationOutput(BaseModel, RootModel):
    root: list[api.Scenario] | api.DataFrame


@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: IamcScenarioFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.scenarios.enumerate(table=bool(table), _filter=filter)


@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: IamcScenarioFilter = Body(IamcScenarioFilter(id=None, name=None)),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.scenarios.enumerate(table=bool(table), _filter=filter)
