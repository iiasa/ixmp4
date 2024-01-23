from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.scenario.filter import IamcScenarioFilter

from .. import deps
from ..base import EnumerationOutput, Pagination

router: APIRouter = APIRouter(
    prefix="/scenarios",
    tags=["iamc", "scenarios"],
)


@router.patch("/", response_model=EnumerationOutput[api.Scenario])
def query(
    filter: IamcScenarioFilter = Body(IamcScenarioFilter()),
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
