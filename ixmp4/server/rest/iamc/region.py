from fastapi import APIRouter, Depends, Query, Body

from ixmp4.data.backend import Backend
from ixmp4.data.db.region.filter import SimpleIamcRegionFilter, IamcRegionFilter
from ixmp4.data import api

from ..base import BaseModel
from .. import deps

router: APIRouter = APIRouter(
    prefix="/regions",
    tags=["iamc", "regions"],
)


class EnumerationOutput(BaseModel):
    __root__: list[api.Region] | api.DataFrame


@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: SimpleIamcRegionFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.regions.enumerate(table=bool(table), _filter=filter)


@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: IamcRegionFilter = Body(IamcRegionFilter()),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.regions.enumerate(table=bool(table), _filter=filter)
