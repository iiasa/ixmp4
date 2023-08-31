from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.region.filter import IamcRegionFilter, SimpleIamcRegionFilter

from .. import deps
from ..base import BaseModel

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
