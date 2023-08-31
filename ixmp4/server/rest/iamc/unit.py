from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.unit.filter import IamcUnitFilter, SimpleIamcUnitFilter

from .. import deps
from ..base import BaseModel

router: APIRouter = APIRouter(
    prefix="/units",
    tags=["iamc", "units"],
)


class EnumerationOutput(BaseModel):
    __root__: list[api.Unit] | api.DataFrame


@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: SimpleIamcUnitFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.units.enumerate(table=bool(table), _filter=filter)


@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: IamcUnitFilter = Body(IamcUnitFilter()),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.units.enumerate(table=bool(table), _filter=filter)
