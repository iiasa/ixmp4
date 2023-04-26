from fastapi import APIRouter, Depends, Query, Body

from ixmp4.data.backend import Backend
from ixmp4.data.db.model.filter import IamcModelFilter
from ixmp4.data import api

from ..base import BaseModel
from .. import deps

router: APIRouter = APIRouter(
    prefix="/models",
    tags=["iamc", "models"],
)


class EnumerationOutput(BaseModel):
    __root__: list[api.Model] | api.DataFrame


@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: IamcModelFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.models.enumerate(table=bool(table), _filter=filter)


@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: IamcModelFilter = Body(IamcModelFilter()),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.models.enumerate(table=bool(table), _filter=filter)
