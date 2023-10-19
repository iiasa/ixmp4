from fastapi import APIRouter, Body, Depends, Query
from pydantic import RootModel

from ixmp4.data import api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.model.filter import IamcModelFilter

from .. import deps
from ..base import BaseModel

router: APIRouter = APIRouter(
    prefix="/models",
    tags=["iamc", "models"],
)


class EnumerationOutput(BaseModel, RootModel):
    root: list[api.Model] | api.DataFrame


@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: IamcModelFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.models.enumerate(table=bool(table), _filter=filter)


@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: IamcModelFilter = Body(IamcModelFilter(id=None, name=None)),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.models.enumerate(table=bool(table), _filter=filter)
