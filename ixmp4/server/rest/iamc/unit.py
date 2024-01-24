from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.unit.filter import IamcUnitFilter

from .. import deps
from ..base import EnumerationOutput, Pagination

router: APIRouter = APIRouter(
    prefix="/units",
    tags=["iamc", "units"],
)


@router.patch("/", response_model=EnumerationOutput[api.Unit])
def query(
    filter: IamcUnitFilter = Body(IamcUnitFilter()),
    table: bool | None = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.units.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.units.count(_filter=filter),
        pagination=pagination,
    )
