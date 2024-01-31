from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.region.filter import IamcRegionFilter

from .. import deps
from ..base import EnumerationOutput, Pagination

router: APIRouter = APIRouter(
    prefix="/regions",
    tags=["iamc", "regions"],
)


@router.patch("/", response_model=EnumerationOutput[api.Region])
def query(
    filter: IamcRegionFilter = Body(IamcRegionFilter()),
    table: bool | None = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.regions.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.regions.count(_filter=filter),
        pagination=pagination,
    )
