from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.model.filter import IamcModelFilter

from .. import deps
from ..base import EnumerationOutput, Pagination

router: APIRouter = APIRouter(
    prefix="/models",
    tags=["iamc", "models"],
)


@router.patch("/", response_model=EnumerationOutput[api.Model])
def query(
    filter: IamcModelFilter = Body(IamcModelFilter()),
    table: bool | None = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.models.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.models.count(_filter=filter),
        pagination=pagination,
    )
