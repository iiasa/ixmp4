from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.model.filter import ModelFilter

from . import deps
from .base import BaseModel, EnumerationOutput, Pagination
from .decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/models",
    tags=["models"],
)


class ModelInput(BaseModel):
    name: str


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.Model])
def query(
    filter: ModelFilter = Body(None),
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


@autodoc
@router.post("/", response_model=api.Model)
def create(
    model: ModelInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.models.create(**model.model_dump())
