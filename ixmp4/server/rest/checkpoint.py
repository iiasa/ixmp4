from fastapi import APIRouter, Body, Depends, Path, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.checkpoint import Checkpoint, CheckpointFilter

from . import deps
from .base import BaseModel, EnumerationOutput, Pagination
from .decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/checkpoints",
    tags=["checkpoints"],
)


class CheckpointInput(BaseModel):
    run__id: int
    message: str


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.Checkpoint])
def query(
    filter: CheckpointFilter = Body(None),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[Checkpoint]:
    return EnumerationOutput(
        results=backend.checkpoints.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.units.count(_filter=filter),
        pagination=pagination,
    )


@autodoc
@router.post("/", response_model=api.Checkpoint)
def create(
    checkpoint: CheckpointInput = Body(),
    backend: Backend = Depends(deps.get_backend),
) -> Checkpoint:
    return backend.checkpoints.create(checkpoint.run__id, checkpoint.message)


@autodoc
@router.delete("/{id}/")
def delete(
    id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.checkpoints.delete(id)
