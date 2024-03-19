from fastapi import APIRouter, Body, Depends, Path, Query

from ixmp4.core.exceptions import BadRequest
from ixmp4.data import abstract, api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.meta.filter import RunMetaEntryFilter

from . import deps
from .base import BaseModel, EnumerationOutput, Pagination

router: APIRouter = APIRouter(
    prefix="/meta",
    tags=["meta"],
)


class RunMetaEntryInput(BaseModel):
    run__id: int
    key: str
    value: abstract.StrictMetaValue


@router.patch("/", response_model=EnumerationOutput[api.RunMetaEntry])
def query(
    filter: RunMetaEntryFilter = Body(None),
    join_run_index: bool | None = Query(False),
    table: bool | None = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    if join_run_index and not table:
        raise BadRequest("`join_run_index` can only be used with `table=true`.")

    return EnumerationOutput(
        results=backend.meta.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
            join_run_index=join_run_index,
        ),
        total=backend.meta.count(_filter=filter),
        pagination=pagination,
    )


@router.post("/", response_model=api.RunMetaEntry)
def create(
    runmeta: RunMetaEntryInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.meta.create(**runmeta.model_dump())


@router.delete("/{id}/")
def delete(
    id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
):
    backend.meta.delete(id)


@router.post("/bulk/")
def bulk_upsert(
    df: api.DataFrame,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.meta.bulk_upsert(df.to_pandas())


@router.patch("/bulk/")
def bulk_delete(
    df: api.DataFrame,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.meta.bulk_delete(df.to_pandas())
