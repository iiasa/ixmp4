from fastapi import APIRouter, Body, Depends, Path, Query
from pydantic import RootModel

from ixmp4.data import abstract, api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.meta.filter import RunMetaEntryFilter

from . import deps
from .base import BaseModel

router: APIRouter = APIRouter(
    prefix="/meta",
    tags=["meta"],
)


class RunMetaEntryInput(BaseModel):
    run__id: int
    key: str
    value: abstract.StrictMetaValue


class EnumerationOutput(BaseModel, RootModel):
    root: api.DataFrame | list[api.RunMetaEntry]


@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: RunMetaEntryFilter = Depends(),
    table: bool = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.meta.enumerate(
        _filter=filter,
        table=bool(table),
    )


@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: RunMetaEntryFilter = Body(None),
    table: bool = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.meta.enumerate(_filter=filter, table=bool(table))


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
