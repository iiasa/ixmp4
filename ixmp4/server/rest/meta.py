from fastapi import APIRouter, Depends, Path, Query

from ixmp4.data import abstract, api
from ixmp4.data.backend.base import Backend

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


class EnumerationOutput(BaseModel):
    __root__: api.DataFrame | list[api.RunMetaEntry]


@router.get("/", response_model=EnumerationOutput)
def enumerate(
    run_ids: list[int] | None = Query(None),
    keys: list[str] | None = Query(None),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.meta.enumerate(
        table=bool(table),
        run_ids=run_ids,
        keys=keys,
    )


@router.post("/", response_model=api.RunMetaEntry)
def create(
    runmeta: RunMetaEntryInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.meta.create(**runmeta.dict())


@router.delete("/{id}/")
def delete(
    id: int = Path(None),
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
