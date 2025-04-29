from fastapi import APIRouter, Body, Depends, Path, Query
from pydantic import Field

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.run.filter import RunFilter
from ixmp4.data.db.run.model import Run

from . import deps
from .base import BaseModel, EnumerationOutput, Pagination, TabulateVersionArgs

router: APIRouter = APIRouter(
    prefix="/runs",
    tags=["runs"],
)


class RunInput(BaseModel):
    name_of_model: str = Field(..., alias="model_name")
    scenario_name: str


class CloneInput(BaseModel):
    run_id: int
    name_of_model: str | None = Field(None, alias="model_name")
    scenario_name: str | None = Field(None)
    keep_solution: bool = Field(True)


@router.patch("/", response_model=EnumerationOutput[api.Run])
def query(
    filter: RunFilter = Body(RunFilter()),
    table: bool | None = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[Run]:
    return EnumerationOutput(
        results=backend.runs.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.runs.count(_filter=filter),
        pagination=pagination,
    )


@router.post("/", response_model=api.Run)
def create(
    run: RunInput,
    backend: Backend = Depends(deps.get_backend),
) -> Run:
    return backend.runs.create(**run.model_dump(by_alias=True))


@router.delete("/{id}/")
def delete(
    id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.runs.delete(id)


@router.post("/{id}/set-as-default-version/")
def set_as_default_version(
    id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.runs.set_as_default_version(id)


@router.post("/{id}/unset-as-default-version/")
def unset_as_default_version(
    id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.runs.unset_as_default_version(id)


@router.get("/{id}/", response_model=api.Run)
def get_by_id(
    id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> Run:
    return backend.runs.get_by_id(id)


class RevertInput(BaseModel):
    transaction__id: int


@router.post("/{id}/revert/")
def revert(
    id: int = Path(),
    input: RevertInput = Body(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.runs.revert(id, input.transaction__id)


@router.post("/{id}/lock/", response_model=api.Run)
def lock(
    id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> Run:
    return backend.runs.lock(id)


@router.post("/{id}/unlock/", response_model=api.Run)
def unlock(
    id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> Run:
    return backend.runs.unlock(id)


@router.patch("/versions/", response_model=api.DataFrame)
def tabulate_versions(
    filter: TabulateVersionArgs = Body(TabulateVersionArgs()),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> api.DataFrame:
    return api.DataFrame.model_validate(
        backend.runs.tabulate_versions(
            limit=pagination.limit,
            offset=pagination.offset,
            **filter.model_dump(),
        )
    )


@router.patch("/transactions/", response_model=api.DataFrame)
def tabulate_transactions(
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> api.DataFrame:
    return api.DataFrame.model_validate(
        backend.runs.tabulate_transactions(
            limit=pagination.limit, offset=pagination.offset
        )
    )


@router.post("/clone/", response_model=api.Run)
def clone(
    run: CloneInput,
    backend: Backend = Depends(deps.get_backend),
) -> Run:
    return backend.runs.clone(**run.model_dump(by_alias=True))
