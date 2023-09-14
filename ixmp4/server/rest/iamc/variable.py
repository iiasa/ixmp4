from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.iamc.variable.filter import VariableFilter

from .. import deps
from ..base import BaseModel
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/variables",
    tags=["iamc", "variables"],
)


class VariableInput(BaseModel):
    name: str


class EnumerationOutput(BaseModel):
    __root__: list[api.Variable] | api.DataFrame


@autodoc
@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: VariableFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.iamc.variables.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: VariableFilter = Body(VariableFilter()),
    table: bool = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.iamc.variables.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.post("/", response_model=api.Variable)
def create(
    variable: VariableInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.iamc.variables.create(**variable.dict())
