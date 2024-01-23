from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.iamc.variable.filter import VariableFilter

from .. import deps
from ..base import BaseModel, EnumerationOutput, Pagination
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/variables",
    tags=["iamc", "variables"],
)


class VariableInput(BaseModel):
    name: str


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.Variable])
def query(
    filter: VariableFilter = Body(VariableFilter()),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.iamc.variables.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.iamc.variables.count(_filter=filter),
        pagination=pagination,
    )


@autodoc
@router.post("/", response_model=api.Variable)
def create(
    variable: VariableInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.iamc.variables.create(**variable.model_dump())
