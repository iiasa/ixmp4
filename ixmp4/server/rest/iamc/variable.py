from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.iamc.variable.filter import VariableFilter
from ixmp4.data.db.iamc.variable.model import Variable

from .. import deps
from ..base import BaseModel, EnumerationOutput, Pagination, TabulateVersionArgs
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
) -> EnumerationOutput[Variable]:
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
) -> Variable:
    return backend.iamc.variables.create(**variable.model_dump())


@router.patch("/versions/", response_model=api.DataFrame)
def tabulate_versions(
    filter: TabulateVersionArgs = Body(TabulateVersionArgs()),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> api.DataFrame:
    return api.DataFrame.model_validate(
        backend.iamc.variables.tabulate_versions(
            limit=pagination.limit,
            offset=pagination.offset,
            **filter.model_dump(),
        )
    )
