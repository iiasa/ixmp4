from typing import Any

from fastapi import APIRouter, Body, Depends, Query, Response

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.iamc.timeseries.filter import TimeSeriesFilter

from .. import deps
from ..base import BaseModel, EnumerationOutput, Pagination
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/timeseries",
    tags=["iamc", "timeseries"],
)


class TimeSeriesInput(BaseModel):
    run__id: int
    parameters: dict[str, Any]


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.TimeSeries])
def query(
    join_parameters: bool | None = Query(False),
    filter: TimeSeriesFilter = Body(TimeSeriesFilter()),
    table: bool | None = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.iamc.timeseries.paginate(
            _filter=filter,
            join_parameters=join_parameters,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.iamc.timeseries.count(_filter=filter),
        pagination=pagination,
    )


@router.post("/", response_model=api.Run)
def create(
    timeseries: TimeSeriesInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.iamc.timeseries.create(**timeseries.model_dump())


@router.get("/{id}/", response_model=api.TimeSeries)
def get_by_id(
    id: int,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.iamc.timeseries.get_by_id(id)


@router.post("/bulk/")
def bulk_upsert(
    df: api.DataFrame,
    create_related: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    backend.iamc.timeseries.bulk_upsert(df.to_pandas(), create_related=create_related)
    return Response(status_code=201)
