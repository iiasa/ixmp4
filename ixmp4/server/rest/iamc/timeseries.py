from typing import Any

from fastapi import APIRouter, Depends, Query, Request, Response

from ixmp4.data import api
from ixmp4.data.backend.base import Backend

from .. import deps
from ..base import BaseModel

router: APIRouter = APIRouter(
    prefix="/timeseries",
    tags=["iamc", "timeseries"],
)


class TimeSeriesInput(BaseModel):
    run__id: int
    parameters: dict[str, Any]


class EnumerationOutput(BaseModel):
    __root__: list[api.TimeSeries] | api.DataFrame


@router.get("/", response_model=EnumerationOutput)
def enumerate(
    request: Request,
    join_parameters: bool | None = Query(False),
    run_ids: list[int] = Query(None),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    q = request.query_params
    parameters = {
        k: q[k] for k in q if k not in ["table", "run_ids", "join_parameters"]
    }
    kwargs = dict(
        run_ids=run_ids, join_parameters=join_parameters, parameters=parameters
    )
    return backend.iamc.timeseries.enumerate(table=bool(table), **kwargs)


@router.post("/", response_model=api.Run)
def create(
    timeseries: TimeSeriesInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.iamc.timeseries.create(**timeseries.dict())


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
