from fastapi import APIRouter, Body, Depends, Query

from ixmp4.core.exceptions import BadRequest
from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.iamc.datapoint.filter import DataPointFilter

from .. import deps
from ..base import EnumerationOutput, Pagination
from ..decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/datapoints",
    tags=["iamc", "datapoints"],
)


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.DataPoint])
def query(
    filter: DataPointFilter = Body(DataPointFilter()),
    join_parameters: bool | None = Query(False),
    join_runs: bool = Query(False),
    table: bool | None = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    """This endpoint is used to retrieve and optionally filter data.add()

    Filter parameters are provided as keyword arguments.

    The available filters can be found here:
    :class:`ixmp4.data.db.iamc.datapoint.filter.DataPointFilter`.

    Examples
    --------

    Filter data points for a given model, scenario combination, and a number of years:

    ..  code-block:: json

        {
            "model" : {"name": "model 1"},
            "scenario" : {"name": "scenario 1"},
            "year__in" : [2020, 2025]
        }

    Return all data for a given variable:

    .. code-block:: json

        {
            "variable": {"name": "Final Energy"}
        }


    """
    if (join_parameters or join_runs) and not table:
        raise BadRequest(
            "`join_parameters` or `join_run` can only be used with `table=true`."
        )

    return EnumerationOutput(
        results=backend.iamc.datapoints.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
            join_parameters=join_parameters,
            join_runs=join_runs,
        ),
        total=backend.iamc.datapoints.count(
            _filter=filter,
            join_parameters=join_parameters,
            join_runs=join_runs,
        ),
        pagination=pagination,
    )


@autodoc
@router.post("/bulk/")
def bulk_upsert(
    df: api.DataFrame,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.iamc.datapoints.bulk_upsert(df.to_pandas())


@autodoc
@router.patch("/bulk/")
def bulk_delete(
    df: api.DataFrame,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.iamc.datapoints.bulk_delete(df.to_pandas())
