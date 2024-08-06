from datetime import datetime, timezone

from fastapi import Depends, FastAPI, Path, Request
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ixmp4.conf import settings
from ixmp4.core.exceptions import IxmpError

from . import deps, docs, meta, model, region, run, scenario, unit
from .base import BaseModel
from .iamc import datapoint, timeseries
from .iamc import model as iamc_model
from .iamc import region as iamc_region
from .iamc import scenario as iamc_scenario
from .iamc import unit as iamc_unit
from .iamc import variable as iamc_variable
from .middleware import RequestSizeLoggerMiddleware, RequestTimeLoggerMiddleware
from .optimization import equation as optimization_equation
from .optimization import indexset as optimization_indexset
from .optimization import parameter as optimization_parameter
from .optimization import scalar as optimization_scalar
from .optimization import table as optimization_table
from .optimization import variable as optimization_variable

v1 = FastAPI(
    servers=[{"url": "/v1", "description": "v1"}],
    redirect_slashes=False,
    docs_url="/docs/",
    redoc_url="/redoc/",
)

if settings.mode == "debug":
    v1.add_middleware(RequestSizeLoggerMiddleware)
    v1.add_middleware(RequestTimeLoggerMiddleware)

v1.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

v1.include_router(datapoint.router, prefix="/iamc")
v1.include_router(docs.router)
v1.include_router(iamc_model.router, prefix="/iamc")
v1.include_router(iamc_scenario.router, prefix="/iamc")
v1.include_router(iamc_region.router, prefix="/iamc")
v1.include_router(iamc_unit.router, prefix="/iamc")
v1.include_router(iamc_variable.router, prefix="/iamc")
v1.include_router(meta.router)
v1.include_router(model.router)
v1.include_router(optimization_equation.router, prefix="/optimization")
v1.include_router(optimization_indexset.router, prefix="/optimization")
v1.include_router(optimization_parameter.router, prefix="/optimization")
v1.include_router(optimization_scalar.router, prefix="/optimization")
v1.include_router(optimization_table.router, prefix="/optimization")
v1.include_router(optimization_variable.router, prefix="/optimization")
v1.include_router(region.router)
v1.include_router(run.router)
v1.include_router(scenario.router)
v1.include_router(timeseries.router, prefix="/iamc")
v1.include_router(unit.router)


class APIInfo(BaseModel):
    name: str
    version: str
    is_managed: bool
    manager_url: None | str
    utcnow: datetime


@v1.get("/", response_model=APIInfo)
def root(
    platform: str = Path(),
    version: str = Depends(deps.get_version),
):
    return APIInfo(
        name=platform,
        version=version,
        is_managed=settings.managed,
        manager_url=str(settings.manager_url),
        utcnow=datetime.now(tz=timezone.utc),
    )


@v1.exception_handler(IxmpError)
async def http_exception_handler(request: Request, exc: IxmpError):
    return JSONResponse(
        content=jsonable_encoder(
            {
                "message": exc._message,
                "kwargs": exc.kwargs,
                "error_name": exc.http_error_name,
            }
        ),
        status_code=exc.http_status_code,
    )
