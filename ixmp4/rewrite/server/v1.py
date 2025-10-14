from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ixmp4.rewrite import data
from ixmp4.rewrite.conf import settings
from ixmp4.rewrite.exceptions import ServiceException, registry
from ixmp4.rewrite.services import Service

from .deps import get_direct_toml_transport
from .middleware import RequestSizeLoggerMiddleware, RequestTimeLoggerMiddleware

app = FastAPI(
    servers=[{"url": "/v1", "description": "v1"}],
    redirect_slashes=False,
    docs_url="/docs/",
    redoc_url="/redoc/",
)

if settings.mode == "debug":
    app.add_middleware(RequestSizeLoggerMiddleware)
    app.add_middleware(RequestTimeLoggerMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(ServiceException)
async def http_exception_handler(request: Request, exc: ServiceException):
    exc_dict = registry.exception_to_response_dict(exc)

    return JSONResponse(
        content=jsonable_encoder(exc_dict),
        status_code=exc.http_status_code,
    )


v1_services: list[type[Service]] = [
    data.RunMetaEntryService,
    data.ModelService,
    data.RegionService,
    data.RunService,
    data.ScenarioService,
    data.UnitService,
    data.CheckpointService,
    data.iamc.DataPointService,
    data.iamc.TimeSeriesService,
]

for service in v1_services:
    router = service.build_router(get_direct_toml_transport)
    app.include_router(router)
