from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ixmp4.conf import settings
from ixmp4.rewrite import data
from ixmp4.rewrite.services import Service

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
    router = service.build_router()
