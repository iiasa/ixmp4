import logging
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Sequence,
)

from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from toolkit.exceptions import ServiceException
from toolkit.manager.client import ManagerClient

from ixmp4 import data
from ixmp4.core.exceptions import registry
from ixmp4.services.middleware import TransportMiddleware

if TYPE_CHECKING:
    from ixmp4.services import Service
    from ixmp4.transport import DirectTransport

logger = logging.getLogger(__name__)


v1_services: list[type["Service"]] = [
    data.RunMetaEntryService,
    data.ModelService,
    data.RegionService,
    data.RunService,
    data.ScenarioService,
    data.UnitService,
    data.CheckpointService,
    data.iamc.VariableService,
    data.iamc.TimeSeriesService,
    data.iamc.DataPointService,
    data.optimization.EquationService,
    data.optimization.IndexSetService,
    data.optimization.ParameterService,
    data.optimization.ScalarService,
    data.optimization.TableService,
    data.optimization.VariableService,
]


class V1Application(Starlette):
    platform_app: Starlette

    def __init__(
        self,
        secret_hs256: str | None = None,
        toml_file: Path | None = None,
        manager_client: ManagerClient | None = None,
        service_classes: Sequence[type["Service"]] | None = None,
        override_transport: "DirectTransport | None" = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

        if service_classes is None:
            service_classes = v1_services

        self.add_exception_handler(ServiceException, self.service_exception_handler)

        self.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        self.platform_app = Starlette()

        self.platform_app.add_middleware(
            TransportMiddleware,
            secret_hs256,
            toml_file=toml_file,
            manager_client=manager_client,
            override_transport=override_transport,
        )

        for service in service_classes:
            logger.info(f"Mounting {service.__name__}:")
            logger.info(f"   Path: {service.router_prefix}")
            service_app = service.get_v1_app()
            service_app.add_exception_handler(
                ServiceException, self.service_exception_handler
            )
            self.platform_app.mount(service.router_prefix, service_app)
        self.mount("/{platform}", self.platform_app)

    def service_exception_handler(
        self, request: Request, exc: Exception, /
    ) -> JSONResponse:
        assert isinstance(exc, ServiceException)
        exc_dict = registry.exception_to_response_dict(exc)
        logger.info(
            f"Received `{exc.__class__.__name__}` exception, "
            "returning appropriate error response."
        )
        return JSONResponse(
            content=exc_dict,
            status_code=exc.http_status_code,
        )
