import inspect
import json
import re
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, TypedDict, cast
from unittest import mock

import pytest
from litestar.routes import HTTPRoute
from typer.testing import CliRunner

from ixmp4.cli import app
from ixmp4.conf.settings import Settings
from ixmp4.data.docs.controller import DocsCompatibilityController
from ixmp4.data.services import Service
from ixmp4.server.v1 import v1_services

OpenAPIOperation = dict[str, Any]
OpenAPIPathItem = dict[str, OpenAPIOperation]
OpenAPIPaths = dict[str, OpenAPIPathItem]


class OpenAPISchema(TypedDict):
    paths: OpenAPIPaths


@dataclass(frozen=True)
class CollectedEndpoint:
    source: str
    method: str
    schema_path: str
    relative_path: str
    summary: str
    operation_id: str | None
    signature: inspect.Signature | None


def _normalize_openapi_path(path: str) -> str:
    normalized = re.sub(r":[^}/]+", "", path).rstrip("/")
    return normalized or "/"


def _join_relative_path(*parts: str) -> str:
    joined = "/".join(part.strip("/") for part in parts if part)
    return joined


def _get_service_signature(
    service_class: type[Service], summary: str
) -> inspect.Signature | None:
    descriptor = getattr(service_class, summary, None)
    procedure = getattr(descriptor, "procedure", None)
    if procedure is None:
        return None
    return inspect.signature(procedure.func)


def collect_service_endpoints() -> list[CollectedEndpoint]:
    endpoints: list[CollectedEndpoint] = []
    settings = Settings().server

    for service_class in v1_services:
        router = service_class.get_router(settings)
        for route in router.routes:
            if not isinstance(route, HTTPRoute):
                continue
            for handler in route.route_handlers:
                methods = sorted(str(method) for method in handler.http_methods)
                if methods == ["OPTIONS"]:
                    continue

                method = methods[0]
                route_path = next(iter(handler.paths))
                relative_path = _normalize_openapi_path(
                    "/" + _join_relative_path(service_class.router_prefix, route_path)
                )
                schema_path = _normalize_openapi_path(
                    "/v1/{platform_name}" + relative_path
                )
                summary = getattr(handler, "summary", "") or ""
                signature = _get_service_signature(service_class, summary)
                source = "compatibility" if summary == "query" else "service"

                endpoints.append(
                    CollectedEndpoint(
                        source=source,
                        method=method.lower(),
                        schema_path=schema_path,
                        relative_path=relative_path,
                        summary=summary,
                        operation_id=getattr(handler, "operation_id", None),
                        signature=signature,
                    )
                )

    return endpoints


def collect_controller_endpoints() -> list[CollectedEndpoint]:
    endpoints: list[CollectedEndpoint] = []
    for name, handler in DocsCompatibilityController.__dict__.items():
        if not hasattr(handler, "paths"):
            continue

        methods = sorted(str(method) for method in handler.http_methods)
        if methods == ["OPTIONS"]:
            continue

        method = methods[0]
        relative_path = _normalize_openapi_path(
            "/"
            + _join_relative_path(
                DocsCompatibilityController.path, next(iter(handler.paths))
            )
        )
        schema_path = _normalize_openapi_path("/v1/{platform_name}" + relative_path)
        target = handler.fn if hasattr(handler, "fn") else handler
        endpoints.append(
            CollectedEndpoint(
                source="docs-controller",
                method=method.lower(),
                schema_path=schema_path,
                relative_path=relative_path,
                summary=name,
                operation_id=None,
                signature=inspect.signature(target),
            )
        )

    return endpoints


def _build_settings(storage_directory: Path) -> Settings:
    return Settings(storage_directory=storage_directory)


@pytest.fixture(scope="session")
def openapi_schema() -> OpenAPISchema:
    with TemporaryDirectory() as temp_dir:
        settings = _build_settings(Path(temp_dir))
        with mock.patch("ixmp4.conf.settings.Settings", new=settings):
            runner = CliRunner(
                env={"IXMP4_STORAGE_DIRECTORY": str(settings.storage_directory)}
            )
            result = runner.invoke(app, ["server", "dump-schema"])
        assert result.exit_code == 0, result.stdout
        return cast(OpenAPISchema, json.loads(result.stdout))


def test_openapi_schema_contains_all_service_endpoints(
    openapi_schema: OpenAPISchema,
) -> None:
    schema_paths = openapi_schema["paths"]
    for endpoint in collect_service_endpoints():
        path_item = schema_paths[endpoint.schema_path]
        operation = path_item[endpoint.method]

        assert operation["summary"] == endpoint.summary
        if endpoint.operation_id is not None:
            assert operation["operationId"] == endpoint.operation_id
        assert endpoint.signature is None or isinstance(
            endpoint.signature, inspect.Signature
        )


def test_openapi_schema_contains_controller_endpoints(
    openapi_schema: OpenAPISchema,
) -> None:
    schema_paths = openapi_schema["paths"]
    for endpoint in collect_controller_endpoints():
        operation = schema_paths[endpoint.schema_path][endpoint.method]
        assert operation["deprecated"] is True
        assert endpoint.signature is not None


def test_openapi_schema_matches_direct_api_route_count(
    openapi_schema: OpenAPISchema,
) -> None:
    collected = collect_service_endpoints() + collect_controller_endpoints()
    relevant_schema_routes = {
        (path, method)
        for path, methods in openapi_schema["paths"].items()
        for method in methods
        if path.startswith("/v1/{platform_name}/") and not path.endswith("/status")
    }
    assert relevant_schema_routes >= {
        (endpoint.schema_path, endpoint.method) for endpoint in collected
    }
