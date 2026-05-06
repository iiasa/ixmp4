from collections.abc import Iterable, Mapping
from typing import Any, Generic, TypeVar

import httpx
import pytest

from ixmp4.data.services import Service
from ixmp4.transport import DirectTransport, HttpxTransport, Transport
from tests import backends
from tests.base import TransportTest

ServiceT = TypeVar("ServiceT", bound=Service)

api_transport = backends.get_transport_fixture(
    backends=["rest-sqlite", "rest-postgres"], scope="class"
)


class ApiServiceTest(TransportTest, Generic[ServiceT]):
    service_class: type[ServiceT]

    @pytest.fixture(scope="class")
    def client(self, transport: Transport) -> httpx.Client:
        if isinstance(transport, HttpxTransport):
            return transport.http_client
        self.skip_transport(transport, "does not provide an httpx client")

    @pytest.fixture(scope="class")
    def direct_transport(self, transport: Transport) -> DirectTransport:
        return self.get_direct_or_skip(transport)

    @pytest.fixture(scope="class")
    def direct_service(self, direct_transport: DirectTransport) -> ServiceT:
        return self.service_class(direct_transport)

    def request(
        self,
        client: httpx.Client,
        method: str,
        path: str,
        *,
        json: Any | None = None,
        params: dict[str, Any] | None = None,
        expected_status: int = 200,
    ) -> httpx.Response:
        response = client.request(
            method, path.lstrip("/"), json=json, params=params
        )
        assert response.status_code == expected_status, response.text
        return response


def assert_paginated_list(
    response_json: dict[str, Any], *, expected_count: int
) -> None:
    assert response_json["total"] == expected_count
    assert len(response_json["results"]) == expected_count
    assert set(response_json["pagination"]) == {"limit", "offset"}


def assert_frame_payload(
    frame: Mapping[str, Any], *, expected_columns: Iterable[str]
) -> None:
    assert set(frame) == {"index", "columns", "dtypes", "data"}
    assert set(frame["columns"]) >= set(expected_columns)
