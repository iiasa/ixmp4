from typing import Generic, TypeVar

import pytest

from ixmp4.services import Service
from ixmp4.transport import Transport
from tests.base import TransportTest

ServiceT = TypeVar("ServiceT", bound=Service)


class ServiceTest(TransportTest, Generic[ServiceT]):
    service_class: type[ServiceT]

    @pytest.fixture(scope="class")
    def service(self, transport: Transport) -> ServiceT:
        return self.service_class(transport)

    @pytest.fixture(scope="class")
    def versioning_service(self, transport: Transport) -> ServiceT:
        direct = self.get_direct_or_skip(transport)
        if self.transport_is_pgsql(direct):
            return self.service_class(direct)
        else:
            self.skip_transport(transport, "does not support versioning")

    @pytest.fixture(scope="class")
    def unauthorized_service(self, transport: Transport) -> ServiceT:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return self.service_class(direct)
