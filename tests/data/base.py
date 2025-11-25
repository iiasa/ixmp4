from typing import Generic, NoReturn, TypeVar

import pytest

from ixmp4.transport import DirectTransport, HttpxTransport, Transport

ServiceT = TypeVar("ServiceT")


def is_pgsql(t: DirectTransport) -> bool:
    return t.session.bind.dialect.name == "postgresql"


class ServiceTest(Generic[ServiceT]):
    service_class: ServiceT

    @pytest.fixture(scope="class")
    def service(self, transport: Transport) -> ServiceT:
        return self.service_class(transport)

    @pytest.fixture(scope="class")
    def versioning_service(self, transport: Transport) -> ServiceT:
        direct = self.get_direct_or_skip(transport)
        if is_pgsql(direct):
            return self.service_class(direct)
        else:
            self.skip_transport(transport, "does not support versioning")

    @classmethod
    def get_direct_or_skip(cls, transport: Transport):
        if isinstance(transport, DirectTransport):
            return transport
        elif isinstance(transport, HttpxTransport) and transport.direct is not None:
            return transport.direct
        else:
            cls.skip_transport(transport, "does not provide a direct transport class")

    @classmethod
    def skip_transport(cls, transport: Transport, reason: str) -> NoReturn:
        pytest.skip(f"Transport `{transport}` {reason}.")
