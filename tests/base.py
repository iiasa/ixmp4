from typing import NoReturn

import pytest

from ixmp4.transport import DirectTransport, HttpxTransport, Transport


class TransportTest(object):
    @classmethod
    def transport_is_pgsql(cls, t: DirectTransport) -> bool:
        return t.session.bind.dialect.name == "postgresql"

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
