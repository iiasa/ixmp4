import asyncio
from types import SimpleNamespace

import pytest
from httpx import ConnectError

from ixmp4.core.exceptions import ServiceUnavailable
from ixmp4.server.middleware import AuthenticationMiddleware


class TestAuthenticationMiddleware:
    def test_authenticate_request_raises_service_unavailable_on_manager_connect_error(
        self,
    ) -> None:
        async def app(scope: object, receive: object, send: object) -> None:
            return None

        middleware = AuthenticationMiddleware(app=app, secret_hs256=None)
        middleware.get_auth_context = lambda token, manager_client: (  # type: ignore[method-assign]
            _ for _ in ()
        ).throw(ConnectError("manager unavailable"))

        connection = SimpleNamespace(
            headers={},
            app=SimpleNamespace(state=SimpleNamespace(manager_client=object())),
        )

        async def _run() -> None:
            with pytest.raises(ServiceUnavailable, match="Could not reach manager"):
                await middleware.authenticate_request(connection)  # type: ignore[arg-type]

        asyncio.run(_run())
