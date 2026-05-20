from unittest import mock

from toolkit.exceptions import InvalidToken

from ixmp4.core.exceptions import Forbidden, PlatformNotFound
from ixmp4.server import Ixmp4Server


class TestServiceExceptionHandler:
    def test_service_exception_handler_returns_correct_status_and_body(self) -> None:
        """exception is converted to a Response with the right status."""

        exc = PlatformNotFound("test-platform")
        request = mock.Mock()

        response = Ixmp4Server.service_exception_handler(request, exc)

        assert response.status_code == exc.http_status_code

    def test_service_exception_handler_works_for_forbidden(self) -> None:
        """service_exception_handler handles any Ixmp4Error subclass."""

        exc = Forbidden("not allowed", reason="not_tall_enough")
        request = mock.Mock()

        response = Ixmp4Server.service_exception_handler(request, exc)

        assert response.status_code == exc.http_status_code
        assert response.content["data"]["reason"] == "not_tall_enough"

    def test_service_exception_handler_works_for_tk_exceptions(self) -> None:
        """service_exception_handler handles any Ixmp4Error subclass."""

        exc = InvalidToken()
        request = mock.Mock()

        response = Ixmp4Server.service_exception_handler(request, exc)

        assert response.status_code == exc.http_status_code
        assert response.content["name"] == "InvalidToken"
