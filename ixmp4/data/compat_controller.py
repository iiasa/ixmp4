from typing import Any, cast

from litestar import Request, patch

from ixmp4.data.pagination import GenericPaginatedResult
from ixmp4.data.services.http import ServiceController


class EnumerationCompatibilityController(ServiceController[Any]):
    path = "/"

    @patch(
        path="/",
        summary="query",
        deprecated=True,
        description=(
            "This endpoint is deprecated, use the 'list' and 'tabulate' "
            "endpoints instead"
        ),
    )
    async def query(
        self,
        service: Any,
        request: Request[Any, Any, Any],
        table: bool = False,
    ) -> GenericPaginatedResult:
        """Compatibility endpoint for a deprecated enumeration method."""
        return cast(
            GenericPaginatedResult,
            await self.call_procedure(
                service, "tabulate" if table else "list", request
            ),
        )
