import json
from typing import Any, cast

from litestar import Request, patch

from ixmp4.core.exceptions import BadRequest
from ixmp4.data.pagination import GenericPaginatedResult
from ixmp4.data.services.controller import ServiceController


class EnumerationCompatibilityController(ServiceController[Any]):
    path = "/"

    def _get_compat_payload(self, query_params: dict[str, Any], body: bytes) -> bytes:
        """Merge legacy query filters into a PATCH body payload.

        The deprecated ``query`` endpoint historically accepted procedure
        arguments via query parameters. Procedures now consume JSON request
        bodies for PATCH routes, so we preserve backward compatibility by
        moving non-pagination query arguments into the body.
        """
        payload: dict[str, Any]

        if len(body) > 0:
            parsed_body = json.loads(body)
            payload = parsed_body if isinstance(parsed_body, dict) else {}
        else:
            payload = {}

        reserved = {"table", "limit", "offset"}
        for key, value in query_params.items():
            if key in reserved:
                continue
            payload.setdefault(key, value)

        return json.dumps(payload).encode("utf-8")

    @patch(
        path="/",
        summary="query",
        deprecated=True,
        sync_to_thread=True,
        description=(
            "This endpoint is deprecated, use the 'list' and 'tabulate' "
            "endpoints instead"
        ),
    )
    def query(
        self,
        service: Any,
        request: Request[Any, Any, Any],
        body: bytes,
        table: bool = False,
    ) -> GenericPaginatedResult:
        """Compatibility endpoint for a deprecated enumeration method."""
        try:
            handler = self.get_handler(service, "tabulate" if table else "list")
        except (IndexError, AttributeError):
            raise BadRequest(
                f"Invalid query parameter: `table={table}` "
                "is not supported by this endpoint."
            )
        query_params = dict(request.query_params)

        bound_func = handler.bind_endpoint_func(service, query_params)
        args, kwargs = handler.build_call_args(
            request.path_params,
            query_params,
            self._get_compat_payload(query_params, body),
        )

        return cast(
            GenericPaginatedResult,
            bound_func(*args, **kwargs),
        )
