import json
from typing import Any, Union, cast

from litestar import Request, patch

from ixmp4.core.exceptions import BadRequest
from ixmp4.data.pagination import GenericPaginatedResult
from ixmp4.data.services.controller import ServiceController

ExpandType = Union[str, list[str]]

# Filter keys whose values may arrive as plain strings or lists from legacy
# clients but must be forwarded as nested ``name``-keyed filter dicts.
NAMED_ENTITY_FILTER_KEYS: frozenset[str] = frozenset(
    {"region", "variable", "unit", "model", "scenario"}
)


def expand_simple_filter(
    value: "ExpandType | dict[str, ExpandType]",
) -> "dict[str, ExpandType]":
    """Expand a plain string or list filter value into a ``name``-keyed dict.

    Legacy clients send entity filters as bare strings or lists (e.g.
    ``region="Asia"``).  Current filter machinery expects nested dicts (e.g.
    ``region={"name": "Asia"}``).  This helper performs that conversion so
    the compat controller can bridge the two formats.

    Parameters
    ----------
    value:
        The raw filter value from the legacy request.

    Returns
    -------
    dict:
        A filter dict compatible with the current named-entity filter schema.

    Raises
    ------
    NotImplementedError:
        When a list value contains a wildcard (``*``), which is not supported.
    """
    if isinstance(value, str):
        return dict(name__like=value) if "*" in value else dict(name=value)
    elif isinstance(value, list):
        if any(["*" in v for v in value]):
            raise NotImplementedError("Filter by list with wildcard is not implemented")
        return dict(name__in=value)

    return value


class EnumerationCompatibilityController(ServiceController[Any]):
    path = "/"

    def _get_compat_payload(
        self, query_params: dict[str, Any], body: bytes
    ) -> dict[str, Any]:
        """Merge legacy query filters into a PATCH body payload.

        The deprecated ``query`` endpoint historically accepted procedure
        arguments via query parameters. Procedures now consume JSON request
        bodies for PATCH routes, so we preserve backward compatibility by
        moving non-pagination query arguments into the body.

        Named-entity filter values that arrive as plain strings or lists (e.g.
        from ixmp4 ≤ 0.14) are expanded into nested filter dicts so they are
        accepted by the current filter machinery.
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

        for key in NAMED_ENTITY_FILTER_KEYS:
            if key in payload and not isinstance(payload[key], dict):
                if payload[key] is None:
                    del payload[key]
                    continue
                try:
                    payload[key] = expand_simple_filter(payload[key])
                except NotImplementedError as exc:
                    raise BadRequest(str(exc)) from exc
        return payload

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
        payload = self._get_compat_payload(query_params, body)
        payload_str = json.dumps(payload).encode("utf-8")
        args, kwargs = handler.build_call_args(
            request.path_params,
            query_params,
            payload_str,
        )

        return cast(
            GenericPaginatedResult,
            bound_func(*args, **kwargs),
        )
