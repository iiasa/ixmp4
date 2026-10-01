import json
from typing import Any, cast

from litestar import Request, post

from ixmp4.core.exceptions import BadRequest
from ixmp4.data.compat_controller import EnumerationCompatibilityController

from .dto import Run


class RunCompatibilityController(EnumerationCompatibilityController):
    path = "/"

    def _get_compat_payload(
        self, query_params: dict[str, Any], body: bytes
    ) -> dict[str, Any]:
        payload = super()._get_compat_payload(query_params, body)
        table_arg = query_params.get("table", "false").lower()
        table_bool = table_arg == "true"
        if table_bool:
            payload.setdefault("include_internal_columns", True)
        return payload

    def _split_id_from_body(self, body: bytes) -> tuple[Any, bytes]:
        """Split a legacy payload into its ``id`` and the remaining arguments.

        Procedures taking ``id`` as first argument read it from the URL path.
        Clients of ixmp4 <=0.16.0 sent it in the request body instead, so the
        value has to be moved out of the payload before dispatch.

        Parameters
        ----------
        body:
            The raw request body.

        Returns
        -------
        tuple:
            The run id and the JSON-encoded remaining arguments.

        Raises
        ------
        :class:`BadRequest`:
            If the body is not a JSON object or does not contain an ``id``.
        """
        if len(body) > 0:
            try:
                parsed_body = json.loads(body)
            except json.JSONDecodeError as exc:
                raise BadRequest("Request body is not valid JSON.") from exc
        else:
            parsed_body = {}

        if not isinstance(parsed_body, dict):
            raise BadRequest("Request body must be a JSON object.")

        payload = dict(parsed_body)
        if "id" not in payload:
            raise BadRequest("Missing required argument `id`.")

        id = payload.pop("id")
        return id, json.dumps(payload).encode("utf-8")

    def call_procedure_with_id(
        self,
        service: Any,
        name: str,
        request: Request[Any, Any, Any],
        body: bytes,
    ) -> Any:
        """Call a procedure that expects the run ``id`` as path parameter.

        Parameters
        ----------
        service:
            The run service to dispatch to.
        name:
            Name of the procedure to call.
        request:
            The incoming request, used for query parameters.
        body:
            The raw request body, expected to contain the run ``id``.

        Returns
        -------
        Any:
            Whatever the procedure returns.
        """
        handler = self.get_handler(service, name)
        query_params = dict(request.query_params)
        id, payload = self._split_id_from_body(body)

        bound_func = handler.bind_endpoint_func(service, query_params)
        args, kwargs = handler.build_call_args({"id": id}, query_params, payload)
        return bound_func(*args, **kwargs)

    # NOTE: The endpoints below take the run id from the request body instead
    # of the URL path, as served by ixmp4 <=0.16.0
    # TODO: Remove before 1.0.0
    @post(
        "/set-as-default-version",
        deprecated=True,
        sync_to_thread=True,
        status_code=200,
        summary="set_as_default_version",
        description="Use `POST /runs/{id}/set-as-default-version` instead.",
    )
    def set_as_default_version(
        self, service: Any, request: Request[Any, Any, Any], body: bytes
    ) -> None:
        """Compatibility endpoint for `set_as_default_version`."""
        self.call_procedure_with_id(service, "set_as_default_version", request, body)

    @post(
        "/unset-as-default-version",
        deprecated=True,
        sync_to_thread=True,
        status_code=200,
        summary="unset_as_default_version",
        description="Use `POST /runs/{id}/unset-as-default-version` instead.",
    )
    def unset_as_default_version(
        self, service: Any, request: Request[Any, Any, Any], body: bytes
    ) -> None:
        """Compatibility endpoint for `unset_as_default_version`."""
        self.call_procedure_with_id(service, "unset_as_default_version", request, body)

    @post(
        "/revert",
        deprecated=True,
        sync_to_thread=True,
        status_code=200,
        summary="revert",
        description="Use `POST /runs/{id}/revert` instead.",
    )
    def revert(
        self, service: Any, request: Request[Any, Any, Any], body: bytes
    ) -> None:
        """Compatibility endpoint for `revert`."""
        self.call_procedure_with_id(service, "revert", request, body)

    @post(
        "/lock",
        deprecated=True,
        sync_to_thread=True,
        status_code=200,
        summary="lock",
        description="Use `POST /runs/{id}/lock` instead.",
    )
    def lock(self, service: Any, request: Request[Any, Any, Any], body: bytes) -> Run:
        """Compatibility endpoint for `lock`."""
        return cast(Run, self.call_procedure_with_id(service, "lock", request, body))

    @post(
        "/unlock",
        deprecated=True,
        sync_to_thread=True,
        status_code=200,
        summary="unlock",
        description="Use `POST /runs/{id}/unlock` instead.",
    )
    def unlock(self, service: Any, request: Request[Any, Any, Any], body: bytes) -> Run:
        """Compatibility endpoint for `unlock`."""
        return cast(Run, self.call_procedure_with_id(service, "unlock", request, body))
