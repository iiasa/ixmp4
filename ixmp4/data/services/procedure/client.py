from concurrent import futures
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar, cast

import httpx
import pandas as pd
import pydantic as pyd
from litestar.types.internal_types import PathParameterDefinition
from litestar.utils.path import join_paths

from ixmp4.base_exceptions import ProgrammingError
from ixmp4.core.exceptions import InvalidArguments
from ixmp4.transport import HttpxTransport

from .endpoint import ProcedureRouteHandler

if TYPE_CHECKING:
    from ..base import Service

ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")
ServiceT = TypeVar("ServiceT", bound="Service")


class ProcedureClient(Generic[ServiceT, Params, ReturnT]):
    """HTTP client adapter for a ProcedureRouteHandler.

    When a procedure is accessed on a service backed by an
    :class:`ixmp4.transport.HttpxTransport`, the descriptor returns an
    instance of :class:`ProcedureClient` which performs HTTP requests to
    the service endpoint, validates arguments, and handles paginated
    responses by dispatching concurrent requests when needed.
    """

    transport: HttpxTransport
    handler: ProcedureRouteHandler[ServiceT, Params, ReturnT]
    method: str

    def __init__(
        self,
        service: ServiceT,
        handler: ProcedureRouteHandler[ServiceT, Params, ReturnT],
    ) -> None:
        self.handler = handler
        self.method = str(list(handler.http_methods)[0])

        if not isinstance(service.transport, HttpxTransport):
            raise ProgrammingError(
                f"Cannot instantiate http client for transport: {service.transport}"
            )

        self.transport = service.transport

    def __call__(self, *args: Params.args, **kwargs: Params.kwargs) -> ReturnT:
        path_params, payload = self.classify_arguments(*args, **kwargs)
        path = self.reverse_path(path_params)

        json = None
        params = None

        if self.handler.supports_body:
            json = payload
            params = None
        else:
            json = None
            params = payload

        res = self.transport.request(self.method, path, json=json, params=params)
        self.transport.raise_service_exception(res)
        if self.handler.procedure.pagination.has_pagination:
            return self.handle_paginated_response(res, path, params=params, json=json)
        else:
            return cast(
                ReturnT, self.handler.return_type_adapter.validate_json(res.text)
            )

    def reverse_path(self, path_parameters: dict[str, Any]) -> str:
        svc_router_prefix = self.handler.service_class.router_prefix
        output = [svc_router_prefix]
        for component in self.handler.proto_route.path_components:
            if isinstance(component, PathParameterDefinition):
                val = path_parameters.get(component.name)
                if not isinstance(val, component.type):
                    raise InvalidArguments(
                        f"Expected value of type `{component.type}` "
                        f"for path parameter '{component.name}', "
                        f"got argument of type `{type(val)}` instead."
                    )
                output.append(str(val))
            else:
                output.append(component)

        return join_paths(output)

    def pos_args_to_named(self, args: tuple[Any]) -> dict[str, Any]:
        arg_names = [
            name
            for name, param in self.handler.procedure.signature.parameters.items()
            if param.kind in [param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD]
        ]
        arg_names = arg_names[: len(args)]
        return {name: val for name, val in zip(arg_names, args)}

    def classify_arguments(
        self, *args: Params.args, **kwargs: Params.kwargs
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        named_pos_args = self.pos_args_to_named(cast(tuple[Any], args))
        all_args = {**named_pos_args, **kwargs}
        path_args = {k: v for k, v in all_args.items() if k in self.handler.path_fields}
        payload_args = {
            k: v for k, v in all_args.items() if k not in self.handler.path_fields
        }
        try:
            path_obj = self.handler.path_model.model_validate(path_args, strict=True)
            payload_obj = self.handler.payload_model.model_validate(
                payload_args, strict=True
            )
        except pyd.ValidationError as e:
            raise InvalidArguments(validation_error=e)

        path_params = path_obj.model_dump(mode="json", exclude_unset=True)
        payload = payload_obj.model_dump(mode="json", exclude_unset=True)
        return path_params, payload

    def handle_paginated_response(
        self,
        response: httpx.Response,
        path: str,
        params: dict[str, Any] | None,
        json: dict[str, Any] | None,
    ) -> ReturnT:
        result = self.handler.return_type_adapter.validate_json(response.text)
        result_items = [result.results]

        if result.total >= (result.pagination.offset + result.pagination.limit):
            # TODO: We could check if the `total` changed
            # since we started the pagination...
            result_items += self.dispatch_pagination_requests(
                path,
                total=result.total,
                start=result.pagination.limit,
                limit=result.pagination.limit,
                params=params,
                json=json,
            )

        return self.merge_results(result_items)

    def dispatch_pagination_requests(
        self,
        path: str,
        total: int,
        start: int,
        limit: int,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> list[ReturnT]:
        requests: list[futures.Future[httpx.Response]] = []

        for req_offset in range(start, total, limit):
            req_params = params.copy() if params is not None else {}

            req_params.update({"limit": limit, "offset": req_offset})
            future: futures.Future[httpx.Response] = self.transport.executor.submit(
                self.transport.request,
                self.method,
                path,
                params=req_params,
                json=json,
            )
            requests.append(future)

        executor_results = futures.wait(requests)
        responses = [f.result() for f in executor_results.done]
        pagination_results = []

        for res in responses:
            self.transport.raise_service_exception(res)
            result = self.handler.return_type_adapter.validate_json(res.text)
            pagination_results.append(result.results)

        return pagination_results

    def merge_results(self, results: list[ReturnT]) -> ReturnT:
        result_type = type(results[0])
        if issubclass(result_type, list):
            return cast(ReturnT, self.merge_lists(cast(list[list[Any]], results)))
        elif issubclass(result_type, pd.DataFrame):
            return cast(
                ReturnT, self.merge_dataframes(cast(list[pd.DataFrame], results))
            )
        else:
            raise ProgrammingError(
                f"Unable to merge paginated results of type `{result_type}`."
            )

    def merge_dataframes(self, results: list[pd.DataFrame]) -> pd.DataFrame:
        return pd.concat(results)

    def merge_lists(self, results: list[list[Any]]) -> list[Any]:
        return [i for page in results for i in page]
