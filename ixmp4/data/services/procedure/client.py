from concurrent import futures
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar, cast

import httpx
import pandas as pd
import pydantic as pyd
from litestar.types.internal_types import PathParameterDefinition
from litestar.utils.path import join_paths

from ixmp4.base_exceptions import ProgrammingError
from ixmp4.core.exceptions import InvalidArguments
from ixmp4.data.dataframe import parse_df, serialize_df
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
    the service endpoint, validates arguments, handles paginated
    responses by dispatching concurrent requests when needed, and splits
    chunked write payloads into client-side chunks.
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

        if self.handler.procedure.chunking.has_chunking:
            return self.handle_chunked_request(path, params=params, json=json)

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

    def handle_chunked_request(
        self,
        path: str,
        params: dict[str, Any] | None,
        json: dict[str, Any] | None,
    ) -> ReturnT:
        """Splits a write payload into client-side chunks and dispatches them.

        The payload argument recorded by the procedure's chunking
        descriptor is split into chunks of ``default_upload_chunk_size``
        rows. Each chunk is sent as an independent request to the same
        endpoint and the responses are merged.
        """
        chunking = self.handler.procedure.chunking
        chunk_arg = chunking.chunk_arg_name
        if chunk_arg is None:
            raise ProgrammingError(
                f"Procedure `{self.handler.procedure.func.__name__}` is marked as "
                "chunked but does not declare a chunkable argument."
            )

        chunk_size = self.transport.settings.default_upload_chunk_size

        if json is not None:
            chunks = self.chunk_payload_value(json[chunk_arg], chunk_size)
        else:
            if params is None:
                raise ProgrammingError(
                    "Chunked procedures require either a request body or "
                    "query parameters."
                )
            chunks = self.chunk_payload_value(params[chunk_arg], chunk_size)

        if len(chunks) == 0:
            return cast(ReturnT, None)

        results = self.dispatch_chunked_requests(
            path, chunk_arg, chunks, params=params, json=json
        )
        return self.merge_chunked_results(results)

    def chunk_payload_value(self, value: Any, chunk_size: int) -> list[Any]:
        """Splits *value* into a list of serialized chunks.

        Supports :class:`pandas.DataFrame` (and its serialized dict form)
        as well as plain lists. Other payload types cannot be chunked.
        """
        if isinstance(value, pd.DataFrame):
            df = value
        elif isinstance(value, list):
            return [value[i : i + chunk_size] for i in range(0, len(value), chunk_size)]
        elif self.is_serialized_dataframe(value):
            df = parse_df(dict(value))
        else:
            raise ProgrammingError(
                f"Unable to chunk payload of type `{type(value)}`; expected a "
                "`pandas.DataFrame` or `list`."
            )

        return [
            serialize_df(df.iloc[i : i + chunk_size])
            for i in range(0, len(df), chunk_size)
        ]

    @staticmethod
    def is_serialized_dataframe(value: Any) -> bool:
        return (
            isinstance(value, dict)
            and "data" in value
            and ("columns" in value or "dtypes" in value or "index" in value)
        )

    def dispatch_chunked_requests(
        self,
        path: str,
        chunk_arg: str,
        chunks: list[Any],
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> list[Any]:
        requests: list[futures.Future[httpx.Response]] = []

        for chunk in chunks:
            req_params = params.copy() if params is not None else None
            req_json = json.copy() if json is not None else None

            if req_json is not None:
                req_json[chunk_arg] = chunk
            else:
                if req_params is None:
                    raise ProgrammingError(
                        "Chunked procedures require either a request body or "
                        "query parameters."
                    )
                req_params[chunk_arg] = chunk

            future: futures.Future[httpx.Response] = self.transport.executor.submit(
                self.transport.request,
                self.method,
                path,
                params=req_params,
                json=req_json,
            )
            requests.append(future)

        executor_results = futures.wait(requests)
        responses = [f.result() for f in executor_results.done]
        results = []

        for res in responses:
            self.transport.raise_service_exception(res)
            results.append(self.handler.return_type_adapter.validate_json(res.text))

        return results

    def merge_chunked_results(self, results: list[Any]) -> ReturnT:
        if len(results) == 0 or all(result is None for result in results):
            return cast(ReturnT, None)

        return self.merge_results(cast(list[ReturnT], results))

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
