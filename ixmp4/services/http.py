import functools
import inspect
from concurrent import futures
from string import Formatter
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Literal,
    NotRequired,
    ParamSpec,
    Sequence,
    TypedDict,
    TypeVar,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

import httpx
import pandas as pd
import pydantic as pyd
from litestar import HttpMethod, Request, Response
from litestar.handlers import HTTPRouteHandler
from litestar.routes import BaseRoute
from litestar.types.internal_types import PathParameterDefinition
from litestar.utils.path import join_paths
from typing_extensions import Unpack

from ixmp4.core.exceptions import InvalidArguments, ProgrammingError
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.transport import HttpxTransport

if TYPE_CHECKING:
    from .base import Service
    from .procedure import ServiceProcedure

ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")
ServiceT = TypeVar("ServiceT", bound="Service")

HttpMethodLiteral = Literal[
    "GET", "POST", "DELETE", "PATCH", "PUT", "HEAD", "TRACE", "OPTIONS"
]


class HttpConfig(TypedDict):
    name: NotRequired[str]
    path: NotRequired[str]
    methods: NotRequired[Sequence[HttpMethod] | Sequence[HttpMethodLiteral]]


class ServiceProcedureClient(Generic[ServiceT, Params, ReturnT]):
    service: ServiceT
    endpoint: "HttpProcedureEndpoint[ServiceT, Params, ReturnT]"
    route_handler: HTTPRouteHandler
    transport: HttpxTransport
    path: str
    method: str

    def __init__(
        self,
        endpoint: "HttpProcedureEndpoint[ServiceT, Params, ReturnT]",
        service: ServiceT,
    ):
        self.endpoint = endpoint
        self.service = service
        self.method = str(endpoint.methods[0])
        self.route_handler = self.service.get_procedure_route(self.endpoint)

        if not isinstance(service.transport, HttpxTransport):
            raise ProgrammingError(
                f"Cannot instantiate http client for transport: {service.transport}"
            )

        self.transport = service.transport

    def reverse_path(self, route: BaseRoute, path_parameters: dict[str, Any]) -> str:
        output = []
        for component in route.path_components:
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

    def __call__(self, *args: Params.args, **kwargs: Params.kwargs) -> ReturnT:
        path_params, payload = self.classify_arguments(*args, **kwargs)
        route = self.endpoint.routes[0]
        path = self.reverse_path(route, path_params)

        json = None
        params = None

        if self.endpoint.supports_body:
            json = payload
            params = None
        else:
            json = None
            params = payload

        res = self.transport.http_client.request(
            self.method, path, json=json, params=params
        )
        self.transport.raise_service_exception(res)
        if self.endpoint.procedure.has_paginated_func:
            return self.handle_paginated_response(res, path, params=params, json=json)
        else:
            return self.validate_return_dto(res.text)

    def pos_args_to_named(self, args: tuple[Any]) -> dict[str, Any]:
        arg_names = [
            name
            for name, param in self.endpoint.procedure.signature.parameters.items()
            if param.kind in [param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD]
        ]
        arg_names = arg_names[: len(args)]
        return {name: val for name, val in zip(arg_names, args)}

    def classify_arguments(
        self, *args: Params.args, **kwargs: Params.kwargs
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        named_pos_args = self.pos_args_to_named(cast(tuple[Any], args))
        all_args = {**named_pos_args, **kwargs}
        path_args = {
            k: v for k, v in all_args.items() if k in self.endpoint.path_fields
        }
        payload_args = {
            k: v for k, v in all_args.items() if k not in self.endpoint.path_fields
        }
        try:
            path_obj = self.endpoint.path_model.model_validate(path_args, strict=True)
            payload_obj = self.endpoint.payload_model.model_validate(
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
        result = self.validate_paginated_return_dto(response.text)
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
                self.transport.http_client.request,
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
            result = self.validate_paginated_return_dto(res.text)
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

    def validate_return_dto(self, data: str) -> ReturnT:
        return self.endpoint.return_type_adapter.validate_json(data)

    def validate_paginated_return_dto(self, data: str) -> PaginatedResult[Any]:
        return self.endpoint.paginated_return_type_adapter.validate_json(data)


class HttpProcedureEndpoint(Generic[ServiceT, Params, ReturnT]):
    procedure: "ServiceProcedure[ServiceT, Params, ReturnT]"
    shortname: str
    name: str
    path: str
    methods: Sequence[HttpMethod] | Sequence[HttpMethodLiteral]
    supports_body: bool
    description: str | None
    service_class: "type[Service]"
    routes: "list[BaseRoute]"

    path_fields: list[str]
    path_model: type[pyd.BaseModel]
    payload_model: type[pyd.BaseModel]
    return_type_adapter: pyd.TypeAdapter[ReturnT]
    paginated_return_type_adapter: pyd.TypeAdapter[PaginatedResult[Any]]

    def __init__(
        self,
        procedure: "ServiceProcedure[ServiceT, Params, ReturnT]",
        service_class: "type[Service]",
        http_config: HttpConfig,
    ):
        self.procedure = procedure
        self.service_class = service_class

        self.shortname = http_config.get("name", procedure.func.__name__)
        self.name = ".".join(
            [service_class.__module__, service_class.__name__, self.shortname]
        )

        kebab_func_name = procedure.func.__name__.replace("_", "-")
        self.path = http_config.get("path", "/" + kebab_func_name)

        self.methods = http_config.get("methods", [HttpMethod.POST])
        if len(self.methods) == 0:
            self.methods = [HttpMethod.POST]

        self.description = procedure.func.__doc__

        self.path_fields = self.get_path_fields(self.path)
        self.supports_body = not ("GET" in self.methods or "HEAD" in self.methods)

        self.path_model = self.build_path_model(self.path_fields)
        self.payload_model = self.build_payload_model(self.path_fields)

        self.return_type_adapter = pyd.TypeAdapter(
            self.procedure.signature.return_annotation
        )

        if self.procedure.has_paginated_func:
            self.paginated_return_type_adapter = pyd.TypeAdapter(
                self.procedure.paginated_signature.return_annotation
            )

    def get_path_fields(self, path: str) -> list[str]:
        return [name for (_, name, *_) in Formatter().parse(path) if name is not None]

    def build_payload_model(self, path_fields: list[str]) -> type[pyd.BaseModel]:
        def callback(index: int, name: str, param: Any) -> Literal["skip"] | None:
            if index == 0 and name == "self":
                return "skip"
            if name in path_fields:
                return "skip"
            return None

        return generate_arguments_model(
            self.procedure.signature,
            self.get_model_name("Params"),
            __module__=self.procedure.func.__module__,
            parameter_callback=callback,
        )

    def build_path_model(self, path_fields: list[str]) -> type[pyd.BaseModel]:
        def callback(index: int, name: str, param: Any) -> Literal["skip"] | None:
            if index == 0 and name == "self":
                return "skip"
            if name not in path_fields:
                return "skip"
            return None

        return generate_arguments_model(
            self.procedure.signature,
            self.get_model_name("PathParams"),
            __module__=self.procedure.func.__module__,
            parameter_callback=callback,
        )

    def get_model_name(self, suffix: str) -> str:
        func_name = self.procedure.func.__name__
        return func_name.title().replace("_", "") + suffix

    def get_pagination_params(self, query_params: dict[str, Any]) -> Pagination:
        pagination = Pagination.model_validate(query_params, extra="ignore")
        return pagination

    def wrap_return_serializer(
        self, func: Callable[Params, Any], type_adapter: pyd.TypeAdapter[Any]
    ) -> Callable[Params, bytes]:
        @functools.wraps(func)
        def wrapper(*args: Params.args, **kwargs: Params.kwargs) -> bytes:
            result = func(*args, **kwargs)
            return type_adapter.dump_json(result)

        return wrapper

    def bind_endpoint_func(
        self, service: ServiceT, query_params: dict[str, Any]
    ) -> Callable[Params, Any]:
        bound_func: Callable[Params, Any]
        if self.procedure.has_paginated_func:
            pagination = self.get_pagination_params(query_params)
            bound_func = functools.partial(
                self.procedure.paginated_func, service, pagination
            )
            bound_func = self.wrap_return_serializer(
                bound_func, self.paginated_return_type_adapter
            )
        else:
            bound_func = functools.partial(self.procedure.func, service)
            bound_func = self.wrap_return_serializer(
                bound_func, self.return_type_adapter
            )

        auth_func = self.procedure.maybe_add_auth_check(service, bound_func)
        return auth_func

    async def handle_request(
        self,
        request: Request[Any, Any, Any],
        service: Any,
        query: dict[str, Any],
        body: bytes,
    ) -> Response[Any]:
        bound_func = self.bind_endpoint_func(cast(ServiceT, service), query)
        args, kwargs = self.build_call_args(request.path_params, query, body)
        result = bound_func(*args, **kwargs)
        return Response(result, media_type="application/json")

    def build_call_args(
        self,
        path: dict[str, Any],
        query: dict[str, Any],
        body: bytes,
        varargs_key: str = "__varargs__",
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        try:
            path_params = self.path_model.model_validate(path)
            if self.supports_body:
                payload = self.payload_model.model_validate_json(body)
            else:
                payload = self.payload_model.model_validate(query, extra="ignore")

        except pyd.ValidationError as e:
            raise InvalidArguments(validation_error=e)

        payload_dict = payload.model_dump(mode="python", exclude_unset=True)
        varargs = payload_dict.pop(varargs_key, [])

        bound_params = self.procedure.signature.bind(
            *varargs, **path_params.model_dump(mode="python"), **payload_dict
        )
        return bound_params.args, bound_params.kwargs


# this function tries to remain similar to
# pydantic.experimental.generate_arguments_schema
def generate_arguments_model(
    signature: inspect.Signature,
    model_name: str,
    __module__: str,
    parameter_callback: Callable[[int, str, inspect.Parameter], Literal["skip"] | None],
    varargs_key: str = "__varargs__",
) -> type[pyd.BaseModel]:
    fields: dict[str, Any] = {}
    model_config = pyd.ConfigDict(arbitrary_types_allowed=True)

    for index, (name, param) in enumerate(signature.parameters.items()):
        if parameter_callback(index, name, param) == "skip":
            continue

        if param.kind == inspect.Parameter.POSITIONAL_ONLY:
            raise ProgrammingError(
                "`generate_arguments_model` does not support positional-only arguments."
            )

        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            elem_type: type[object] = param.annotation
            # equivaltent to list[type] but works dynamically
            fields[varargs_key] = (list.__class_getitem__(elem_type), [])

        elif param.kind == inspect.Parameter.VAR_KEYWORD:
            origin = get_origin(param.annotation)
            if origin is Unpack:
                # we received a function with **kwargs: Unpack[TypedDict]
                # update the payload model to contain all items from the TypedDict
                (td,) = get_args(param.annotation)
                annots = get_type_hints(td)

                # total=False makes everything optional
                if not td.__total__:
                    for key, type_ in annots.items():
                        annots[key] = (type_ | None, None)

                fields.update(annots)
            else:
                # we received a function with **kwargs: <T>, so anything goes.
                model_config["extra"] = "allow"
        else:
            if param.default == inspect.Parameter.empty:
                fields[name] = param.annotation
            else:
                fields[name] = (param.annotation, param.default)

    return pyd.create_model(
        model_name,
        __module__=__module__,
        __config__=model_config,
        **fields,
    )
