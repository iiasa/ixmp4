import functools
import inspect
from concurrent import futures
from json import JSONDecodeError
from string import Formatter
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Literal,
    NotRequired,
    ParamSpec,
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
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
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


class HttpConfig(TypedDict):
    name: NotRequired[str]
    path: NotRequired[str]
    methods: NotRequired[list[str]]


class ServiceProcedureClient(Generic[ServiceT, Params, ReturnT]):
    service: ServiceT
    endpoint: "HttpProcedureEndpoint[ServiceT, Params, ReturnT]"
    route: Route
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
        self.method = endpoint.methods[0]

        self.route = endpoint.get_route()
        if not isinstance(service.transport, HttpxTransport):
            raise ProgrammingError(
                f"Cannot instantiate http client for transport: {service.transport}"
            )

        self.transport = service.transport

    def __call__(self, *args: Params.args, **kwargs: Params.kwargs) -> ReturnT:
        path_params, payload = self.classify_arguments(*args, **kwargs)
        path = str(self.route.url_path_for(self.route.name, **path_params))
        path = self.service.router_prefix + path
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
            return self.endpoint.result_adapter.validate_python(res.json())

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
            raise InvalidArguments(e)

        path_params = path_obj.model_dump(mode="json", exclude_unset=True)
        payload = payload_obj.model_dump(mode="json", exclude_unset=True)
        return path_params, payload

    def validate_result(self, value: Any) -> ReturnT:
        return self.endpoint.result_adapter.validate_python(value)

    def handle_paginated_response(
        self,
        response: httpx.Response,
        path: str,
        params: dict[str, Any] | None,
        json: dict[str, Any] | None,
    ) -> ReturnT:
        json = response.json()
        result: PaginatedResult[ReturnT] = PaginatedResult.model_validate(json)
        result_items = [self.validate_result(result.results)]

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
            page: PaginatedResult[ReturnT] = PaginatedResult.model_validate(res.json())
            result = self.validate_result(page.results)
            pagination_results.append(result)

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


class HttpProcedureEndpoint(Generic[ServiceT, Params, ReturnT]):
    procedure: "ServiceProcedure[ServiceT, Params, ReturnT]"
    name: str
    path: str
    methods: list[str]
    supports_body: bool
    description: str | None

    path_fields: list[str]
    path_model: type[pyd.BaseModel]
    payload_model: type[pyd.BaseModel]

    result_adapter: pyd.TypeAdapter[ReturnT]
    paginated_result_adapter: pyd.TypeAdapter[PaginatedResult[ReturnT]]

    def __init__(
        self,
        procedure: "ServiceProcedure[ServiceT, Params, ReturnT]",
        http_config: HttpConfig,
    ):
        self.procedure = procedure

        self.name = http_config.get("name", procedure.func.__name__)

        kebab_func_name = procedure.func.__name__.replace("_", "-")
        self.path = http_config.get("path", "/" + kebab_func_name)

        self.methods = http_config.get("methods", ["POST"])
        if len(self.methods) == 0:
            self.methods = ["POST"]

        self.description = procedure.func.__doc__

        self.path_fields = self.get_path_fields(self.path)
        self.supports_body = not ("GET" in self.methods or "HEAD" in self.methods)

        self.path_model = self.build_path_model(self.path_fields)
        self.payload_model = self.build_payload_model(self.path_fields)

        self.result_adapter = pyd.TypeAdapter(procedure.signature.return_annotation)

        if self.procedure.has_paginated_func:
            self.paginated_result_adapter = pyd.TypeAdapter(
                procedure.paginated_signature.return_annotation
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
        query_params.pop("limit", None)
        query_params.pop("offset", None)
        return pagination

    def bind_endpoint_func(
        self, service: ServiceT, query_params: dict[str, Any]
    ) -> Callable[Params, Any]:
        bound_func: Callable[Params, Any]
        if self.procedure.has_paginated_func:
            pagination = self.get_pagination_params(query_params)
            bound_func = functools.partial(
                self.procedure.paginated_func, service, pagination
            )
        else:
            bound_func = functools.partial(self.procedure.func, service)

        auth_func = self.procedure.maybe_add_auth_check(service, bound_func)
        return auth_func

    async def handle_request(self, request: Request) -> Response:
        path_params = dict(request.path_params)
        path_params.pop("platform", None)

        query_params = dict(request.query_params)
        try:
            json_params = await request.json()
        except JSONDecodeError:
            json_params = {}

        service = cast(ServiceT, request.state.service)
        bound_func = self.bind_endpoint_func(service, query_params)
        args, kwargs = await self.build_call_args(
            path_params, query_params, json_params
        )
        result = bound_func(*args, **kwargs)

        if self.procedure.has_paginated_func:
            content = self.paginated_result_adapter.dump_json(result)
        else:
            content = self.result_adapter.dump_json(result)

        return Response(content, media_type="application/json")

    def get_route(self) -> Route:
        return Route(
            self.path,
            self.handle_request,
            name=self.name,
            methods=self.methods,
        )

    async def build_call_args(
        self,
        path_params: dict[str, Any],
        query_params: dict[str, Any],
        json_params: dict[str, Any],
        varargs_key: str = "__varargs__",
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        if self.supports_body:
            payload = json_params
        else:
            payload = query_params

        try:
            path_obj = self.path_model.model_validate(path_params)
            payload_obj = self.payload_model.model_validate(payload)
        except pyd.ValidationError as e:
            raise InvalidArguments(e)

        payload = dict(((k, v) for k, v in dict(payload_obj).items() if v is not None))
        varargs = payload.pop(varargs_key, [])

        bound_params = self.procedure.signature.bind(
            *varargs, **dict(path_obj), **payload
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
    model_config = pyd.ConfigDict()

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
