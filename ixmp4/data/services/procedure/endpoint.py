import functools
import inspect
from dataclasses import dataclass
from string import Formatter
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Literal,
    ParamSpec,
    Sequence,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

import pydantic as pyd
from litestar import HttpMethod, Request
from litestar.enums import HttpMethod
from litestar.handlers import HTTPRouteHandler
from litestar.openapi.spec import (
    OpenAPIMediaType,
    OpenAPIResponse,
    Operation,
    Reference,
    Schema,
)
from litestar.response import Response
from litestar.routes import HTTPRoute
from litestar.types import Method
from typing_extensions import Unpack

from ixmp4.base_exceptions import InvalidArguments, ProgrammingError
from ixmp4.data.pagination import Pagination

if TYPE_CHECKING:
    from ..base import Service
    from . import Procedure

ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")
ServiceT = TypeVar("ServiceT", bound="Service")


@dataclass
class ProcedureHttpConfig:
    methods: HttpMethod | Method | Sequence[HttpMethod | Method]
    path: str | None = None
    status_code: int = 200


class ProcedureRouteHandler(HTTPRouteHandler, Generic[ServiceT, Params, ReturnT]):
    config: ProcedureHttpConfig
    procedure: "Procedure[ServiceT, Params, ReturnT]"
    proto_route: HTTPRoute
    service_class: type[ServiceT]

    def __init__(
        self,
        procedure: "Procedure[ServiceT, Params, ReturnT]",
        service_class: type[ServiceT],
        config: ProcedureHttpConfig,
    ) -> None:
        self.procedure = procedure
        self.config = config
        self.service_class = service_class
        kebab_func_name = procedure.func.__name__.replace("_", "-")
        path = config.path or "/" + kebab_func_name
        methods = config.methods or [HttpMethod.POST]

        super().__init__(
            path=path,
            http_method=methods,
            status_code=config.status_code,
            summary=procedure.func.__name__,
            description=procedure.func.__doc__,
            operation_class=self.get_openapi_operation_class(),
        )
        self.name = ".".join(
            [service_class.__module__, service_class.__name__, procedure.func.__name__]
        )
        self.operation_id = self.name

        self.path_fields = self.get_path_fields(path)
        self.supports_body = not ("GET" in methods or "HEAD" in methods)

        self.path_model = self.build_path_model(self.path_fields)
        self.payload_model = self.build_payload_model(self.path_fields)

        if self.procedure.pagination.has_pagination:
            self.return_type_adapter = pyd.TypeAdapter(
                self.procedure.pagination.signature.return_annotation
            )
        else:
            self.return_type_adapter = pyd.TypeAdapter(
                self.procedure.signature.return_annotation
            )

        super().__call__(self.handle_request)

        self.proto_route = HTTPRoute(path=next(iter(self.paths)), route_handlers=[self])

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

    def get_openapi_operation_class(pe_self) -> type[Operation]:
        @dataclass
        class ProcedureOperation(Operation):
            def __post_init__(self) -> None:
                self.responses = pe_self.get_openapi_responses()

        return ProcedureOperation

    def get_openapi_responses(self) -> dict[str, OpenAPIResponse | Reference]:
        responses: dict[str, OpenAPIResponse | Reference] = {}
        return_schema = self.return_type_adapter.json_schema(
            mode="serialization", ref_template="'#/components/schemas/{model}'"
        )
        return_schema.pop("$defs", None)

        schema = Schema(**return_schema)
        responses["200"] = OpenAPIResponse(
            content={"application/json": OpenAPIMediaType(schema=schema)},
            description="",
        )

        return responses

    async def handle_request(
        self,
        request: Request[Any, Any, Any],
        service: Any,
        query: dict[str, Any],
        body: bytes,
    ) -> Response[Any]:
        bound_func = self.bind_endpoint_func(service, query)
        args, kwargs = self.build_call_args(request.path_params, query, body)
        result = bound_func(*args, **kwargs)
        json_bytes = self.return_type_adapter.dump_json(result)
        return Response(json_bytes, media_type="application/json")

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
                if len(body) > 0:
                    payload = self.payload_model.model_validate_json(body)
                else:
                    payload = self.payload_model()
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

    def get_pagination_params(self, query_params: dict[str, Any]) -> Pagination:
        pagination = Pagination.model_validate(query_params, extra="ignore")
        return pagination

    def bind_endpoint_func(
        self, service: ServiceT, query_params: dict[str, Any]
    ) -> Callable[Params, Any]:
        bound_func: Callable[Params, Any]
        if self.procedure.pagination.has_pagination:
            pagination = self.get_pagination_params(query_params)
            bound_func = functools.partial(
                self.procedure.pagination.paginated_func, service, pagination
            )
        else:
            bound_func = functools.partial(self.procedure.func, service)

        return self.procedure.get_authorized_callable(service, bound_func)


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
