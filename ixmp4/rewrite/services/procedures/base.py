import functools
import inspect
from enum import Enum
from string import Formatter
from typing import (
    Annotated,
    Any,
    Callable,
    Concatenate,
    Generic,
    NotRequired,
    ParamSpec,
    Sequence,
    TypedDict,
    TypeVar,
    Unpack,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

import fastapi as fa
import httpx
import pydantic as pyd
from fastapi.params import Depends
from toolkit.exceptions import ProgrammingError

from ixmp4.rewrite.transport import DirectTransport, HttpxTransport

from ..base import Service

ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")

HttpReturnT = TypeVar("HttpReturnT")
HttpParams = ParamSpec("HttpParams")

EndpointFunc = Callable[[fa.APIRouter, Callable[..., Any]], None]
ClientFunc = Callable[Params, ReturnT]
ServiceT = TypeVar("ServiceT", bound="Service")
ClientFuncGetter = Callable[[ServiceT, httpx.Client], ClientFunc[Params, ReturnT]]
AnyFuncArgs = tuple[tuple[Any, ...], dict[str, Any]]


class BaseFastApiEndpointOptions(TypedDict, total=False):
    name: str | None
    tags: list[str | Enum] | None
    status_code: int | None
    description: str | None
    summary: str | None
    dependencies: Sequence[Depends] | None
    responses: dict[int | str, dict[str, Any]] | None
    deprecated: bool | None
    operation_id: str | None


class IncompleteFastApiEndpointOptions(BaseFastApiEndpointOptions, total=False):
    path: NotRequired[str]
    methods: NotRequired[list[str]]
    response_class: NotRequired[type[fa.Response]]


class FastApiEndpointOptions(BaseFastApiEndpointOptions, total=False):
    path: str
    methods: list[str]
    response_class: type[fa.Response]


class ServiceProcedure(Generic[ServiceT, Params, ReturnT]):
    varargs_key: str = "__varargs__"

    func: Callable[Concatenate[ServiceT, Params], ReturnT]
    signature: inspect.Signature
    has_required_parameters: bool
    "Indicates whether the functions has postional args without defaults."

    return_type_adapter: pyd.TypeAdapter[ReturnT]
    fastapi_options: FastApiEndpointOptions

    path_model: type[pyd.BaseModel] | None
    parameters_model: type[pyd.BaseModel] | None

    def __init__(
        self,
        func: Callable[Concatenate[ServiceT, Params], ReturnT],
        fastapi_options: IncompleteFastApiEndpointOptions,
    ):
        self.func = func
        self.signature = self.get_signature(self.func)
        self.has_required_parameters = self.determine_required_params(self.signature)
        self.return_type_adapter = pyd.TypeAdapter(self.signature.return_annotation)

        if (
            fastapi_options.get("methods", None) is None
            or len(fastapi_options["methods"]) == 0
        ):
            fastapi_options["methods"] = ["POST"]

        kebab_func_name = self.func.__name__.replace("_", "-")
        fastapi_options.setdefault("path", "/" + kebab_func_name)
        fastapi_options.setdefault("name", self.func.__name__)
        fastapi_options.setdefault("response_class", fa.responses.JSONResponse)
        fastapi_options.setdefault("description", func.__doc__)

        self.fastapi_options = cast(FastApiEndpointOptions, fastapi_options)
        self.path_fields = self.get_path_fields()
        self.path_model = self.build_path_model()
        self.parameters_model = self.get_parameters_model(
            self.get_model_name("Params"),
        )

    def __call__(self, *args: Params.args, **kwds: Params.kwargs) -> ReturnT:
        raise ProgrammingError("`ServiceProcedure` cannot be called directly.")

    @overload
    def __get__(
        self, obj: Any, cls: type[Any] | None = None
    ) -> "ServiceProcedure[ServiceT, Params, ReturnT]": ...

    @overload
    def __get__(
        self, obj: ServiceT, cls: type[ServiceT] | None = None
    ) -> Callable[Params, ReturnT]: ...

    def __get__(self, obj: Any, cls: type[Any] | None = None) -> Any:
        if isinstance(obj, Service):
            obj = cast(ServiceT, obj)

            if isinstance(obj.transport, DirectTransport):
                bound_func = functools.partial(self.func, obj)
                return bound_func
            elif isinstance(obj.transport, HttpxTransport):
                client = self.get_client(obj)
                return client
            else:
                raise ProgrammingError(
                    f"Transport class `{obj.transport.__class__.__name__}` "
                    "is not supported."
                )
        else:
            return self

    def has_fields(self, model_class: type[pyd.BaseModel]):
        return len(model_class.model_fields.keys()) != 0

    def get_dep_annotation(
        self,
        model_class: type[pyd.BaseModel],
        dep_func: type[fa.Path] | type[fa.Query] | type[fa.Body],
    ):
        include_in_schema = required = self.has_fields(model_class)

        if required:
            required = self.has_required_parameters

        return Annotated[
            model_class,
            dep_func(include_in_schema=include_in_schema, required=required),
        ]

    def get_path_depedency(self) -> Callable[..., pyd.BaseModel | None]:
        if self.path_model is not None:
            PathParam = Annotated[self.path_model, fa.Path()]

            async def depedency(
                request: fa.Request,
                path: PathParam,  # type: ignore[reportInvalidTypeForm]
            ) -> pyd.BaseModel:
                request.state.path_params = path
                return path
        else:

            async def depedency(
                request: fa.Request,
            ) -> None:
                request.state.path_params = None
                return None

        return depedency

    def get_params_depedency(self) -> Callable[..., pyd.BaseModel | None]:
        if self.parameters_model is not None:
            if self.supports_body():
                Params = Annotated[
                    self.parameters_model,
                    fa.Body(),
                ]
            else:
                Params = Annotated[
                    self.parameters_model,
                    fa.Query(),
                ]

            if self.has_required_parameters:

                async def depedency(
                    params: Params,  # type: ignore[reportInvalidTypeForm]
                ) -> pyd.BaseModel:
                    return params
            else:

                async def depedency(
                    params: Params = None,  # type: ignore[reportInvalidTypeForm]
                ) -> pyd.BaseModel:
                    return params
        else:

            async def depedency() -> None:
                return None

        return depedency

    def get_endpoint_dependency(self) -> Callable[..., AnyFuncArgs]:
        async def depedency(
            path: Any = fa.Depends(self.get_path_depedency()),
            params: Any = fa.Depends(self.get_params_depedency()),
        ) -> AnyFuncArgs:
            args, kwargs = self.build_endpoint_func_params([path, params])
            return args, kwargs

        return depedency

    def register_endpoint(
        self, router: fa.APIRouter, svc_dep: Callable[..., Any]
    ) -> None:
        endpoint_options = self.fastapi_options.copy()
        endpoint_options.setdefault("response_model", self.signature.return_annotation)

        @router.api_route(**endpoint_options)
        def endpoint(
            params: AnyFuncArgs = fa.Depends(self.get_endpoint_dependency()),
            svc: "Service" = fa.Depends(svc_dep),
        ) -> object:
            svc_func = self.get_service_func(svc)
            args, kwargs = params
            result = svc_func(*args, **kwargs)
            return result

    def get_path_fields(self) -> list[str]:
        return [
            name
            for (_, name, *_) in Formatter().parse(self.fastapi_options["path"])
            if name is not None
        ]

    def supports_body(self):
        return not (
            "GET" in self.fastapi_options["methods"]
            or "HEAD" in self.fastapi_options["methods"]
        )

    def get_parameters_model(self, model_name: str) -> type[pyd.BaseModel] | None:
        fields = {}
        model_config = pyd.ConfigDict()
        param_dict = self.signature.parameters.items()

        for name, param in param_dict:
            if name in self.path_fields:
                continue  # skip parameter if it is supplied via the endpoint path

            if param.kind == inspect.Parameter.VAR_KEYWORD:
                origin = get_origin(param.annotation)
                if origin is Unpack:
                    # we received a function with **kwargs: Unpack[TypedDict]
                    # update the payload model to contain all items from the TypedDict
                    (td,) = get_args(param.annotation)
                    annots = get_type_hints(td)
                    if not td.__total__:
                        for key, type_ in annots.items():
                            annots[key] = (type_ | None, None)
                    fields.update(annots)
                elif param.annotation != inspect.Parameter.empty:
                    # we received a function with **kwargs: <T>, so anything goes.
                    model_config["extra"] = "allow"
                else:
                    raise ProgrammingError(
                        f"Cannot handle `{origin.__name__}` annotation "
                        f"for variadic keyword arguments of `{self.func.__name__}`."
                    )
            elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                elem_type: type[object] = param.annotation
                # equivaltent to list[type] but works dynamically
                fields[self.varargs_key] = list.__class_getitem__(elem_type)
            elif param.default == inspect.Parameter.empty:
                fields[name] = param.annotation
            else:
                fields[name] = (param.annotation, param.default)

        if len(fields) == 0:
            return None

        return pyd.create_model(
            model_name,
            __module__=self.func.__module__,
            __config__=model_config,
            **fields,
        )

    def build_path_model(self) -> type[pyd.BaseModel] | None:
        fields = {}
        for name, param in self.signature.parameters.items():
            if name in self.path_fields:
                fields[name] = (param.annotation, param.default)

        if len(fields) == 0:
            return None  # no path_fields match func signature

        return pyd.create_model(
            self.get_model_name("PathParams"),
            __module__=self.func.__module__,
            **fields,
        )

    def get_model_name(self, suffix: str):
        func_name = self.func.__name__
        return func_name.title().replace("_", "") + suffix

    def determine_required_params(self, sig: inspect.Signature) -> bool:
        has_required_params = False
        param_dict = sig.parameters.items()

        for _, param in param_dict:
            if param.default == inspect.Parameter.empty and (
                param.kind
                in [
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                ]
            ):
                has_required_params = True
        return has_required_params

    def get_signature(
        self, func: Callable[Concatenate[ServiceT, Params], ReturnT]
    ) -> inspect.Signature:
        org_sig = inspect.signature(func)
        valid_params = []
        param_dict = org_sig.parameters.items()

        for name, param in param_dict:
            if name == "self":
                continue  # skip self parameter as it will not be bound yet

            if param.annotation == inspect.Parameter.empty:
                raise ProgrammingError(
                    f"Parameter `{name}` of `{func.__name__}` requires "
                    "a type annotation."
                )

            valid_params.append(param)

        return inspect.Signature(
            valid_params, return_annotation=org_sig.return_annotation
        )

    def get_client(self, service: Service) -> Callable[Params, ReturnT]:
        if not isinstance(service.transport, HttpxTransport):
            raise ProgrammingError(
                "Cannot instantiate http client for transport class "
                f"`{service.transport.__class__.__name__}`."
            )

        client: ServiceProcedureClient[ServiceT, Params, ReturnT] = (
            ServiceProcedureClient(self, service.transport, service.router_prefix)
        )
        return client

    def build_args(
        self, sig: inspect.Signature, payload: dict[str, Any]
    ) -> tuple[Any, ...]:
        args = []
        for name, param in sig.parameters.items():
            if param.kind in [
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ]:
                args.append(payload.pop(name))

        varargs = payload.pop(self.varargs_key, None)
        if varargs is not None:
            args.extend(varargs)
        return tuple(args)

    def build_kwargs(
        self, sig: inspect.Signature, payload: dict[str, Any]
    ) -> dict[str, Any]:
        kwargs = {}
        for name, param in sig.parameters.items():
            if param.kind == inspect.Parameter.KEYWORD_ONLY:
                kwargs[name] = payload.pop(name)
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                kwargs.update(payload)
                break  # should always be last anyway
        return kwargs

    def build_endpoint_func_params(
        self, sources: Sequence[pyd.BaseModel | None]
    ) -> AnyFuncArgs:
        payload = {}
        for s in sources:
            if s is not None:
                payload.update(s.model_dump(exclude_unset=True))

        args = self.build_args(self.signature, payload)
        kwargs = self.build_kwargs(self.signature, payload)
        return (args, kwargs)

    def get_service_func(self, svc: Service):
        return getattr(svc, self.func.__name__)


class ServiceProcedureClient(Generic[ServiceT, Params, ReturnT]):
    procedure: "ServiceProcedure[ServiceT, Params, ReturnT]"
    transport: HttpxTransport
    path: str
    method: str

    def __init__(
        self,
        procedure: "ServiceProcedure[ServiceT, Params, ReturnT]",
        transport: HttpxTransport,
        url_prefix: str,
    ):
        self.procedure = procedure
        self.transport = transport
        self.path = url_prefix + procedure.fastapi_options["path"]
        self.method = procedure.fastapi_options["methods"][0]

    def __call__(self, *args: Params.args, **kwargs: Params.kwargs) -> ReturnT:
        payload = self.build_payload(self.procedure.signature, args, kwargs)
        res = self.transport.client.request(self.method, self.path, json=payload)

        return self.procedure.return_type_adapter.validate_python(res.json())

    def build_payload(
        self, sig: inspect.Signature, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        payload = {}
        for p_item, argval in zip(sig.parameters.items(), args):
            name, param = p_item
            payload[name] = argval

        payload.update(kwargs)
        # TODO: Raise sane exceptions for wrong arguments
        return payload


def procedure(
    **kwargs: Unpack[IncompleteFastApiEndpointOptions],
) -> Callable[
    [Callable[Concatenate[ServiceT, Params], ReturnT]],
    ServiceProcedure[ServiceT, Params, ReturnT],
]:
    def decorator(
        func: Callable[Concatenate[ServiceT, Params], ReturnT],
    ) -> ServiceProcedure[ServiceT, Params, ReturnT]:
        return ServiceProcedure(func, kwargs)

    return decorator
