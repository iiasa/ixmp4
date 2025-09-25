import functools
import inspect
from enum import Enum
from typing import (
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
import pandas as pd
import pydantic as pyd
from fastapi.params import Depends
from toolkit.exceptions import ProgrammingError

from ixmp4.rewrite.data import dataframe

from ..base import Service
from ..transport import DirectTransport, HttpxTransport

ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")

HttpReturnT = TypeVar("HttpReturnT")
HttpParams = ParamSpec("HttpParams")

EndpointFunc = Callable[[fa.APIRouter, Callable[..., Any]], None]
ClientFunc = Callable[Params, ReturnT]
ServiceT = TypeVar("ServiceT", bound="Service")
ClientFuncGetter = Callable[[ServiceT, httpx.Client], ClientFunc[Params, ReturnT]]


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
    func: Callable[Concatenate[ServiceT, Params], ReturnT]
    signature: inspect.Signature

    return_type_adapter: pyd.TypeAdapter[ReturnT]
    payload_model: type[pyd.BaseModel]
    fastapi_options: FastApiEndpointOptions
    varargs_key: str = "__varargs__"

    def __init__(
        self,
        func: Callable[Concatenate[ServiceT, Params], ReturnT],
        fastapi_options: IncompleteFastApiEndpointOptions,
    ):
        self.func = func

        if fastapi_options.get("path", None) is None:
            kebab_func_name = self.func.__name__.replace("_", "-")
            fastapi_options["path"] = "/" + kebab_func_name
        if fastapi_options.get("name", None) is None:
            fastapi_options["name"] = self.func.__name__
        if (
            fastapi_options.get("methods", None) is None
            or len(fastapi_options["methods"]) == 0
        ):
            fastapi_options["methods"] = ["POST"]
        if fastapi_options.get("response_class", None) is None:
            fastapi_options["response_class"] = fa.responses.JSONResponse

        self.fastapi_options = cast(FastApiEndpointOptions, fastapi_options)
        self.signature = self.get_signature(self.func)
        self.payload_model = self.build_payload_model()

        return_type = self.signature.return_annotation
        if return_type is pd.DataFrame:
            return_type = dataframe.DataFrame

        self.return_type_adapter = pyd.TypeAdapter(return_type)

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
                    f"Transport class `{obj.transport.__class__.__name__}` is not supported"
                )
        else:
            return self

    def register_endpoint(
        self, router: fa.APIRouter, svc_dep: Callable[..., Any]
    ) -> None:
        func_name = self.func.__name__
        endpoint_options = self.fastapi_options.copy()

        @router.api_route(**endpoint_options)
        def endpoint(
            svc: "Service" = fa.Depends(svc_dep),
            any_payload: dict[str, Any] = fa.Body(),
        ) -> Any:
            payload = self.payload_model(**any_payload).model_dump(exclude_none=True)
            svc_func = getattr(svc, func_name)
            args = self.build_args(self.signature, payload)
            kwargs = self.build_kwargs(self.signature, payload)
            result = svc_func(*args, **kwargs)
            return result

    def build_payload_model(self) -> type[pyd.BaseModel]:
        func_name = self.func.__name__
        payload_model_name = func_name.title().replace("_", "") + "Payload"
        fields = {}
        model_config = pyd.ConfigDict()

        param_dict = self.signature.parameters.items()

        for name, param in param_dict:
            if param.annotation == inspect.Parameter.empty:
                raise ProgrammingError(
                    f"Parameter `{name}` of `{func_name}` requires a type annotation."
                )

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
                    # we received a function with **kwargs: Any, so anything goes.
                    model_config["extra"] = "allow"
                else:
                    raise ProgrammingError(
                        f"Cannot handle `{origin.__name__}` annotation "
                        f"for variadic keyword arguments of `{func_name}`."
                    )
            elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                elem_type: type[object] = param.annotation
                # TODO: test if this works
                fields[self.varargs_key] = list.__class_getitem__(elem_type)
            elif param.default == inspect.Parameter.empty:
                fields[name] = param.annotation
            else:
                fields[name] = (param.annotation, param.default)

        return pyd.create_model(payload_model_name, __config__=model_config, **fields)

    def get_signature(
        self, func: Callable[Concatenate[ServiceT, Params], ReturnT]
    ) -> inspect.Signature:
        org_sig = inspect.signature(func)
        params_without_self = [p for n, p in org_sig.parameters.items() if n != "self"]
        return inspect.Signature(
            params_without_self, return_annotation=org_sig.return_annotation
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
