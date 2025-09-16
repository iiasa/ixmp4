import abc
import functools
import inspect
from typing import (
    Any,
    Callable,
    ClassVar,
    Concatenate,
    Generic,
    ParamSpec,
    TypeVar,
    Unpack,
    get_args,
    get_origin,
    get_type_hints,
)

import fastapi as fa
import httpx
import pandas as pd
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict, TypeAdapter, create_model
from sqlalchemy import orm
from toolkit.auth import AuthorizationContext
from toolkit.exceptions import ProgrammingError

from .dto import DataFrame


class AbstractTransport(abc.ABC):
    pass


class DirectTransport(AbstractTransport):
    session: orm.Session
    auth_ctx: AuthorizationContext

    def __init__(self, session: orm.Session, auth_ctx: AuthorizationContext):
        self.session = session
        self.auth_ctx = auth_ctx


class HttpxTransport(AbstractTransport):
    client: httpx.Client | TestClient

    def __init__(self, client: httpx.Client | TestClient):
        self.client = client


TransportT = TypeVar("TransportT", bound=AbstractTransport)


class AbstractService(abc.ABC):
    router_prefix: ClassVar[str]
    transport: AbstractTransport

    def __init__(self, transport: AbstractTransport):
        self.transport = transport
        if isinstance(transport, DirectTransport):
            self.__init_direct__(transport)
        elif isinstance(transport, HttpxTransport):
            self.__init_httpx__(transport)

    def __init_direct__(self, transport: DirectTransport) -> None:
        pass

    def __init_httpx__(self, transport: HttpxTransport) -> None:
        pass

    @classmethod
    def build_router(
        cls,
        session_dep: Callable[..., Any],
        auth_dep: Callable[..., Any],
    ) -> fa.APIRouter:
        def svc_dep(
            session: orm.Session = fa.Depends(session_dep),
            auth_ctx: AuthorizationContext = fa.Depends(auth_dep),
        ) -> AbstractService:
            transport = DirectTransport(session, auth_ctx)
            return cls(transport)

        router = fa.APIRouter(prefix=cls.router_prefix)
        for proc in cls.collect_procedures():
            proc.register_endpoint(router, svc_dep)
        return router

    @classmethod
    def collect_procedures(cls) -> "list[ServiceProcedure[Any, Any, Any]]":
        procedures = []
        for _, val in vars(cls).items():
            if isinstance(val, ServiceProcedure):
                procedures.append(val)

        return procedures


ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")

HttpReturnT = TypeVar("HttpReturnT")
HttpParams = ParamSpec("HttpParams")

EndpointFunc = Callable[[fa.APIRouter, Callable[..., Any]], None]
ClientFunc = Callable[Params, ReturnT]
ServiceT = TypeVar("ServiceT", bound=AbstractService)
ClientFuncGetter = Callable[[ServiceT, httpx.Client], ClientFunc[Params, ReturnT]]


class ServiceProcedure(Generic[ServiceT, Params, ReturnT]):
    func: Callable[Concatenate[ServiceT, Params], ReturnT]
    service_class: type[AbstractService]
    RetModel: type[BaseModel] | None = None
    RetType: type[Any] | None = None
    PayloadModel: type[BaseModel]
    path: str
    signature: inspect.Signature

    def __init__(self, func: Callable[Concatenate[ServiceT, Params], ReturnT]):
        self.func = func
        func_name = self.func.__name__
        payload_class_name = func_name.title().replace("_", "") + "Payload"
        kebab_func_name = func_name.replace("_", "-")

        self.signature = self.get_signature(self.func)
        fields = {}
        param_dict = self.signature.parameters.items()
        model_config = ConfigDict()
        model_base = BaseModel

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
                elif param.annotation is Any:
                    # we received a function with **kwargs: Any, so anything goes.
                    model_config["extra"] = "allow"
                else:
                    raise ProgrammingError(
                        f"Cannot handle `{origin.__name__}` annotation "
                        f"for variadic keyword arguments of `{func_name}`."
                    )

            elif param.default == inspect.Parameter.empty:
                fields[name] = param.annotation
            else:
                fields[name] = (param.annotation, param.default)

        if self.signature.return_annotation is pd.DataFrame:
            self.RetModel = DataFrame
        elif issubclass(self.signature.return_annotation, BaseModel):
            self.RetModel = self.signature.return_annotation
        elif self.signature.return_annotation is not None:
            self.RetType = self.signature.return_annotation

        self.PayloadModel = create_model(
            payload_class_name, __config__=model_config, __base__=model_base, **fields
        )
        self.path = "/" + kebab_func_name

    def get_signature(
        self, func: Callable[Concatenate[ServiceT, Params], ReturnT]
    ) -> inspect.Signature:
        org_sig = inspect.signature(self.func)
        params_without_self = [p for n, p in org_sig.parameters.items() if n != "self"]
        return inspect.Signature(
            params_without_self, return_annotation=org_sig.return_annotation
        )

    def register_endpoint(
        self, router: fa.APIRouter, svc_dep: Callable[..., Any]
    ) -> None:
        func_name = self.func.__name__

        @router.post(
            self.path,
            name=func_name,
            description=self.func.__doc__,
            response_model=self.RetModel or self.RetType or None,
        )
        def endpoint(
            svc: AbstractService = fa.Depends(svc_dep),
            any_payload: dict[str, Any] = fa.Body(),
        ) -> Any:
            payload = self.PayloadModel(**any_payload).model_dump(exclude_none=True)
            svc_func = getattr(svc, func_name)
            args = self.build_args(self.signature, payload)
            kwargs = self.build_kwargs(self.signature, payload)
            result = svc_func(*args, **kwargs)
            if self.RetModel is not None:
                return self.RetModel.model_validate(result)
            elif self.RetType is not None:
                return result
            else:
                return None

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

    def build_payload(
        self, sig: inspect.Signature, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> BaseModel:
        payload = {}
        for p_item, argval in zip(sig.parameters.items(), args):
            name, param = p_item
            payload[name] = argval

        payload.update(kwargs)
        return self.PayloadModel(**payload)

    def get_client_function(
        self, client: httpx.Client, prefix: str
    ) -> Callable[..., Any]:
        @functools.wraps(self.func)
        def client_function(*args: Any, **kwargs: Any) -> Any:
            payload = self.build_payload(self.signature, args, kwargs)
            res = client.post(prefix + self.path, json=payload.model_dump())

            if self.RetModel is not None:
                return self.RetModel.model_validate(res.json())
            elif self.RetType is not None:
                return TypeAdapter(self.RetType).validate_python(res.json())
            else:
                return None

        return client_function

    def __get__(
        self, obj: ServiceT, cls: type[ServiceT] | None = None
    ) -> Callable[Params, ReturnT]:
        if isinstance(obj.transport, DirectTransport):
            bound_func = functools.partial(self.func, obj)
            return bound_func
        elif isinstance(obj.transport, HttpxTransport):
            client_func = self.get_client_function(
                obj.transport.client, obj.router_prefix
            )
            return client_func
        else:
            raise ProgrammingError(
                f"Transport class {obj.transport.__class__.__name__} is not supported"
            )

    def __set_name__(self, owner: type[Any], name: str) -> None:
        if not issubclass(owner, AbstractService):
            raise ProgrammingError(
                f"`ServiceProcedure` cannot be a method of `{owner.__name__}`."
            )

        self.service_class = owner


def procedure(
    cls: type[ServiceProcedure[Any, Any, Any]] = ServiceProcedure,
    **kwargs: Any,
) -> Callable[
    [Callable[Concatenate[ServiceT, Params], ReturnT]],
    ServiceProcedure[ServiceT, Params, ReturnT],
]:
    def decorator(
        func: Callable[Concatenate[ServiceT, Params], ReturnT],
    ) -> ServiceProcedure[ServiceT, Params, ReturnT]:
        return cls(func)

    return decorator
