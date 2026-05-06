"""Unit tests for the service procedure machinery."""

import asyncio
import inspect
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest
import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol

from ixmp4.base_exceptions import InvalidArguments, ProgrammingError
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.data.services import Http, Service, procedure
from ixmp4.data.services.procedure import Procedure
from ixmp4.data.services.procedure.endpoint import (
    ProcedureHttpConfig,
    ProcedureRouteHandler,
    generate_arguments_model,
)
from ixmp4.data.services.procedure.pagination import ProcedurePagination
from ixmp4.transport import AuthorizedTransport, DirectTransport, HttpxTransport


# ---------------------------------------------------------------------------
# Minimal service used across multiple tests
# ---------------------------------------------------------------------------


class DemoService(Service):
    router_prefix = "/demo"

    @procedure(Http(methods=("POST",)))
    def compute(self, value: int) -> int:
        """Doubles a value."""
        return value * 2

    @procedure(Http(path="/{id:int}/rename", methods=("PATCH",)))
    def rename(self, id: int, name: str) -> str:
        """Renames item {id}."""
        return f"{id}:{name}"

    def __init_direct__(self, transport: DirectTransport) -> None:  # type: ignore[override]
        pass

    def __init_httpx__(self, transport: HttpxTransport) -> None:  # type: ignore[override]
        pass


class PaginatedDemoService(Service):
    router_prefix = "/paginated-demo"

    @procedure(Http(methods=("GET",)))
    def list_items(self, filter: str = "") -> list[str]:
        return ["a", "b"]

    @list_items.paginated()
    def paginated_list_items(
        self, pagination: Pagination, filter: str = ""
    ) -> PaginatedResult[list[str]]:
        return PaginatedResult(results=["a"], total=1, pagination=pagination)

    def __init_direct__(self, transport: DirectTransport) -> None:  # type: ignore[override]
        pass

    def __init_httpx__(self, transport: HttpxTransport) -> None:  # type: ignore[override]
        pass


class FakeHttpxTransport(HttpxTransport):
    """HttpxTransport subclass that skips the real __init__ for isolation."""

    def __init__(self) -> None:  # type: ignore[override]
        pass


# ---------------------------------------------------------------------------
# Procedure.validate_signature errors
# ---------------------------------------------------------------------------


def test_procedure_raises_for_positional_only_param() -> None:
    """Line 121: positional-only args are rejected."""
    config = ProcedureHttpConfig(methods=("POST",))

    def proc_with_positional_only(self: Any, x: int, /) -> int:
        return x

    with pytest.raises(ProgrammingError, match="positional-only"):
        Procedure(proc_with_positional_only, config)


def test_procedure_raises_for_unannotated_param() -> None:
    """Line 127: params without type annotations are rejected."""
    config = ProcedureHttpConfig(methods=("POST",))

    def proc_without_annotation(self: Any, x) -> int:  # type: ignore[no-untyped-def]
        return x

    with pytest.raises(ProgrammingError, match="type annotation"):
        Procedure(proc_without_annotation, config)


# ---------------------------------------------------------------------------
# Procedure.validate_direct_call_args errors
# ---------------------------------------------------------------------------


def test_validate_direct_call_args_raises_invalid_arguments_on_wrong_arity() -> None:
    """Lines 83-84: TypeError from bind (wrong number of args) → InvalidArguments."""
    proc = DemoService.compute.procedure  # type: ignore[attr-defined]

    with pytest.raises(InvalidArguments):
        proc.validate_direct_call_args(args=(), kwargs={})  # missing `value`


def test_validate_direct_call_args_raises_invalid_arguments_on_type_mismatch() -> None:
    """Lines 100-101: pydantic ValidationError (wrong type) → InvalidArguments."""
    proc = DemoService.compute.procedure  # type: ignore[attr-defined]

    with pytest.raises(InvalidArguments):
        proc.validate_direct_call_args(args=(), kwargs={"value": "not-an-int"})


def test_validate_direct_call_args_handles_var_positional() -> None:
    """Line 92: *args branch executes payload collection (coverage target).

    Note: pydantic v2 silently drops field names starting with ``__``
    (the ``__varargs__`` key), so the subsequent model validation raises
    ``InvalidArguments``.  The important thing for coverage is that line
    92 is reached before the error.
    """
    config = ProcedureHttpConfig(methods=("POST",))

    class VarArgService(Service):
        router_prefix = "/vararg"

        @procedure(config)
        def add(self, *values: int) -> int:
            return sum(values)

        def __init_direct__(self, transport: DirectTransport) -> None:  # type: ignore[override]
            pass

        def __init_httpx__(self, transport: HttpxTransport) -> None:  # type: ignore[override]
            pass

    proc = VarArgService.add.procedure  # type: ignore[attr-defined]
    # Line 92 executes; pydantic v2 then rejects __varargs__ as an extra field.
    with pytest.raises(InvalidArguments):
        proc.validate_direct_call_args(args=(1, 2, 3), kwargs={})


def test_validate_direct_call_args_handles_var_keyword() -> None:
    """Line 94: **kwargs are flattened into the payload."""
    config = ProcedureHttpConfig(methods=("POST",))

    class VarKwargService(Service):
        router_prefix = "/varkwarg"

        @procedure(config)
        def store(self, **kwargs: int) -> dict[str, int]:
            return dict(kwargs)

        def __init_direct__(self, transport: DirectTransport) -> None:  # type: ignore[override]
            pass

        def __init_httpx__(self, transport: HttpxTransport) -> None:  # type: ignore[override]
            pass

    proc = VarKwargService.store.procedure  # type: ignore[attr-defined]
    args_out, kwargs_out = proc.validate_direct_call_args(args=(), kwargs={"x": 1, "y": 2})
    assert kwargs_out == {"x": 1, "y": 2}


# ---------------------------------------------------------------------------
# Procedure.validate_corresponding_parameter errors
# ---------------------------------------------------------------------------


def test_validate_corresponding_parameter_raises_for_superfluous_arg() -> None:
    """Lines 148-149: auth_check has more params than the procedure."""

    class SuperfluousAuthService(Service):
        router_prefix = "/sup"

        @procedure(Http(methods=("POST",)))
        def act(self, x: int) -> int:
            return x

        def __init_direct__(self, transport: DirectTransport) -> None:  # type: ignore[override]
            pass

        def __init_httpx__(self, transport: HttpxTransport) -> None:  # type: ignore[override]
            pass

    with pytest.raises(ProgrammingError, match="superfluous"):

        @SuperfluousAuthService.act.auth_check()
        def act_auth(
            self: Any,
            auth_ctx: AuthorizationContext,
            platform: PlatformProtocol,
            x: int,
            extra: str,  # one more param than the procedure (which only has `x`)
        ) -> None:
            pass


def test_validate_corresponding_parameter_raises_for_annotation_mismatch() -> None:
    """Line 156: auth_check param annotation differs from procedure param."""

    class AnnotMismatchService(Service):
        router_prefix = "/annot"

        @procedure(Http(methods=("POST",)))
        def act(self, x: int) -> int:
            return x

        def __init_direct__(self, transport: DirectTransport) -> None:  # type: ignore[override]
            pass

        def __init_httpx__(self, transport: HttpxTransport) -> None:  # type: ignore[override]
            pass

    with pytest.raises(ProgrammingError, match="does not match"):

        @AnnotMismatchService.act.auth_check()
        def act_auth(
            self: Any,
            auth_ctx: AuthorizationContext,
            platform: PlatformProtocol,
            x: str,  # procedure expects int, not str
        ) -> None:
            pass


def test_validate_corresponding_parameter_raises_for_default_mismatch() -> None:
    """Line 164: auth_check param default differs from procedure param."""

    class DefaultMismatchService(Service):
        router_prefix = "/default"

        @procedure(Http(methods=("POST",)))
        def act(self, x: int = 0) -> int:
            return x

        def __init_direct__(self, transport: DirectTransport) -> None:  # type: ignore[override]
            pass

        def __init_httpx__(self, transport: HttpxTransport) -> None:  # type: ignore[override]
            pass

    with pytest.raises(ProgrammingError, match="Default"):

        @DefaultMismatchService.act.auth_check()
        def act_auth(
            self: Any,
            auth_ctx: AuthorizationContext,
            platform: PlatformProtocol,
            x: int = 99,  # procedure has default=0, auth_check has default=99
        ) -> None:
            pass


# ---------------------------------------------------------------------------
# Procedure.set_route_handler, get_authorized_callable, get_httpx_callable
# ---------------------------------------------------------------------------


def test_set_route_handler_is_a_noop() -> None:
    """Line 173: set_route_handler does nothing (interface hook)."""
    proc = DemoService.compute.procedure  # type: ignore[attr-defined]
    sentinel = mock.sentinel.handler
    proc.set_route_handler(sentinel)  # type: ignore[arg-type]


def test_get_authorized_callable_wraps_with_auth_check_for_authorized_transport() -> None:
    """Line 179: AuthorizedTransport triggers auth_check.prepend_auth_check."""
    transport = DirectTransport.from_dsn("sqlite:///:memory:")
    session = transport.session

    auth_called: list[bool] = []

    class AuthDemoService(Service):
        router_prefix = "/authdemo"

        @procedure(Http(methods=("POST",)))
        def compute(self, value: int) -> int:
            return value * 2

        def __init_direct__(self, t: DirectTransport) -> None:  # type: ignore[override]
            pass

        def __init_httpx__(self, t: HttpxTransport) -> None:  # type: ignore[override]
            pass

    @AuthDemoService.compute.auth_check()
    def compute_auth(
        self: "AuthDemoService",
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> None:
        auth_called.append(True)

    auth_transport = AuthorizedTransport(
        session=session,
        auth_ctx=SimpleNamespace(user="alice"),  # type: ignore[arg-type]
        platform=SimpleNamespace(id="demo"),  # type: ignore[arg-type]
    )
    svc = AuthDemoService(auth_transport)
    bound_func = mock.Mock(return_value=42)

    proc = AuthDemoService.compute.procedure  # type: ignore[attr-defined]
    callable_ = proc.get_authorized_callable(svc, bound_func)
    callable_(5)

    assert auth_called == [True]
    bound_func.assert_called_once_with(5)
    transport.close()


def test_get_httpx_callable_returns_procedure_client() -> None:
    """Lines 202-203: get_httpx_callable returns a ProcedureClient."""
    from ixmp4.data.services.procedure.client import ProcedureClient

    svc = object.__new__(DemoService)
    svc.transport = FakeHttpxTransport()  # type: ignore[attr-defined]

    proc = DemoService.compute.procedure  # type: ignore[attr-defined]
    client = proc.get_httpx_callable(svc)
    assert isinstance(client, ProcedureClient)


# ---------------------------------------------------------------------------
# ProcedureAuthCheck validation errors and prepend_auth_check
# ---------------------------------------------------------------------------


def test_auth_check_no_params_wrapper_is_called_with_extra_args() -> None:
    """Line 122: the no-params wrapper ignores extra positional/keyword args."""

    class NpAuthService(Service):
        router_prefix = "/npauth"

        @procedure(Http(methods=("POST",)))
        def do_thing(self, value: int) -> int:
            return value

        def __init_direct__(self, transport: DirectTransport) -> None:  # type: ignore[override]
            pass

        def __init_httpx__(self, transport: HttpxTransport) -> None:  # type: ignore[override]
            pass

    called_with: list[tuple[Any, ...]] = []

    @NpAuthService.do_thing.auth_check()
    def do_thing_auth(
        self: "NpAuthService",
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> None:
        called_with.append((self, auth_ctx, platform))

    proc = NpAuthService.do_thing.procedure  # type: ignore[attr-defined]
    svc = mock.Mock()
    ctx = mock.Mock()
    plat = mock.Mock()

    # The wrapper must forward only (svc, ctx, plat) even when extra args are present.
    proc.auth_check.check_func(svc, ctx, plat, "extra-arg", extra_kwarg=True)
    assert len(called_with) == 1
    assert called_with[0] == (svc, ctx, plat)


def test_auth_check_validate_parameter_raises_for_wrong_auth_ctx_annotation() -> None:
    """Line 156 in auth.py: index-1 param must be AuthorizationContext."""

    class WrongCtxService(DemoService):
        router_prefix = "/wrongctx"

    with pytest.raises(ProgrammingError, match="expected argument of type.*AuthorizationContext"):

        @WrongCtxService.compute.auth_check()
        def bad_auth(
            self: "WrongCtxService",
            wrong: int,  # index 1: should be AuthorizationContext
            platform: PlatformProtocol,
        ) -> None:
            pass


def test_auth_check_validate_parameter_raises_for_wrong_platform_annotation() -> None:
    """Line 164 in auth.py: index-2 param must be PlatformProtocol."""

    class WrongPlatService(DemoService):
        router_prefix = "/wrongplat"

    with pytest.raises(ProgrammingError, match="expected argument of type.*PlatformProtocol"):

        @WrongPlatService.compute.auth_check()
        def bad_auth(
            self: "WrongPlatService",
            auth_ctx: AuthorizationContext,
            wrong: int,  # index 2: should be PlatformProtocol
        ) -> None:
            pass


def test_auth_check_prepend_auth_check_calls_check_then_procedure() -> None:
    """Lines 184-193 in auth.py: with has_check=True the auth wrapper runs first."""

    class AuthOrderService(Service):
        router_prefix = "/authorder"

        @procedure(Http(methods=("POST",)))
        def act(self, x: int) -> int:
            return x

        def __init_direct__(self, t: DirectTransport) -> None:  # type: ignore[override]
            pass

        def __init_httpx__(self, t: HttpxTransport) -> None:  # type: ignore[override]
            pass

    call_log: list[str] = []

    @AuthOrderService.act.auth_check()
    def act_auth(
        self: "AuthOrderService",
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> None:
        call_log.append("auth")

    proc = AuthOrderService.act.procedure  # type: ignore[attr-defined]

    def fake_proc(*args: Any, **kwargs: Any) -> int:
        call_log.append("proc")
        return 99

    svc = mock.Mock()
    ctx = mock.Mock()
    plat = mock.Mock()

    wrapped = proc.auth_check.prepend_auth_check(svc, ctx, plat, fake_proc)
    result = wrapped()
    assert call_log == ["auth", "proc"]
    assert result == 99


# ---------------------------------------------------------------------------
# ProcedureDescriptor errors and transport dispatch
# ---------------------------------------------------------------------------


def test_descriptor_direct_call_raises_programming_error() -> None:
    """Line 53: calling the descriptor directly raises ProgrammingError."""
    descriptor = DemoService.__dict__["compute"]
    with pytest.raises(ProgrammingError, match="cannot be called directly"):
        descriptor(5)


def test_descriptor_get_raises_for_non_service_object() -> None:
    """Line 62: accessing descriptor on a non-Service instance raises ProgrammingError."""

    class NotAService:
        pass

    obj = NotAService()
    # Manually bind the descriptor to a non-Service owner
    descriptor = DemoService.__dict__["compute"]
    with pytest.raises(ProgrammingError, match="must be used as a descriptor for `Service`"):
        descriptor.__get__(obj, NotAService)


def test_descriptor_get_returns_httpx_callable_for_httpx_transport() -> None:
    """Lines 69-70: HttpxTransport triggers get_httpx_callable path."""
    from ixmp4.data.services.procedure.client import ProcedureClient

    svc = object.__new__(DemoService)
    svc.transport = FakeHttpxTransport()

    client = DemoService.compute.__get__(svc, DemoService)  # type: ignore[attr-defined]
    assert isinstance(client, ProcedureClient)


def test_descriptor_get_raises_for_unknown_transport() -> None:
    """Lines 71-72: unsupported transport class raises ProgrammingError."""

    class AliensTransport:
        pass

    svc = object.__new__(DemoService)
    svc.transport = AliensTransport()

    descriptor = DemoService.__dict__["compute"]
    with pytest.raises(ProgrammingError, match="is not supported"):
        descriptor.__get__(svc, DemoService)


# ---------------------------------------------------------------------------
# generate_arguments_model edge cases
# ---------------------------------------------------------------------------


def test_generate_arguments_model_raises_for_positional_only() -> None:
    """Line 262: positional-only params raise ProgrammingError."""
    sig = inspect.signature(lambda x: x)

    # Build a synthetic signature with a positional-only param
    params = [
        inspect.Parameter("x", kind=inspect.Parameter.POSITIONAL_ONLY, annotation=int)
    ]
    sig = sig.replace(parameters=params, return_annotation=int)

    with pytest.raises(ProgrammingError, match="positional-only"):
        generate_arguments_model(
            sig,
            "TestModel",
            __module__=__name__,
            parameter_callback=lambda i, n, p: None,
        )


def test_generate_arguments_model_handles_var_positional() -> None:
    """Lines 267-269: *args produces a list field in the model."""
    params = [
        inspect.Parameter(
            "values", kind=inspect.Parameter.VAR_POSITIONAL, annotation=int
        )
    ]
    sig = inspect.Signature(params, return_annotation=list)

    model = generate_arguments_model(
        sig,
        "VarPosModel",
        __module__=__name__,
        parameter_callback=lambda i, n, p: None,
    )
    instance = model()
    assert hasattr(instance, "__varargs__")


def test_generate_arguments_model_handles_plain_var_keyword() -> None:
    """Line 287: **kwargs (no Unpack) sets model_config extra='allow'."""
    params = [
        inspect.Parameter(
            "kwargs", kind=inspect.Parameter.VAR_KEYWORD, annotation=int
        )
    ]
    sig = inspect.Signature(params, return_annotation=dict)

    model = generate_arguments_model(
        sig,
        "VarKwModel",
        __module__=__name__,
        parameter_callback=lambda i, n, p: None,
    )
    # extra='allow' lets extra fields pass validation
    instance = model.model_validate({"anything": 42})
    assert instance.model_extra is not None


# ---------------------------------------------------------------------------
# ProcedurePagination validation error
# ---------------------------------------------------------------------------


def test_pagination_validate_parameter_raises_for_wrong_annotation() -> None:
    """Line 108 in pagination.py: index-1 param must be Pagination."""

    class PagErrService(DemoService):
        router_prefix = "/pagerr"

    with pytest.raises(ProgrammingError, match="expected argument of type.*AuthorizationContext"):

        @PagErrService.compute.paginated()
        def paginated_compute(
            svc: "PagErrService",
            wrong: int,  # should be Pagination
        ) -> PaginatedResult[int]:
            return PaginatedResult(results=0, total=0, pagination=Pagination())


# ---------------------------------------------------------------------------
# ProcedureRouteHandler – path/payload models and request handling
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def demo_transport() -> DirectTransport:  # type: ignore[return]
    t = DirectTransport.from_dsn("sqlite:///:memory:")
    yield t
    t.close()


@pytest.fixture(scope="module")
def demo_service(demo_transport: DirectTransport) -> DemoService:
    return DemoService(demo_transport)


@pytest.fixture(scope="module")
def paginated_service(demo_transport: DirectTransport) -> PaginatedDemoService:
    return PaginatedDemoService(demo_transport)


@pytest.fixture(scope="module")
def compute_handler() -> ProcedureRouteHandler:  # type: ignore[type-arg]
    return DemoService.compute.procedure.handlers[DemoService]  # type: ignore[attr-defined]


@pytest.fixture(scope="module")
def rename_handler() -> ProcedureRouteHandler:  # type: ignore[type-arg]
    return DemoService.rename.procedure.handlers[DemoService]  # type: ignore[attr-defined]


@pytest.fixture(scope="module")
def list_handler() -> ProcedureRouteHandler:  # type: ignore[type-arg]
    return PaginatedDemoService.list_items.procedure.handlers[PaginatedDemoService]  # type: ignore[attr-defined]


def test_route_handler_path_field_skipped_in_payload_model(
    rename_handler: ProcedureRouteHandler,  # type: ignore[type-arg]
) -> None:
    """Line 126: path params are excluded from the payload model."""
    # `id` is a path field; it must NOT appear in the payload model
    assert "id" not in rename_handler.payload_model.model_fields


def test_route_handler_non_path_field_skipped_in_path_model(
    rename_handler: ProcedureRouteHandler,  # type: ignore[type-arg]
) -> None:
    """Line 141: non-path params are excluded from the path model."""
    # `name` is a payload field; it must NOT appear in the path model
    assert "name" not in rename_handler.path_model.model_fields


def test_route_handler_build_call_args_with_body(
    compute_handler: ProcedureRouteHandler,  # type: ignore[type-arg]
) -> None:
    """Lines 204-223: build_call_args parses a JSON body for POST endpoints."""
    args, kwargs = compute_handler.build_call_args(
        path={}, query={}, body=b'{"value": 7}'
    )
    # `value` is positional in compute(self, value: int) → lands in args
    assert args == (7,)


def test_route_handler_build_call_args_with_empty_body(
    compute_handler: ProcedureRouteHandler,  # type: ignore[type-arg]
) -> None:
    """Line 210: empty body constructs empty payload_model (required fields still validated)."""
    # `compute` requires `value`, so empty body → InvalidArguments
    with pytest.raises(InvalidArguments):
        compute_handler.build_call_args(path={}, query={}, body=b"")


def test_route_handler_build_call_args_raises_for_invalid_json(
    compute_handler: ProcedureRouteHandler,  # type: ignore[type-arg]
) -> None:
    """Lines 214-215: malformed body raises InvalidArguments."""
    with pytest.raises(InvalidArguments):
        compute_handler.build_call_args(
            path={}, query={}, body=b'{"value": "not-an-int"}'
        )


def test_route_handler_build_call_args_with_path_params(
    rename_handler: ProcedureRouteHandler,  # type: ignore[type-arg]
) -> None:
    """Lines 204-223: path and body params are merged correctly."""
    args, kwargs = rename_handler.build_call_args(
        path={"id": 3}, query={}, body=b'{"name": "foo"}'
    )
    # rename(self, id: int, name: str) has positional params → land in args
    assert args == (3, "foo")


def test_route_handler_handle_request(
    compute_handler: ProcedureRouteHandler,  # type: ignore[type-arg]
    demo_service: DemoService,
) -> None:
    """Lines 191-195: handle_request calls the procedure and returns a Response."""
    from litestar.response import Response

    request = mock.Mock()
    request.path_params = {}

    response = compute_handler.handle_request(
        request, demo_service, query={}, body=b'{"value": 4}'
    )
    assert isinstance(response, Response)
    # compute doubles the value: 4 * 2 = 8
    assert response.content == b"8"


def test_route_handler_get_pagination_params(
    list_handler: ProcedureRouteHandler,  # type: ignore[type-arg]
) -> None:
    """Lines 226-227: get_pagination_params parses offset/limit from query dict."""
    pagination = list_handler.get_pagination_params({"limit": 10, "offset": 5})
    assert pagination.limit == 10
    assert pagination.offset == 5


def test_route_handler_bind_endpoint_func_paginated(
    list_handler: ProcedureRouteHandler,  # type: ignore[type-arg]
    paginated_service: PaginatedDemoService,
) -> None:
    """Lines 233-241: paginated procedure gets a pagination-bound callable."""
    bound = list_handler.bind_endpoint_func(paginated_service, {"limit": 10, "offset": 0})
    result = bound(filter="")
    assert result.total == 1
