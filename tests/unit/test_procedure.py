import inspect
from collections.abc import Generator
from types import SimpleNamespace
from typing import Any, Callable, cast
from unittest import mock

import pytest
from litestar.handlers import HTTPRouteHandler
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
from ixmp4.transport import (
    AuthorizedTransport,
    DirectTransport,
    HttpxTransport,
    Transport,
)


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

    def __init_direct__(self, transport: DirectTransport) -> None:
        pass

    def __init_httpx__(self, transport: HttpxTransport) -> None:
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

    def __init_direct__(self, transport: DirectTransport) -> None:
        pass

    def __init_httpx__(self, transport: HttpxTransport) -> None:
        pass


class FakeHttpxTransport(HttpxTransport):
    """HttpxTransport subclass that skips the real __init__ for isolation."""

    def __init__(self) -> None:
        pass


class TestProcedureInit:
    def test_procedure_raises_for_positional_only_param(self) -> None:
        """Positional-only args are rejected."""
        config = ProcedureHttpConfig(methods=("POST",))

        def proc_with_positional_only(self: Any, x: int, /) -> int:
            return x

        with pytest.raises(ProgrammingError, match="positional-only"):
            Procedure(proc_with_positional_only, config)

    def test_procedure_raises_for_unannotated_param(self) -> None:
        """Params without type annotations are rejected."""
        config = ProcedureHttpConfig(methods=("POST",))

        def proc_without_annotation(self: Any, x) -> int:  # type: ignore[no-untyped-def]
            return 0

        with pytest.raises(ProgrammingError, match="type annotation"):
            Procedure(proc_without_annotation, config)


class TestProcedureCallArgValidation:
    def test_validate_direct_call_args_raises_invalid_arguments_on_wrong_arity(
        self,
    ) -> None:
        """TypeError from bind (wrong number of args) -> InvalidArguments."""
        proc = DemoService.compute.procedure

        with pytest.raises(InvalidArguments):
            proc.validate_direct_call_args(args=(), kwargs={})  # missing `value`

    def test_validate_direct_call_args_raises_invalid_arguments_on_type_mismatch(
        self,
    ) -> None:
        """Pydantic ValidationError (wrong type) -> InvalidArguments."""
        proc = DemoService.compute.procedure

        with pytest.raises(InvalidArguments):
            proc.validate_direct_call_args(args=(), kwargs={"value": "not-an-int"})

    def test_validate_direct_call_args_handles_var_positional(self) -> None:
        """*args branch executes payload collection (coverage target).

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

            def __init_direct__(self, transport: DirectTransport) -> None:
                pass

            def __init_httpx__(self, transport: HttpxTransport) -> None:
                pass

        proc = VarArgService.add.procedure
        # Line 92 executes; pydantic v2 then rejects __varargs__ as an extra field.
        with pytest.raises(InvalidArguments):
            proc.validate_direct_call_args(args=(1, 2, 3), kwargs={})

    def test_validate_direct_call_args_handles_var_keyword(self) -> None:
        """**kwargs are flattened into the payload."""
        config = ProcedureHttpConfig(methods=("POST",))

        class VarKwargService(Service):
            router_prefix = "/varkwarg"

            @procedure(config)
            def store(self, /, **kwargs: int) -> dict[str, int]:
                return dict(kwargs)

            def __init_direct__(self, transport: DirectTransport) -> None:
                pass

            def __init_httpx__(self, transport: HttpxTransport) -> None:
                pass

        proc = VarKwargService.store.procedure
        args_out, kwargs_out = proc.validate_direct_call_args(
            args=(), kwargs={"x": 1, "y": 2}
        )
        assert kwargs_out == {"x": 1, "y": 2}


class TestAuthCheckArgValidation:
    def test_validate_corresponding_parameter_raises_for_superfluous_arg(self) -> None:
        """Auth_check has more params than the procedure."""

        class SuperfluousAuthService(Service):
            router_prefix = "/sup"

            @procedure(Http(methods=("POST",)))
            def act(self, x: int) -> int:
                return x

            def __init_direct__(self, transport: DirectTransport) -> None:
                pass

            def __init_httpx__(self, transport: HttpxTransport) -> None:
                pass

        with pytest.raises(ProgrammingError, match="superfluous"):
            auth_check = cast(
                Callable[[Callable[..., None]], Callable[..., None]],
                SuperfluousAuthService.act.auth_check(),
            )

            @auth_check
            def act_auth(
                self: Any,
                auth_ctx: AuthorizationContext,
                platform: PlatformProtocol,
                x: int,
                extra: str,  # one more param than the procedure (which only has `x`)
            ) -> None:
                pass

    def test_validate_corresponding_parameter_raises_for_annotation_mismatch(
        self,
    ) -> None:
        """Auth_check param annotation differs from procedure param."""

        class AnnotMismatchService(Service):
            router_prefix = "/annot"

            @procedure(Http(methods=("POST",)))
            def act(self, x: int) -> int:
                return x

            def __init_direct__(self, transport: DirectTransport) -> None:
                pass

            def __init_httpx__(self, transport: HttpxTransport) -> None:
                pass

        with pytest.raises(ProgrammingError, match="does not match"):
            auth_check = cast(
                Callable[[Callable[..., None]], Callable[..., None]],
                AnnotMismatchService.act.auth_check(),
            )

            @auth_check
            def act_auth(
                self: Any,
                auth_ctx: AuthorizationContext,
                platform: PlatformProtocol,
                x: str,  # procedure expects int, not str
            ) -> None:
                pass

    def test_validate_corresponding_parameter_raises_for_default_mismatch(self) -> None:
        """Auth_check param default differs from procedure param."""

        class DefaultMismatchService(Service):
            router_prefix = "/default"

            @procedure(Http(methods=("POST",)))
            def act(self, x: int = 0) -> int:
                return x

            def __init_direct__(self, transport: DirectTransport) -> None:
                pass

            def __init_httpx__(self, transport: HttpxTransport) -> None:
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

    def test_auth_check_validate_parameter_raises_for_wrong_auth_ctx_annotation(
        self,
    ) -> None:
        """Index-1 param must be AuthorizationContext."""

        class WrongCtxService(DemoService):
            router_prefix = "/wrongctx"

        with pytest.raises(
            ProgrammingError, match="expected argument of type.*AuthorizationContext"
        ):
            auth_check = cast(
                Callable[[Callable[..., None]], Callable[..., None]],
                WrongCtxService.compute.auth_check(),
            )

            @auth_check
            def bad_auth(
                self: "WrongCtxService",
                wrong: int,  # index 1: should be AuthorizationContext
                platform: PlatformProtocol,
            ) -> None:
                pass

    def test_auth_check_validate_parameter_raises_for_wrong_platform_annotation(
        self,
    ) -> None:
        """Index-2 param must be PlatformProtocol."""

        class WrongPlatService(DemoService):
            router_prefix = "/wrongplat"

        with pytest.raises(
            ProgrammingError, match="expected argument of type.*PlatformProtocol"
        ):
            auth_check = cast(
                Callable[[Callable[..., None]], Callable[..., None]],
                WrongPlatService.compute.auth_check(),
            )

            @auth_check
            def bad_auth(
                self: "WrongPlatService",
                auth_ctx: AuthorizationContext,
                wrong: int,  # index 2: should be PlatformProtocol
            ) -> None:
                pass


class TestProcedureCallableWrappers:
    def test_set_route_handler_is_a_noop(self) -> None:
        """set_route_handler does nothing (interface hook)."""
        proc = DemoService.compute.procedure
        sentinel = mock.sentinel.handler
        proc.set_route_handler(cast(HTTPRouteHandler, sentinel))

    def test_get_authorized_callable_wraps_with_auth_check_for_authorized_transport(
        self,
    ) -> None:
        """AuthorizedTransport triggers auth_check.prepend_auth_check."""
        transport = DirectTransport.from_dsn("sqlite:///:memory:")
        session = transport.session

        auth_called: list[bool] = []

        class AuthDemoService(Service):
            router_prefix = "/authdemo"

            @procedure(Http(methods=("POST",)))
            def compute(self, value: int) -> int:
                return value * 2

            def __init_direct__(self, t: DirectTransport) -> None:
                pass

            def __init_httpx__(self, t: HttpxTransport) -> None:
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
            auth_ctx=cast(AuthorizationContext, SimpleNamespace(user="alice")),
            platform=cast(PlatformProtocol, SimpleNamespace(id="demo")),
        )
        svc = AuthDemoService(auth_transport)
        bound_func = mock.Mock(return_value=42)

        proc = AuthDemoService.compute.procedure
        callable_ = proc.get_authorized_callable(svc, bound_func)
        callable_(5)

        assert auth_called == [True]
        bound_func.assert_called_once_with(5)
        transport.close()

    def test_get_httpx_callable_returns_procedure_client(self) -> None:
        """get_httpx_callable returns a ProcedureClient."""
        from ixmp4.data.services.procedure.client import ProcedureClient

        svc = object.__new__(DemoService)
        svc.transport = FakeHttpxTransport()

        proc = DemoService.compute.procedure
        client = proc.get_httpx_callable(svc)
        assert isinstance(client, ProcedureClient)

    def test_auth_check_no_params_wrapper_is_called_with_extra_args(self) -> None:
        """The no-params wrapper ignores extra positional/keyword args."""

        class NpAuthService(Service):
            router_prefix = "/npauth"

            @procedure(Http(methods=("POST",)))
            def do_thing(self, value: int) -> int:
                return value

            def __init_direct__(self, transport: DirectTransport) -> None:
                pass

            def __init_httpx__(self, transport: HttpxTransport) -> None:
                pass

        called_with: list[tuple[Any, ...]] = []

        @NpAuthService.do_thing.auth_check()
        def do_thing_auth(
            self: "NpAuthService",
            auth_ctx: AuthorizationContext,
            platform: PlatformProtocol,
        ) -> None:
            called_with.append((self, auth_ctx, platform))

        proc = NpAuthService.do_thing.procedure
        svc = mock.Mock()
        ctx = mock.Mock()
        plat = mock.Mock()

        proc.auth_check.check_func(svc, ctx, plat, "extra-arg", extra_kwarg=True)
        assert len(called_with) == 1
        assert called_with[0] == (svc, ctx, plat)

    def test_auth_check_prepend_auth_check_calls_check_then_procedure(self) -> None:
        """With has_check=True the auth wrapper runs first."""

        class AuthOrderService(Service):
            router_prefix = "/authorder"

            @procedure(Http(methods=("POST",)))
            def act(self, x: int) -> int:
                return x

            def __init_direct__(self, t: DirectTransport) -> None:
                pass

            def __init_httpx__(self, t: HttpxTransport) -> None:
                pass

        call_log: list[str] = []

        @AuthOrderService.act.auth_check()
        def act_auth(
            self: "AuthOrderService",
            auth_ctx: AuthorizationContext,
            platform: PlatformProtocol,
        ) -> None:
            call_log.append("auth")

        proc = AuthOrderService.act.procedure

        def fake_proc(*args: Any, **kwargs: Any) -> int:
            call_log.append("proc")
            return 99

        svc = mock.Mock()
        ctx = mock.Mock()
        plat = mock.Mock()

        wrapped = proc.auth_check.prepend_auth_check(svc, ctx, plat, fake_proc)
        result = wrapped(1)
        assert call_log == ["auth", "proc"]
        assert result == 99


class TestProcedureDescriptor:
    def test_descriptor_direct_call_raises_programming_error(self) -> None:
        """Calling the descriptor directly raises ProgrammingError."""
        descriptor = DemoService.__dict__["compute"]
        with pytest.raises(ProgrammingError, match="cannot be called directly"):
            descriptor(5)

    def test_descriptor_get_raises_for_non_service_object(self) -> None:
        """Accessing descriptor on a non-Service instance raises ProgrammingError."""

        class NotAService:
            pass

        obj = NotAService()
        # Manually bind the descriptor to a non-Service owner
        descriptor = DemoService.__dict__["compute"]
        with pytest.raises(
            ProgrammingError, match="must be used as a descriptor for `Service`"
        ):
            descriptor.__get__(obj, NotAService)

    def test_descriptor_get_returns_httpx_callable_for_httpx_transport(self) -> None:
        """HttpxTransport triggers get_httpx_callable path."""
        from ixmp4.data.services.procedure.client import ProcedureClient

        svc = object.__new__(DemoService)
        svc.transport = FakeHttpxTransport()

        client = DemoService.compute.__get__(svc, DemoService)
        assert isinstance(client, ProcedureClient)

    def test_descriptor_get_raises_for_unknown_transport(self) -> None:
        """Unsupported transport class raises ProgrammingError."""

        class AliensTransport(Transport):
            pass

        svc = object.__new__(DemoService)
        svc.transport = AliensTransport()

        descriptor = DemoService.__dict__["compute"]
        with pytest.raises(ProgrammingError, match="is not supported"):
            descriptor.__get__(svc, DemoService)


class TestGenerateArgModel:
    def test_generate_arguments_model_raises_for_positional_only(self) -> None:
        """Positional-only params raise ProgrammingError."""
        sig = inspect.signature(lambda x: x)

        # Build a synthetic signature with a positional-only param
        params = [
            inspect.Parameter(
                "x", kind=inspect.Parameter.POSITIONAL_ONLY, annotation=int
            )
        ]
        sig = sig.replace(parameters=params, return_annotation=int)

        with pytest.raises(ProgrammingError, match="positional-only"):
            generate_arguments_model(
                sig,
                "TestModel",
                __module__=__name__,
                parameter_callback=lambda i, n, p: None,
            )

    def test_generate_arguments_model_handles_var_positional(self) -> None:
        """*args produces a list field in the model."""
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

    def test_generate_arguments_model_handles_plain_var_keyword(self) -> None:
        """**kwargs (no Unpack) sets model_config extra='allow'."""
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


class TestProcedurePagination:
    def test_pagination_validate_parameter_raises_for_wrong_annotation(self) -> None:
        """Index-1 param must be Pagination."""

        class PagErrService(DemoService):
            router_prefix = "/pagerr"

        with pytest.raises(
            ProgrammingError, match="expected argument of type.*AuthorizationContext"
        ):
            paginated = cast(
                Callable[[Callable[..., PaginatedResult[int]]], Callable[..., object]],
                PagErrService.compute.paginated(),
            )

            @paginated
            def paginated_compute(
                svc: "PagErrService",
                wrong: int,  # should be Pagination
            ) -> PaginatedResult[int]:
                return PaginatedResult(results=0, total=0, pagination=Pagination())


class TestProcedureRouteHandler:
    @pytest.fixture(scope="module")
    def demo_transport(self) -> Generator[DirectTransport, None, None]:
        t = DirectTransport.from_dsn("sqlite:///:memory:")
        yield t
        t.close()

    @pytest.fixture(scope="module")
    def demo_service(self, demo_transport: DirectTransport) -> DemoService:
        return DemoService(demo_transport)

    @pytest.fixture(scope="module")
    def paginated_service(
        self, demo_transport: DirectTransport
    ) -> PaginatedDemoService:
        return PaginatedDemoService(demo_transport)

    @pytest.fixture(scope="module")
    def compute_handler(self) -> ProcedureRouteHandler[Any, Any, Any]:
        return cast(
            ProcedureRouteHandler[Any, Any, Any],
            DemoService.compute.procedure.handlers[DemoService],
        )

    @pytest.fixture(scope="module")
    def rename_handler(self) -> ProcedureRouteHandler[Any, Any, Any]:
        return cast(
            ProcedureRouteHandler[Any, Any, Any],
            DemoService.rename.procedure.handlers[DemoService],
        )

    @pytest.fixture(scope="module")
    def list_handler(self) -> ProcedureRouteHandler[Any, Any, Any]:
        return cast(
            ProcedureRouteHandler[Any, Any, Any],
            PaginatedDemoService.list_items.procedure.handlers[PaginatedDemoService],
        )

    def test_route_handler_path_field_skipped_in_payload_model(
        self,
        rename_handler: ProcedureRouteHandler[Any, Any, Any],
    ) -> None:
        """Path params are excluded from the payload model."""
        # `id` is a path field; it must NOT appear in the payload model
        assert "id" not in rename_handler.payload_model.model_fields

    def test_route_handler_non_path_field_skipped_in_path_model(
        self,
        rename_handler: ProcedureRouteHandler[Any, Any, Any],
    ) -> None:
        """Non-path params are excluded from the path model."""
        # `name` is a payload field; it must NOT appear in the path model
        assert "name" not in rename_handler.path_model.model_fields

    def test_route_handler_build_call_args_with_body(
        self,
        compute_handler: ProcedureRouteHandler[Any, Any, Any],
    ) -> None:
        """build_call_args parses a JSON body for POST endpoints."""
        args, kwargs = compute_handler.build_call_args(
            path={}, query={}, body=b'{"value": 7}'
        )
        # `value` is positional in compute(self, value: int) -> lands in args
        assert args == (7,)

    def test_route_handler_build_call_args_with_empty_body(
        self,
        compute_handler: ProcedureRouteHandler[Any, Any, Any],
    ) -> None:
        """Empty body constructs empty payload_model (required fields still validated)."""
        # `compute` requires `value`, so empty body -> InvalidArguments
        with pytest.raises(InvalidArguments):
            compute_handler.build_call_args(path={}, query={}, body=b"")

    def test_route_handler_build_call_args_raises_for_invalid_json(
        self,
        compute_handler: ProcedureRouteHandler[Any, Any, Any],
    ) -> None:
        """Malformed body raises InvalidArguments."""
        with pytest.raises(InvalidArguments):
            compute_handler.build_call_args(
                path={}, query={}, body=b'{"value": "not-an-int"}'
            )

    def test_route_handler_build_call_args_with_path_params(
        self,
        rename_handler: ProcedureRouteHandler[Any, Any, Any],
    ) -> None:
        """Path and body params are merged correctly."""
        args, kwargs = rename_handler.build_call_args(
            path={"id": 3}, query={}, body=b'{"name": "foo"}'
        )
        # rename(self, id: int, name: str) has positional params -> land in args
        assert args == (3, "foo")

    def test_route_handler_handle_request(
        self,
        compute_handler: ProcedureRouteHandler[Any, Any, Any],
        demo_service: DemoService,
    ) -> None:
        """handle_request calls the procedure and returns a Response."""
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
        self,
        list_handler: ProcedureRouteHandler[Any, Any, Any],
    ) -> None:
        """get_pagination_params parses offset/limit from query dict."""
        pagination = list_handler.get_pagination_params({"limit": 10, "offset": 5})
        assert pagination.limit == 10
        assert pagination.offset == 5

    def test_route_handler_bind_endpoint_func_paginated(
        self,
        list_handler: ProcedureRouteHandler[Any, Any, Any],
        paginated_service: PaginatedDemoService,
    ) -> None:
        """Paginated procedure gets a pagination-bound callable."""
        bound = list_handler.bind_endpoint_func(
            paginated_service, {"limit": 10, "offset": 0}
        )
        result = bound(filter="")
        assert result.total == 1
