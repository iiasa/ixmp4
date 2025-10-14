import inspect
from concurrent import futures
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Concatenate,
    ParamSpec,
    Protocol,
    TypeVar,
    Unpack,
    cast,
    overload,
)

import fastapi as fa
import pandas as pd
from toolkit.exceptions import ProgrammingError

from ixmp4.rewrite.data.dataframe import SerializableDataFrame
from ixmp4.rewrite.data.pagination import PaginatedResult, Pagination

from .base import (
    HttpxTransport,
    IncompleteFastApiEndpointOptions,
    Params,
    Service,
    ServiceProcedure,
    ServiceProcedureClient,
    ServiceT,
)

if TYPE_CHECKING:
    from .base import AnyFuncArgs, Service

PaginationT = TypeVar("PaginationT", contravariant=True)
ResultT = TypeVar("ResultT", covariant=True)
FuncServiceT = TypeVar("FuncServiceT", contravariant=True)
FuncParams = ParamSpec("FuncParams")


class PaginatedFunction(Protocol[FuncServiceT, FuncParams, PaginationT, ResultT]):
    __name__: str

    def __call__(
        self,
        svc: FuncServiceT,
        pagination: PaginationT,
        /,
        *args: FuncParams.args,
        **kwds: FuncParams.kwargs,
    ) -> ResultT: ...


DefaultPaginatedFunction = PaginatedFunction[
    ServiceT, Params, Pagination, PaginatedResult[Any]
]

PaginatedReturnT = TypeVar("PaginatedReturnT", bound=list[Any] | SerializableDataFrame)


class PaginatedProcedure(ServiceProcedure[ServiceT, Params, PaginatedReturnT]):
    paginated_func: DefaultPaginatedFunction[ServiceT, Params]
    paginated_signature: inspect.Signature

    def __init__(
        self,
        func: Callable[Concatenate[ServiceT, Params], PaginatedReturnT],
        fastapi_options: IncompleteFastApiEndpointOptions,
    ):
        super().__init__(func, fastapi_options)

    def register_endpoint(
        self, router: fa.APIRouter, svc_dep: Callable[..., Any]
    ) -> None:
        endpoint_options = self.fastapi_options.copy()
        endpoint_options.setdefault(
            "response_model", self.paginated_signature.return_annotation
        )

        @router.api_route(**endpoint_options)
        def endpoint(
            params: "AnyFuncArgs" = fa.Depends(self.get_endpoint_dependency()),
            svc: "Service" = fa.Depends(svc_dep),
            pagination: Pagination = fa.Query(),
        ) -> Any:
            args, kwargs = params
            svc_func = self.get_service_func(svc)
            result = svc_func(pagination, *args, **kwargs)
            return result

    def get_client(self, service: Service) -> Callable[Params, PaginatedReturnT]:
        if not isinstance(service.transport, HttpxTransport):
            raise ProgrammingError(
                "Cannot instantiate http client for transport class "
                f"`{service.transport.__class__.__name__}`."
            )

        client: PaginatedServiceProcedureClient[ServiceT, Params, PaginatedReturnT] = (
            PaginatedServiceProcedureClient(
                self, service.transport, service.router_prefix
            )
        )
        return client

    def paginated(
        self,
    ) -> Callable[
        [DefaultPaginatedFunction[ServiceT, Params]],
        DefaultPaginatedFunction[ServiceT, Params],
    ]:
        def decorator(
            paginated_func: DefaultPaginatedFunction[ServiceT, Params],
        ) -> DefaultPaginatedFunction[ServiceT, Params]:
            self.paginated_func = paginated_func
            self.paginated_signature = self.get_signature(paginated_func)
            return paginated_func

        return decorator

    def check_paginated_func(self) -> None:
        if getattr(self, "paginated_func", None) is None:
            raise ProgrammingError(
                "`PaginatedProcedure` requires a paginated version "
                "of the main function. Did you forget to use "
                f"`@{self.func.__name__}.paginated_procedure()`?"
            )

    def get_service_func(self, svc: Service):
        return getattr(svc, self.paginated_func.__name__)


class PaginatedServiceProcedureClient(
    ServiceProcedureClient[ServiceT, Params, PaginatedReturnT]
):
    procedure: "PaginatedProcedure[ServiceT, Params, PaginatedReturnT]"
    transport: HttpxTransport
    path: str
    method: str

    def __init__(
        self,
        procedure: "PaginatedProcedure[ServiceT, Params, PaginatedReturnT]",
        transport: HttpxTransport,
        url_prefix: str,
    ):
        self.procedure = procedure
        self.transport = transport
        self.path = url_prefix + procedure.fastapi_options["path"]
        self.method = procedure.fastapi_options["methods"][0]

    def __call__(self, *args: Params.args, **kwargs: Params.kwargs) -> PaginatedReturnT:
        payload = self.build_payload(self.procedure.signature, args, kwargs)
        response = self.transport.client.request(self.method, self.path, json=payload)
        paginated_result = PaginatedResult(**response.json())
        result_items = [paginated_result.results]
        result_type = type(paginated_result.results)

        if paginated_result.total <= (
            paginated_result.pagination.offset + paginated_result.pagination.limit
        ):
            # TODO: We could check if the `total` changed
            # since we started the pagination...
            result_items += self.dispatch_pagination_requests(
                paginated_result.total,
                paginated_result.pagination.limit,
                paginated_result.pagination.limit,
                json=payload,
            )

        return cast(PaginatedReturnT, self.merge_results(result_items, result_type))

    @overload
    def merge_results(
        self,
        results: list[SerializableDataFrame],
        result_type: type[SerializableDataFrame],
    ) -> pd.DataFrame: ...

    @overload
    def merge_results(
        self, results: list[list[Any]], result_type: type[list[Any]]
    ) -> list[Any]: ...

    def merge_results(
        self,
        results: list[list[Any]] | list[SerializableDataFrame],
        result_type: type[Any],
    ) -> list[Any] | pd.DataFrame:
        if issubclass(result_type, SerializableDataFrame):
            return self.merge_dataframes(cast(list[SerializableDataFrame], results))
        elif issubclass(result_type, list):
            return self.merge_lists(cast(list[list[Any]], results))
        else:
            raise ProgrammingError(
                f"Unable to merge paginated results of type `{result_type}`."
            )

    def merge_dataframes(self, results: list[SerializableDataFrame]) -> pd.DataFrame:
        return pd.concat(results)

    def merge_lists(self, results: list[list[Any]]) -> list[Any]:
        return [i for page in results for i in page]

    def dispatch_pagination_requests(
        self,
        total: int,
        start: int,
        limit: int,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> list[list[Any]] | list[dict[str, Any]]:
        requests: list[futures.Future[dict[str, Any]]] = []
        for req_offset in range(start, total, limit):
            req_params = params.copy() if params is not None else {}

            req_params.update({"limit": limit, "offset": req_offset})
            future: futures.Future[dict[str, Any]] = self.transport.executor.submit(
                self.transport.client.request,  # type: ignore [arg-type]
                self.method,
                self.path,
                params=req_params,
                json=json,
            )
            requests.append(future)
        results = futures.wait(requests)
        responses = [f.result() for f in results.done]
        return [r.pop("results") for r in responses]


def paginated_procedure(
    **kwargs: Unpack[IncompleteFastApiEndpointOptions],
) -> Callable[
    [Callable[Concatenate[ServiceT, Params], PaginatedReturnT]],
    PaginatedProcedure[ServiceT, Params, PaginatedReturnT],
]:
    def decorator(
        func: Callable[Concatenate[ServiceT, Params], PaginatedReturnT],
    ) -> PaginatedProcedure[ServiceT, Params, PaginatedReturnT]:
        return PaginatedProcedure(func, kwargs)

    return decorator
