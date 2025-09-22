from concurrent import futures
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Concatenate,
    Protocol,
    TypeVar,
    Unpack,
    cast,
    overload,
)

import fastapi as fa
import pandas as pd
from toolkit.exceptions import ProgrammingError

from ixmp4.services import dto

from ..dto import PaginatedResult, Pagination
from .base import (
    HttpxTransport,
    IncompleteFastApiEndpointOptions,
    Params,
    ServiceProcedure,
    ServiceProcedureClient,
    ServiceT,
)

if TYPE_CHECKING:
    from .base import AbstractService

PaginationT = TypeVar("PaginationT", contravariant=True)
ResultT = TypeVar("ResultT", covariant=True)


class PaginatedFunction(Protocol[PaginationT, ResultT]):
    __name__: str

    def __call__(self, *args: Any, pagination: PaginationT, **kwds: Any) -> ResultT: ...


DefaultPaginatedFunction = PaginatedFunction[Pagination, PaginatedResult[Any]]

PaginatedReturnT = TypeVar("PaginatedReturnT", bound=list[Any] | pd.DataFrame)


class PaginatedProcedure(ServiceProcedure[ServiceT, Params, PaginatedReturnT]):
    paginated_func: DefaultPaginatedFunction

    def __init__(
        self,
        func: Callable[Concatenate[ServiceT, Params], PaginatedReturnT],
        fastapi_options: IncompleteFastApiEndpointOptions,
    ):
        super().__init__(func, fastapi_options)

    def register_endpoint(
        self, router: fa.APIRouter, svc_dep: Callable[..., Any]
    ) -> Callable[..., Any]:
        func_name = self.paginated_func.__name__
        endpoint_options = self.fastapi_options.copy()

        @router.api_route(**endpoint_options)
        def endpoint(
            svc: "AbstractService" = fa.Depends(svc_dep),
            pagination: Pagination = fa.Depends(),
            any_payload: dict[str, Any] = fa.Body(),
        ) -> Any:
            payload = self.payload_model(**any_payload).model_dump(exclude_none=True)
            svc_func = getattr(svc, func_name)
            args = self.build_args(self.signature, payload)
            kwargs = self.build_kwargs(self.signature, payload)
            result = svc_func(*args, pagination=pagination, **kwargs)
            return result

        return endpoint

    def get_client(
        self, service: AbstractService
    ) -> Callable[Params, PaginatedReturnT]:
        if not isinstance(service.transport, HttpxTransport):
            raise ProgrammingError(
                "Cannot instantiate http client for transport class "
                f"`{service.transport.__class__.__name__}`."
            )

        client: PaginatedServiceProcedureClient[
            AbstractService, Params, PaginatedReturnT
        ] = PaginatedServiceProcedureClient(
            self, service.transport, service.router_prefix
        )
        return client

    def paginated_procedure(
        self,
    ) -> Callable[
        [DefaultPaginatedFunction],
        DefaultPaginatedFunction,
    ]:
        def decorator(
            paginated_func: DefaultPaginatedFunction,
        ) -> DefaultPaginatedFunction:
            self.paginated_func = paginated_func
            return paginated_func

        return decorator


# raise ProgrammingError(
#     "`PaginatedProcedure` requires a paginated version "
#     "of the main function. Did you forget to use "
#     f"`@{name}.paginated_procedure()`?"
# )


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
        self, results: list[dto.DataFrame], result_type: type[dto.DataFrame]
    ) -> pd.DataFrame: ...

    @overload
    def merge_results(
        self, results: list[list[Any]], result_type: type[list[Any]]
    ) -> list[Any]: ...

    def merge_results(
        self, results: list[list[Any]] | list[dto.DataFrame], result_type: type[Any]
    ) -> list[Any] | pd.DataFrame:
        if issubclass(result_type, dto.DataFrame):
            return self.merge_dataframes(cast(list[dto.DataFrame], results))
        elif issubclass(result_type, list):
            return self.merge_lists(cast(list[list[Any]], results))
        else:
            raise ProgrammingError(
                f"Unable to merge paginated results of type `{result_type}`."
            )

    def merge_dataframes(self, results: list[dto.DataFrame]) -> pd.DataFrame:
        return pd.concat([res.to_pandas() for res in results])

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
