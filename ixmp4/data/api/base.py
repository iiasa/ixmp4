import logging
import time
from collections.abc import Callable, Generator, Iterable, Mapping
from concurrent import futures
from datetime import datetime
from json.decoder import JSONDecodeError

# TODO Use `type` instead of TypeAlias when dropping Python 3.11
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeAlias, TypeVar, cast

import httpx
import numpy as np
import pandas as pd
from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict, Field, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import TypedDict, Unpack

from ixmp4.conf import settings
from ixmp4.core.exceptions import (
    ApiEncumbered,
    ImproperlyConfigured,
    IxmpError,
    UnknownApiError,
    registry,
)
from ixmp4.data import abstract

if TYPE_CHECKING:
    from ixmp4.data.backend.api import RestBackend

logger = logging.getLogger(__name__)

JsonType: TypeAlias = Mapping[
    str,
    Iterable[float]
    | Iterable[int]
    | Iterable[str]
    | Mapping[str, Any]
    | abstract.annotations.PrimitiveTypes
    | None,
]
ParamType: TypeAlias = dict[
    str, bool | int | str | list[int] | Iterable[int] | Mapping[str, Any] | None
]
_RequestParamType: TypeAlias = Mapping[
    str,
    abstract.annotations.PrimitiveTypes
    | abstract.annotations.PrimitiveIterableTypes
    | Mapping[str, Any]
    | None,
]


class BaseModel(PydanticBaseModel):
    NotFound: ClassVar[type[IxmpError]]
    NotUnique: ClassVar[type[IxmpError]]

    model_config = ConfigDict(from_attributes=True)


class DataFrameDict(TypedDict):
    index: list[int] | list[str]
    columns: list[str]
    dtypes: list[str]
    # NOTE This is deliberately slightly out of sync with DataFrame.data below
    # (cf positioning of int and float) to demonstrate that only the one below seems to
    # affect our tests by causing ValidationErrors
    data: list[
        list[
            abstract.annotations.PrimitiveTypes
            | datetime
            | dict[str, Any]
            | list[float]
            | list[int]
            | list[str]
            | None
        ]
    ]


def df_to_dict(df: pd.DataFrame) -> DataFrameDict:
    columns = []
    dtypes = []
    for c in df.columns:
        columns.append(c)
        dtypes.append(df[c].dtype.name)

    return DataFrameDict(
        index=df.index.to_list(),
        columns=columns,
        dtypes=dtypes,
        # https://github.com/numpy/numpy/issues/27944
        data=df.values.tolist(),  # type: ignore[arg-type]
    )


class DataFrame(PydanticBaseModel):
    index: list[int] | list[str] | None = Field(None)
    columns: list[str] | None
    dtypes: list[str] | None
    # TODO The order is important here at the moment, in particular having int before
    # float! This should likely not be the case, but using StrictInt and StrictFloat
    # from pydantic only created even more errors.
    data: (
        list[
            list[
                bool
                | datetime
                | int
                | float
                | str
                | dict[str, Any]
                | list[float]
                | list[int]
                | list[str]
                | None
            ]
        ]
        | None
    )

    model_config = ConfigDict(json_encoders={pd.Timestamp: lambda x: x.isoformat()})

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_before_validator_function(cls.validate, handler(cls))

    @classmethod
    def validate(cls, df: pd.DataFrame | DataFrameDict) -> DataFrameDict:
        return df_to_dict(df) if isinstance(df, pd.DataFrame) else df

    def to_pandas(self) -> pd.DataFrame:
        df = pd.DataFrame(
            index=self.index or None,
            columns=self.columns,
            data=self.data,
        )
        if self.columns and self.dtypes:
            for c, dt in zip(self.columns, self.dtypes):
                # there seems to be a type incompatbility between StrDtypeArg and str
                df[c] = df[c].astype(dt)  # type: ignore[call-overload]
        return df


class _RequestKwargs(TypedDict, total=False):
    params: _RequestParamType | None
    json: JsonType | None
    max_retries: int


class RequestKwargs(TypedDict, total=False):
    content: str


ModelType = TypeVar("ModelType", bound=BaseModel)


class BaseRepository(Generic[ModelType]):
    model_class: type[ModelType]
    prefix: ClassVar[str]
    enumeration_method: str = "PATCH"

    backend: "RestBackend"

    def __init__(self, backend: "RestBackend") -> None:
        self.backend = backend

    def sanitize_params(self, params: Mapping[str, Any]) -> dict[str, Any]:
        return {k: params[k] for k in params if params[k] is not None}

    def get_remote_exception(self, res: httpx.Response, status_code: int) -> IxmpError:
        try:
            json = res.json()
        except (ValueError, JSONDecodeError):
            raise UnknownApiError(content=res.text, response_status=res.status_code)

        try:
            error_name = json["error_name"]
        except KeyError:
            raise UnknownApiError(json_data=json, response_status=status_code)

        try:
            exc = registry[error_name]
        except KeyError:
            raise ImproperlyConfigured(
                "Could not find remote exception in registry. "
                "Are you sure client and server ixmp versions are compatible?"
            )
        return exc.from_dict(json)

    def _request(
        self,
        method: str,
        path: str,
        params: _RequestParamType | None = None,
        json: JsonType | None = None,
        max_retries: int = settings.client_max_request_retries,
        **kwargs: Unpack[RequestKwargs],
    ) -> dict[str, Any] | list[Any] | None:
        """Sends a request and handles potential error responses.
        Re-raises a remote `IxmpError` if thrown and transferred from the backend.
        Handles read timeouts and rate limiting responses via retries with backoffs.
        Returns `None` if the response body is empty
        but has a status code less than 300.
        """

        def retry(max_retries: int = max_retries) -> dict[str, Any] | list[Any] | None:
            if max_retries == 0:
                logger.error(f"API Encumbered: '{self.backend.info.dsn}'")
                raise ApiEncumbered(
                    f"The service connected to the backend '{self.backend.info.name}' "
                    "is currently overencumbered with requests. "
                    "Try again at a later time or re-configure your client "
                    "to behave more leniently."
                )
            # increase backoff by `settings.client_backoff_factor`
            # for each retry
            backoff_s = settings.client_backoff_factor * (
                settings.client_max_request_retries - max_retries
            )
            logger.debug(f"Retrying request in {backoff_s} seconds.")
            time.sleep(backoff_s)
            return self._request(
                method,
                path,
                params=params,
                json=json,
                max_retries=max_retries - 1,
                **kwargs,
            )

        params = self.sanitize_params(params) if params else {}

        try:
            res = self.backend.client.request(
                method,
                path,
                params=params,
                json=json,
                **kwargs,
            )
        except httpx.ReadTimeout:
            logger.warning("Read timeout, retrying request...")
            return retry()

        return self._handle_response(res, retry)

    def _handle_response(
        self,
        res: httpx.Response,
        retry: Callable[..., dict[str, Any] | list[Any] | None],
    ) -> dict[str, Any] | list[Any] | None:
        if res.status_code in [
            429,  # Too Many Requests
            420,  # Enhance Your Calm
        ]:
            return retry()
        elif res.status_code >= 400:
            if res.status_code == 413:
                raise ImproperlyConfigured(
                    "Received status code 413 (Payload Too Large). "
                    "Consider decreasing `IXMP4_CLIENT_DEFAULT_UPLOAD_CHUNK_SIZE` "
                    f"(current: {settings.client_default_upload_chunk_size})."
                )
            raise self.get_remote_exception(res, res.status_code)
        else:
            try:
                # res.json just returns Any...
                json_decoded: dict[str, Any] | list[Any] = res.json()
                return json_decoded
            except JSONDecodeError:
                if res.status_code < 300 and res.text == "":
                    return None
            except ValueError:
                pass
            raise UnknownApiError(res.text)

    def _get_by_id(self, id: int) -> dict[str, Any]:
        # we can assume this type on create endpoints
        return self._request("GET", self.prefix + str(id) + "/")  # type: ignore[return-value]

    def _request_enumeration(
        self,
        table: bool = False,
        params: ParamType | None = None,
        json: JsonType | None = None,
        path: str = "",
    ) -> dict[str, Any] | list[Any]:
        """Convenience method for requests to the enumeration endpoint."""
        if params is None:
            params = {}

        # See https://github.com/iiasa/ixmp4/pull/129#discussion_r1841829519 for why we
        # are keeping this assumption
        # we can assume these types on enumeration endpoints
        return self._request(
            self.enumeration_method,
            self.prefix + path,
            params={**params, "table": table},
            json=json,
        )  # type: ignore[return-value]

    def _dispatch_pagination_requests(
        self,
        total: int,
        start: int,
        limit: int,
        params: ParamType | None,
        json: JsonType | None,
        path: str = "",
    ) -> list[list[Any]] | list[dict[str, Any]]:
        """Uses the backends executor to send many pagination requests concurrently."""
        requests: list[futures.Future[dict[str, Any]]] = []
        for req_offset in range(start, total, limit):
            req_params = params.copy() if params is not None else {}

            req_params.update({"limit": limit, "offset": req_offset})
            # Based on usage below, we seem to rely on self._request always returning a
            # dict[str, Any] here
            futu: futures.Future[dict[str, Any]] = self.backend.executor.submit(
                self._request,  # type: ignore [arg-type]
                self.enumeration_method,
                self.prefix + path,
                params=req_params,
                json=json,
            )
            requests.append(futu)
        results = futures.wait(requests)
        responses = [f.result() for f in results.done]
        # This seems to imply that type(responses) == list[dict[str, Any]]
        return [r.pop("results") for r in responses]

    def _handle_pagination(
        self,
        data: dict[str, Any],
        table: bool = False,
        params: ParamType | None = None,
        json: JsonType | None = None,
        path: str = "",
    ) -> list[list[Any]] | list[dict[str, Any]]:
        """Handles paginated response and sends subsequent requests if necessary.
        Returns aggregated pages as a list."""

        if params is None:
            params = {"table": table}
        else:
            params["table"] = table

        total = data.pop("total")
        pagination = data.pop("pagination")
        offset = pagination.pop("offset")
        limit = pagination.pop("limit")
        if total <= offset + limit:
            return [data.pop("results")]
        else:
            results = self._dispatch_pagination_requests(
                total, offset + limit, limit, params, json, path=path
            )
            return [data.pop("results")] + results

    def _list(
        self,
        params: ParamType | None = None,
        json: JsonType | None = None,
        path: str = "",
    ) -> list[ModelType]:
        data = self._request_enumeration(params=params, table=False, json=json)
        if isinstance(data, dict):
            # we can assume this type on list endpoints
            pages: list[list[Any]] = self._handle_pagination(
                data, table=False, params=params, json=json
            )  # type: ignore[assignment]
            results = [i for page in pages for i in page]
        else:
            results = data
        return [self.model_class(**i) for i in results]

    def _tabulate(
        self,
        params: ParamType | None = {},
        json: JsonType | None = None,
        path: str = "",
    ) -> pd.DataFrame:
        # we can assume this type on table endpoints
        data: dict[str, Any] = self._request_enumeration(
            table=True, params=params, json=json, path=path
        )  # type: ignore[assignment]
        pagination = data.get("pagination", None)
        if pagination is not None:
            # we can assume this type on table endpoints
            pages: list[dict[str, Any]] = self._handle_pagination(
                data, table=True, params=params, json=json, path=path
            )  # type: ignore[assignment]
            dfs = [DataFrame(**page).to_pandas() for page in pages]
            return pd.concat(dfs).replace({np.nan: None})
        else:
            return DataFrame(**data).to_pandas().replace({np.nan: None})

    def _create(
        self, *args: Unpack[tuple[str]], **kwargs: Unpack[_RequestKwargs]
    ) -> dict[str, Any]:
        # we can assume this type on create endpoints
        return self._request("POST", *args, **kwargs)  # type: ignore[return-value]

    def _delete(self, id: int) -> None:
        self._request("DELETE", f"{self.prefix}{str(id)}/")


class GetKwargs(TypedDict, total=False):
    dimension_id: int
    run_ids: list[int]
    parameters: Mapping[str, Any]
    name: str
    run_id: int
    key: str
    model: dict[str, str]
    scenario: dict[str, str]
    version: int
    default_only: bool
    is_default: bool | None


class Retriever(BaseRepository[ModelType]):
    def get(self, **kwargs: Unpack[GetKwargs]) -> ModelType:
        _kwargs = cast(
            dict[
                str,
                bool | int | str | list[int] | Iterable[int] | Mapping[str, Any] | None,
            ],
            kwargs,
        )
        list_ = (
            self._list(params=_kwargs)
            if self.enumeration_method == "GET"
            else self._list(json=_kwargs)
        )

        try:
            [obj] = list_
        except ValueError as e:
            raise self.model_class.NotFound(
                f"Expected exactly one result, got {len(list_)} instead."
            ) from e
        return obj


class Creator(BaseRepository[ModelType]):
    def create(
        self,
        **kwargs: int
        | str
        | Mapping[str, Any]
        | abstract.MetaValue
        | list[str]
        | float
        | None,
    ) -> ModelType:
        res = self._create(self.prefix, json=kwargs)
        return self.model_class(**res)


class Deleter(BaseRepository[ModelType]):
    def delete(self, id: int) -> None:
        self._delete(id)


class ListKwargs(TypedDict, total=False):
    run_id: int
    name: str


class Lister(BaseRepository[ModelType]):
    def list(self, **kwargs: Unpack[ListKwargs]) -> list[ModelType]:
        return self._list(json=kwargs)  # type: ignore[arg-type]


class Tabulator(BaseRepository[ModelType]):
    def tabulate(self, **kwargs: Any) -> pd.DataFrame:
        return self._tabulate(json=kwargs)


class Enumerator(Lister[ModelType], Tabulator[ModelType]):
    def enumerate(
        self, table: bool = False, **kwargs: Any
    ) -> list[ModelType] | pd.DataFrame:
        return self.tabulate(**kwargs) if table else self.list(**kwargs)


class BulkOperator(BaseRepository[ModelType]):
    def yield_chunks(
        self, df: pd.DataFrame, chunk_size: int
    ) -> Generator[pd.DataFrame, Any, None]:
        for _, chunk in df.groupby(df.index // chunk_size):
            yield chunk


class BulkUpsertKwargs(TypedDict, total=False):
    create_related: bool


class BulkUpserter(BulkOperator[ModelType]):
    def bulk_upsert(
        self,
        df: pd.DataFrame,
        chunk_size: int = settings.client_default_upload_chunk_size,
        **kwargs: Unpack[BulkUpsertKwargs],
    ) -> None:
        for chunk in self.yield_chunks(df, chunk_size):
            self.bulk_upsert_chunk(chunk, **kwargs)

    def bulk_upsert_chunk(
        self, df: pd.DataFrame, **kwargs: Unpack[BulkUpsertKwargs]
    ) -> None:
        dict_ = df_to_dict(df)
        json_ = DataFrame(**dict_).model_dump_json()
        self._request(
            "POST",
            self.prefix + "bulk/",
            params=cast(dict[str, bool | None], kwargs),
            content=json_,
        )


class BulkDeleter(BulkOperator[ModelType]):
    def bulk_delete(
        self,
        df: pd.DataFrame,
        chunk_size: int = settings.client_default_upload_chunk_size,
        # NOTE nothing in our code base supplies kwargs here
        **kwargs: Any,
    ) -> None:
        for chunk in self.yield_chunks(df, chunk_size):
            self.bulk_delete_chunk(chunk, **kwargs)

    # NOTE this only gets kwargs from bulk_delete()
    def bulk_delete_chunk(self, df: pd.DataFrame, **kwargs: Any) -> None:
        dict_ = df_to_dict(df)
        json_ = DataFrame(**dict_).model_dump_json()
        self._request(
            "PATCH",
            self.prefix + "bulk/",
            params=kwargs,
            content=json_,
        )


class VersionManager(BaseRepository[ModelType]):
    def tabulate_versions(
        self, transaction__id: int | None = None, **kwargs: Any
    ) -> pd.DataFrame:
        return self._tabulate(
            path="versions/", json={"transaction__id": transaction__id, **kwargs}
        )
