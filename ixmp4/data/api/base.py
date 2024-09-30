import logging
import time
from concurrent import futures
from json.decoder import JSONDecodeError
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Generic,
    Type,
    TypeVar,
)

import httpx
import pandas as pd
from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict, Field, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema

from ixmp4.conf import settings
from ixmp4.core.exceptions import (
    ApiEncumbered,
    ImproperlyConfigured,
    IxmpError,
    UnknownApiError,
    registry,
)

if TYPE_CHECKING:
    from ixmp4.data.backend.api import RestBackend

logger = logging.getLogger(__name__)


class BaseModel(PydanticBaseModel):
    NotFound: ClassVar[type[IxmpError]]
    NotUnique: ClassVar[type[IxmpError]]

    model_config = ConfigDict(from_attributes=True)


def df_to_dict(df: pd.DataFrame) -> dict:
    columns = []
    dtypes = []
    for c in df.columns:
        columns.append(c)
        dtypes.append(df[c].dtype.name)

    return {
        "index": df.index.to_list(),
        "columns": columns,
        "dtypes": dtypes,
        "data": df.values.tolist(),
    }


class DataFrame(PydanticBaseModel):
    index: list | None = Field(None)
    columns: list[str] | None
    dtypes: list[str] | None
    data: list | None

    model_config = ConfigDict(json_encoders={pd.Timestamp: lambda x: x.isoformat()})

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_before_validator_function(cls.validate, handler(cls))
        # yield cls.validate

    @classmethod
    def validate(cls, df: pd.DataFrame | dict):
        if isinstance(df, pd.DataFrame):
            return df_to_dict(df)
        else:
            return df

    def to_pandas(self) -> pd.DataFrame:
        df = pd.DataFrame(
            index=self.index or None,
            columns=self.columns,
            data=self.data,
        )
        if self.columns and self.dtypes:
            for c, dt in zip(self.columns, self.dtypes):
                # there seems to be a type incompatbility between StrDtypeArg and str
                df[c] = df[c].astype(dt)  # type: ignore
        return df


ModelType = TypeVar("ModelType", bound=BaseModel)


class BaseRepository(Generic[ModelType]):
    model_class: Type[ModelType]
    prefix: ClassVar[str]
    enumeration_method: str = "PATCH"

    backend: "RestBackend"

    def __init__(self, backend: "RestBackend", *args, **kwargs) -> None:
        self.backend = backend

    def sanitize_params(self, params: dict):
        return {k: params[k] for k in params if params[k] is not None}

    def get_remote_exception(self, res: httpx.Response, status_code: int):
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
        params: dict | None = None,
        json: dict | None = None,
        max_retries: int = settings.client_max_request_retries,
        **kwargs,
    ) -> dict | list | None:
        """Sends a request and handles potential error responses.
        Re-raises a remote `IxmpError` if thrown and transferred from the backend.
        Handles read timeouts and rate limiting responses via retries with backoffs.
        Returns `None` if the response body is empty
        but has a status code less than 300.
        """

        def retry(max_retries=max_retries) -> dict | list | None:
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
        retry: Callable[..., dict | list | None],
    ) -> dict | list | None:
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
                return res.json()
            except JSONDecodeError:
                if res.status_code < 300 and res.text == "":
                    return None
            except ValueError:
                pass
            raise UnknownApiError(res.text)

    def _get_by_id(self, id: int, *args, **kwargs) -> dict[str, Any]:
        # we can assume this type on create endpoints
        return self._request("GET", self.prefix + str(id) + "/", **kwargs)  # type: ignore

    def _request_enumeration(
        self,
        table: bool = False,
        params: dict | None = None,
        json: dict | None = None,
    ):
        """Convenience method for requests to the enumeration endpoint."""
        if params is None:
            params = {}

        return self._request(
            self.enumeration_method,
            self.prefix,
            params={**params, "table": table},
            json=json,
        )

    def _dispatch_pagination_requests(
        self,
        total: int,
        start: int,
        limit: int,
        params: dict | None,
        json: dict | None,
    ) -> list[list | dict]:
        """Uses the backends executor to send many pagination requests concurrently."""
        requests: list[futures.Future] = []
        for req_offset in range(start, total, limit):
            if params is not None:
                req_params = params.copy()
            else:
                req_params = {}

            req_params.update({"limit": limit, "offset": req_offset})
            futu = self.backend.executor.submit(
                self._request,
                self.enumeration_method,
                self.prefix,
                params=req_params,
                json=json,
            )
            requests.append(futu)
        results = futures.wait(requests)
        responses = [f.result() for f in results.done]
        return [r.pop("results") for r in responses]

    def _handle_pagination(
        self,
        data: dict,
        table: bool = False,
        params: dict | None = None,
        json: dict | None = None,
    ) -> list[list] | list[dict]:
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
                total, offset + limit, limit, params, json
            )
            return [data.pop("results")] + results

    def _list(
        self, params: dict | None = None, json: dict | None = None, **kwargs
    ) -> list[ModelType]:
        data = self._request_enumeration(params=params, table=False, json=json)
        if isinstance(data, dict):
            # we can assume this type on list endpoints
            pages: list[list] = self._handle_pagination(
                data, table=False, params=params, json=json
            )  # type: ignore
            results = [i for page in pages for i in page]
        else:
            results = data
        return [self.model_class(**i) for i in results]

    def _tabulate(
        self, params: dict | None = {}, json: dict | None = None, **kwargs
    ) -> pd.DataFrame:
        data = self._request_enumeration(table=True, params=params, json=json)
        pagination = data.get("pagination", None)
        if pagination is not None:
            # we can assume this type on table endpoints
            pages: list[dict] = self._handle_pagination(
                data,
                table=True,
                params=params,
                json=json,
            )  # type: ignore
            dfs = [DataFrame(**page).to_pandas() for page in pages]
            return pd.concat(dfs)
        else:
            return DataFrame(**data).to_pandas()

    def _create(self, *args, **kwargs) -> dict[str, Any]:
        # we can assume this type on create endpoints
        return self._request("POST", *args, **kwargs)  # type: ignore

    def _delete(self, id: int):
        self._request("DELETE", f"{self.prefix}{str(id)}/")


class Retriever(BaseRepository[ModelType]):
    def get(self, **kwargs) -> ModelType:
        if self.enumeration_method == "GET":
            list_ = self._list(params=kwargs)
        else:
            list_ = self._list(json=kwargs)

        try:
            [obj] = list_
        except ValueError as e:
            raise self.model_class.NotFound(
                f"Expected exactly one result, got {len(list_)} instead."
            ) from e
        return obj


class Creator(BaseRepository[ModelType]):
    def create(self, **kwargs) -> ModelType:
        res = self._create(
            self.prefix,
            json=kwargs,
        )
        return self.model_class(**res)


class Deleter(BaseRepository[ModelType]):
    def delete(self, id: int):
        self._delete(id)


class Lister(BaseRepository[ModelType]):
    def list(self, *args, **kwargs) -> list[ModelType]:
        return self._list(json=kwargs)


class Tabulator(BaseRepository[ModelType]):
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return self._tabulate(json=kwargs)


class Enumerator(Lister[ModelType], Tabulator[ModelType]):
    def enumerate(
        self, *args, table: bool = False, **kwargs
    ) -> list[ModelType] | pd.DataFrame:
        if table:
            return self.tabulate(*args, **kwargs)
        else:
            return self.list(*args, **kwargs)


class BulkOperator(BaseRepository[ModelType]):
    def yield_chunks(self, df: pd.DataFrame, chunk_size: int):
        for _, chunk in df.groupby(df.index // chunk_size):
            yield chunk


class BulkUpserter(BulkOperator[ModelType]):
    def bulk_upsert(
        self,
        df: pd.DataFrame,
        chunk_size: int = settings.client_default_upload_chunk_size,
        **kwargs,
    ):
        for chunk in self.yield_chunks(df, chunk_size):
            self.bulk_upsert_chunk(chunk, **kwargs)

    def bulk_upsert_chunk(self, df: pd.DataFrame, **kwargs) -> None:
        dict_ = df_to_dict(df)
        json_ = DataFrame(**dict_).model_dump_json()
        self._request(
            "POST",
            self.prefix + "bulk/",
            params=kwargs,
            content=json_,
        )


class BulkDeleter(BulkOperator[ModelType]):
    def bulk_delete(
        self,
        df: pd.DataFrame,
        chunk_size: int = settings.client_default_upload_chunk_size,
        **kwargs,
    ):
        for chunk in self.yield_chunks(df, chunk_size):
            self.bulk_delete_chunk(chunk, **kwargs)

    def bulk_delete_chunk(self, df: pd.DataFrame, **kwargs) -> None:
        dict_ = df_to_dict(df)
        json_ = DataFrame(**dict_).model_dump_json()
        self._request(
            "PATCH",
            self.prefix + "bulk/",
            params=kwargs,
            content=json_,
        )
