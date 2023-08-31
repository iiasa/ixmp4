from json.decoder import JSONDecodeError
from typing import Any, ClassVar, Generic, Iterable, Mapping, Sequence, Type, TypeVar

import httpx
import pandas as pd
from pydantic import BaseModel as PydanticBaseModel

from ixmp4.core.exceptions import (
    ImproperlyConfigured,
    IxmpError,
    UnknownApiError,
    registry,
)


class BaseModel(PydanticBaseModel):
    NotFound: ClassVar[type[IxmpError]]
    NotUnique: ClassVar[type[IxmpError]]

    class Config:
        orm_mode = True


class DataFrame(PydanticBaseModel):
    index: list | None
    columns: list[str]
    dtypes: list[str]
    data: list

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, df: pd.DataFrame | dict):
        if isinstance(df, pd.DataFrame):
            return cls.from_pandas(df)
        else:
            return cls(**df)

    class Config:
        json_encoders = {pd.Timestamp: lambda x: x.isoformat()}

    @classmethod
    def from_pandas(cls, df: pd.DataFrame) -> "DataFrame":
        columns = []
        dtypes = []
        for c in df.columns:
            columns.append(c)
            dtypes.append(df[c].dtype.name)

        return DataFrame(
            index=df.index.tolist(),
            columns=columns,
            dtypes=dtypes,
            data=df.values.tolist(),
        )

    def to_pandas(self) -> pd.DataFrame:
        df = pd.DataFrame(
            index=self.index or None,
            columns=self.columns,
            data=self.data,
        )
        for c, dt in zip(self.columns, self.dtypes):
            # there seems to be a type incompatbility between StrDtypeArg and str
            df[c] = df[c].astype(dt)  # type: ignore
        return df


ModelType = TypeVar("ModelType", bound=BaseModel)


class BaseRepository(Generic[ModelType]):
    model_class: Type[ModelType]
    prefix: ClassVar[str]
    enumeration_method: str = "GET"

    client: httpx.Client

    def __init__(self, client: httpx.Client, *args, **kwargs) -> None:
        self.client = client

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

    def _request(self, method: str, path: str, *args, **kwargs) -> Mapping | Sequence:
        res = self.client.request(method, path, *args, **kwargs)

        if res.status_code >= 400:
            raise self.get_remote_exception(res, res.status_code)
        else:
            try:
                return res.json()
            except (ValueError, JSONDecodeError):
                return res.text

    def _get_by_id(self, id: int, *args, **kwargs) -> Mapping[str, Any]:
        # we can assume this type on create endpoints
        return self._request("GET", self.prefix + str(id) + "/", **kwargs)  # type: ignore

    def _enumerate(self, *args, table: bool = False, **kwargs) -> Mapping | Sequence:
        json = None
        params = {}
        params["table"] = table
        join_parameters = kwargs.pop("join_parameters", None)
        join_runs = kwargs.pop("join_runs", None)

        if join_parameters is not None:
            params["join_parameters"] = join_parameters
        if join_runs is not None:
            params["join_runs"] = join_runs

        if self.enumeration_method == "GET":
            params.update(kwargs)
        else:
            json = kwargs
        # we can assume this type on list endpoints
        return self._request(
            self.enumeration_method,
            self.prefix,
            params=self.sanitize_params(params),
            json=json,
        )

    def _list(self, **kwargs) -> Sequence[Mapping[str, Any]]:
        # we can assume this type on list endpoints
        return self._enumerate(table=False, **kwargs)  # type: ignore

    def _tabulate(self, **kwargs) -> pd.DataFrame:
        # we can assume this type on table endpoints
        jdf: Mapping[str, Any] = self._enumerate(table=True, **kwargs)  # type: ignore
        return DataFrame(**jdf).to_pandas()

    def _create(self, *args, **kwargs) -> Mapping[str, Any]:
        # we can assume this type on create endpoints
        return self._request("POST", *args, **kwargs)  # type: ignore

    def _delete(self, id: int):
        self._request("DELETE", self.prefix + str(id))


class Retriever(BaseRepository[ModelType]):
    def get(self, *args, **kwargs) -> ModelType:
        list_ = self._list(*args, **kwargs)

        try:
            [obj] = list_
        except ValueError as e:
            raise self.model_class.NotFound(
                f"Expected exactly one result, got {len(list_)} instead."
            ) from e
        return self.model_class(**obj)


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
    def list(self, *args, **kwargs) -> Iterable[ModelType]:
        return [
            self.model_class(**obj)
            for obj in self._list(
                **kwargs,
            )
        ]


class Tabulator(BaseRepository[ModelType]):
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return self._tabulate(*args, **kwargs)


class Enumerator(Lister[ModelType], Tabulator[ModelType]):
    def enumerate(
        self, *args, table: bool = False, **kwargs
    ) -> Iterable[ModelType] | pd.DataFrame:
        return self._enumerate(*args, table=table, **kwargs)


class BulkUpserter(BaseRepository[ModelType]):
    def bulk_upsert(self, df: pd.DataFrame, **kwargs) -> None:
        sdf = DataFrame.from_pandas(df)
        self._request(
            "POST",
            self.prefix + "bulk/",
            params=kwargs,
            data=sdf.json(),
        )


class BulkDeleter(BaseRepository[ModelType]):
    def bulk_delete(self, df: pd.DataFrame, **kwargs) -> None:
        sdf = DataFrame.from_pandas(df)
        self._request(
            "PATCH",
            self.prefix + "bulk/",
            params=kwargs,
            data=sdf.json(),
        )
