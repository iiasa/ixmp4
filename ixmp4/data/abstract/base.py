from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, _ProtocolMeta

from typing_extensions import Unpack

if TYPE_CHECKING:
    from ixmp4.data.db.base import EnumerateKwargs

import pandas as pd

from ixmp4.core.exceptions import DeletionPrevented, IxmpError, NotFound, NotUnique


class BaseMeta(_ProtocolMeta):
    def __init__(
        self, name: str, bases: tuple[type, ...], namespace: dict[str, Any]
    ) -> None:
        super().__init__(name, bases, namespace)
        self.NotUnique = type(
            self.__name__ + "NotUnique",
            (NotUnique,),
            {"http_error_name": name.lower() + "_not_unique"},
        )
        self.NotFound = type(
            self.__name__ + "NotFound",
            (NotFound,),
            {"http_error_name": name.lower() + "_not_found"},
        )

        self.DeletionPrevented = type(
            self.__name__ + "DeletionPrevented",
            (DeletionPrevented,),
            {"http_error_name": name.lower() + "_deletion_prevented"},
        )


class BaseModel(Protocol, metaclass=BaseMeta):
    NotUnique: ClassVar[type[IxmpError]]
    NotFound: ClassVar[type[IxmpError]]
    DeletionPrevented: ClassVar[type[IxmpError]]

    id: int
    "Unique integer id."


class BaseRepository(Protocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...


class Retriever(BaseRepository, Protocol):
    def get(self, *args: Any, **kwargs: Any) -> BaseModel: ...


class Creator(BaseRepository, Protocol):
    def create(self, *args: Any, **kwargs: Any) -> BaseModel: ...


class Deleter(BaseRepository, Protocol):
    def delete(self, *args: Any, **kwargs: Any) -> None: ...


class Lister(BaseRepository, Protocol):
    def list(self, *args: Any, **kwargs: Any) -> Sequence[BaseModel]: ...


class Tabulator(BaseRepository, Protocol):
    def tabulate(self, *args: Any, **kwargs: Any) -> pd.DataFrame: ...


class Enumerator(Lister, Tabulator, Protocol):
    def enumerate(
        self, table: bool = False, **kwargs: Unpack["EnumerateKwargs"]
    ) -> Sequence[BaseModel] | pd.DataFrame: ...


class BulkUpserter(BaseRepository, Protocol):
    def bulk_upsert(self, *args: Any, **kwargs: Any) -> None: ...


class BulkDeleter(BaseRepository, Protocol):
    def bulk_delete(self, *args: Any, **kwargs: Any) -> None: ...


class VersionManager(Lister, Tabulator, Protocol):
    def tabulate_versions(
        self, transaction__id: int | None = None, **kwargs: Any
    ) -> pd.DataFrame: ...
