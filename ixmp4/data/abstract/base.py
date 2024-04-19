from typing import ClassVar, Protocol, _ProtocolMeta

import pandas as pd

from ixmp4.core.exceptions import DeletionPrevented, IxmpError, NotFound, NotUnique
from ixmp4.data import types


class BaseMeta(_ProtocolMeta):
    def __init__(self, name, bases, namespace):
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

    id: types.Integer
    "Unique integer id."


class BaseRepository(Protocol):
    def __init__(self, *args, **kwargs) -> None: ...


class Retriever(BaseRepository, Protocol):
    def get(self, *args, **kwargs) -> BaseModel: ...


class Creator(BaseRepository, Protocol):
    def create(self, *args, **kwargs) -> BaseModel: ...


class Deleter(BaseRepository, Protocol):
    def delete(self, *args, **kwargs) -> None: ...


class Lister(BaseRepository, Protocol):
    def list(self, *args, **kwargs) -> list: ...


class Tabulator(BaseRepository, Protocol):
    def tabulate(self, *args, **kwargs) -> pd.DataFrame: ...


class Enumerator(Lister, Tabulator, Protocol):
    def enumerate(
        self, *args, table: bool = False, **kwargs
    ) -> list | pd.DataFrame: ...


class BulkUpserter(BaseRepository, Protocol):
    def bulk_upsert(self, *args, **kwargs) -> None: ...


class BulkDeleter(BaseRepository, Protocol):
    def bulk_delete(self, *args, **kwargs) -> None: ...
