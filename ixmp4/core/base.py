import abc
import logging
from typing import Generic, TypeVar, cast

import pandas as pd
from typing_extensions import TYPE_CHECKING, Unpack

from ixmp4.data.backend import Backend
from ixmp4.data.base.dto import BaseModel
from ixmp4.data.docs.filter import DocsFilter
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.docs.service import DocsService
from ixmp4.data.services.base import GetByIdService, Service
from ixmp4.data.versions.model import Operation

if TYPE_CHECKING:
    from ixmp4.core.checkpoint import Checkpoint
    from ixmp4.core.run import Run

logger = logging.getLogger(__name__)


class BaseBackendFacade(object):
    _backend: Backend

    def __init__(self, backend: Backend) -> None:
        self._backend = backend


class BaseCheckpointView(BaseBackendFacade):
    _run: "Run"
    _checkpoint: "Checkpoint"
    _version_columns = ["transaction_id", "end_transaction_id", "operation_type"]

    def __init__(self, backend: Backend, run: "Run", checkpoint: "Checkpoint") -> None:
        super().__init__(backend)
        self._run = run
        self._checkpoint = checkpoint

    @classmethod
    def _drop_version_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=cls._version_columns)

    @classmethod
    def _map_op_type(cls, df: pd.DataFrame) -> pd.DataFrame:
        df["operation_type"] = df["operation_type"].apply(lambda i: Operation(i).name)
        return df


KeyT = TypeVar("KeyT")
ItemT = TypeVar("ItemT")


class ItemLookupFacade(abc.ABC, Generic[KeyT, ItemT]):
    @abc.abstractmethod
    def _get_item_id(self, ref: KeyT) -> int:
        raise NotImplementedError


ServiceT = TypeVar("ServiceT", bound=Service)


class BaseServiceFacade(abc.ABC, BaseBackendFacade, Generic[ServiceT]):
    _service: ServiceT

    def __init__(self, backend: Backend) -> None:
        BaseBackendFacade.__init__(self, backend)
        self._service = self._get_service(backend)

    @abc.abstractmethod
    def _get_service(self, backend: Backend) -> ServiceT:
        raise NotImplementedError


DocsServiceT = TypeVar("DocsServiceT", bound=DocsService)


class BaseDocsServiceFacade(
    ItemLookupFacade[KeyT, ItemT],
    BaseServiceFacade[DocsServiceT],
    Generic[KeyT, ItemT, DocsServiceT],
):
    def get_docs(self, x: KeyT) -> str | None:
        item_id = self._get_item_id(x)
        try:
            return self._service.get_docs(dimension__id=item_id).description
        except DocsNotFound:
            return None

    def set_docs(self, x: KeyT, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(x)
            return None
        item_id = self._get_item_id(x)
        return self._service.set_docs(
            dimension__id=item_id, description=description
        ).description

    def delete_docs(self, x: KeyT) -> None:
        item_id = self._get_item_id(x)
        try:
            self._service.delete_docs(dimension__id=item_id)
            return None
        except DocsNotFound:
            logger.debug(f"Tried to delete docs for {x}, but no docs were found.")
            return None

    def list_docs(self, **kwargs: Unpack[DocsFilter]) -> list[str]:
        return [item.description for item in self._service.list_docs(**kwargs)]


DtoT = TypeVar("DtoT", bound=BaseModel)

GetByIdServiceT = TypeVar("GetByIdServiceT", bound=GetByIdService)


class BaseFacadeObject(
    BaseServiceFacade[GetByIdServiceT], Generic[GetByIdServiceT, DtoT]
):
    _service: GetByIdServiceT
    _dto: DtoT

    def __init__(self, backend: Backend, dto: DtoT) -> None:
        BaseServiceFacade.__init__(self, backend)
        self._dto = dto

    def _refresh(self) -> None:
        self._dto = cast(DtoT, self._service.get_by_id(self._dto.id))
