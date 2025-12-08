import abc
from typing import Generic, TypeVar, cast

from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.data.base.dto import BaseModel
from ixmp4.data.docs.filter import DocsFilter
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.docs.service import DocsService
from ixmp4.services.base import GetByIdService, Service


class BaseBackendFacade(object):
    _backend: Backend

    def __init__(self, backend: Backend) -> None:
        self._backend = backend


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
        equation_id = self._get_item_id(x)
        try:
            return self._service.get_docs(dimension__id=equation_id).description
        except DocsNotFound:
            return None

    def set_docs(self, x: KeyT, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(x)
            return None
        equation_id = self._get_item_id(x)
        return self._service.set_docs(
            dimension__id=equation_id, description=description
        ).description

    def delete_docs(self, x: KeyT) -> None:
        # TODO: this function is failing silently, which we should avoid
        equation_id = self._get_item_id(x)
        try:
            self._service.delete_docs(dimension__id=equation_id)
            return None
        except DocsNotFound:
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
