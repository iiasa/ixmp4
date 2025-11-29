import abc
from collections.abc import Iterable
from typing import Generic, TypeVar

from ixmp4.backend import Backend
from ixmp4.data.base.dto import BaseModel
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.docs.service import DocsService
from ixmp4.services.base import Service
from ixmp4.services.protocols import GetByIdService


class BaseBackendFacade(object):
    backend: Backend

    def __init__(self, backend: Backend) -> None:
        self.backend = backend


KeyT = TypeVar("KeyT")
ItemT = TypeVar("ItemT")


class ItemLookupFacade(abc.ABC[KeyT, ItemT]):
    @abc.abstractmethod
    def get_item_id(self, key: KeyT) -> int:
        raise NotImplementedError


ServiceT = TypeVar("ServiceT", bound=Service)


class BaseServiceFacade(Generic[ServiceT]):
    service: ServiceT

    def __init__(self, service: ServiceT) -> None:
        self.service = service


DocsServiceT = TypeVar("DocsServiceT", bound=DocsService)


class BaseDocsServiceFacade(
    ItemLookupFacade[KeyT, ItemT], Generic[KeyT, ItemT, DocsServiceT]
):
    service: DocsServiceT

    def __init__(self, service: DocsServiceT) -> None:
        self.service = service

    def get_docs(self, x: KeyT) -> str | None:
        equation_id = self.get_item_id(x)
        if equation_id is None:
            return None
        try:
            return self.service.get_docs(dimension__id=equation_id).description
        except DocsNotFound:
            return None

    def set_docs(self, x: KeyT, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(x)
            return None
        equation_id = self.get_item_id(x)
        if equation_id is None:
            return None
        return self.service.set_docs(
            dimension__id=equation_id, description=description
        ).description

    def delete_docs(self, x: KeyT) -> None:
        # TODO: this function is failing silently, which we should avoid
        equation_id = self.get_item_id(x)
        if equation_id is None:
            return None
        try:
            self.service.delete_docs(dimension__id=equation_id)
            return None
        except DocsNotFound:
            return None

    def list_docs(
        self, id: int | None = None, id__in: Iterable[int] | None = None
    ) -> Iterable[str]:
        return [
            item.description
            for item in self.service.list_docs(
                dimension__id=id, dimension__id__in=id__in
            )
        ]


DtoT = TypeVar("DtoT", bound=BaseModel)
GetByIdServiceT = TypeVar("GetByIdServiceT", bound=GetByIdService[DtoT])


class BaseFacadeObject(Generic[GetByIdServiceT, DtoT]):
    service: GetByIdServiceT
    dto: DtoT

    def __init__(self, service: GetByIdServiceT, dto: DtoT) -> None:
        self.service = service
        self.dto = dto

    def refresh(self):
        self.dto = self.service.get_by_id(self.dto.id)
