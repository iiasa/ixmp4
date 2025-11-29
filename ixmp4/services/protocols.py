from typing import Protocol, TypeVar

from ixmp4.data.base.dto import BaseModel

from .base import Service

DtoT = TypeVar("DtoT", bound=BaseModel)


class GetByIdService(Service, Protocol[DtoT]):
    def get_by_id(self, id: int) -> DtoT: ...
