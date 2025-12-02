from typing import Protocol, TypeVar

from ixmp4.data.base.dto import BaseModel

DtoT = TypeVar("DtoT", bound=BaseModel)


class GetByIdService(Protocol[DtoT]):
    def get_by_id(self, id: int) -> DtoT: ...
