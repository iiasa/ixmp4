from typing import TYPE_CHECKING

from ixmp4.backend import Backend
from ixmp4.core.base import (
    BaseDocsServiceFacade,
    BaseFacadeObject,
    DocsServiceT,
    DtoT,
    GetByIdServiceT,
    ItemT,
    KeyT,
)

if TYPE_CHECKING:
    from ixmp4.core.run import Run


class BaseOptimizationFacadeObject(BaseFacadeObject[GetByIdServiceT, DtoT]):
    run: "Run"

    def __init__(self, backend: Backend, dto: DtoT, run: "Run"):
        super().__init__(backend, dto)
        self.run = run


class BaseOptimizationServiceFacade(BaseDocsServiceFacade[KeyT, ItemT, DocsServiceT]):
    run: "Run"

    def __init__(self, backend: Backend, run: "Run"):
        super().__init__(backend)
        self.run = run
