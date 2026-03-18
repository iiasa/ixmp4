from toolkit.db.repositories import ItemRepository as BaseItemRepository

from ixmp4.base_exceptions import (
    NotFound,
    NotUnique,
    registry,
)

from .db import AbstractDocs


@registry.register()
class DocsNotFound(NotFound):
    pass


@registry.register()
class DocsNotUnique(NotUnique):
    pass


class ItemRepository(BaseItemRepository[AbstractDocs]):
    NotFound = DocsNotFound
    NotUnique = DocsNotUnique
