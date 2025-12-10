from toolkit import db

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


class ItemRepository(db.r.ItemRepository[AbstractDocs]):
    NotFound = DocsNotFound
    NotUnique = DocsNotUnique
