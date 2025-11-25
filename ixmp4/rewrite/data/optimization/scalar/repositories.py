from toolkit import db

from ixmp4.rewrite.exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    registry,
)

from .db import Scalar, ScalarVersion
from .filter import ScalarFilter


@registry.register()
class ScalarNotFound(NotFound):
    pass


@registry.register()
class ScalarNotUnique(NotUnique):
    pass


@registry.register()
class ScalarDeletionPrevented(DeletionPrevented):
    pass


class ItemRepository(db.r.ItemRepository[Scalar]):
    NotFound = ScalarNotFound
    NotUnique = ScalarNotUnique
    target = db.r.ModelTarget(Scalar)
    filter = db.r.Filter(ScalarFilter, Scalar)


class PandasRepository(db.r.PandasRepository):
    NotFound = ScalarNotFound
    NotUnique = ScalarNotUnique
    target = db.r.ModelTarget(Scalar)
    filter = db.r.Filter(ScalarFilter, Scalar)


class VersionPandasRepository(db.r.PandasRepository):
    NotFound = ScalarNotFound
    NotUnique = ScalarNotUnique
    target = db.r.ModelTarget(ScalarVersion)
    filter = db.r.Filter(ScalarFilter, ScalarVersion)
