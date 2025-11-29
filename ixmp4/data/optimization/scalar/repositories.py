from toolkit import db

from .db import Scalar, ScalarVersion
from .exceptions import ScalarNotFound, ScalarNotUnique
from .filter import ScalarFilter


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
