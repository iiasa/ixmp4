from toolkit import db
from toolkit.exceptions import NotFound, NotUnique

from ixmp4.core.exceptions import DeletionPrevented

from .db import Model
from .filter import ModelFilter


class ModelNotFound(NotFound):
    pass


class ModelNotUnique(NotUnique):
    pass


class ModelDeletionPrevented(DeletionPrevented):
    pass


class ItemRepository(db.r.ItemRepository[Model]):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target = db.r.ModelTarget(Model)
    filter = db.r.Filter(ModelFilter, Model)


class PandasRepository(db.r.PandasRepository):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target = db.r.ModelTarget(Model)
    filter = db.r.Filter(ModelFilter, Model)
