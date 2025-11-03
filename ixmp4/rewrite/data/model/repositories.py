from toolkit import db

from ixmp4.rewrite.exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    registry,
)

from .db import Model, ModelVersion
from .filter import ModelFilter


@registry.register()
class ModelNotFound(NotFound):
    pass


@registry.register()
class ModelNotUnique(NotUnique):
    pass


@registry.register()
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


class VersionPandasRepository(db.r.PandasRepository):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target = db.r.ModelTarget(ModelVersion)
    filter = db.r.Filter(ModelFilter, ModelVersion)
