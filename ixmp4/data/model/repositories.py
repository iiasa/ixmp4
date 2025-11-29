from toolkit import db

from .db import Model, ModelVersion
from .exceptions import ModelNotFound, ModelNotUnique
from .filter import ModelFilter


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
