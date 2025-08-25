from typing import ClassVar

from ixmp4 import db
from ixmp4.data import abstract, types
from ixmp4.data.db import mixins

from .. import base, versions


class Model(base.BaseModel, mixins.HasCreationInfo):
    __tablename__ = "model"
    NotFound: ClassVar = abstract.Model.NotFound
    NotUnique: ClassVar = abstract.Model.NotUnique
    DeletionPrevented: ClassVar = abstract.Model.DeletionPrevented

    name: types.UniqueName


class ModelVersion(versions.DefaultVersionModel):
    __tablename__ = "model_version"
    name: types.String = db.Column(db.String(255), nullable=False)

    created_at: types.DateTime = db.Column(nullable=True)
    created_by: types.Username


version_triggers = versions.PostgresVersionTriggers(
    Model.__table__, ModelVersion.__table__
)
