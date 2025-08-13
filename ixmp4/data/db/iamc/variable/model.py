from typing import ClassVar

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import iamc as abstract
from ixmp4.data.db import mixins, versions

from .. import base


class Variable(base.BaseModel, mixins.HasCreationInfo):
    __tablename__ = "iamc_variable"

    NotFound: ClassVar = abstract.Variable.NotFound
    NotUnique: ClassVar = abstract.Variable.NotUnique
    DeletionPrevented: ClassVar = abstract.Variable.DeletionPrevented

    name: types.UniqueName


class VariableVersion(versions.DefaultVersionModel):
    __tablename__ = "iamc_variable_version"
    name: types.String = db.Column(db.String(255), nullable=False)

    created_at: types.DateTime = db.Column(nullable=True)
    created_by: types.Username


version_triggers = versions.PostgresVersionTriggers(
    Variable.__table__, VariableVersion.__table__
)
