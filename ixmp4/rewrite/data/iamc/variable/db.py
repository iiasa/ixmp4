import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.rewrite.data import versions
from ixmp4.rewrite.data.base.db import BaseModel, HasCreationInfo


class Variable(BaseModel, HasCreationInfo):
    __tablename__ = "iamc_variable"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False, unique=True)


class VariableVersion(versions.BaseVersionModel):
    __tablename__ = "iamc_variable_version"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)
    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(
    Variable.__table__, VariableVersion.__table__
)
