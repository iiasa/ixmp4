from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.orm.decl_api import declared_attr
from toolkit import db

from ixmp4.data import versions
from ixmp4.data.base.db import BaseModel, HasCreationInfo
from ixmp4.data.docs.db import docs_model

if TYPE_CHECKING:
    from ixmp4.data.iamc.timeseries.db import TimeSeries


class Variable(BaseModel, HasCreationInfo):
    __tablename__ = "iamc_variable"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False, unique=True)

    @declared_attr
    def timeseries(cls) -> orm.Relationship["TimeSeries"]:
        from ixmp4.data.iamc.measurand.db import Measurand

        return orm.relationship(
            "ixmp4.data.iamc.timeseries.db.TimeSeries",
            secondary=Measurand.__table__,
            viewonly=True,
        )


VariableDocs = docs_model(Variable)


class VariableVersion(versions.BaseVersionModel):
    __tablename__ = "iamc_variable_version"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)
    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(
    Variable.__table__, VariableVersion.__table__
)
