from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.data import versions
from ixmp4.data.base.db import BaseModel, HasCreationInfo
from ixmp4.data.docs.db import docs_model

if TYPE_CHECKING:
    from ixmp4.data.run.db import Run


class Model(BaseModel, HasCreationInfo):
    __tablename__ = "model"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False, unique=True)

    runs: db.t.Mapped[list["Run"]] = orm.relationship(viewonly=True)


ModelDocs = docs_model(Model)


class ModelVersion(versions.BaseVersionModel):
    __tablename__ = "model_version"
    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(
    Model.__table__, ModelVersion.__table__
)
