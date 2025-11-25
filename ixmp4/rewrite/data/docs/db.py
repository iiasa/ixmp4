import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.rewrite.data.base.db import BaseModel


class AbstractDocs(BaseModel):
    __abstract__ = True

    description: db.t.String = orm.mapped_column(sa.Text)
    dimension__id: db.t.Integer = orm.mapped_column(unique=True)


def docs_model(model: type[BaseModel]) -> type[AbstractDocs]:
    dimension__id = sa.Column(
        sa.Integer, sa.ForeignKey(model.id, ondelete="CASCADE"), unique=True
    )

    table_dict = {
        "__module__": model.__module__,
        "__tablename__": model.__tablename__ + "_docs",
        "dimension__id": dimension__id,
    }

    DocsModel = type(model.__name__ + "Docs", (AbstractDocs,), table_dict)
    return DocsModel
