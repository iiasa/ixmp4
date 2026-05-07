import sqlalchemy as sa
from sqlalchemy import orm
from toolkit.db.types import Integer, String

from ixmp4.data.base.db import BaseModel


class AbstractDocs(BaseModel):
    __abstract__ = True

    description: String = orm.mapped_column(sa.Text)
    dimension__id: Integer = orm.mapped_column(unique=True)


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
