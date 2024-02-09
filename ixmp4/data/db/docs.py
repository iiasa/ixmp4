from typing import ClassVar, TypeVar

import pandas as pd
from sqlalchemy.exc import NoResultFound

from ixmp4 import db
from ixmp4.data import abstract, types

from ..auth.decorators import guard
from . import base


class AbstractDocs(base.BaseModel):
    NotFound: ClassVar = abstract.Docs.NotFound
    NotUnique: ClassVar = abstract.Docs.NotUnique
    DeletionPrevented: ClassVar = abstract.Docs.DeletionPrevented

    __abstract__ = True

    description: types.Mapped

    dimension__id: types.Mapped


def docs_model(model: type[base.BaseModel]) -> type[AbstractDocs]:
    dimension__id = db.Column(db.Integer, db.ForeignKey(model.id), unique=True)

    table_dict = {
        "NotFound": AbstractDocs.NotFound,
        "NotUnique": AbstractDocs.NotUnique,
        "__tablename__": model.__tablename__ + "_docs",
        "description": db.Column(db.Text, nullable=False),
        "dimension__id": dimension__id,
    }

    DocsModel = type(model.__name__ + "Docs", (AbstractDocs,), table_dict)
    return DocsModel


DocsType = TypeVar("DocsType", bound=AbstractDocs)


class BaseDocsRepository(
    base.Creator[DocsType],
    base.Retriever[DocsType],
    base.Deleter[DocsType],
    base.Enumerator[DocsType],
    abstract.DocsRepository,
):
    dimension_model_class: ClassVar[type[base.BaseModel]]

    def select(
        self, *, _exc: db.sql.Select | None = None, dimension_id: int | None = None
    ) -> db.sql.Select:
        if _exc is None:
            _exc = db.select(self.model_class)

        if dimension_id is not None:
            _exc = _exc.where(self.model_class.dimension__id == dimension_id)

        return _exc

    def add(self, dimension_id: int, description: str) -> DocsType:
        docs = self.model_class(description=description, dimension__id=dimension_id)
        self.session.add(docs)
        return docs

    @guard("view")
    def get(self, dimension_id: int) -> DocsType:
        exc = self.select(dimension_id=dimension_id)
        try:
            docs = self.session.execute(exc).scalar_one()
            return docs
        except NoResultFound:
            raise self.model_class.NotFound

    @guard("edit")
    def set(self, dimension_id: int, description: str) -> DocsType:
        exc = self.select(dimension_id=dimension_id)
        try:
            docs = self.session.execute(exc).scalar_one()
            docs.description = description
            self.session.commit()
            return docs
        except NoResultFound:
            docs = self.create(dimension_id=dimension_id, description=description)
            return docs

    @guard("edit")
    def delete(self, dimension_id: int) -> None:
        exc: db.sql.Delete = db.delete(self.model_class).where(
            self.model_class.dimension__id == dimension_id
        )

        try:
            self.session.execute(
                exc, execution_options={"synchronize_session": "fetch"}
            )
            self.session.commit()
        # This is weird: if there is no region, we might want to return None
        # for a delete request; but then how do we distinguish a
        # typo also returning None?
        except NoResultFound:
            raise self.model_class.NotFound

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    @guard("view")
    def list(self, *args, **kwargs) -> list[DocsType]:
        return super().list(*args, **kwargs)
