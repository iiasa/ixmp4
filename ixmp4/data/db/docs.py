import logging
from collections.abc import Iterable
from typing import ClassVar, TypeVar

from sqlalchemy.exc import NoResultFound

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

from ixmp4 import db
from ixmp4.data import abstract, types

from ..auth.decorators import guard
from . import base

logger = logging.getLogger(__name__)


class AbstractDocs(base.BaseModel):
    NotFound: ClassVar = abstract.Docs.NotFound
    NotUnique: ClassVar = abstract.Docs.NotUnique
    DeletionPrevented: ClassVar = abstract.Docs.DeletionPrevented

    __abstract__ = True

    description: types.Mapped[str]

    dimension__id: types.Mapped[int]


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


class ListKwargs(TypedDict, total=False):
    dimension_id: int | None
    dimension_id__in: Iterable[int] | None


class BaseDocsRepository(
    base.Creator[DocsType],
    base.Retriever[DocsType],
    base.Deleter[DocsType],
    base.Enumerator[DocsType],
    abstract.DocsRepository,
):
    dimension_model_class: ClassVar[type[base.BaseModel]]

    def select(
        self,
        *,
        _exc: db.sql.Select[tuple[DocsType]] | None = None,
        dimension_id: int | None = None,
        dimension_id__in: Iterable[int] | None = None,
    ) -> db.sql.Select[tuple[DocsType]]:
        if _exc is None:
            _exc = db.select(self.model_class)

        # TODO Should we enforce that only one of these filters can be active?
        if dimension_id is not None:
            _exc = _exc.where(self.model_class.dimension__id == dimension_id)
        if dimension_id__in is not None:
            if dimension_id and dimension_id not in dimension_id__in:
                logger.warning(
                    "Applying incompatible filters to select for docs: "
                    f"dimension_id '{dimension_id}' is not in dimension_id__in "
                    f"{dimension_id__in}!"
                )
            _exc = _exc.where(self.model_class.dimension__id.in_(dimension_id__in))

        return _exc

    def select_for_count(
        self,
        _exc: db.sql.Select[tuple[int]],
        dimension_id: int | None = None,
        dimension_id__in: Iterable[int] | None = None,
    ) -> db.sql.Select[tuple[int]]:
        if dimension_id is not None:
            _exc = _exc.where(self.model_class.dimension__id == dimension_id)
        if dimension_id__in is not None:
            if dimension_id and dimension_id not in dimension_id__in:
                logger.warning(
                    "Applying incompatible filters to select for docs: "
                    f"dimension_id '{dimension_id}' is not in dimension_id__in "
                    f"{dimension_id__in}!"
                )
            _exc = _exc.where(self.model_class.dimension__id.in_(dimension_id__in))

        return _exc

    def add(self, dimension_id: int, description: str) -> DocsType:
        docs = self.model_class(description=description, dimension__id=dimension_id)
        self.session.add(docs)
        return docs

    @guard("view")
    def get(self, dimension_id: int) -> DocsType:
        exc = self.select(dimension_id=dimension_id)
        try:
            return self.session.execute(exc).scalar_one()
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
        exc = db.delete(self.model_class).where(
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
    def list(self, **kwargs: Unpack[ListKwargs]) -> list[DocsType]:
        return super().list(**kwargs)
