from functools import partial, reduce
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.db.base import BaseModel as RootBaseModel
from ixmp4.data.db.optimization.base import Reverter
from ixmp4.data.db.optimization.indexset.model import IndexSetVersion
from ixmp4.data.db.versions.model import DefaultVersionModel
from ixmp4.data.db.versions.repository import VersionRepository
from ixmp4.db import utils
from ixmp4.db.utils.revert import apply_transaction__id

from .utils import create_id_map_subquery

if TYPE_CHECKING:
    from ixmp4.data.db.optimization import (
        EquationRepository,
        IndexSet,
        ParameterRepository,
        TableRepository,
        VariableRepository,
    )


class BaseIndexSetAssociation(RootBaseModel):
    __abstract__ = True

    indexset__id: types.IndexSetId

    @db.declared_attr
    def indexset(cls) -> types.Mapped["IndexSet"]:
        return db.relationship("IndexSet")

    column_name: types.String = db.Column(db.String(255), nullable=True)


class BaseIndexSetAssociationVersion(DefaultVersionModel):
    __abstract__ = True

    indexset__id: db.MappedColumn[int] = db.Column(
        db.Integer, nullable=False, index=True
    )

    column_name: types.String = db.Column(db.String(255), nullable=True)


AssociationModelType = TypeVar("AssociationModelType", bound=BaseIndexSetAssociation)
AssociationVersionModelType = TypeVar(
    "AssociationVersionModelType", bound=BaseIndexSetAssociationVersion
)


class BaseIndexSetAssociationVersionRepository(
    VersionRepository[AssociationVersionModelType]
):
    model_class: type[AssociationVersionModelType]
    parent_version: type[DefaultVersionModel]

    _item_id_column: Literal[
        "equation__id", "parameter__id", "table__id", "variable__id"
    ]

    def select(
        self,
        transaction__id: int | None = None,
        valid: Literal["at_transaction", "after_transaction"] = "at_transaction",
        indexset__ids: list[int] | None = None,
        item__ids: list[int] | None = None,
        **kwargs: Any,
    ) -> db.sql.Select[Any]:
        exc = (
            db.select(self.bundle)
            .join_from(
                self.model_class,
                IndexSetVersion,
                self.model_class.indexset__id == IndexSetVersion.id,
            )
            .join_from(
                self.model_class,
                self.parent_version,
                getattr(self.model_class, self._item_id_column)
                == self.parent_version.id,
            )
        )

        _apply_transaction__id = partial(
            apply_transaction__id, transaction__id=transaction__id, valid=valid
        )

        exc = reduce(
            _apply_transaction__id,
            {self.model_class, IndexSetVersion, self.parent_version},
            exc,
        )

        if indexset__ids is not None and item__ids is not None:
            exc = exc.where(
                db.and_(
                    self.model_class.indexset__id.in_(indexset__ids),
                    getattr(self.model_class, self._item_id_column).in_(item__ids),
                )
            )

        exc = utils.where_matches_kwargs(exc, model_class=self.model_class, **kwargs)
        return exc.distinct()


class BaseIndexSetAssociationRepository(
    Reverter[AssociationModelType],
    Generic[AssociationModelType, AssociationVersionModelType],
):
    model_class: type[AssociationModelType]
    versions: BaseIndexSetAssociationVersionRepository[AssociationVersionModelType]


RepoType = TypeVar(
    "RepoType",
    "EquationRepository",
    "ParameterRepository",
    "TableRepository",
    "VariableRepository",
)


class BaseIndexSetAssociationReverter(
    Reverter[AssociationModelType],
    Generic[AssociationModelType, AssociationVersionModelType],
):
    _associations: BaseIndexSetAssociationRepository[
        AssociationModelType, AssociationVersionModelType
    ]

    _item_id_column: Literal[
        "equation__id", "parameter__id", "table__id", "variable__id"
    ]

    def _revert_indexset_association(
        self, repo: RepoType, transaction__id: int, run__id: int
    ) -> None:
        # Create subquery to map IDs from before rollback to new ones
        item_map_subquery = create_id_map_subquery(
            transaction__id=transaction__id, run__id=run__id, repo=repo
        )
        indexset_map_subquery = create_id_map_subquery(
            transaction__id=transaction__id,
            run__id=run__id,
            repo=self.backend.optimization.indexsets,
        )

        # Prepare columns with correctly updated IDs for SELECT
        columns = utils.collect_columns_to_select(
            columns=utils.get_columns(self._associations.versions.model_class),
            exclude={self._item_id_column, "indexset__id"},
        )

        # Construct SELECT to find correct versions with updated IDs
        select_correct_versions = (
            db.select(
                item_map_subquery.c.new_id.label(self._item_id_column),
                indexset_map_subquery.c.new_id.label("indexset__id"),
                *columns.values(),
            )
            .select_from(self._associations.versions.model_class)
            .join(
                item_map_subquery,
                getattr(self._associations.versions.model_class, self._item_id_column)
                == item_map_subquery.c.old_id,
            )
            .join(
                indexset_map_subquery,
                self._associations.versions.model_class.indexset__id
                == indexset_map_subquery.c.old_id,
            )
        )
        select_correct_versions = apply_transaction__id(
            exc=select_correct_versions,
            model_class=self._associations.versions.model_class,
            transaction__id=transaction__id,
        ).order_by(self._associations.versions.model_class.id.asc())

        correct_versions = self.tabulate_query(select_correct_versions)

        self._associations.revert(
            transaction__id=transaction__id, correct_versions=correct_versions
        )
