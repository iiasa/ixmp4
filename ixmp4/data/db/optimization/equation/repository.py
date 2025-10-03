import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationItemUsageError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db import versions
from ixmp4.data.db.optimization.associations import (
    BaseIndexSetAssociationRepository,
    BaseIndexSetAssociationReverter,
    BaseIndexSetAssociationVersionRepository,
)

from .. import base
from .docs import EquationDocsRepository
from .model import (
    Equation,
    EquationIndexsetAssociation,
    EquationIndexsetAssociationVersion,
    EquationVersion,
)

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

logger = logging.getLogger(__name__)


class EquationVersionRepository(versions.VersionRepository[EquationVersion]):
    model_class = EquationVersion


class EquationIndexSetAssociationVersionRepository(
    BaseIndexSetAssociationVersionRepository[EquationIndexsetAssociationVersion]
):
    model_class = EquationIndexsetAssociationVersion
    parent_version = EquationVersion
    _item_id_column = "equation__id"


class EquationIndexSetAssociationRepository(
    BaseIndexSetAssociationRepository[
        EquationIndexsetAssociation, EquationIndexsetAssociationVersion
    ]
):
    model_class = EquationIndexsetAssociation
    versions: EquationIndexSetAssociationVersionRepository

    def __init__(self, backend: "SqlAlchemyBackend") -> None:
        super().__init__(backend)

        self.versions = EquationIndexSetAssociationVersionRepository(backend)

        from .filter import OptimizationEquationIndexSetAssociationFilter

        self.filter_class = OptimizationEquationIndexSetAssociationFilter


class EquationRepository(
    base.Creator[Equation],
    base.Deleter[Equation],
    base.Retriever[Equation],
    base.Enumerator[Equation],
    BaseIndexSetAssociationReverter[
        EquationIndexsetAssociation, EquationIndexsetAssociationVersion
    ],
    abstract.EquationRepository,
):
    model_class = Equation

    UsageError = OptimizationItemUsageError

    versions: EquationVersionRepository

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)
        self.docs = EquationDocsRepository(*args)

        from .filter import EquationFilter

        self.filter_class = EquationFilter

        self.versions = EquationVersionRepository(*args)

        self._associations = EquationIndexSetAssociationRepository(*args)

        self._item_id_column = "equation__id"

    def add(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str] | None = None,
        column_names: list[str] | None = None,
    ) -> Equation:
        equation = Equation(name=name, run__id=run_id)

        if constrained_to_indexsets:
            indexsets = {
                indexset: self.backend.optimization.indexsets.get(
                    run_id=run_id, name=indexset
                )
                for indexset in set(constrained_to_indexsets)
            }
            for i in range(len(constrained_to_indexsets)):
                _ = EquationIndexsetAssociation(
                    equation=equation,
                    indexset=indexsets[constrained_to_indexsets[i]],
                    column_name=column_names[i] if column_names else None,
                )

        equation.set_creation_info(auth_context=self.backend.auth_context)
        self.session.add(equation)

        return equation

    @guard("view")
    def get(self, run_id: int, name: str) -> Equation:
        exc = db.select(Equation).where(
            (Equation.name == name) & (Equation.run__id == run_id)
        )
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Equation.NotFound

    @guard("view")
    def get_by_id(self, id: int) -> Equation:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise Equation.NotFound(id=id)

        return obj

    @guard("edit")
    def create(
        self,
        name: str,
        run_id: int,
        constrained_to_indexsets: list[str] | None = None,
        column_names: list[str] | None = None,
    ) -> Equation:
        if column_names:
            # TODO If this is removed, need to check above that constrained_to_indexsets
            #  is not None
            if constrained_to_indexsets is None:
                raise self.UsageError(
                    f"While processing Variable {name}: \n"
                    "Received `column_names` to name columns, but no "
                    "`constrained_to_indexsets` to indicate which IndexSets to use for "
                    "these columns. Please provide `constrained_to_indexsets` or "
                    "remove `column_names`!"
                )
            elif len(column_names) != len(constrained_to_indexsets):
                raise OptimizationItemUsageError(
                    f"While processing Equation {name}: \n"
                    "`constrained_to_indexsets` and `column_names` not equal in length!"
                    "Please provide the same number of entries for both!"
                )
            elif len(column_names) != len(set(column_names)):
                raise OptimizationItemUsageError(
                    f"While processing Equation {name}: \n"
                    "The given `column_names` are not unique!"
                )

        return super().create(
            run_id=run_id,
            name=name,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )

    @guard("edit")
    def delete(self, id: int) -> None:
        # Manually ensure associations are deleted first to fix order of version updates
        associations_to_delete = self._associations.tabulate(equation_id=id)
        self._associations.bulk_delete(associations_to_delete)
        super().delete(id=id)

    @guard("view")
    def list(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> Iterable[Equation]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    @guard("edit")
    def add_data(self, id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, dict):
            try:
                data = pd.DataFrame.from_dict(data=data)
            except ValueError as e:
                raise Equation.DataInvalid(str(e)) from e

        if data.empty:
            return  # nothing to do

        equation = self.get_by_id(id=id)

        missing_columns = set(["levels", "marginals"]) - set(data.columns)
        if missing_columns:
            raise OptimizationItemUsageError(
                f"Equation.data must include the column(s): "
                f"{', '.join(missing_columns)}!"
            )

        index_list = equation.column_names or equation.indexset_names
        existing_data = pd.DataFrame(equation.data)
        if index_list:
            data = data.set_index(index_list)
            if not existing_data.empty:
                existing_data.set_index(index_list, inplace=True)
        data = data.combine_first(existing_data)
        if index_list:
            data = data.reset_index()

        equation.data = cast(types.JsonDict, data.to_dict(orient="list"))

        self.session.commit()

    @guard("edit")
    def remove_data(
        self, id: int, data: dict[str, Any] | pd.DataFrame | None = None
    ) -> None:
        equation = self.get_by_id(id=id)

        if data is None:
            # Remove all data per default
            # TODO Is there a better way to reset .data?
            equation.data = {}
        else:
            if isinstance(data, dict):
                data = pd.DataFrame.from_dict(data=data)

            if data.empty:
                return

            index_list = equation.column_names or equation.indexset_names
            if not index_list:
                logger.warning(
                    f"Trying to remove {data.to_dict(orient='list')} from Equation '"
                    f"{equation.name}', but that is not indexed; not removing anything!"
                )
                return  # can't remove specific data from unindexed variable

            existing_data = pd.DataFrame(equation.data)
            if not existing_data.empty:
                existing_data.set_index(index_list, inplace=True)

            # This is the only kind of validation we do for removal data
            try:
                data.set_index(index_list, inplace=True)
            except KeyError as e:
                logger.error(
                    f"Data to be removed must include {index_list} as keys/columns, "
                    f"but {[name for name in data.columns]} were provided."
                )
                raise OptimizationItemUsageError(
                    "The data to be removed must specify one or more complete indices "
                    "to remove associated levels and marginals!"
                ) from e

            remaining_data = existing_data[~existing_data.index.isin(data.index)]
            if not remaining_data.index.empty:
                remaining_data.reset_index(inplace=True)

            equation.data = cast(types.JsonDict, remaining_data.to_dict(orient="list"))

        self.session.commit()

    @guard("edit")
    def revert(self, transaction__id: int, run__id: int) -> None:
        super().revert(transaction__id=transaction__id, run__id=run__id)

        # Revert EquationIndexSetAssociation
        self._revert_indexset_association(
            repo=self, transaction__id=transaction__id, run__id=run__id
        )
