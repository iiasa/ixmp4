import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

import pandas as pd

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationItemUsageError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import OptimizationVariableDocsRepository
from .model import OptimizationVariable as Variable
from .model import VariableIndexsetAssociation

logger = logging.getLogger(__name__)


class VariableRepository(
    base.Creator[Variable],
    base.Deleter[Variable],
    base.Retriever[Variable],
    base.Enumerator[Variable],
    abstract.VariableRepository,
):
    model_class = Variable

    UsageError = OptimizationItemUsageError

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)
        self.docs = OptimizationVariableDocsRepository(*args)

        from .filter import OptimizationVariableFilter

        self.filter_class = OptimizationVariableFilter

    def add(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str] | None = None,
        column_names: list[str] | None = None,
    ) -> Variable:
        variable = Variable(name=name, run__id=run_id)

        if constrained_to_indexsets:
            indexsets = {
                indexset: self.backend.optimization.indexsets.get(
                    run_id=run_id, name=indexset
                )
                for indexset in set(constrained_to_indexsets)
            }
            for i in range(len(constrained_to_indexsets)):
                _ = VariableIndexsetAssociation(
                    variable=variable,
                    indexset=indexsets[constrained_to_indexsets[i]],
                    column_name=column_names[i] if column_names else None,
                )

        variable.set_creation_info(auth_context=self.backend.auth_context)
        self.session.add(variable)

        return variable

    @guard("view")
    def get(self, run_id: int, name: str) -> Variable:
        # TODO by avoiding super().select, don't we also miss out on filters and auth?
        exc = db.select(Variable).where(
            (Variable.name == name) & (Variable.run__id == run_id)
        )
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Variable.NotFound

    @guard("view")
    def get_by_id(self, id: int) -> Variable:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise Variable.NotFound(id=id)

        return obj

    @guard("edit")
    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str] | None = None,
        column_names: list[str] | None = None,
    ) -> Variable:
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
                raise self.UsageError(
                    f"While processing Variable {name}: \n"
                    "`constrained_to_indexsets` and `column_names` not equal in "
                    "length! Please provide the same number of entries for both!"
                )
            elif len(column_names) != len(set(column_names)):
                raise self.UsageError(
                    f"While processing Variable {name}: \n"
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
        super().delete(id=id)

    @guard("view")
    def list(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> Iterable[Variable]:
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
                raise Variable.DataInvalid(str(e)) from e

        if data.empty:
            return  # nothing to do

        variable = self.get_by_id(id=id)

        missing_columns = set(["levels", "marginals"]) - set(data.columns)
        if missing_columns:
            raise OptimizationItemUsageError(
                "Variable.data must include the column(s): "
                f"{', '.join(missing_columns)}!"
            )

        index_list = variable.column_names or variable.indexset_names
        existing_data = pd.DataFrame(variable.data)
        if index_list:
            data = data.set_index(index_list)
            if not existing_data.empty:
                existing_data.set_index(index_list, inplace=True)
        data = data.combine_first(existing_data)
        if index_list:
            data = data.reset_index()

        variable.data = cast(types.JsonDict, data.to_dict(orient="list"))

        self.session.commit()

    @guard("edit")
    def remove_data(
        self, id: int, data: dict[str, Any] | pd.DataFrame | None = None
    ) -> None:
        variable = self.get_by_id(id=id)

        if data is None:
            # Remove all data per default
            # TODO Is there a better way to reset .data?
            variable.data = {}
        else:
            if isinstance(data, dict):
                data = pd.DataFrame.from_dict(data=data)

            if data.empty:
                return

            index_list = variable.column_names or variable.indexset_names
            if not index_list:
                logger.warning(
                    f"Trying to remove {data.to_dict(orient='list')} from Variable '"
                    f"{variable.name}', but that is not indexed; not removing anything!"
                )
                return  # can't remove specific data from unindexed variable

            existing_data = pd.DataFrame(variable.data)
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

            variable.data = cast(types.JsonDict, remaining_data.to_dict(orient="list"))

        self.session.commit()
