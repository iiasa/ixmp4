from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend


import logging

import pandas as pd

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationItemUsageError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db.unit import Unit

from .. import base
from .docs import ParameterDocsRepository
from .model import Parameter, ParameterIndexsetAssociation

logger = logging.getLogger(__name__)


class ParameterRepository(
    base.Creator[Parameter],
    base.Deleter[Parameter],
    base.Retriever[Parameter],
    base.Enumerator[Parameter],
    abstract.ParameterRepository,
):
    model_class = Parameter

    UsageError = OptimizationItemUsageError

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)
        self.docs = ParameterDocsRepository(*args)

        from .filter import OptimizationParameterFilter

        self.filter_class = OptimizationParameterFilter

    def add(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Parameter:
        parameter = Parameter(name=name, run__id=run_id)

        indexsets = {
            indexset: self.backend.optimization.indexsets.get(
                run_id=run_id, name=indexset
            )
            for indexset in set(constrained_to_indexsets)
        }
        for i in range(len(constrained_to_indexsets)):
            _ = ParameterIndexsetAssociation(
                parameter=parameter,
                indexset=indexsets[constrained_to_indexsets[i]],
                column_name=column_names[i] if column_names else None,
            )

        parameter.set_creation_info(auth_context=self.backend.auth_context)
        self.session.add(parameter)

        return parameter

    @guard("view")
    def get(self, run_id: int, name: str) -> Parameter:
        exc = db.select(Parameter).where(
            (Parameter.name == name) & (Parameter.run__id == run_id)
        )
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Parameter.NotFound

    @guard("view")
    def get_by_id(self, id: int) -> Parameter:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise Parameter.NotFound(id=id)

        return obj

    @guard("edit")
    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Parameter:
        if column_names and len(column_names) != len(constrained_to_indexsets):
            raise self.UsageError(
                f"While processing Parameter {name}: \n"
                "`constrained_to_indexsets` and `column_names` not equal in length! "
                "Please provide the same number of entries for both!"
            )

        if column_names and len(column_names) != len(set(column_names)):
            raise self.UsageError(
                f"While processing Parameter {name}: \n"
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
    def list(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> Iterable[Parameter]:
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
                raise Parameter.DataInvalid(str(e)) from e

        if data.empty:
            return  # nothing to do

        parameter = self.get_by_id(id=id)

        missing_columns = set(["values", "units"]) - set(data.columns)
        if missing_columns:
            raise OptimizationItemUsageError(
                "Parameter.data must include the column(s): "
                f"{', '.join(missing_columns)}!"
            )

        # Can use a set for now, need full column if we care about order
        for unit_name in set(data["units"]):
            try:
                self.backend.units.get(name=unit_name)
            except Unit.NotFound as e:
                # TODO Add a helpful hint on how to check defined Units
                raise Unit.NotFound(
                    message=f"'{unit_name}' is not defined for this Platform!"
                ) from e

        index_list = parameter.column_names or parameter.indexset_names
        existing_data = pd.DataFrame(parameter.data)
        if not existing_data.empty:
            existing_data.set_index(index_list, inplace=True)

        parameter.data = cast(
            types.JsonDict,
            (
                data.set_index(index_list).combine_first(existing_data).reset_index()
            ).to_dict(orient="list"),
        )

        self.session.commit()

    @guard("edit")
    def remove_data(self, id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, dict):
            data = pd.DataFrame.from_dict(data=data)

        if data.empty:
            return

        parameter = self.get_by_id(id=id)
        index_list = parameter.column_names or parameter.indexset_names
        existing_data = pd.DataFrame(parameter.data)
        if not existing_data.empty:
            existing_data.set_index(index_list, inplace=True)

        # This is the only kind of validation we do for removal data
        try:
            data.set_index(index_list, inplace=True)
        except KeyError as e:
            logger.error(
                f"Data to be removed must include {index_list} as keys/columns, but "
                f"{[name for name in data.columns]} were provided."
            )
            raise OptimizationItemUsageError(
                "The data to be removed must specify one or more complete indices to "
                "remove associated units and values!"
            ) from e

        remaining_data = existing_data[~existing_data.index.isin(data.index)]
        if not remaining_data.index.empty:
            remaining_data.reset_index(inplace=True)

        parameter.data = cast(types.JsonDict, remaining_data.to_dict(orient="list"))
