from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend


import pandas as pd

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationItemUsageError
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db.unit import Unit

from .. import ColumnRepository, base
from .docs import ParameterDocsRepository
from .model import Parameter, ParameterIndexsetAssociation


class ParameterRepository(
    base.Creator[Parameter],
    base.Retriever[Parameter],
    base.Enumerator[Parameter],
    abstract.ParameterRepository,
):
    model_class = Parameter

    UsageError = OptimizationItemUsageError

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)
        self.docs = ParameterDocsRepository(*args)
        self.columns = ColumnRepository(*args)

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
        parameter.set_creation_info(auth_context=self.backend.auth_context)
        indexsets = self.backend.optimization.indexsets.list(
            name__in=constrained_to_indexsets, run_id=run_id
        )

        for i in range(len(indexsets)):
            _ = ParameterIndexsetAssociation(
                parameter=parameter,
                indexset=indexsets[i],
                column_name=column_names[i] if column_names else None,
            )
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
        # TODO: activate something like this if each column must be indexed by a unique
        # indexset
        # if len(constrained_to_indexsets) != len(set(constrained_to_indexsets)):
        #     raise self.UsageError("Each dimension must be constrained to a unique indexset!") # noqa
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

    @guard("view")
    def list(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> Iterable[Parameter]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack["base.EnumerateKwargs"]) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    @guard("edit")
    def add_data(self, parameter_id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, dict):
            try:
                data = pd.DataFrame.from_dict(data=data)
            except ValueError as e:
                raise Parameter.DataInvalid(str(e)) from e

        parameter = self.get_by_id(id=parameter_id)

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

        index_list = (
            parameter.column_names if parameter.column_names else parameter.indexsets
        )
        existing_data = pd.DataFrame(parameter.data)
        if not existing_data.empty:
            existing_data.set_index(index_list, inplace=True)
        # TODO Ignoring this for now since I'll likely refactor this soon, anyway
        # Same applies to equation, table, and variable.
        parameter.data = (
            data.set_index(index_list).combine_first(existing_data).reset_index()
        ).to_dict(orient="list")  # type: ignore[assignment]

        self.session.commit()
