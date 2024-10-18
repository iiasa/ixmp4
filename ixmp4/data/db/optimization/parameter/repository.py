from typing import Any, Iterable

import pandas as pd

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationItemUsageError
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db.unit import Unit

from .. import ColumnRepository, base
from .docs import ParameterDocsRepository
from .model import Parameter


class ParameterRepository(
    base.Creator[Parameter],
    base.Retriever[Parameter],
    base.Enumerator[Parameter],
    abstract.ParameterRepository,
):
    model_class = Parameter

    UsageError = OptimizationItemUsageError

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = ParameterDocsRepository(*args, **kwargs)
        self.columns = ColumnRepository(*args, **kwargs)

        from .filter import OptimizationParameterFilter

        self.filter_class = OptimizationParameterFilter

    def _add_column(
        self,
        run_id: int,
        parameter_id: int,
        column_name: str,
        indexset_name: str,
        **kwargs,
    ) -> None:
        r"""Adds a Column to a Parameter.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which the
            :class:`ixmp4.data.abstract.optimization.Parameter` is defined.
        parameter_id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Parameter`.
        column_name : str
            The name of the Column, which must be unique in connection with the names of
            :class:`ixmp4.data.abstract.Run` and
            :class:`ixmp4.data.abstract.optimization.Parameter`.
        indexset_name : str
            The name of the :class:`ixmp4.data.abstract.optimization.IndexSet` the
            Column will be linked to.
        \*\*kwargs: any
            Keyword arguments to be passed to
            :func:`ixmp4.data.abstract.optimization.Column.create`.
        """
        indexset = self.backend.optimization.indexsets.get(
            run_id=run_id, name=indexset_name
        )
        self.columns.create(
            name=column_name,
            constrained_to_indexset=indexset.id,
            dtype=pd.Series(indexset.data).dtype.name,
            parameter_id=parameter_id,
            unique=True,
            **kwargs,
        )

    def add(
        self,
        run_id: int,
        name: str,
    ) -> Parameter:
        parameter = Parameter(name=name, run__id=run_id)
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
        **kwargs,
    ) -> Parameter:
        # Convert to list to avoid enumerate() splitting strings to letters
        if isinstance(constrained_to_indexsets, str):
            constrained_to_indexsets = list(constrained_to_indexsets)
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

        parameter = super().create(
            run_id=run_id,
            name=name,
            **kwargs,
        )
        for i, name in enumerate(constrained_to_indexsets):
            self._add_column(
                run_id=run_id,
                parameter_id=parameter.id,
                column_name=column_names[i] if column_names else name,
                indexset_name=name,
            )

        return parameter

    @guard("view")
    def list(self, *args, **kwargs) -> Iterable[Parameter]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

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

        index_list = [column.name for column in parameter.columns]
        existing_data = pd.DataFrame(parameter.data)
        if not existing_data.empty:
            existing_data.set_index(index_list, inplace=True)
        parameter.data = (
            data.set_index(index_list).combine_first(existing_data).reset_index()
        ).to_dict(orient="list")

        self.session.commit()
