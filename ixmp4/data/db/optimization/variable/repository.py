from typing import Any, Iterable

import pandas as pd

from ixmp4 import db
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import ColumnRepository, base
from .docs import OptimizationVariableDocsRepository
from .model import OptimizationVariable as Variable


class VariableRepository(
    base.Creator[Variable],
    base.Retriever[Variable],
    base.Enumerator[Variable],
    abstract.VariableRepository,
):
    model_class = Variable

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = OptimizationVariableDocsRepository(*args, **kwargs)
        self.columns = ColumnRepository(*args, **kwargs)

        from .filter import OptimizationVariableFilter

        self.filter_class = OptimizationVariableFilter

    def _add_column(
        self,
        run_id: int,
        variable_id: int,
        column_name: str,
        indexset_name: str,
        **kwargs,
    ) -> None:
        r"""Adds a Column to a Variable.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which the
            :class:`ixmp4.data.abstract.optimization.Variable` is defined.
        variable_id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Variable`.
        column_name : str
            The name of the Column, which must be unique in connection with the names of
            :class:`ixmp4.data.abstract.Run` and
            :class:`ixmp4.data.abstract.optimization.Variable`.
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
            dtype=pd.Series(indexset.elements).dtype.name,
            variable_id=variable_id,
            unique=True,
            **kwargs,
        )

    def add(
        self,
        run_id: int,
        name: str,
    ) -> Variable:
        variable = Variable(name=name, run__id=run_id)
        variable.set_creation_info(auth_context=self.backend.auth_context)
        self.session.add(variable)

        return variable

    @guard("view")
    def get(self, run_id: int, name: str) -> Variable:
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
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
        **kwargs,
    ) -> Variable:
        # Convert to list to avoid enumerate() splitting strings to letters
        if isinstance(constrained_to_indexsets, str):
            constrained_to_indexsets = list(constrained_to_indexsets)
        if column_names and len(column_names) != len(constrained_to_indexsets):
            raise ValueError(
                "`constrained_to_indexsets` and `column_names` not equal in length! "
                "Please provide the same number of entries for both!"
            )
        # TODO: activate something like this if each column must be indexed by a unique
        # indexset
        # if len(constrained_to_indexsets) != len(set(constrained_to_indexsets)):
        #     raise ValueError("Each dimension must be constrained to a unique indexset!") # noqa
        if column_names and len(column_names) != len(set(column_names)):
            raise ValueError("The given `column_names` are not unique!")

        variable = super().create(
            run_id=run_id,
            name=name,
            **kwargs,
        )
        for i, name in enumerate(constrained_to_indexsets):
            self._add_column(
                run_id=run_id,
                variable_id=variable.id,
                column_name=column_names[i] if column_names else name,
                indexset_name=name,
            )

        return variable

    @guard("view")
    def list(self, *args, **kwargs) -> Iterable[Variable]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    @guard("edit")
    def add_data(self, variable_id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, dict):
            data = pd.DataFrame.from_dict(data=data)
        variable = self.get_by_id(id=variable_id)

        missing_columns = set(["levels", "marginals"]) - set(data.columns)
        assert (
            not missing_columns
        ), f"Variable.data must include the column(s): {', '.join(missing_columns)}!"

        variable.data = pd.concat(
            [pd.DataFrame.from_dict(variable.data), data]
        ).to_dict(orient="list")

        self.session.add(variable)
        self.session.commit()
