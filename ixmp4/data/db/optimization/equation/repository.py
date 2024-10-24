from typing import Any, Iterable

import pandas as pd

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationItemUsageError
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import ColumnRepository, base
from .docs import EquationDocsRepository
from .model import Equation


class EquationRepository(
    base.Creator[Equation],
    base.Retriever[Equation],
    base.Enumerator[Equation],
    abstract.EquationRepository,
):
    model_class = Equation

    UsageError = OptimizationItemUsageError

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = EquationDocsRepository(*args, **kwargs)
        self.columns = ColumnRepository(*args, **kwargs)

        from .filter import EquationFilter

        self.filter_class = EquationFilter

    def _add_column(
        self,
        run_id: int,
        equation_id: int,
        column_name: str,
        indexset_name: str,
        **kwargs,
    ) -> None:
        r"""Adds a Column to an Equation.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which the
            :class:`ixmp4.data.abstract.optimization.Equation` is defined.
        equation_id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Equation`.
        column_name : str
            The name of the Column, which must be unique in connection with the names of
            :class:`ixmp4.data.abstract.Run` and
            :class:`ixmp4.data.abstract.optimization.Equation`.
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
            equation_id=equation_id,
            unique=True,
            **kwargs,
        )

    def add(
        self,
        run_id: int,
        name: str,
    ) -> Equation:
        equation = Equation(name=name, run__id=run_id)
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
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
        **kwargs,
    ) -> Equation:
        # Convert to list to avoid enumerate() splitting strings to letters
        if isinstance(constrained_to_indexsets, str):
            constrained_to_indexsets = list(constrained_to_indexsets)
        if column_names and len(column_names) != len(constrained_to_indexsets):
            raise OptimizationItemUsageError(
                f"While processing Equation {name}: \n"
                "`constrained_to_indexsets` and `column_names` not equal in length! "
                "Please provide the same number of entries for both!"
            )
        # TODO: activate something like this if each column must be indexed by a unique
        # indexset
        # if len(constrained_to_indexsets) != len(set(constrained_to_indexsets)):
        #     raise ValueError("Each dimension must be constrained to a unique indexset!") # noqa
        if column_names and len(column_names) != len(set(column_names)):
            raise OptimizationItemUsageError(
                f"While processing Equation {name}: \n"
                "The given `column_names` are not unique!"
            )

        equation = super().create(
            run_id=run_id,
            name=name,
            **kwargs,
        )
        for i, name in enumerate(constrained_to_indexsets):
            self._add_column(
                run_id=run_id,
                equation_id=equation.id,
                column_name=column_names[i] if column_names else name,
                indexset_name=name,
            )

        return equation

    @guard("view")
    def list(self, *args, **kwargs) -> Iterable[Equation]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    @guard("edit")
    def add_data(self, equation_id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, dict):
            try:
                data = pd.DataFrame.from_dict(data=data)
            except ValueError as e:
                raise Equation.DataInvalid(str(e)) from e
        equation = self.get_by_id(id=equation_id)

        missing_columns = set(["levels", "marginals"]) - set(data.columns)
        if missing_columns:
            raise OptimizationItemUsageError(
                f"Equation.data must include the column(s): "
                f"{', '.join(missing_columns)}!"
            )

        index_list = [column.name for column in equation.columns]
        existing_data = pd.DataFrame(equation.data)
        if not existing_data.empty:
            existing_data.set_index(index_list, inplace=True)
        equation.data = (
            data.set_index(index_list).combine_first(existing_data).reset_index()
        ).to_dict(orient="list")

        self.session.commit()

    @guard("edit")
    def remove_data(self, equation_id: int) -> None:
        equation = self.get_by_id(id=equation_id)
        # TODO Is there a better way to reset .data?
        equation.data = {}
        self.session.commit()
