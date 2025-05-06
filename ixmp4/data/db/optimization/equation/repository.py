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
from .docs import EquationDocsRepository
from .model import Equation, EquationIndexsetAssociation


class EquationRepository(
    base.Creator[Equation],
    base.Retriever[Equation],
    base.Enumerator[Equation],
    abstract.EquationRepository,
):
    model_class = Equation

    UsageError = OptimizationItemUsageError

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)
        self.docs = EquationDocsRepository(*args)

        from .filter import EquationFilter

        self.filter_class = EquationFilter

    def add(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Equation:
        equation = Equation(name=name, run__id=run_id)

        indexsets = self.backend.optimization.indexsets.list(
            name__in=constrained_to_indexsets, run_id=run_id
        )

        for i in range(len(indexsets)):
            _ = EquationIndexsetAssociation(
                equation=equation,
                indexset=indexsets[i],
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
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Equation:
        if column_names and len(column_names) != len(constrained_to_indexsets):
            raise OptimizationItemUsageError(
                f"While processing Equation {name}: \n"
                "`constrained_to_indexsets` and `column_names` not equal in length! "
                "Please provide the same number of entries for both!"
            )

        if column_names and len(column_names) != len(set(column_names)):
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
        equation = self.get_by_id(id=id)

        missing_columns = set(["levels", "marginals"]) - set(data.columns)
        if missing_columns:
            raise OptimizationItemUsageError(
                f"Equation.data must include the column(s): "
                f"{', '.join(missing_columns)}!"
            )

        index_list = (
            equation.column_names if equation.column_names else equation.indexset_names
        )
        existing_data = pd.DataFrame(equation.data)
        if not existing_data.empty:
            existing_data.set_index(index_list, inplace=True)

        equation.data = cast(
            types.JsonDict,
            (
                data.set_index(index_list).combine_first(existing_data).reset_index()
            ).to_dict(orient="list"),
        )

        self.session.commit()

    @guard("edit")
    def remove_data(self, id: int) -> None:
        equation = self.get_by_id(id=id)
        # TODO Is there a better way to reset .data?
        equation.data = {}
        self.session.commit()
