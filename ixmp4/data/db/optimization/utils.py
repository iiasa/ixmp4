from typing import TYPE_CHECKING, Any, TypeVar, Union

import pandas as pd

from ixmp4 import db
from ixmp4.db.utils.revert import select_for_id_map

from . import base

if TYPE_CHECKING:
    from .equation.model import Equation
    from .equation.repository import EquationRepository
    from .indexset.model import IndexSet
    from .indexset.repository import IndexSetRepository
    from .parameter.model import Parameter
    from .parameter.repository import ParameterRepository
    from .table.model import Table
    from .table.repository import TableRepository
    from .variable.model import OptimizationVariable
    from .variable.repository import VariableRepository


def validate_data(
    host: base.BaseModel,
    data: dict[str, Any],
    indexsets: list["IndexSet"],
    column_names: list[str] | None = None,
    has_extra_columns: bool = True,
) -> None:
    data_frame = pd.DataFrame.from_dict(data)

    # Can't validate ("values","units") or ("levels", "marginals") when they are present
    number_columns = (
        len(data_frame.columns) - 2 if has_extra_columns else len(data_frame.columns)
    )
    columns = (
        column_names if column_names else [indexset.name for indexset in indexsets]
    )

    # TODO for all of the following, we might want to create unique exceptions
    # Could me make both more specific by specifiying missing/extra columns?
    if number_columns < len(indexsets):
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \n"
            f"Data is missing for some columns! \n Data: {data} \n "
            f"Columns: {columns}"
        )
    elif number_columns > len(indexsets):
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \n"
            f"Trying to add data to unknown columns! \n Data: {data} \n "
            f"Columns: {columns}"
        )

    # We could make this more specific maybe by pointing to the missing values
    if data_frame.isna().any(axis=None):
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \n"
            "The data is missing values, please make sure it "
            "does not contain None or NaN, either!"
        )

    limited_to_indexsets = {
        columns[i]: indexsets[i].data for i in range(len(indexsets))
    }

    # We can make this more specific e.g. highlighting all duplicate rows via
    # pd.DataFrame.duplicated(keep="False")
    if data_frame[limited_to_indexsets.keys()].value_counts().max() > 1:
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \nThe data contains duplicate rows!"
        )

    # Can we make this more specific? Iterating over columns; if any is False,
    # return its name or something?
    if (
        not data_frame[limited_to_indexsets.keys()]
        .isin(limited_to_indexsets)
        .all(axis=None)
    ):
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \n"
            "The data contains values that are not allowed as per the IndexSets "
            "it is constrained to!"
        )


# NOTE | does not seem to work yet for stringified type hints
def _find_columns_linked_to_indexset(
    item: Union["Table", "Parameter", "Equation", "OptimizationVariable"], name: str
) -> list[str]:
    """Determine columns in `item`.data that are linked to IndexSet `name`.

    Only works for `items` linked to IndexSet `name`.
    """
    if not item.column_names:
        # The item's indexset_names must be a unique list of names
        return [name]
    else:
        # If we have column_names, we must also have indexsets
        assert item.indexset_names

        # Handle possible duplicate values
        return [
            item.column_names[i]
            for i in range(len(item.column_names))
            if item.indexset_names[i] == name
        ]


ReposForIdMapType = TypeVar(
    "ReposForIdMapType",
    "EquationRepository",
    "IndexSetRepository",
    "ParameterRepository",
    "TableRepository",
    "VariableRepository",
)


def create_id_map_subquery(
    transaction__id: int, run__id: int, repo: ReposForIdMapType
) -> db.sql.Subquery:
    old_items_subquery = select_for_id_map(
        model_class=repo.versions.model_class,
        run__id=run__id,
        transaction__id=transaction__id,
    ).subquery()
    new_items_subquery = select_for_id_map(
        model_class=repo.model_class, run__id=run__id
    ).subquery()

    return db.utils.create_id_map_subquery(
        old_exc=old_items_subquery, new_exc=new_items_subquery
    )
