from typing import TYPE_CHECKING

from ixmp4.exceptions import OptimizationItemUsageError
from ixmp4.services import Service

if TYPE_CHECKING:
    pass


class IndexSetAssociatedService(Service):
    def check_optional_column_args(
        self,
        name: str,
        item_type_str: str,
        constrained_to_indexsets: list[str] | None = None,
        column_names: list[str] | None = None,
    ):
        if column_names:
            if constrained_to_indexsets is None:
                raise OptimizationItemUsageError(
                    f"While processing {item_type_str} {name}: \n"
                    "Received `column_names` to name columns, but no "
                    "`constrained_to_indexsets` to indicate which IndexSets to use for "
                    "these columns. Please provide `constrained_to_indexsets` or "
                    "remove `column_names`!"
                )
            self.check_column_args(
                name, item_type_str, constrained_to_indexsets, column_names
            )

    def check_column_args(
        self,
        name: str,
        item_type_str: str,
        constrained_to_indexsets: list[str],
        column_names: list[str],
    ):
        if len(column_names) != len(constrained_to_indexsets):
            raise OptimizationItemUsageError(
                f"While processing {item_type_str} {name}: \n"
                "`constrained_to_indexsets` and `column_names` not equal in length!"
                "Please provide the same number of entries for both!"
            )
        if len(column_names) != len(set(column_names)):
            raise OptimizationItemUsageError(
                f"While processing {item_type_str} {name}: \n"
                "The given `column_names` are not unique!"
            )
