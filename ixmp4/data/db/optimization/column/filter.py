from ixmp4.data.db import filters as base
from ixmp4.data.db.optimization.table import Table
from ixmp4.db import filters, utils

from .model import Column


# NB: This is currently not in use, but would be the basis to filter Columns by
# corresponding Tables
class OptimizationTableFilter(
    base.OptimizationTableFilter, metaclass=filters.FilterMeta
):
    def join(self, exc, session=None):
        if not utils.is_joined(exc, Table):
            exc = exc.join(Table, onclause=Column.table__id == Table.id)
        return exc


class OptimizationColumnFilter(
    base.OptimizationColumnFilter, metaclass=filters.FilterMeta
):
    def join(self, exc, session=None):
        return exc
