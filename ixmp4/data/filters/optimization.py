from .base import (
    IdFilter,
    NameFilter,
    RunIdFilter,
)


class EquationFilter(IdFilter, NameFilter, RunIdFilter, total=False):
    pass


class IndexSetFilter(IdFilter, NameFilter, RunIdFilter, total=False):
    pass


class ParameterFilter(IdFilter, NameFilter, RunIdFilter, total=False):
    pass


class ScalarFilter(IdFilter, NameFilter, RunIdFilter, total=False):
    pass


class TableFilter(IdFilter, NameFilter, RunIdFilter, total=False):
    pass


class VariableFilter(IdFilter, NameFilter, RunIdFilter, total=False):
    pass
