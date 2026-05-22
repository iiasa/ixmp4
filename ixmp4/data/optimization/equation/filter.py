from ixmp4.data.versions.filter import VersionFilter

from ixmp4.data.filters import optimization as opt


class EquationFilter(opt.EquationFilter, total=False):
    pass


class EquationVersionFilter(VersionFilter, opt.EquationFilter, total=False):
    pass
