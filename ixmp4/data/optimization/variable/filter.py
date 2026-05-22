from ixmp4.data.filters import optimization as opt
from ixmp4.data.versions.filter import VersionFilter


class VariableFilter(opt.VariableFilter, total=False):
    pass


class VariableVersionFilter(VersionFilter, opt.VariableFilter, total=False):
    pass
