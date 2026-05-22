from ixmp4.data.versions.filter import VersionFilter

from ixmp4.data.filters import optimization as opt


class VariableFilter(opt.VariableFilter, total=False):
    pass


class VariableVersionFilter(VersionFilter, opt.VariableFilter, total=False):
    pass
