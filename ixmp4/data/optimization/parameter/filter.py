from ixmp4.data.filters import optimization as opt
from ixmp4.data.versions.filter import VersionFilter


class ParameterFilter(opt.ParameterFilter, total=False):
    pass


class ParameterVersionFilter(VersionFilter, opt.ParameterFilter, total=False):
    pass
