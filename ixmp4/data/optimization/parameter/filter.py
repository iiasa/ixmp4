from ixmp4.data.versions.filter import VersionFilter

from ixmp4.data.filters import optimization as opt


class ParameterFilter(opt.ParameterFilter, total=False):
    pass


class ParameterVersionFilter(VersionFilter, opt.ParameterFilter, total=False):
    pass
