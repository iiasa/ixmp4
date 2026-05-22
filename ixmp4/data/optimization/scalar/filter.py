from ixmp4.data.versions.filter import VersionFilter

from ixmp4.data.filters import optimization as opt


class ScalarFilter(opt.ScalarFilter, total=False):
    pass


class ScalarVersionFilter(VersionFilter, opt.ScalarFilter, total=False):
    pass
