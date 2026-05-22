from ixmp4.data.versions.filter import VersionFilter

from ixmp4.data.filters import optimization as opt


class TableFilter(opt.TableFilter, total=False):
    pass


class TableVersionFilter(VersionFilter, opt.TableFilter, total=False):
    pass
