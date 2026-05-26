from ixmp4.data.filters import optimization as opt
from ixmp4.data.versions.filter import VersionFilter


class IndexSetFilter(opt.IndexSetFilter, total=False):
    pass


class IndexSetVersionFilter(VersionFilter, opt.IndexSetFilter, total=False):
    pass


class IndexSetDataVersionFilter(VersionFilter, total=False):
    indexset__id: int
    indexset__id__in: list[int]
    value: str
