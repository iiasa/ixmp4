# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict

from ixmp4.data.backend import Backend

from .data import OptimizationData


class InitKwargs(TypedDict):
    _backend: Backend | None
