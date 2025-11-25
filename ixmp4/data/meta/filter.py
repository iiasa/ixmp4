from typing import Annotated

from ixmp4.data import filters as base

from .db import RunMetaEntry


class RunMetaEntryFilter(base.RunMetaEntryFilter, total=False):
    run: Annotated[base.RunFilter, RunMetaEntry.run]
