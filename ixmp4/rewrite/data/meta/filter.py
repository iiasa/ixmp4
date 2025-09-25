from typing import Annotated

from ixmp4.rewrite.data import filters as base

from .db import RunMetaEntry


class RunMetaEntryFilter(base.ModelFilter, total=False):
    run: Annotated[base.RunFilter, RunMetaEntry.run]
