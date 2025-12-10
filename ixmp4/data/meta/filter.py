from typing import Annotated

from ixmp4.data import filters as base
from ixmp4.data.run.db import Run

from .db import RunMetaEntry


class RunFilter(base.RunFilter, total=False):
    model: Annotated[base.ModelFilter, Run.model]
    scenario: Annotated[base.ScenarioFilter, Run.scenario]


class RunMetaEntryFilter(base.RunMetaEntryFilter, total=False):
    run: Annotated[RunFilter, RunMetaEntry.run]
