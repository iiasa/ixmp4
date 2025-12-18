import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.data.meta.filter import RunMetaEntryFilter
from ixmp4.data.meta.service import RunMetaEntryService

from .base import BaseServiceFacade


class RunMetaServiceFacade(BaseServiceFacade[RunMetaEntryService]):
    def _get_service(self, backend: Backend) -> RunMetaEntryService:
        return backend.meta

    def tabulate(self, **kwargs: Unpack[RunMetaEntryFilter]) -> pd.DataFrame:
        # TODO: accept list of `Run` instances as arg
        # TODO: expand run-id to model-scenario-version-id columns
        return self._service.tabulate(join_run_index=True, **kwargs).drop(
            columns=["id", "dtype"]
        )
