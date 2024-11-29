import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.data.abstract.meta import EnumerateKwargs

from .base import BaseFacade


class MetaRepository(BaseFacade):
    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        # TODO: accept list of `Run` instances as arg
        # TODO: expand run-id to model-scenario-version-id columns
        return self.backend.meta.tabulate(join_run_index=True, **kwargs).drop(
            columns=["id", "dtype"]
        )
