from collections import UserDict
from typing import TYPE_CHECKING, cast

import numpy as np
import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

# TODO Import this from typing when dropping Python 3.11
from ixmp4.data.meta.dto import MetaValueType
from ixmp4.data.meta.filter import RunMetaEntryFilter
from ixmp4.data.meta.service import RunMetaEntryService

from .base import BaseServiceFacade

if TYPE_CHECKING:
    from ixmp4.backend import Backend

    from .run import Run


class PlatformRunMetaFacade(BaseServiceFacade[RunMetaEntryService]):
    """Used to query run meta indicators on a platform.

    .. code:: python

        df = platform.meta.tabulate(
            run={
                "default_only": False,
                "model": {"name": "Model"},
            },
        )

        print(df)

    """

    def _get_service(self, backend: "Backend") -> RunMetaEntryService:
        return backend.meta

    def tabulate(self, **kwargs: Unpack[RunMetaEntryFilter]) -> pd.DataFrame:
        r"""Tabulates metadata entries by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `RunMetaEntryFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - key
                - value
                - model
                - scenario
                - version
        """
        # TODO: accept list of `Run` instances as arg
        return self._service.tabulate(include_run_index=True, **kwargs).drop(
            columns=["id", "dtype"]
        )


class RunMetaFacade(
    BaseServiceFacade[RunMetaEntryService], UserDict[str, MetaValueType | None]
):
    """Behaves like a dictionary with the meta indicator data for a specific run.

    To set entries:

    .. code:: python

        run.meta = {"key": "value"}

        run.meta["other key"] = -1.2

    To delete entries:

    .. code:: python

        del run.meta["other key"]

        run.meta = {}

    """

    run: "Run"

    def _get_service(self, backend: "Backend") -> RunMetaEntryService:
        return backend.meta

    def __init__(self, backend: "Backend", run: "Run") -> None:
        super().__init__(backend)
        self.run = run
        self._refresh()

    def _refresh(self) -> None:
        self.df, self.data = self._get()

    def _get(self) -> tuple[pd.DataFrame, dict[str, MetaValueType | None]]:
        df = self._service.tabulate(run__id=self.run.id, run={"default_only": False})
        if df.empty:
            return df, {}
        return df, dict(zip(df["key"], df["value"]))

    def _set(self, meta: dict[str, MetaValueType | np.generic | None]) -> None:
        self.run.require_lock()

        df = pd.DataFrame({"key": self.data.keys()})
        df["run__id"] = self.run.id
        self._service.bulk_delete(df)
        df = pd.DataFrame(
            {"key": meta.keys(), "value": [numpy_to_pytype(v) for v in meta.values()]}
        )
        df.dropna(axis=0, inplace=True)
        df["run__id"] = self.run.id
        self._service.bulk_upsert(df)
        self.df, self.data = self._get()

    def __setitem__(self, key: str, value: MetaValueType | np.generic | None) -> None:
        self.run.require_lock()

        try:
            del self[key]
        except KeyError:
            pass

        py_value = numpy_to_pytype(value)
        if py_value is not None:
            self._service.create(self.run.id, key, py_value)
        self.df, self.data = self._get()

    def __delitem__(self, key: str) -> None:
        self.run.require_lock()
        id = dict(zip(self.df["key"], self.df["id"]))[key]
        self._service.delete_by_id(id)
        self.df, self.data = self._get()

    def __set__(
        self,
        obj: object,
        value: "dict[str, MetaValueType | np.generic | None] | RunMetaFacade",
    ) -> None:
        self._set(dict(value))

    def __dict__(self) -> dict[str, MetaValueType | None]:
        return dict(self.data)


def numpy_to_pytype(
    value: MetaValueType | np.generic | None,
) -> MetaValueType | None:
    """Cast numpy-types to basic Python types"""
    if value is np.nan:  # np.nan is cast to 'float', not None
        return None
    elif isinstance(value, np.generic):
        return cast(MetaValueType, value.item())
    else:
        return value
