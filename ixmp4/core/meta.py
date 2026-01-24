from collections import UserDict
from typing import TYPE_CHECKING, Any, cast

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
    from ixmp4.data.backend import Backend

    from .run import Run


class PlatformRunMetaFacade(BaseServiceFacade[RunMetaEntryService]):
    """Used to query run meta indicators on a platform."""

    def _get_service(self, backend: "Backend") -> RunMetaEntryService:
        return backend.meta

    def tabulate(self, **kwargs: Unpack[RunMetaEntryFilter]) -> pd.DataFrame:
        r"""Tabulates metadata entries by specified criteria.

        .. code:: python

            df = platform.meta.tabulate(
                run={
                    "default_only": False,
                    "model": {"name": "Model"},
                }
            )
            #>     key      value  model   scenario  version
            # 0  indicator  1.23   Model  Scenario  1


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


class RunMetaDictFacade(
    BaseServiceFacade[RunMetaEntryService], UserDict[str, MetaValueType | None]
):
    """Behaves like a dictionary with the meta indicator data for a specific run."""

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

    def __setitem__(self, key: str, value: MetaValueType | np.generic | None) -> None:
        """
        .. code:: python

            run.meta["key"] = -1.2

            run.meta["key"]
            #> 'value'
        """
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
        """
        .. code:: python

            del run.meta["key"]
            run.meta["key"]
            #> `KeyError`

        """
        self.run.require_lock()
        id = dict(zip(self.df["key"], self.df["id"]))[key]
        self._service.delete_by_id(id)
        self.df, self.data = self._get()

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


class RunMetaDescriptor(object):
    """Descriptor class for the 'meta' property of a run."""

    def _get_entry_df(self, run: "Run") -> pd.DataFrame:
        return run._backend.meta.tabulate(
            run__id=run._dto.id, run={"default_only": False}
        )

    def _delete_existing(self, run: "Run") -> None:
        existing_df = self._get_entry_df(run)
        run._backend.meta.bulk_delete(existing_df[["run__id", "key"]])

    def __set__(
        self, obj: "Run", value: dict[str, MetaValueType | np.generic | None]
    ) -> None:
        """
        Sets meta indicators for a run consistent with normal property syntax.

        .. code:: python

            run.meta = {"key": "value"}

        """
        obj.require_lock()
        self._delete_existing(obj)

        df = pd.DataFrame(
            {"key": value.keys(), "value": [numpy_to_pytype(v) for v in value.values()]}
        )
        df.dropna(axis=0, inplace=True)
        df["run__id"] = obj._dto.id
        obj._backend.meta.bulk_upsert(df)

    def __get__(self, obj: "Run", objtype: type[Any]) -> RunMetaDictFacade:
        """
        Retrieves the meta indicators for a run object.

        .. code:: python

            run.meta
            #> {"key": "value"}

        Returns
        =======
        :class:`ixmp4.core.meta.RunMetaDictFacade`
            A special object that behaves like a dictionary.
        """
        return RunMetaDictFacade(obj._backend, obj)
