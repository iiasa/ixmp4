# TODO Use `type` instead of TypeAlias when dropping Python 3.11
from datetime import datetime
from typing import Any, ClassVar, TypeAlias, cast

import pandas as pd
from pydantic import Field

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.data import abstract

from . import base
from .model import Model
from .scenario import Scenario


class Run(base.BaseModel):
    NotFound: ClassVar = abstract.Run.NotFound
    NotUnique: ClassVar = abstract.Run.NotUnique
    DeletionPrevented: ClassVar = abstract.Run.DeletionPrevented

    NoDefaultVersion: ClassVar = abstract.Run.NoDefaultVersion

    id: int

    model: Model
    id_of_model: int = Field(..., alias="model__id")

    scenario: Scenario
    scenario__id: int

    version: int
    is_default: bool

    created_at: datetime | None
    created_by: str

    updated_at: datetime | None
    updated_by: str | None

    lock_transaction: int | None = None


JsonType: TypeAlias = dict[
    str,
    bool
    | abstract.annotations.IntFilterAlias
    | dict[
        str,
        abstract.annotations.DefaultFilterAlias
        | dict[str, abstract.annotations.DefaultFilterAlias],
    ]
    | None,
]


class RunRepository(
    base.Creator[Run],
    base.Deleter[Run],
    base.Retriever[Run],
    base.Enumerator[Run],
    base.VersionManager[Run],
    abstract.RunRepository,
):
    model_class = Run
    prefix = "runs/"

    def create(self, model_name: str, scenario_name: str) -> Run:
        return super().create(model_name=model_name, scenario_name=scenario_name)

    def delete(self, id: int) -> None:
        super().delete(id)

    def get(self, model_name: str, scenario_name: str, version: int) -> Run:
        return super().get(
            model={"name": model_name},
            scenario={"name": scenario_name},
            version=version,
            default_only=False,
            is_default=None,
        )

    def get_by_id(self, id: int) -> Run:
        res = self._get_by_id(id)
        return Run(**res)

    def enumerate(
        self, **kwargs: Unpack[abstract.run.EnumerateKwargs]
    ) -> list[Run] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(self, **kwargs: Unpack[abstract.run.EnumerateKwargs]) -> list[Run]:
        json = cast(JsonType, kwargs)
        return super()._list(json=json)

    def tabulate(self, **kwargs: Unpack[abstract.run.EnumerateKwargs]) -> pd.DataFrame:
        json = cast(JsonType, kwargs)
        return super()._tabulate(json=json)

    def get_default_version(self, model_name: str, scenario_name: str) -> Run:
        try:
            return super().get(
                model={"name": model_name},
                scenario={"name": scenario_name},
                is_default=True,
            )
        except Run.NotFound:
            raise Run.NoDefaultVersion

    def set_as_default_version(self, id: int) -> None:
        self._request(
            "POST",
            self.prefix + "/".join([str(id), "set-as-default-version/"]),
        )

    def unset_as_default_version(self, id: int) -> None:
        self._request(
            "POST",
            self.prefix + "/".join([str(id), "unset-as-default-version/"]),
        )

    def revert(self, id: int, transaction__id: int) -> None:
        self._request(
            "POST",
            self.prefix + "/".join([str(id), "revert/"]),
            json={"transaction__id": transaction__id},
        )

    def lock(self, id: int) -> Run:
        run_dict = self._request(
            "POST",
            self.prefix + "/".join([str(id), "lock/"]),
        )
        return Run(**cast(dict[str, Any], run_dict))

    def unlock(self, id: int) -> Run:
        run_dict = self._request(
            "POST",
            self.prefix + "/".join([str(id), "unlock/"]),
        )
        return Run(**cast(dict[str, Any], run_dict))

    def tabulate_versions(self, /, run__id: int | None = None) -> pd.DataFrame:
        return self._tabulate(path="versions/", json={"run__id": run__id})

    def tabulate_transactions(self, /, run__id: int | None = None) -> pd.DataFrame:
        return self._tabulate(path="transactions/", json={"run__id": run__id})

    def clone(
        self,
        run_id: int,
        model_name: str | None = None,
        scenario_name: str | None = None,
        keep_solution: bool = True,
    ) -> Run:
        # Can expect this endpoint, so cast should always be fine
        res = cast(
            dict[str, Any],
            self._request(
                "POST",
                self.prefix + "clone/",
                json={
                    "run_id": run_id,
                    "model_name": model_name,
                    "scenario_name": scenario_name,
                    "keep_solution": keep_solution,
                },
            ),
        )
        return Run(**res)
