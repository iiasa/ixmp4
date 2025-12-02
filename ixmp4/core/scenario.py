from datetime import datetime

import pandas as pd
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.core.base import BaseDocsServiceFacade, BaseFacadeObject
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.scenario.dto import Scenario as ScenarioDto
from ixmp4.data.scenario.exceptions import (
    ScenarioDeletionPrevented,
    ScenarioNotFound,
    ScenarioNotUnique,
)
from ixmp4.data.scenario.filter import ScenarioFilter
from ixmp4.data.scenario.service import ScenarioService


class Scenario(BaseFacadeObject[ScenarioService, ScenarioDto]):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    DeletionPrevented = ScenarioDeletionPrevented

    @property
    def id(self) -> int:
        return self.dto.id

    @property
    def name(self) -> str:
        return self.dto.name

    @property
    def created_at(self) -> datetime | None:
        return self.dto.created_at

    @property
    def created_by(self) -> str | None:
        return self.dto.created_by

    @property
    def docs(self) -> str | None:
        try:
            return self.service.get_docs(self.id).description
        except DocsNotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self.service.delete_docs(self.id)
        else:
            self.service.set_docs(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self.service.delete_docs(self.id)
        # TODO: silently failing
        except DocsNotFound:
            return None

    def delete(self) -> None:
        self.service.delete_by_id(self.dto.id)

    def get_service(self, backend: Backend) -> ScenarioService:
        return backend.scenarios

    def __str__(self) -> str:
        return f"<Scenario {self.id} name='{self.name}'>"


class ScenarioServiceFacade(
    BaseDocsServiceFacade[Scenario | int | str, Scenario, ScenarioService]
):
    def get_service(self, backend: Backend) -> ScenarioService:
        return backend.scenarios

    def get_item_id(self, ref: Scenario | int | str) -> int:
        if isinstance(ref, Scenario):
            return ref.id
        elif isinstance(ref, int):
            return ref
        elif isinstance(ref, str):
            dto = self.service.get_by_name(ref)
            return dto.id
        else:
            raise ValueError(f"Invalid reference to scenario: {ref}")

    def create(
        self,
        name: str,
    ) -> Scenario:
        scen = self.service.create(name)
        return Scenario(backend=self.backend, dto=scen)

    def delete(self, ref: Scenario | int | str) -> None:
        id = self.get_item_id(ref)
        self.service.delete_by_id(id)

    def get_by_name(self, name: str) -> Scenario:
        scen = self.service.get_by_name(name)
        return Scenario(self.backend, scen)

    def list(self, **kwargs: Unpack[ScenarioFilter]) -> list[Scenario]:
        units = self.service.list(**kwargs)
        return [Scenario(self.backend, dto) for dto in units]

    def tabulate(self, **kwargs: Unpack[ScenarioFilter]) -> pd.DataFrame:
        return self.service.tabulate(**kwargs)
