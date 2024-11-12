from typing import ClassVar

from ixmp4.db import Session, filters, sql, utils

from .. import Run, Scenario


class ScenarioFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)

    sqla_model: ClassVar[type] = Scenario

    def join(self, exc: sql.Select, session: Session | None = None) -> sql.Select:
        if not utils.is_joined(exc, Scenario):
            exc = exc.join(Scenario, Run.scenario)
        return exc
