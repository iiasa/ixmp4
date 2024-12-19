from typing import Any, ClassVar

from ixmp4.db import Session, filters, sql, typing_column, utils

from .. import Run
from ..base import SelectType
from ..iamc import TimeSeries
from .model import ModelFilter
from .scenario import ScenarioFilter


class RunFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    version: filters.Integer | None = filters.Field(None)
    default_only: filters.Boolean = filters.Field(True)
    is_default: filters.Boolean | None = filters.Field(None)
    model: ModelFilter | None = filters.Field(None)
    scenario: ScenarioFilter | None = filters.Field(None)

    sqla_model: ClassVar[type] = Run

    def filter_default_only(
        self,
        exc: sql.Select[tuple[Run]],
        c: typing_column[Any],  # Any since it is unused
        v: bool,
        session: Session | None = None,
    ) -> sql.Select[tuple[Run]]:
        return exc.where(Run.is_default) if v else exc

    def join(self, exc: SelectType, session: Session | None = None) -> SelectType:
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, TimeSeries.run)
        return exc
