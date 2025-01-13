from ixmp4.data.db import filters as base
from ixmp4.data.db.run import Run
from ixmp4.db import Session, filters, sql, utils

from .model import RunMetaEntry


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    def join(
        self, exc: sql.Select[tuple[RunMetaEntry]], session: Session | None = None
    ) -> sql.Select[tuple[RunMetaEntry]]:
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=RunMetaEntry.run__id == Run.id)
        return exc


class RunMetaEntryFilter(base.RunMetaEntryFilter, metaclass=filters.FilterMeta):
    run: RunFilter = filters.Field(
        default=RunFilter(id=None, version=None, is_default=True)
    )
