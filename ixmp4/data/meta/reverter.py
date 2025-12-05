from typing import Any

import sqlalchemy as sa
from toolkit import db

from ixmp4.data.versions.reverter import Reverter, ReverterRepository

from .db import (
    RunMetaEntry,
    RunMetaEntryVersion,
)


class MetaReverterRepository(ReverterRepository):
    target = db.r.ModelTarget(RunMetaEntry)
    version_target = db.r.ModelTarget(RunMetaEntryVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(RunMetaEntryVersion).where(
            RunMetaEntryVersion.run__id == run__id
        )


run_reverter = Reverter(targets=[MetaReverterRepository])
