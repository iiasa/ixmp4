import pandas as pd

from .base import BaseFacade


class MetaRepository(BaseFacade):
    def tabulate(self, **kwargs) -> pd.DataFrame:
        # TODO: accept list of `Run` instances as arg

        runs = self.backend.runs.tabulate(**kwargs.get("run", {})).set_index("id")
        meta = self.backend.meta.tabulate(**kwargs)

        for column, mapper in [
            ("model", self.backend.models.map()),
            ("scenario", self.backend.scenarios.map()),
        ]:
            meta[column] = meta["run__id"].map(dict(runs[f"{column}__id"])).map(mapper)
        meta["version"] = meta["run__id"].map(dict(runs["version"])).map(int)
        return meta[["model", "scenario", "version", "key", "value"]]
