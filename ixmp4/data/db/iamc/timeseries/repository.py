from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Bundle

from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db.iamc.measurand import Measurand
from ixmp4.data.db.region import Region, RegionRepository
from ixmp4.data.db.run import RunRepository
from ixmp4.data.db.timeseries import (
    TimeSeriesRepository as BaseTimeSeriesRepository,
)
from ixmp4.data.db.unit import Unit, UnitRepository
from ixmp4.data.db.utils import map_existing

from ..measurand import MeasurandRepository
from ..variable import Variable
from .model import TimeSeries


class TimeSeriesRepository(
    BaseTimeSeriesRepository[TimeSeries], abstract.TimeSeriesRepository
):
    model_class = TimeSeries

    regions: RegionRepository
    measurands: MeasurandRepository
    units: UnitRepository

    def __init__(self, *args, **kwargs) -> None:
        from .filter import TimeSeriesFilter

        self.filter_class = TimeSeriesFilter

        self.runs = RunRepository(*args, **kwargs)
        self.regions = RegionRepository(*args, **kwargs)
        self.measurands = MeasurandRepository(*args, **kwargs)
        self.units = UnitRepository(*args, **kwargs)
        super().__init__(*args, **kwargs)

    @guard("view")
    def get(self, run_id: int, **kwargs: Any) -> TimeSeries:
        return super().get(run_id, **kwargs)

    def select_joined_parameters(self):
        return (
            select(
                self.bundle,
                Bundle(
                    "Region",
                    Region.name.label("region"),
                ),
                Bundle(
                    "Unit",
                    Unit.name.label("unit"),
                ),
                Bundle(
                    "Variable",
                    Variable.name.label("variable"),
                ),
            )
            .join(Region, onclause=TimeSeries.region__id == Region.id)
            .join(Measurand, onclause=TimeSeries.measurand__id == Measurand.id)
            .join(Unit, onclause=Measurand.unit__id == Unit.id)
            .join(Variable, onclause=Measurand.variable__id == Variable.id)
        )

    @guard("view")
    def list(self, **kwargs) -> list[TimeSeries]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    @guard("edit")
    def bulk_upsert(self, df: pd.DataFrame, create_related: bool = False) -> None:
        if self.backend.auth_context is not None:
            run_ids = set(df["run__id"].unique().tolist())
            self.runs.check_access(
                run_ids,
                access_type="edit",
                is_default=None,
                default_only=False,
            )

        if create_related:
            df = self.map_relationships(df)
            df = df.drop_duplicates()
        super().bulk_upsert(df)

    def map_regions(self, df: pd.DataFrame):
        existing_regions = self.regions.tabulate(name__in=df["region"].unique())
        df, missing = map_existing(
            df,
            existing_df=existing_regions,
            join_on=("name", "region"),
            map=("id", "region__id"),
        )
        if len(missing) > 0:
            raise Region.NotFound(", ".join(missing))

        return df

    def map_measurands(self, df: pd.DataFrame) -> pd.DataFrame:
        df, missing = map_existing(
            df,
            existing_df=self.units.tabulate(),
            join_on=("name", "unit"),
            map=("id", "unit__id"),
        )
        if len(missing) > 0:
            raise Unit.NotFound(", ".join(missing))

        df["measurand__id"] = np.nan

        def map_measurand(df):
            variable_name, unit__id = df.name
            measurand = self.measurands.get_or_create(
                variable_name=variable_name, unit__id=int(unit__id)
            )
            df["measurand__id"] = measurand.id
            df["variable"] = variable_name
            df["unit__id"] = unit__id
            return df

        # ensure compatibility with pandas < 2.2
        # TODO remove legacy-handling when dropping support for pandas < 2.2
        if pd.__version__[0:3] in ["2.0", "2.1"]:
            apply_args = dict()
        else:
            apply_args = dict(include_groups=False)

        return pd.DataFrame(
            df.groupby(["variable", "unit__id"], group_keys=False).apply(
                map_measurand, **apply_args
            )
        )

    def map_relationships(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.map_regions(df)
        df = self.map_measurands(df)
        return df
