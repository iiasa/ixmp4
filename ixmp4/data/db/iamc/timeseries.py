from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Bundle

from ixmp4 import db
from ixmp4.data import abstract, types
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db.iamc.measurand import Measurand
from ixmp4.db import utils

from ..region import Region, RegionRepository
from ..run import RunRepository
from ..timeseries import TimeSeries as BaseTimeSeries
from ..timeseries import TimeSeriesRepository as BaseTimeSeriesRepository
from ..unit import Unit, UnitRepository
from ..utils import map_existing
from . import base
from .measurand import MeasurandRepository
from .variable import Variable


class TimeSeries(BaseTimeSeries, base.BaseModel):
    __table_args__ = (db.UniqueConstraint("run__id", "region__id", "measurand__id"),)

    region__id: types.Integer = db.Column(
        db.Integer, db.ForeignKey("region.id"), nullable=False, index=True
    )
    region: types.Mapped[Region] = db.relationship(
        "Region", backref="metadata", foreign_keys=[region__id], lazy="select"
    )

    measurand__id: types.Integer = db.Column(
        db.Integer, db.ForeignKey("iamc_measurand.id"), nullable=False, index=True
    )
    measurand: types.Mapped[Measurand] = db.relationship(
        "Measurand", backref="metadata", foreign_keys=[measurand__id], lazy="select"
    )

    @property
    def parameters(self) -> Mapping:
        return {
            "region": self.region.name,
            "unit": self.measurand.unit.name,
            "variable": self.measurand.variable.name,
        }


class TimeSeriesRepository(
    BaseTimeSeriesRepository[TimeSeries], abstract.TimeSeriesRepository
):
    model_class = TimeSeries

    regions: RegionRepository
    measurands: MeasurandRepository
    units: UnitRepository

    def __init__(self, *args, **kwargs) -> None:
        self.runs = RunRepository(*args, **kwargs)
        self.regions = RegionRepository(*args, **kwargs)
        self.measurands = MeasurandRepository(*args, **kwargs)
        self.units = UnitRepository(*args, **kwargs)
        super().__init__(*args, **kwargs)

    @guard("view")
    def get(self, run_id: int, **kwargs: Any) -> TimeSeries:
        return super().get(run_id, **kwargs)

    def filter_by_parameters(
        self, exc: db.sql.Select, parameters: Any
    ) -> db.sql.Select:
        if not utils.is_joined(exc, Measurand):
            exc = exc.join(TimeSeries.measurand)

        for key, col in [
            ("region", TimeSeries.region__id),
            ("variable", Measurand.variable__id),
            ("unit", Measurand.unit__id),
        ]:
            value = parameters.pop(key, None)
            if value is not None:
                exc = exc.where(col == value)

        if len(parameters) > 0:
            raise ValueError(
                "Invalid `parameters` supplied: " + ", ".join(parameters.keys())
            )
        return exc

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
    def list(self, *args, **kwargs) -> Iterable[TimeSeries]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

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
            return df

        return pd.DataFrame(
            df.groupby(["variable", "unit__id"], group_keys=False).apply(map_measurand)
        )

    def map_relationships(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.map_regions(df)
        df = self.map_measurands(df)
        return df
