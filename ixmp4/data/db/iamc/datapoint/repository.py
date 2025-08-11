from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pandas as pd
import pandera as pa
from pandera.engines import pandas_engine
from pandera.pandas import DataFrameModel
from pandera.typing import DataFrame, Series

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4 import db
from ixmp4.core.decorators import check_types
from ixmp4.core.exceptions import InconsistentIamcType, ProgrammingError
from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db import versions
from ixmp4.data.db.model import Model
from ixmp4.data.db.region import Region, RegionVersion
from ixmp4.data.db.run import Run, RunRepository
from ixmp4.data.db.scenario import Scenario
from ixmp4.data.db.unit import Unit, UnitVersion
from ixmp4.db.filters import BaseFilter

from .. import base
from ..measurand import Measurand, MeasurandVersion
from ..timeseries import TimeSeries, TimeSeriesRepository, TimeSeriesVersion
from ..variable import Variable, VariableVersion
from . import get_datapoint_model
from .filter import DataPointFilter
from .model import DataPoint, UniversalDataPointVersion

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend


class RemoveDataPointFrameSchema(DataFrameModel):
    type: Series[pa.String] | None = pa.Field(isin=[t for t in DataPoint.Type])
    step_year: Series[pa.Int] | None = pa.Field(coerce=True, nullable=True)
    step_datetime: Series[pandas_engine.DateTime] | None = pa.Field(
        coerce=True, nullable=True
    )
    step_category: Series[pa.String] | None = pa.Field(nullable=True)

    time_series__id: Series[pa.Int] = pa.Field(coerce=True)

    @pa.dataframe_check
    def validate_type(cls, df: pd.DataFrame) -> bool:
        types = df.type.unique()

        # mixed types are currently not supported
        if len(types) != 1:
            raise InconsistentIamcType

        if types[0] == DataPoint.Type.ANNUAL:
            cols = dict(step_year="valid", step_category="empty", step_datetime="empty")
        elif types[0] == DataPoint.Type.CATEGORICAL:
            cols = dict(step_year="valid", step_category="valid", step_datetime="empty")
        elif types[0] == DataPoint.Type.DATETIME:
            cols = dict(step_year="empty", step_category="empty", step_datetime="valid")
        else:
            raise ProgrammingError

        for col, content in cols.items():
            if infer_content(df, col) is not content:
                raise InconsistentIamcType
        return True


class AddDataPointFrameSchema(RemoveDataPointFrameSchema):
    value: Series[pa.Float] = pa.Field(coerce=True)


class UpdateDataPointFrameSchema(AddDataPointFrameSchema):
    id: Series[pa.Int] = pa.Field(coerce=True)


def infer_content(df: pd.DataFrame, col: str) -> str:
    if col not in df.columns or all(pd.isna(df[col])):
        return "empty"
    elif not any(pd.isna(df[col])):
        return "valid"
    else:
        raise InconsistentIamcType


class EnumerateKwargs(abstract.iamc.datapoint.EnumerateKwargs, total=False):
    join_parameters: bool | None
    join_runs: bool
    _filter: BaseFilter


class DatapointVersionRepository(versions.VersionRepository[UniversalDataPointVersion]):
    model_class = UniversalDataPointVersion

    def select(
        self,
        transaction__id: int | None = None,
        run__id: int | None = None,
        **kwargs: Any,
    ) -> db.sql.Select[Any]:
        exc = db.select(
            self.bundle,
            RegionVersion.name.label("region"),
            UnitVersion.name.label("unit"),
            VariableVersion.name.label("variable"),
        ).select_from(self.model_class)

        exc = (
            exc.join(
                TimeSeriesVersion,
                onclause=self.model_class.time_series__id == TimeSeriesVersion.id,
            )
            .join(
                RegionVersion,
                onclause=TimeSeriesVersion.region__id == RegionVersion.id,
            )
            .join(
                MeasurandVersion,
                onclause=TimeSeriesVersion.measurand__id == MeasurandVersion.id,
            )
            .join(
                UnitVersion,
                onclause=MeasurandVersion.unit__id == UnitVersion.id,
            )
            .join(
                VariableVersion,
                onclause=MeasurandVersion.variable__id == VariableVersion.id,
            )
        )

        if transaction__id is not None:
            for vclass in [
                self.model_class,
                RegionVersion,
                MeasurandVersion,
                UnitVersion,
                VariableVersion,
            ]:
                exc = self.where_valid_at_transaction(exc, transaction__id, vclass)

        if run__id is not None:
            exc = exc.where(TimeSeriesVersion.run__id == run__id)

        exc = self.where_matches_kwargs(exc, **kwargs)
        return exc.distinct()


class DataPointRepository(
    base.Enumerator[DataPoint],
    base.BulkUpserter[DataPoint],
    base.BulkDeleter[DataPoint],
    abstract.DataPointRepository,
):
    model_class = DataPoint
    timeseries: TimeSeriesRepository
    runs: RunRepository
    versions: DatapointVersionRepository

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        backend, *_ = args
        # A different table was used for ORACLE databases (deprecated since ixmp4 0.3.0)
        self.model_class = get_datapoint_model(backend.session)

        self.timeseries = TimeSeriesRepository(*args)
        self.runs = RunRepository(*args)
        self.versions = DatapointVersionRepository(*args)

        self.filter_class = DataPointFilter
        super().__init__(*args)

    def join_auth(
        self, exc: db.sql.Select[tuple[DataPoint]]
    ) -> db.sql.Select[tuple[DataPoint]]:
        if not db.utils.is_joined(exc, TimeSeries):
            exc = exc.join(
                TimeSeries, onclause=self.model_class.time_series__id == TimeSeries.id
            )
        if not db.utils.is_joined(exc, Run):
            exc = exc.join(Run, TimeSeries.run)
        if not db.utils.is_joined(exc, Model):
            exc = exc.join(Model, Run.model)

        return exc

    def select_joined_parameters(
        self, join_runs: bool = False
    ) -> db.sql.Select[tuple[DataPoint]]:
        # NOTE Not quite sure about this bundle, seems to possibly take all types of
        # all model classes?
        bundle: list[db.Label[str] | db.Label[int] | db.Bundle[Any]] = []
        if join_runs:
            bundle.extend(
                [
                    Model.name.label("model"),
                    Scenario.name.label("scenario"),
                    Run.version.label("version"),
                ]
            )

        bundle.extend(
            [
                Region.name.label("region"),
                Unit.name.label("unit"),
                Variable.name.label("variable"),
                self.bundle,
            ]
        )

        _exc = (
            db.select(*bundle)
            .join(
                TimeSeries, onclause=self.model_class.time_series__id == TimeSeries.id
            )
            .join(Region, onclause=TimeSeries.region__id == Region.id)
            .join(Measurand, onclause=TimeSeries.measurand__id == Measurand.id)
            .join(Unit, onclause=Measurand.unit__id == Unit.id)
            .join(Variable, onclause=Measurand.variable__id == Variable.id)
        )

        if join_runs:
            _exc = (
                _exc.join(Run, onclause=TimeSeries.run__id == Run.id)
                .join(Model, onclause=Model.id == Run.model__id)
                .join(Scenario, onclause=Scenario.id == Run.scenario__id)
            )
        return _exc

    def select(
        self,
        *,
        join_parameters: bool | None = False,
        join_runs: bool = False,
        _filter: DataPointFilter | None = None,
        _exc: db.sql.Select[tuple[DataPoint]] | None = None,
        **kwargs: Any,
    ) -> db.sql.Select[tuple[DataPoint]]:
        if _exc is not None:
            exc = _exc
        elif join_parameters:
            exc = self.select_joined_parameters(join_runs)
        else:
            exc = db.select(self.bundle)

        return super().select(_exc=exc, _filter=_filter, **kwargs)

    @guard("view")
    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[DataPoint]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(
        self, *args: Any, _raw: bool | None = False, **kwargs: Unpack[EnumerateKwargs]
    ) -> pd.DataFrame:
        if _raw:
            return super().tabulate(*args, **kwargs)
        df = super().tabulate(*args, **kwargs)
        df["value"] = df["value"].astype(np.float64)
        df = df.sort_values(
            by=["time_series__id", "step_year", "step_category", "step_datetime"]
        )
        return df.dropna(axis="columns", how="all")

    def check_df_access(self, df: pd.DataFrame) -> None:
        if self.backend.auth_context is not None:
            ts_ids = cast(set[int], set(df["time_series__id"].unique().tolist()))
            self.timeseries.check_access(
                ts_ids,
                access_type="edit",
                run={"default_only": False, "is_default": None},
            )

    @check_types
    @guard("edit")
    def bulk_upsert(self, df: DataFrame[AddDataPointFrameSchema]) -> None:
        super().bulk_upsert(df)

    @check_types
    @guard("edit")
    def bulk_insert(self, df: DataFrame[AddDataPointFrameSchema]) -> None:
        self.check_df_access(df)
        super().bulk_insert(df)

    @check_types
    @guard("edit")
    def bulk_update(self, df: DataFrame[UpdateDataPointFrameSchema]) -> None:
        self.check_df_access(df)
        super().bulk_update(df)

    @check_types
    @guard("edit")
    def bulk_delete(self, df: DataFrame[RemoveDataPointFrameSchema]) -> None:
        self.check_df_access(df)
        super().bulk_delete(df)
        self.delete_orphans()

    def delete_orphans(self) -> None:
        exc = db.select(TimeSeries).where(
            ~db.exists(
                db.select(self.model_class.id).where(
                    TimeSeries.id == self.model_class.time_series__id
                )
            )
        )
        orphan_ts = self.timeseries.tabulate_query(exc)
        self.timeseries.bulk_delete(orphan_ts)
