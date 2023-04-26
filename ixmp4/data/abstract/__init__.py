"""
This module holds a shared datastructure and interface for normalization
between the database and api data models and repositories.
"""
# flake8: noqa

from .base import (
    BaseModel,
    BaseMeta,
    Retriever,
    Creator,
    Deleter,
    Lister,
    Tabulator,
    Enumerator,
    BulkUpserter,
    BulkDeleter,
)
from .model import Model, ModelRepository
from .run import Run, RunRepository
from .meta import RunMetaEntry, RunMetaEntryRepository, StrictMetaValue, MetaValue
from .scenario import Scenario, ScenarioRepository
from .unit import Unit, UnitRepository
from .region import Region, RegionRepository
from .docs import Docs, DocsRepository

from .iamc import (
    Variable,
    VariableRepository,
    Measurand,
    MeasurandRepository,
    TimeSeries,
    TimeSeriesRepository,
    DataPoint,
    #    AnnualDataPoint,
    #    SubAnnualDataPoint,
    #    CategoricalDataPoint,
    DataPointRepository,
)
