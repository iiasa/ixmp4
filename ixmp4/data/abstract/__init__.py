"""
This module holds a shared datastructure and interface for normalization
between the database and api data models and repositories.
"""

from .base import (
    BaseMeta,
    BaseModel,
    BulkDeleter,
    BulkUpserter,
    Creator,
    Deleter,
    Enumerator,
    Lister,
    Retriever,
    Tabulator,
)
from .docs import Docs, DocsRepository
from .iamc import (  # AnnualDataPoint,; SubAnnualDataPoint,; CategoricalDataPoint,
    DataPoint,
    DataPointRepository,
    Measurand,
    MeasurandRepository,
    TimeSeries,
    TimeSeriesRepository,
    Variable,
    VariableRepository,
)
from .meta import MetaValue, RunMetaEntry, RunMetaEntryRepository, StrictMetaValue
from .model import Model, ModelRepository
from .optimization import (
    Equation,
    EquationRepository,
    IndexSet,
    IndexSetRepository,
    Parameter,
    ParameterRepository,
    Scalar,
    ScalarRepository,
    Table,
    TableRepository,
)

# TODO for PR: avoiding name conflict here Is that okay?
from .optimization import Variable as OptimizationVariable
from .optimization import VariableRepository as OptimizationVariableRepository
from .region import Region, RegionRepository
from .run import Run, RunRepository
from .scenario import Scenario, ScenarioRepository
from .unit import Unit, UnitRepository
