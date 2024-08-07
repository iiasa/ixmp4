"""
This module contains sqlalchemy database models and repositories.
"""

from .base import BaseModel, BaseRepository
from .docs import BaseDocsRepository, docs_model
from .iamc import (  # AnnualDataPoint,; SubAnnualDataPoint,; CategoricalDataPoint,
    DataPoint,
    DataPointRepository,
    Measurand,
    MeasurandRepository,
    TimeSeries,
    TimeSeriesRepository,
    UniversalDataPoint,
    Variable,
    VariableRepository,
)
from .meta import RunMetaEntry, RunMetaEntryRepository
from .model import Model, ModelRepository
from .optimization import (
    Column,
    ColumnRepository,
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
