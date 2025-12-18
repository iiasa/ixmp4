"""Central core exceptions module (single-file).

This module re-exports base framework exceptions and only those
data-layer exceptions that are not already exposed on core facade
classes. Exceptions that are attached to facade classes (e.g.
`Model.NotFound`, `Run.NotFound`, `Equation.NotFound`, etc.) are
intentionally omitted here — they should be caught via the class
attributes on the corresponding facades or the parent exception class.
"""

from ixmp4.base_exceptions import (
    ApiEncumbered as ApiEncumbered,
)
from ixmp4.base_exceptions import (
    BadFilterArguments as BadFilterArguments,
)
from ixmp4.base_exceptions import (
    BadGateway as BadGateway,
)
from ixmp4.base_exceptions import (
    BadRequest as BadRequest,
)
from ixmp4.base_exceptions import (
    ConstraintViolated as ConstraintViolated,
)
from ixmp4.base_exceptions import (
    DeletionPrevented as DeletionPrevented,
)
from ixmp4.base_exceptions import (
    Forbidden as Forbidden,
)
from ixmp4.base_exceptions import (
    ImproperlyConfigured as ImproperlyConfigured,
)
from ixmp4.base_exceptions import (
    InconsistentIamcType as InconsistentIamcType,
)
from ixmp4.base_exceptions import (
    InvalidArguments as InvalidArguments,
)
from ixmp4.base_exceptions import (
    InvalidCredentials as InvalidCredentials,
)
from ixmp4.base_exceptions import (
    InvalidDataFrame as InvalidDataFrame,
)
from ixmp4.base_exceptions import (
    InvalidToken as InvalidToken,
)
from ixmp4.base_exceptions import (
    Ixmp4Error as Ixmp4Error,
)
from ixmp4.base_exceptions import (
    NotFound as NotFound,
)
from ixmp4.base_exceptions import (
    NotUnique as NotUnique,
)
from ixmp4.base_exceptions import (
    OperationNotSupported as OperationNotSupported,
)
from ixmp4.base_exceptions import (
    OptimizationDataValidationError as OptimizationDataValidationError,
)
from ixmp4.base_exceptions import (
    OptimizationItemUsageError as OptimizationItemUsageError,
)
from ixmp4.base_exceptions import (
    PlatformNotFound as PlatformNotFound,
)
from ixmp4.base_exceptions import (
    PlatformNotUnique as PlatformNotUnique,
)
from ixmp4.base_exceptions import (
    ProgrammingError as ProgrammingError,
)
from ixmp4.base_exceptions import (
    SchemaError as SchemaError,
)
from ixmp4.base_exceptions import (
    ServerError as ServerError,
)
from ixmp4.base_exceptions import (
    ServiceUnavailable as ServiceUnavailable,
)
from ixmp4.base_exceptions import (
    Unauthorized as Unauthorized,
)
from ixmp4.base_exceptions import (
    UnknownApiError as UnknownApiError,
)
from ixmp4.base_exceptions import (
    registry as registry,
)

# IAMC: measurand and timeseries exceptions kept here since they have no classes here
from ixmp4.data.iamc.measurand.exceptions import (
    MeasurandDeletionPrevented as MeasurandDeletionPrevented,
)
from ixmp4.data.iamc.measurand.exceptions import (
    MeasurandNotFound as MeasurandNotFound,
)
from ixmp4.data.iamc.measurand.exceptions import (
    MeasurandNotUnique as MeasurandNotUnique,
)
from ixmp4.data.iamc.timeseries.exceptions import (
    TimeSeriesDeletionPrevented as TimeSeriesDeletionPrevented,
)
from ixmp4.data.iamc.timeseries.exceptions import (
    TimeSeriesNotFound as TimeSeriesNotFound,
)
from ixmp4.data.iamc.timeseries.exceptions import (
    TimeSeriesNotUnique as TimeSeriesNotUnique,
)
from ixmp4.data.meta.exceptions import (
    InvalidRunMeta as InvalidRunMeta,
)
from ixmp4.data.meta.exceptions import (
    RunMetaEntryDeletionPrevented as RunMetaEntryDeletionPrevented,
)
from ixmp4.data.meta.exceptions import (
    RunMetaEntryNotFound as RunMetaEntryNotFound,
)
from ixmp4.data.meta.exceptions import (
    RunMetaEntryNotUnique as RunMetaEntryNotUnique,
)
