# TODO Import this from typing when dropping support for 3.10


from toolkit.exceptions import BadRequest as BadRequest
from toolkit.exceptions import ConstraintViolated as ConstraintViolated
from toolkit.exceptions import NotFound as NotFound
from toolkit.exceptions import NotUnique as NotUnique
from toolkit.exceptions import ProgrammingError as ProgrammingError
from toolkit.exceptions import ServiceException as ServiceException
from toolkit.exceptions import registry as tk_registry

registry = tk_registry.copy()


@registry.register()
class InconsistentIamcType(ServiceException):
    http_status_code = 400
    http_error_name = "inconsistent_iamc_type"


@registry.register()
class ImproperlyConfigured(ServiceException):
    http_error_name = "improperly_configured"


@registry.register()
class ManagerApiError(ServiceException):
    http_error_name = "manager_api_error"


@registry.register()
class UnknownApiError(ServiceException):
    http_error_name = "unknown_api_error"


@registry.register()
class ApiEncumbered(ServiceException):
    http_error_name = "api_encumbered"


@registry.register()
class PlatformNotFound(ServiceException):
    http_status_code = 404
    http_error_name = "platform_not_found"


@registry.register()
class PlatformNotUnique(ServiceException):
    http_status_code = 409
    http_error_name = "platform_not_unique"


@registry.register()
class OperationNotSupported(ServiceException):
    http_status_code = 400
    http_error_name = "operation_not_supported"


@registry.register()
class SchemaError(ServiceException):
    http_status_code = 422
    http_error_name = "schema_error"


@registry.register()
class DeletionPrevented(ServiceException):
    http_status_code = 400


# == Filters ==
@registry.register()
class BadFilterArguments(ServiceException):
    message = "The provided filter arguments are malformed."
    http_status_code = 400
    http_error_name = "bad_filter_arguments"

    def __str__(self) -> str:
        return f"{self.data.get('model')} {self.data.get('errors')}"


# == Optimization ==
@registry.register()
class OptimizationDataValidationError(ServiceException):
    http_status_code = 422
    http_error_name = "optimization_data_validation_error"


@registry.register()
class OptimizationItemUsageError(ServiceException):
    http_status_code = 422
    http_error_name = "optimization_item_usage_error"
