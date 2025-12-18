from typing import cast

from pydantic import ValidationError
from toolkit.exceptions import BadGateway as BaseBadGateway
from toolkit.exceptions import BadRequest as BaseBadRequest
from toolkit.exceptions import ConstraintViolated as BaseConstraintViolated
from toolkit.exceptions import Forbidden as BaseForbidden
from toolkit.exceptions import NotFound as BaseNotFound
from toolkit.exceptions import NotUnique as BaseNotUnique
from toolkit.exceptions import PlatformNotFound as BasePlatformNotFound
from toolkit.exceptions import ProgrammingError as ProgrammingError
from toolkit.exceptions import ServerError as BaseServerError
from toolkit.exceptions import ServiceException
from toolkit.exceptions import ServiceUnavailable as BaseServiceUnavailable
from toolkit.exceptions import Unauthorized as BaseUnauthorized
from toolkit.exceptions.registry import ServiceExceptionRegistry
from toolkit.exceptions.serviceexception import DataItemType

registry = ServiceExceptionRegistry()

registry.register()(ServiceException)


@registry.register()
class Ixmp4Error(ServiceException):
    pass


@registry.register(default_for_status_code=500)
class ServerError(BaseServerError, Ixmp4Error):
    pass


@registry.register(default_for_status_code=502)
class BadGateway(BaseBadGateway, Ixmp4Error):
    pass


@registry.register(default_for_status_code=503)
class ServiceUnavailable(BaseServiceUnavailable, Ixmp4Error):
    pass


@registry.register(default_for_status_code=400)
class BadRequest(BaseBadRequest, Ixmp4Error):
    pass


@registry.register(default_for_status_code=401)
class Unauthorized(BaseUnauthorized, Ixmp4Error):
    pass


@registry.register(default_for_status_code=403)
class Forbidden(BaseForbidden, Ixmp4Error):
    pass


@registry.register(default_for_status_code=404)
class NotFound(BaseNotFound, Ixmp4Error):
    message = "Not found."
    http_status_code = 404


@registry.register()
class NotUnique(BaseNotUnique, Ixmp4Error):
    message = "Not unique."
    http_status_code = 409


@registry.register()
class ConstraintViolated(BaseConstraintViolated, Ixmp4Error):
    message = "Database constraint violated."
    http_status_code = 400


@registry.register()
class InvalidToken(Unauthorized):
    message = "The supplied token is invalid."
    http_status_code = 401


@registry.register()
class InvalidCredentials(Unauthorized):
    message = "Authentication credentials rejected."
    http_status_code = 401


@registry.register()
class InconsistentIamcType(Ixmp4Error):
    http_status_code = 400
    http_error_name = "inconsistent_iamc_type"


@registry.register()
class ImproperlyConfigured(Ixmp4Error):
    http_error_name = "improperly_configured"


@registry.register()
class UnknownApiError(Ixmp4Error):
    http_error_name = "unknown_api_error"


@registry.register()
class ApiEncumbered(Ixmp4Error):
    http_error_name = "api_encumbered"


@registry.register()
class PlatformNotFound(NotFound, BasePlatformNotFound, Ixmp4Error):
    pass


@registry.register()
class PlatformNotUnique(Ixmp4Error):
    http_status_code = 409
    http_error_name = "platform_not_unique"


@registry.register()
class OperationNotSupported(Ixmp4Error):
    http_status_code = 400
    http_error_name = "operation_not_supported"


@registry.register()
class SchemaError(Ixmp4Error):
    http_status_code = 422
    http_error_name = "schema_error"


@registry.register()
class DeletionPrevented(Ixmp4Error):
    http_status_code = 400


# == Filters ==
@registry.register()
class BadFilterArguments(Ixmp4Error):
    message = "The provided filter arguments are malformed."
    http_status_code = 400
    http_error_name = "bad_filter_arguments"

    def __str__(self) -> str:
        return f"{self.data.get('model')} {self.data.get('errors')}"


# == Optimization ==
@registry.register()
class OptimizationDataValidationError(Ixmp4Error):
    http_status_code = 422
    http_error_name = "optimization_data_validation_error"


@registry.register()
class OptimizationItemUsageError(Ixmp4Error):
    http_status_code = 422
    http_error_name = "optimization_item_usage_error"


@registry.register()
class InvalidDataFrame(BadRequest):
    message = "The provided dataframe is invalid."


# == Filters ==
@registry.register()
class InvalidArguments(BadRequest):
    message = "The provide arguments are invalid."
    http_status_code = 400
    http_error_name = "invalid_arguments"

    def __init__(
        self,
        message: str | None = None,
        validation_error: ValidationError | None = None,
        **data: DataItemType,
    ) -> None:
        if validation_error is not None:
            validation_errors = cast(
                DataItemType, validation_error.errors(include_url=False)
            )

        else:
            validation_errors = None

        super().__init__(
            message or self.message,
            self.http_status_code,
            validation_errors=validation_errors,
            **data,
        )
