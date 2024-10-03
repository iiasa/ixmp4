from typing import ClassVar, Dict

registry: dict = dict()


class ProgrammingError(Exception):
    pass


ExcMeta: type = type(Exception)


class RemoteExceptionMeta(ExcMeta):
    def __new__(cls, name, bases, namespace, **kwargs):
        http_error_name = namespace.get("http_error_name", None)
        if http_error_name is not None:
            try:
                return registry[http_error_name]
            except KeyError:
                registry[http_error_name] = super().__new__(
                    cls, name, bases, namespace, **kwargs
                )
                return registry[http_error_name]
        else:
            raise ProgrammingError("`IxmpError`s must have `http_error_name`.")


class IxmpError(Exception, metaclass=RemoteExceptionMeta):
    _message: str = ""
    http_status_code: int = 500
    http_error_name: ClassVar[str] = "ixmp_error"
    kwargs: Dict

    def __init__(
        self,
        *args,
        message: str | None = None,
        status_code: int | None = None,
        **kwargs,
    ) -> None:
        if len(args) > 0:
            self._message = args[0]
        if message is not None:
            self._message = message
        if status_code is not None:
            self.http_status_code = status_code
        self.kwargs = kwargs
        super().__init__(*args)

    def __str__(self) -> str:
        return self.message

    @property
    def message(self) -> str:
        message = ""
        if self._message != "":
            message = self._message
        elif len(self.kwargs) > 0:
            kwargs_str = ", ".join(
                ["{}={!r}".format(k, v) for k, v in self.kwargs.items()]
            )
            message = kwargs_str

        return message

    @classmethod
    def from_dict(cls, dict_):
        return cls(message=dict_["message"], **dict_["kwargs"])


class InconsistentIamcType(IxmpError):
    http_status_code = 400
    http_error_name = "inconsistent_iamc_type"


class BadRequest(IxmpError):
    http_status_code = 400
    http_error_name = "bad_request"


class ImproperlyConfigured(IxmpError):
    http_error_name = "improperly_configured"


class ManagerApiError(IxmpError):
    http_error_name = "manager_api_error"


class UnknownApiError(IxmpError):
    http_error_name = "unknown_api_error"


class ApiEncumbered(IxmpError):
    http_error_name = "api_encumbered"


class PlatformNotFound(IxmpError):
    http_status_code = 404
    http_error_name = "platform_not_found"


class PlatformNotUnique(IxmpError):
    http_status_code = 409
    http_error_name = "platform_not_unique"


class MissingToken(IxmpError):
    http_status_code = 401
    http_error_name = "missing_token"


class InvalidToken(IxmpError):
    http_status_code = 401
    http_error_name = "invalid_token"


class Forbidden(IxmpError):
    http_status_code = 403
    http_error_name = "forbidden"


class NotFound(IxmpError):
    http_status_code = 404
    http_error_name = "not_found"


class NotUnique(IxmpError):
    http_status_code = 409
    http_error_name = "not_unique"


class DeletionPrevented(IxmpError):
    http_status_code = 400
    http_error_name = "deletion_prevented"


class OperationNotSupported(IxmpError):
    http_status_code = 400
    http_error_name = "operation_not_supported"


class SchemaError(IxmpError):
    http_status_code = 422
    http_error_name = "schema_error"


# == Run ==


class NoDefaultRunVersion(IxmpError):
    http_status_code = 400
    http_error_name = "run_no_default_version"


class InvalidRunMeta(IxmpError):
    http_status_code = 400
    http_error_name = "run_invalid_meta"


# == Filters ==


class BadFilterArguments(IxmpError):
    _message = "The provided filter arguments are malformed."
    http_status_code = 400
    http_error_name = "bad_filter_arguments"

    def __str__(self) -> str:
        return f"{self.kwargs.get('model')} {self.kwargs.get('errors')}"


# == Api Exceptions ==


class InvalidCredentials(IxmpError):
    _message = "The provided credentials are invalid."
    http_status_code = 401
    http_error_name = "invalid_credentials"


# == Optimization ==


class OptimizationDataValidationError(IxmpError):
    http_status_code = 422
    http_error_name = "optimization_data_validation_error"


class OptimizationItemUsageError(IxmpError):
    http_status_code = 422
    http_error_name = "optimization_item_usage_error"
