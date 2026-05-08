"""Boundary tests for the ixmp4 exceptions public API.

These tests assert that all exceptions intended for external consumers are
importable from the canonical locations (``ixmp4.core.exceptions`` and the
top-level ``ixmp4`` namespace) and that they form the expected inheritance
hierarchy so that callers can rely on catching ``Ixmp4Error`` as the common
base class.
"""

import ixmp4.core.exceptions as exc_module


class TestCoreExceptionsImportable:
    """Every public exception must be importable from ixmp4.core.exceptions."""

    def test_service_exception(self) -> None:
        assert hasattr(exc_module, "ServiceException")

    def test_ixmp4_error(self) -> None:
        assert hasattr(exc_module, "Ixmp4Error")

    def test_not_found(self) -> None:
        assert hasattr(exc_module, "NotFound")

    def test_not_unique(self) -> None:
        assert hasattr(exc_module, "NotUnique")

    def test_constraint_violated(self) -> None:
        assert hasattr(exc_module, "ConstraintViolated")

    def test_deletion_prevented(self) -> None:
        assert hasattr(exc_module, "DeletionPrevented")

    def test_forbidden(self) -> None:
        assert hasattr(exc_module, "Forbidden")

    def test_unauthorized(self) -> None:
        assert hasattr(exc_module, "Unauthorized")

    def test_bad_request(self) -> None:
        assert hasattr(exc_module, "BadRequest")

    def test_bad_gateway(self) -> None:
        assert hasattr(exc_module, "BadGateway")

    def test_invalid_credentials(self) -> None:
        assert hasattr(exc_module, "InvalidCredentials")

    def test_invalid_token(self) -> None:
        assert hasattr(exc_module, "InvalidToken")

    def test_server_error(self) -> None:
        assert hasattr(exc_module, "ServerError")

    def test_service_unavailable(self) -> None:
        assert hasattr(exc_module, "ServiceUnavailable")

    def test_improperly_configured(self) -> None:
        assert hasattr(exc_module, "ImproperlyConfigured")

    def test_operation_not_supported(self) -> None:
        assert hasattr(exc_module, "OperationNotSupported")

    def test_platform_not_found(self) -> None:
        assert hasattr(exc_module, "PlatformNotFound")

    def test_platform_not_unique(self) -> None:
        assert hasattr(exc_module, "PlatformNotUnique")

    def test_programming_error(self) -> None:
        assert hasattr(exc_module, "ProgrammingError")

    def test_schema_error(self) -> None:
        assert hasattr(exc_module, "SchemaError")

    def test_invalid_arguments(self) -> None:
        assert hasattr(exc_module, "InvalidArguments")

    def test_invalid_data_frame(self) -> None:
        assert hasattr(exc_module, "InvalidDataFrame")

    def test_bad_filter_arguments(self) -> None:
        assert hasattr(exc_module, "BadFilterArguments")

    def test_inconsistent_iamc_type(self) -> None:
        assert hasattr(exc_module, "InconsistentIamcType")

    def test_unknown_api_error(self) -> None:
        assert hasattr(exc_module, "UnknownApiError")

    def test_api_encumbered(self) -> None:
        assert hasattr(exc_module, "TooManyRequests")

    def test_optimization_data_validation_error(self) -> None:
        assert hasattr(exc_module, "OptimizationDataValidationError")

    def test_optimization_item_usage_error(self) -> None:
        assert hasattr(exc_module, "OptimizationItemUsageError")

    def test_invalid_run_meta(self) -> None:
        assert hasattr(exc_module, "InvalidRunMeta")

    def test_run_meta_entry_not_found(self) -> None:
        assert hasattr(exc_module, "RunMetaEntryNotFound")

    def test_run_meta_entry_not_unique(self) -> None:
        assert hasattr(exc_module, "RunMetaEntryNotUnique")

    def test_run_meta_entry_deletion_prevented(self) -> None:
        assert hasattr(exc_module, "RunMetaEntryDeletionPrevented")

    def test_measurand_not_found(self) -> None:
        assert hasattr(exc_module, "MeasurandNotFound")

    def test_measurand_not_unique(self) -> None:
        assert hasattr(exc_module, "MeasurandNotUnique")

    def test_measurand_deletion_prevented(self) -> None:
        assert hasattr(exc_module, "MeasurandDeletionPrevented")

    def test_time_series_not_found(self) -> None:
        assert hasattr(exc_module, "TimeSeriesNotFound")

    def test_time_series_not_unique(self) -> None:
        assert hasattr(exc_module, "TimeSeriesNotUnique")

    def test_time_series_deletion_prevented(self) -> None:
        assert hasattr(exc_module, "TimeSeriesDeletionPrevented")

    def test_registry(self) -> None:
        assert hasattr(exc_module, "registry")


class TestExceptionHierarchy:
    """Exceptions must form the expected ``Ixmp4Error`` hierarchy.
    ``InvalidCredentials`` and ``InvalidToken`` are raised by scse-toolkit
    and thus toolkit ServiceExceptions."""

    def test_invalid_credentials(self) -> None:
        assert issubclass(exc_module.InvalidCredentials, exc_module.ServiceException)

    def test_invalid_token(self) -> None:
        assert issubclass(exc_module.InvalidToken, exc_module.ServiceException)

    def test_not_found_is_ixmp4_error(self) -> None:
        assert issubclass(exc_module.NotFound, exc_module.Ixmp4Error)

    def test_not_unique_is_ixmp4_error(self) -> None:
        assert issubclass(exc_module.NotUnique, exc_module.Ixmp4Error)

    def test_constraint_violated_is_ixmp4_error(self) -> None:
        assert issubclass(exc_module.ConstraintViolated, exc_module.Ixmp4Error)

    def test_deletion_prevented_is_ixmp4_error(self) -> None:
        assert issubclass(exc_module.DeletionPrevented, exc_module.Ixmp4Error)

    def test_server_error_is_ixmp4_error(self) -> None:
        assert issubclass(exc_module.ServerError, exc_module.Ixmp4Error)

    def test_bad_gateway_is_ixmp4_error(self) -> None:
        assert issubclass(exc_module.BadGateway, exc_module.Ixmp4Error)

    def test_service_unavailable_is_ixmp4_error(self) -> None:
        assert issubclass(exc_module.ServiceUnavailable, exc_module.Ixmp4Error)

    def test_bad_request_is_ixmp4_error(self) -> None:
        assert issubclass(exc_module.BadRequest, exc_module.Ixmp4Error)

    def test_forbidden_is_ixmp4_error(self) -> None:
        assert issubclass(exc_module.Forbidden, exc_module.Ixmp4Error)

    def test_unauthorized_is_ixmp4_error(self) -> None:
        assert issubclass(exc_module.Unauthorized, exc_module.Ixmp4Error)

    def test_platform_not_found_is_not_found(self) -> None:
        assert issubclass(exc_module.PlatformNotFound, exc_module.NotFound)

    def test_invalid_data_frame_is_bad_request(self) -> None:
        assert issubclass(exc_module.InvalidDataFrame, exc_module.BadRequest)

    def test_invalid_arguments_is_bad_request(self) -> None:
        assert issubclass(exc_module.InvalidArguments, exc_module.BadRequest)

    def test_all_exceptions_are_base_exception(self) -> None:
        for name in (
            "Ixmp4Error",
            "NotFound",
            "NotUnique",
            "ConstraintViolated",
            "DeletionPrevented",
            "ServerError",
            "BadGateway",
            "ServiceUnavailable",
            "BadRequest",
            "Unauthorized",
            "Forbidden",
            "ImproperlyConfigured",
            "OperationNotSupported",
            "SchemaError",
            "InvalidArguments",
            "InvalidDataFrame",
            "BadFilterArguments",
            "InconsistentIamcType",
            "UnknownApiError",
            "TooManyRequests",
            "OptimizationDataValidationError",
            "OptimizationItemUsageError",
        ):
            cls = getattr(exc_module, name)
            assert issubclass(cls, BaseException), (
                f"{name} is not a subclass of BaseException"
            )
