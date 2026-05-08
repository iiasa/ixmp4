"""Boundary tests for the top-level ``ixmp4`` package namespace.

These tests assert that all public symbols intended for external consumers
are importable directly from ``import ixmp4`` and that they are the correct
types so that the package surface stays stable across refactors.
"""

import ixmp4


class TestCoreFacadesExported:
    """Facade classes must be importable from the top-level namespace."""

    def test_platform(self) -> None:
        assert hasattr(ixmp4, "Platform")

    def test_run(self) -> None:
        assert hasattr(ixmp4, "Run")

    def test_model(self) -> None:
        assert hasattr(ixmp4, "Model")

    def test_scenario(self) -> None:
        assert hasattr(ixmp4, "Scenario")

    def test_region(self) -> None:
        assert hasattr(ixmp4, "Region")

    def test_unit(self) -> None:
        assert hasattr(ixmp4, "Unit")


class TestCoreFacadesAreClasses:
    """Exported facade names must actually be classes."""

    def test_platform_is_class(self) -> None:
        assert isinstance(ixmp4.Platform, type)

    def test_run_is_class(self) -> None:
        assert isinstance(ixmp4.Run, type)

    def test_model_is_class(self) -> None:
        assert isinstance(ixmp4.Model, type)

    def test_scenario_is_class(self) -> None:
        assert isinstance(ixmp4.Scenario, type)

    def test_region_is_class(self) -> None:
        assert isinstance(ixmp4.Region, type)

    def test_unit_is_class(self) -> None:
        assert isinstance(ixmp4.Unit, type)


class TestFacadeClasses:
    """Facade object classes expose class-bound exceptions and filters."""

    def test_platform_exceptions(self) -> None:
        assert issubclass(ixmp4.Platform.NotFound, BaseException)
        assert issubclass(ixmp4.Platform.NotUnique, BaseException)

    def test_run_exceptions_and_filter(self) -> None:
        assert issubclass(ixmp4.Run.NotFound, BaseException)
        assert issubclass(ixmp4.Run.NotUnique, BaseException)
        assert issubclass(ixmp4.Run.DeletionPrevented, BaseException)
        assert isinstance(ixmp4.Run.Filter(), dict)

    def test_model_exceptions_and_filter(self) -> None:
        assert issubclass(ixmp4.Model.NotFound, BaseException)
        assert issubclass(ixmp4.Model.NotUnique, BaseException)
        assert issubclass(ixmp4.Model.DeletionPrevented, BaseException)
        assert isinstance(ixmp4.Model.Filter(), dict)

    def test_scenario_exceptions_and_filter(self) -> None:
        assert issubclass(ixmp4.Scenario.NotFound, BaseException)
        assert issubclass(ixmp4.Scenario.NotUnique, BaseException)
        assert issubclass(ixmp4.Scenario.DeletionPrevented, BaseException)
        assert isinstance(ixmp4.Scenario.Filter(), dict)

    def test_region_exceptions_and_filter(self) -> None:
        assert issubclass(ixmp4.Region.NotFound, BaseException)
        assert issubclass(ixmp4.Region.NotUnique, BaseException)
        assert issubclass(ixmp4.Region.DeletionPrevented, BaseException)
        assert isinstance(ixmp4.Region.Filter(), dict)

    def test_unit_exceptions_and_filter(self) -> None:
        assert issubclass(ixmp4.Unit.NotFound, BaseException)
        assert issubclass(ixmp4.Unit.NotUnique, BaseException)
        assert issubclass(ixmp4.Unit.DeletionPrevented, BaseException)
        assert isinstance(ixmp4.Unit.Filter(), dict)


class TestSubNamespaces:
    """Sub-module namespaces must be accessible from the top-level package."""

    def test_iamc_namespace(self) -> None:
        assert hasattr(ixmp4, "iamc")
        assert hasattr(ixmp4.iamc, "PlatformIamcData")
        assert hasattr(ixmp4.iamc, "RunIamcData")
        assert hasattr(ixmp4.iamc, "DataPoint")
        assert hasattr(ixmp4.iamc, "Variable")

    def test_iamc_facade_classes(self) -> None:
        assert issubclass(ixmp4.iamc.DataPoint.NotFound, BaseException)
        assert issubclass(ixmp4.iamc.DataPoint.NotUnique, BaseException)
        assert issubclass(ixmp4.iamc.DataPoint.DeletionPrevented, BaseException)
        assert isinstance(ixmp4.iamc.DataPoint.Filter(), dict)

        assert issubclass(ixmp4.iamc.Variable.NotFound, BaseException)
        assert issubclass(ixmp4.iamc.Variable.NotUnique, BaseException)
        assert issubclass(ixmp4.iamc.Variable.DeletionPrevented, BaseException)
        assert isinstance(ixmp4.iamc.Variable.Filter(), dict)

    def test_optimization_namespace(self) -> None:
        assert hasattr(ixmp4, "optimization")
        assert hasattr(ixmp4.optimization, "RunOptimizationData")
        assert hasattr(ixmp4.optimization, "Equation")
        assert hasattr(ixmp4.optimization, "IndexSet")
        assert hasattr(ixmp4.optimization, "Parameter")
        assert hasattr(ixmp4.optimization, "Scalar")
        assert hasattr(ixmp4.optimization, "Table")
        assert hasattr(ixmp4.optimization, "Variable")

    def test_optimization_facade_classes(self) -> None:
        for cls in (
            ixmp4.optimization.Equation,
            ixmp4.optimization.IndexSet,
            ixmp4.optimization.Parameter,
            ixmp4.optimization.Scalar,
            ixmp4.optimization.Table,
            ixmp4.optimization.Variable,
        ):
            assert issubclass(cls.NotFound, BaseException)
            assert issubclass(cls.NotUnique, BaseException)
            assert issubclass(cls.DeletionPrevented, BaseException)
            assert isinstance(cls.Filter(), dict)


class TestExceptions:
    """The canonical exceptions must be importable from the top-level package."""

    def test_ixmp4_error(self) -> None:
        assert hasattr(ixmp4, "Ixmp4Error")
        assert issubclass(ixmp4.Ixmp4Error, BaseException)

    def test_not_found(self) -> None:
        assert hasattr(ixmp4, "NotFound")
        assert issubclass(ixmp4.NotFound, BaseException)

    def test_not_unique(self) -> None:
        assert hasattr(ixmp4, "NotUnique")
        assert issubclass(ixmp4.NotUnique, BaseException)

    def test_inconsistent_iamc_type(self) -> None:
        assert hasattr(ixmp4, "InconsistentIamcType")
        assert issubclass(ixmp4.InconsistentIamcType, BaseException)

    def test_invalid_token(self) -> None:
        assert hasattr(ixmp4, "InvalidToken")
        assert issubclass(ixmp4.InvalidToken, BaseException)

    def test_invalid_credentials(self) -> None:
        assert hasattr(ixmp4, "InvalidCredentials")
        assert issubclass(ixmp4.InvalidCredentials, BaseException)


class TestVersionMetadata:
    """Version strings must be accessible from the top-level package."""

    def test_version_string_present(self) -> None:
        assert hasattr(ixmp4, "__version__")
        assert isinstance(ixmp4.__version__, str)
        assert ixmp4.__version__  # non-empty

    def test_version_tuple_present(self) -> None:
        assert hasattr(ixmp4, "__version_tuple__")
        assert isinstance(ixmp4.__version_tuple__, tuple)
        assert len(ixmp4.__version_tuple__) >= 3
