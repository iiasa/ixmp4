"""Boundary tests for ixmp4 -> pyam integration.

These tests verify the ixmp4-side contracts consumed by pyam.
"""

import ixmp4


class TestPyam_3_3_1:
    def test_platform_accessible(self) -> None:
        assert hasattr(ixmp4, "Platform")
        assert isinstance(ixmp4.Platform, type)

    def test_run_accessible(self) -> None:
        assert hasattr(ixmp4, "Run")
        assert isinstance(ixmp4.Run, type)

    def test_importable_from_core_iamc(self) -> None:
        from ixmp4.core.iamc import DataPoint

        assert isinstance(DataPoint, type)

    def test_same_object_as_datapoint_module(self) -> None:
        from ixmp4.core.iamc import DataPoint
        from ixmp4.core.iamc.datapoint import DataPoint as DataPointDirect

        assert DataPoint is DataPointDirect

    def test_datapoint_has_type_enum(self) -> None:
        from ixmp4.core.iamc import DataPoint

        assert hasattr(DataPoint, "Type")

    def test_datapoint_has_not_found(self) -> None:
        from ixmp4.core.iamc import DataPoint

        assert issubclass(DataPoint.NotFound, BaseException)

    def test_datapoint_has_filter(self) -> None:
        from ixmp4.core.iamc import DataPoint

        assert isinstance(DataPoint.Filter(), dict)

    def test_region_importable(self) -> None:
        from ixmp4.core.region import Region

        assert isinstance(Region, type)

    def test_region_same_as_top_level(self) -> None:
        from ixmp4.core.region import Region

        assert ixmp4.Region is Region

    def test_region_has_not_found(self) -> None:
        from ixmp4.core.region import Region

        assert issubclass(Region.NotFound, BaseException)

    def test_region_has_filter(self) -> None:
        from ixmp4.core.region import Region

        assert isinstance(Region.Filter(), dict)

    def test_unit_importable(self) -> None:
        from ixmp4.core.unit import Unit

        assert isinstance(Unit, type)

    def test_unit_same_as_top_level(self) -> None:
        from ixmp4.core.unit import Unit

        assert ixmp4.Unit is Unit

    def test_unit_has_not_found(self) -> None:
        from ixmp4.core.unit import Unit

        assert issubclass(Unit.NotFound, BaseException)

    def test_unit_has_filter(self) -> None:
        from ixmp4.core.unit import Unit

        assert isinstance(Unit.Filter(), dict)

    def test_facade_datapoint_filter_importable(self) -> None:
        from ixmp4.data.iamc.datapoint.filter import FacadeDataPointFilter

        assert isinstance(FacadeDataPointFilter(), dict)

    def test_facade_run_filter_importable(self) -> None:
        from ixmp4.data.run.filter import FacadeRunFilter

        assert isinstance(FacadeRunFilter(), dict)

    def test_facade_run_filter_accepts_default_only(self) -> None:
        from ixmp4.data.run.filter import FacadeRunFilter

        f = FacadeRunFilter(default_only=True)
        assert isinstance(f, dict)
        assert f["default_only"] is True

    def test_facade_run_meta_entry_filter_importable(self) -> None:
        from ixmp4.data.meta.filter import FacadeRunMetaEntryFilter

        assert isinstance(FacadeRunMetaEntryFilter(), dict)

    def test_facade_datapoint_filter_same_as_datapoint_class_filter(self) -> None:
        from ixmp4.core.iamc.datapoint import DataPoint
        from ixmp4.data.iamc.datapoint.filter import FacadeDataPointFilter

        assert DataPoint.Filter is FacadeDataPointFilter

    def test_facade_run_filter_same_as_run_class_filter(self) -> None:
        from ixmp4.data.run.filter import FacadeRunFilter

        assert ixmp4.Run.Filter is FacadeRunFilter

    def test_tabulate_manager_platforms_importable(self) -> None:
        from ixmp4.cli.platforms import tabulate_manager_platforms

        assert callable(tabulate_manager_platforms)

    def test_tabulate_manager_platforms_accepts_platforms_argument(self) -> None:
        import inspect

        from ixmp4.cli.platforms import tabulate_manager_platforms

        params = inspect.signature(tabulate_manager_platforms).parameters
        assert "platforms" in params

    def test_settings_importable(self) -> None:
        from ixmp4.conf.settings import Settings

        assert isinstance(Settings, type)

    def test_settings_has_manager_url(self) -> None:
        from ixmp4.conf.settings import Settings

        s = Settings()
        assert hasattr(s, "manager_url")
        assert s.manager_url is not None

    def test_settings_has_get_manager_platforms(self) -> None:
        """pyam.iiasa calls settings.get_manager_platforms()."""
        from ixmp4.conf.settings import Settings

        assert callable(getattr(Settings, "get_manager_platforms", None))
