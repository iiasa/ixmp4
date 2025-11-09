import datetime

import pandas as pd
import pandas.testing as pdt
import pytest
from toolkit import db

from ixmp4.rewrite.data.iamc.timeseries.service import TimeSeriesService
from ixmp4.rewrite.data.iamc.variable.repositories import (
    ItemRepository as VariableRepository,
)
from ixmp4.rewrite.data.region.repositories import RegionNotFound
from ixmp4.rewrite.data.region.service import RegionService
from ixmp4.rewrite.data.run.dto import Run
from ixmp4.rewrite.data.run.service import RunService
from ixmp4.rewrite.data.unit.repositories import UnitNotFound
from ixmp4.rewrite.data.unit.service import UnitService
from ixmp4.rewrite.transport import Transport
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


# TODO: versioning


class TimeSeriesServiceTest(ServiceTest[TimeSeriesService]):
    service_class = TimeSeriesService

    @pytest.fixture(scope="class")
    def runs(self, transport: Transport) -> RunService:
        return RunService(transport)

    @pytest.fixture(scope="class")
    def run(
        self,
        runs: RunService,
    ) -> Run:
        run = runs.create("Model", "Scenario")
        assert run.id == 1
        return run

    @pytest.fixture(scope="class")
    def units(self, transport: Transport) -> UnitService:
        return UnitService(transport)

    @pytest.fixture(scope="class")
    def regions(self, transport: Transport) -> RegionService:
        return RegionService(transport)

    @pytest.fixture(scope="class")
    def variables(self, transport: Transport) -> VariableRepository:
        direct = self.get_direct_or_skip(transport)
        return VariableRepository(db.r.SessionExecutor(direct.session))

    @pytest.fixture(scope="class")
    def test_df_expected(self, run: Run, fake_time: datetime.datetime) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [1, run.id, 1, 1],
                [2, run.id, 1, 2],
                [3, run.id, 2, 1],
            ],
            columns=["id", "run__id", "region__id", "measurand__id"],
        )

    def create_related(
        self,
        regions: RegionService,
        units: UnitService,
    ):
        # assume regions and units have been created
        # by a manager
        regions.create("Region 1", "default")
        regions.create("Region 2", "default")
        units.create("Unit 1")
        units.create("Unit 2")

    def create_variables(
        self,
        variables: VariableRepository,
    ):
        # assume vars have been created
        # by prior insertion
        variables.create({"name": "Variable 1"})
        variables.create({"name": "Variable 2"})


class TestTimeseriesBulkUpsertFullRelated(TimeSeriesServiceTest):
    @pytest.fixture(scope="class")
    def test_df(
        self,
        run: Run,
        regions: RegionService,
        units: UnitService,
    ) -> pd.DataFrame:
        self.create_related(regions, units)

        return pd.DataFrame(
            [
                [run.id, "Region 1", "Variable 1", "Unit 1"],
                [run.id, "Region 1", "Variable 2", "Unit 2"],
                [run.id, "Region 2", "Variable 1", "Unit 1"],
            ],
            columns=["run__id", "region", "variable", "unit"],
        )

    def test_timeseries_bulk_upsert_full_related(
        self,
        service: TimeSeriesService,
        run: Run,
        fake_time: datetime.datetime,
        test_df: pd.DataFrame,
        test_df_expected: pd.DataFrame,
    ) -> None:
        service.bulk_upsert(test_df)
        ret_df = service.tabulate()
        pdt.assert_frame_equal(test_df_expected, ret_df, check_like=True)


class TestTimeseriesBulkUpsertNoRegions(TimeSeriesServiceTest):
    @pytest.fixture(scope="class")
    def test_df(
        self,
        run: Run,
        regions: RegionService,
        units: UnitService,
    ) -> pd.DataFrame:
        self.create_related(regions, units)
        return pd.DataFrame(
            [
                [run.id, 1, "Variable 1", "Unit 1"],
                [run.id, 1, "Variable 2", "Unit 2"],
                [run.id, 2, "Variable 1", "Unit 1"],
            ],
            columns=["run__id", "region__id", "variable", "unit"],
        )

    def test_timeseries_bulk_upsert_no_regions(
        self,
        service: TimeSeriesService,
        run: Run,
        fake_time: datetime.datetime,
        test_df: pd.DataFrame,
        test_df_expected: pd.DataFrame,
    ) -> None:
        service.bulk_upsert(test_df)
        ret_df = service.tabulate()
        pdt.assert_frame_equal(test_df_expected, ret_df, check_like=True)


class TestTimeseriesBulkUpsertNoVars(TimeSeriesServiceTest):
    @pytest.fixture(scope="class")
    def test_df(
        self,
        run: Run,
        regions: RegionService,
        units: UnitService,
        variables: VariableRepository,
    ) -> pd.DataFrame:
        self.create_related(regions, units)
        self.create_variables(variables)
        return pd.DataFrame(
            [
                [run.id, "Region 1", 1, "Unit 1"],
                [run.id, "Region 1", 2, "Unit 2"],
                [run.id, "Region 2", 1, "Unit 1"],
            ],
            columns=["run__id", "region", "variable__id", "unit"],
        )

    def test_timeseries_bulk_upsert_no_vars(
        self,
        service: TimeSeriesService,
        run: Run,
        fake_time: datetime.datetime,
        test_df: pd.DataFrame,
        test_df_expected: pd.DataFrame,
    ) -> None:
        service.bulk_upsert(test_df)
        ret_df = service.tabulate()
        pdt.assert_frame_equal(test_df_expected, ret_df, check_like=True)


class TestTimeseriesBulkUpsertNoUnits(TimeSeriesServiceTest):
    @pytest.fixture(scope="class")
    def test_df(
        self,
        run: Run,
        regions: RegionService,
        units: UnitService,
    ) -> pd.DataFrame:
        self.create_related(regions, units)
        return pd.DataFrame(
            [
                [run.id, "Region 1", "Variable 1", 1],
                [run.id, "Region 1", "Variable 2", 2],
                [run.id, "Region 2", "Variable 1", 1],
            ],
            columns=["run__id", "region", "variable", "unit__id"],
        )

    def test_timeseries_bulk_upsert_no_units(
        self,
        service: TimeSeriesService,
        run: Run,
        fake_time: datetime.datetime,
        test_df: pd.DataFrame,
        test_df_expected: pd.DataFrame,
    ) -> None:
        service.bulk_upsert(test_df)
        ret_df = service.tabulate()
        pdt.assert_frame_equal(test_df_expected, ret_df, check_like=True)


class TestTimeseriesBulkUpsertNoMeasurands(TimeSeriesServiceTest):
    @pytest.fixture(scope="class")
    def test_df(
        self,
        run: Run,
        regions: RegionService,
        units: UnitService,
        variables: VariableRepository,
    ) -> pd.DataFrame:
        self.create_related(regions, units)
        self.create_variables(variables)
        return pd.DataFrame(
            [
                [run.id, "Region 1", 1, 1],
                [run.id, "Region 1", 2, 2],
                [run.id, "Region 2", 1, 1],
            ],
            columns=["run__id", "region", "variable__id", "unit__id"],
        )

    def test_timeseries_bulk_upsert_no_units(
        self,
        service: TimeSeriesService,
        run: Run,
        fake_time: datetime.datetime,
        test_df: pd.DataFrame,
        test_df_expected: pd.DataFrame,
    ) -> None:
        service.bulk_upsert(test_df)
        ret_df = service.tabulate()
        pdt.assert_frame_equal(test_df_expected, ret_df, check_like=True)


class TestTimeseriesBulkUpsertRelatedNotFound(TimeSeriesServiceTest):
    @pytest.fixture(scope="class")
    def test_df(
        self,
        run: Run,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [run.id, "Region 1", "Variable 1", "Unit 1"],
                [run.id, "Region 1", "Variable 2", "Unit 2"],
                [run.id, "Region 2", "Variable 1", "Unit 1"],
            ],
            columns=["run__id", "region", "variable", "unit"],
        )

    def test_timeseries_bulk_upsert_related_not_found(
        self,
        service: TimeSeriesService,
        run: Run,
        fake_time: datetime.datetime,
        test_df: pd.DataFrame,
        regions: RegionService,
        units: UnitService,
    ) -> None:
        with pytest.raises(RegionNotFound, match="Region 1, Region 2"):
            service.bulk_upsert(test_df)

        regions.create("Region 1", "other")

        with pytest.raises(RegionNotFound, match="Region 2"):
            service.bulk_upsert(test_df)

        regions.create("Region 2", "default")

        with pytest.raises(UnitNotFound, match="Unit 1, Unit 2"):
            service.bulk_upsert(test_df)

        ret_df = service.tabulate()
        assert ret_df.empty


class TestpytestTimeseriesTabulate(TimeSeriesServiceTest):
    @pytest.fixture(scope="class")
    def test_df(
        self,
        run: Run,
        regions: RegionService,
        units: UnitService,
        fake_time: datetime.datetime,
    ) -> pd.DataFrame:
        self.create_related(regions, units)

        return pd.DataFrame(
            [
                [1, run.id, 1, 1, "Variable 1"],
                [2, run.id, 1, 2, "Variable 2"],
                [3, run.id, 2, 1, "Variable 1"],
            ],
            columns=["id", "run__id", "region__id", "unit__id", "variable"],
        )

    def test_timeseries_tabulate(
        self,
        service: TimeSeriesService,
        run: Run,
        fake_time: datetime.datetime,
        test_df: pd.DataFrame,
        test_df_expected: pd.DataFrame,
    ) -> None:
        service.bulk_upsert(test_df)
        ret_df = service.tabulate()
        pdt.assert_frame_equal(test_df_expected, ret_df, check_like=True)
