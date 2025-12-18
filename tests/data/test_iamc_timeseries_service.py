import datetime

import pandas as pd
import pandas.testing as pdt
import pytest
from toolkit import db

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.iamc.timeseries.service import TimeSeriesService
from ixmp4.data.iamc.variable.repositories import (
    ItemRepository as VariableRepository,
)
from ixmp4.data.region.exceptions import RegionNotFound
from ixmp4.data.region.service import RegionService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.data.unit.exceptions import UnitNotFound
from ixmp4.data.unit.service import UnitService
from ixmp4.transport import Transport
from tests import auth, backends
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
    ) -> None:
        # assume regions and units have been created
        # by a manager
        regions.create("Region 1", "default")
        regions.create("Region 2", "default")
        units.create("Unit 1")
        units.create("Unit 2")

    def create_variables(
        self,
        variables: VariableRepository,
    ) -> None:
        # assume vars have been created
        # by prior insertion
        variables.create({"name": "Variable 1"})
        variables.create({"name": "Variable 2"})


class TestTimeSeriesBulkUpsertFullRelated(TimeSeriesServiceTest):
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


class TestTimeSeriesBulkUpsertNoRegions(TimeSeriesServiceTest):
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


class TestTimeSeriesBulkUpsertNoVars(TimeSeriesServiceTest):
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


class TestTimeSeriesBulkUpsertNoUnits(TimeSeriesServiceTest):
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


class TestTimeSeriesBulkUpsertNoMeasurands(TimeSeriesServiceTest):
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


class TestTimeSeriesBulkUpsertRelatedNotFound(TimeSeriesServiceTest):
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


class TestTimeSeriesTabulate(TimeSeriesServiceTest):
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


class TestTimeSeriesTabulateByDf(TimeSeriesServiceTest):
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
                [run.id, "Region 1", "Variable 1", "Unit 1"],
                [run.id, "Region 1", "Variable 2", "Unit 2"],
                [run.id, "Region 2", "Variable 1", "Unit 1"],
            ],
            columns=["run__id", "region", "variable", "unit"],
        )

    @pytest.fixture(scope="class")
    def test_df_expected(self, run: Run, fake_time: datetime.datetime) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [1, run.id, "Region 1", "Variable 1", "Unit 1"],
                [2, run.id, "Region 1", "Variable 2", "Unit 2"],
                [3, run.id, "Region 2", "Variable 1", "Unit 1"],
            ],
            columns=["id", "run__id", "region", "variable", "unit"],
        )

    def test_timeseries_tabulate_by_df(
        self,
        service: TimeSeriesService,
        run: Run,
        fake_time: datetime.datetime,
        test_df: pd.DataFrame,
        test_df_expected: pd.DataFrame,
    ) -> None:
        service.bulk_upsert(test_df)
        ret_df = service.tabulate_by_df(test_df)
        pdt.assert_frame_equal(test_df_expected, ret_df, check_like=True)


class TimeSeriesAuthTest(TimeSeriesServiceTest):
    @pytest.fixture(scope="class")
    def runs(self, transport: Transport) -> RunService:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return RunService(direct)

    @pytest.fixture(scope="class")
    def units(self, transport: Transport) -> UnitService:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return UnitService(direct)

    @pytest.fixture(scope="class")
    def regions(self, transport: Transport) -> RegionService:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return RegionService(direct)

    @pytest.fixture(scope="class")
    def variables(self, transport: Transport) -> VariableRepository:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return VariableRepository(db.r.SessionExecutor(direct.session))

    @pytest.fixture(scope="class")
    def run(
        self,
        runs: RunService,
    ) -> Run:
        run = runs.create("Model", "Scenario")
        return run

    @pytest.fixture(scope="class")
    def run1(
        self,
        runs: RunService,
    ) -> Run:
        run = runs.create("Model 1", "Scenario")
        return run

    @pytest.fixture(scope="class")
    def run2(
        self,
        runs: RunService,
    ) -> Run:
        run = runs.create("Model 2", "Scenario")
        return run

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
                [run.id, "Region 1", "Variable 1", "Unit 1"],
                [run.id, "Region 1", "Variable 2", "Unit 2"],
                [run.id, "Region 2", "Variable 1", "Unit 1"],
            ],
            columns=["run__id", "region", "variable", "unit"],
        )

    @pytest.fixture(scope="class")
    def test_df1(
        self,
        run1: Run,
        fake_time: datetime.datetime,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [run1.id, "Region 1", "Variable 1", "Unit 1"],
                [run1.id, "Region 1", "Variable 2", "Unit 2"],
                [run1.id, "Region 2", "Variable 1", "Unit 1"],
            ],
            columns=["run__id", "region", "variable", "unit"],
        )

    @pytest.fixture(scope="class")
    def test_df2(
        self,
        run2: Run,
        fake_time: datetime.datetime,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [run2.id, "Region 1", "Variable 1", "Unit 1"],
                [run2.id, "Region 1", "Variable 2", "Unit 2"],
                [run2.id, "Region 2", "Variable 1", "Unit 1"],
            ],
            columns=["run__id", "region", "variable", "unit"],
        )


class TestTimeSeriesAuthSarahPrivate(
    auth.SarahTest, auth.PrivatePlatformTest, TimeSeriesAuthTest
):
    def test_timeseries_bulk_upsert(
        self,
        service: TimeSeriesService,
        test_df: pd.DataFrame,
    ) -> None:
        service.bulk_upsert(test_df)

    def test_timeseries_tabulate_by_df(
        self,
        service: TimeSeriesService,
        test_df: pd.DataFrame,
    ) -> None:
        ret_df = service.tabulate_by_df(test_df)
        assert len(ret_df) == 3

    def test_timeseries_tabulate(
        self,
        service: TimeSeriesService,
    ) -> None:
        ret_df = service.tabulate()
        assert len(ret_df) == 3


class TestTimeSeriesAuthAlicePrivate(
    auth.AliceTest, auth.PrivatePlatformTest, TimeSeriesAuthTest
):
    def test_timeseries_bulk_upsert(
        self,
        service: TimeSeriesService,
        unauthorized_service: TimeSeriesService,
        test_df: pd.DataFrame,
    ) -> None:
        with pytest.raises(Forbidden):
            service.bulk_upsert(test_df)
        unauthorized_service.bulk_upsert(test_df)

    def test_timeseries_tabulate_by_df(
        self,
        service: TimeSeriesService,
        test_df: pd.DataFrame,
    ) -> None:
        with pytest.raises(Forbidden):
            service.tabulate_by_df(test_df)

    def test_timeseries_tabulate(
        self,
        service: TimeSeriesService,
    ) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()


class TestTimeSeriesAuthBobPrivate(
    auth.BobTest, auth.PrivatePlatformTest, TimeSeriesAuthTest
):
    def test_timeseries_bulk_upsert(
        self,
        service: TimeSeriesService,
        test_df: pd.DataFrame,
    ) -> None:
        service.bulk_upsert(test_df)

    def test_timeseries_tabulate_by_df(
        self,
        service: TimeSeriesService,
        test_df: pd.DataFrame,
    ) -> None:
        ret_df = service.tabulate_by_df(test_df)
        assert len(ret_df) == 3

    def test_timeseries_tabulate(
        self,
        service: TimeSeriesService,
    ) -> None:
        ret_df = service.tabulate()
        assert len(ret_df) == 3


class TestTimeSeriesAuthCarinaPrivate(
    auth.CarinaTest, auth.PrivatePlatformTest, TimeSeriesAuthTest
):
    def test_timeseries_bulk_upsert(
        self,
        service: TimeSeriesService,
        unauthorized_service: TimeSeriesService,
        test_df: pd.DataFrame,
        test_df1: pd.DataFrame,
        test_df2: pd.DataFrame,
    ) -> None:
        with pytest.raises(Forbidden):
            service.bulk_upsert(test_df)
        unauthorized_service.bulk_upsert(test_df)

        service.bulk_upsert(test_df1)

        with pytest.raises(Forbidden):
            service.bulk_upsert(test_df2)
        unauthorized_service.bulk_upsert(test_df2)

    def test_timeseries_tabulate_by_df(
        self,
        service: TimeSeriesService,
        test_df: pd.DataFrame,
        test_df1: pd.DataFrame,
        test_df2: pd.DataFrame,
    ) -> None:
        ret_df = service.tabulate_by_df(test_df)
        assert len(ret_df) == 3
        ret_df = service.tabulate_by_df(test_df1)
        assert len(ret_df) == 3
        ret_df = service.tabulate_by_df(test_df2)
        assert len(ret_df) == 0

    def test_timeseries_tabulate(
        self,
        service: TimeSeriesService,
    ) -> None:
        ret_df = service.tabulate()
        assert len(ret_df) == 6
