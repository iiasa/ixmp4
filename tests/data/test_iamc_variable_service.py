import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.rewrite.data.iamc.timeseries.service import TimeSeriesService
from ixmp4.rewrite.data.iamc.variable.service import VariableService
from ixmp4.rewrite.data.region.service import RegionService
from ixmp4.rewrite.data.run.dto import Run
from ixmp4.rewrite.data.run.service import RunService
from ixmp4.rewrite.data.unit.service import UnitService
from ixmp4.rewrite.transport import Transport
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class VariableServiceTest(ServiceTest[VariableService]):
    service_class = VariableService

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
    def timeseries(self, transport: Transport) -> TimeSeriesService:
        return TimeSeriesService(transport)

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

    @pytest.fixture(scope="class")
    def test_ts_df(
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


class TestVariableTabulate(VariableServiceTest):
    @pytest.fixture(scope="class")
    def test_df_expected(self, run: Run, fake_time: datetime.datetime) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [1, "Variable 1", fake_time.replace(tzinfo=None), "@unknown"],
                [2, "Variable 2", fake_time.replace(tzinfo=None), "@unknown"],
            ],
            columns=["id", "name", "created_at", "created_by"],
        )

    def test_variable_tabulate(
        self,
        service: VariableService,
        timeseries: TimeSeriesService,
        test_ts_df: pd.DataFrame,
        test_df_expected: pd.DataFrame,
        fake_time: datetime.datetime,
    ):
        timeseries.bulk_upsert(test_ts_df)
        ret_df = service.tabulate()
        pdt.assert_frame_equal(test_df_expected, ret_df, check_like=True)
