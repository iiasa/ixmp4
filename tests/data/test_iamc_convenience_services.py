from datetime import datetime

import pandas as pd
import pytest

from ixmp4.data.iamc.datapoint.service import DataPointService
from ixmp4.data.iamc.datapoint.type import Type
from ixmp4.data.iamc.model.service import IamcModelService
from ixmp4.data.iamc.region.service import IamcRegionService
from ixmp4.data.iamc.scenario.service import IamcScenarioService
from ixmp4.data.iamc.timeseries.service import TimeSeriesService
from ixmp4.data.iamc.unit.service import IamcUnitService
from ixmp4.data.region.service import RegionService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.data.unit.service import UnitService
from ixmp4.transport import Transport
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class TestIamcConvenienceServices(ServiceTest[DataPointService]):
    @pytest.fixture(scope="class")
    def runs(self, transport: Transport) -> RunService:
        return RunService(transport)

    @pytest.fixture(scope="class")
    def run(self, runs: RunService) -> Run:
        run = runs.create("Model", "Scenario")
        runs.set_as_default_version(run.id)
        assert run.id == 1

        runs.create("Other Model", "Other Scenario")
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

    @pytest.fixture(scope="class")
    def datapoints(self, transport: Transport) -> DataPointService:
        return DataPointService(transport)

    def create_related(
        self,
        regions: RegionService,
        units: UnitService,
    ) -> None:
        # assume regions and units have been created
        # by a manager
        regions.create("Region 1", "default")
        regions.create("Region 2", "default")
        regions.create("Unused Region 3", "default")
        units.create("Unit 1")
        units.create("Unit 2")
        units.create("Unused Unit 3")

    @pytest.fixture(scope="class")
    def test_ts_df(
        self,
        run: Run,
        regions: RegionService,
        units: UnitService,
        timeseries: DataPointService,
        fake_time: datetime,
    ) -> pd.DataFrame:
        return timeseries.tabulate()

    @pytest.fixture(scope="class")
    def iamc_models(self, transport: Transport) -> IamcModelService:
        return IamcModelService(transport)

    @pytest.fixture(scope="class")
    def iamc_scenarios(self, transport: Transport) -> IamcScenarioService:
        return IamcScenarioService(transport)

    @pytest.fixture(scope="class")
    def iamc_units(self, transport: Transport) -> IamcUnitService:
        return IamcUnitService(transport)

    @pytest.fixture(scope="class")
    def iamc_regions(self, transport: Transport) -> IamcRegionService:
        return IamcRegionService(transport)

    def test_load_iamc_data(
        self,
        datapoints: DataPointService,
        run: Run,
        regions: RegionService,
        units: UnitService,
        timeseries: TimeSeriesService,
    ) -> None:
        self.create_related(regions, units)
        ts_df = pd.DataFrame(
            [
                [run.id, "Region 1", "Variable 1", "Unit 1"],
                [run.id, "Region 1", "Variable 2", "Unit 2"],
                [run.id, "Region 2", "Variable 1", "Unit 1"],
            ],
            columns=["run__id", "region", "variable", "unit"],
        )
        timeseries.bulk_upsert(ts_df)

        df = pd.DataFrame(
            [
                [1, Type.ANNUAL, 2000, 1.1],
                [2, Type.ANNUAL, 2010, 1.3],
                [3, Type.ANNUAL, 2020, 1.5],
                [1, Type.ANNUAL, 2030, 1.7],
            ],
            columns=["time_series__id", "type", "step_year", "value"],
        )
        datapoints.bulk_upsert(df)

    def test_iamc_models(self, iamc_models: IamcModelService) -> None:
        assert len(iamc_models.list()) == 1
        assert len(iamc_models.tabulate()) == 1

    def test_iamc_scenarios(self, iamc_scenarios: IamcScenarioService) -> None:
        assert len(iamc_scenarios.list()) == 1
        assert len(iamc_scenarios.tabulate()) == 1

    def test_iamc_units(self, iamc_units: IamcUnitService) -> None:
        assert len(iamc_units.list()) == 2
        assert len(iamc_units.tabulate()) == 2

    def test_iamc_regions(self, iamc_regions: IamcRegionService) -> None:
        assert len(iamc_regions.list()) == 2
        assert len(iamc_regions.tabulate()) == 2
