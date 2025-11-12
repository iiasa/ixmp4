from datetime import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.rewrite.data.iamc.datapoint.service import DataPointService
from ixmp4.rewrite.data.iamc.datapoint.type import Type
from ixmp4.rewrite.data.iamc.timeseries.service import TimeSeriesService
from ixmp4.rewrite.data.region.service import RegionService
from ixmp4.rewrite.data.run.dto import Run
from ixmp4.rewrite.data.run.service import RunService
from ixmp4.rewrite.data.unit.service import UnitService
from ixmp4.rewrite.transport import Transport
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class DataPointServiceTest(ServiceTest[DataPointService]):
    service_class = DataPointService

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
        timeseries: DataPointService,
        fake_time: datetime,
    ) -> pd.DataFrame:
        self.create_related(regions, units)
        df = pd.DataFrame(
            [
                [run.id, "Region 1", "Variable 1", "Unit 1"],
                [run.id, "Region 1", "Variable 2", "Unit 2"],
                [run.id, "Region 2", "Variable 1", "Unit 1"],
            ],
            columns=["run__id", "region", "variable", "unit"],
        )
        timeseries.bulk_upsert(df)
        return timeseries.tabulate()


class DataPointBulkOperationsTest(DataPointServiceTest):
    @pytest.fixture(scope="class")
    def test_annual_df(
        self,
        test_ts_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [1, 1, Type.ANNUAL, 2000, 1.1],
                [2, 1, Type.ANNUAL, 2010, 1.3],
                [3, 1, Type.ANNUAL, 2020, 1.5],
                [4, 1, Type.ANNUAL, 2030, 1.7],
            ],
            columns=[
                "id",
                "time_series__id",
                "type",
                "step_year",
                "value",
            ],
        )

    @pytest.fixture(scope="class")
    def expected_annual_df(
        self,
        test_annual_df: pd.DataFrame,
    ) -> pd.DataFrame:
        exp_df = test_annual_df.copy()
        exp_df["step_datetime"] = pd.NaT
        exp_df["step_category"] = None
        return exp_df

    @pytest.fixture(scope="class")
    def test_categorical_df(
        self,
        test_ts_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [1, 1, Type.CATEGORICAL, 2000, "A", 1.1],
                [2, 1, Type.CATEGORICAL, 2000, "B", -1.3],
                [3, 1, Type.CATEGORICAL, 2010, "A", 1.5],
                [4, 1, Type.CATEGORICAL, 2010, "B", -1.7],
            ],
            columns=[
                "id",
                "time_series__id",
                "type",
                "step_year",
                "step_category",
                "value",
            ],
        )

    @pytest.fixture(scope="class")
    def expected_categorical_df(
        self,
        test_categorical_df: pd.DataFrame,
    ) -> pd.DataFrame:
        exp_df = test_categorical_df.copy()
        exp_df["step_datetime"] = pd.NaT
        return exp_df

    @pytest.fixture(scope="class")
    def test_datetime_df(
        self,
        test_ts_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [1, 1, Type.DATETIME, datetime(2000, 1, 1, 0, 0, 0), 1.1],
                [2, 1, Type.DATETIME, datetime(2000, 2, 1, 0, 0, 0), 1.3],
                [3, 1, Type.DATETIME, datetime(2000, 3, 1, 0, 0, 0), 1.5],
                [4, 1, Type.DATETIME, datetime(2000, 4, 1, 0, 0, 0), 1.7],
            ],
            columns=["id", "time_series__id", "type", "step_datetime", "value"],
        )

    @pytest.fixture(scope="class")
    def expected_datetime_df(
        self,
        test_datetime_df: pd.DataFrame,
    ) -> pd.DataFrame:
        exp_df = test_datetime_df.copy()
        exp_df["step_year"] = None
        exp_df["step_category"] = None
        return exp_df

    @pytest.fixture(scope="class")
    def test_mixed_df(
        self,
        test_ts_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [1, 1, Type.ANNUAL, 2000, None, pd.NaT, -1.1],
                [2, 1, Type.ANNUAL, 2010, None, pd.NaT, -1.3],
                [3, 1, Type.ANNUAL, 2020, None, pd.NaT, -1.5],
                [4, 1, Type.ANNUAL, 2030, None, pd.NaT, -1.7],
                [5, 2, Type.CATEGORICAL, 2000, "A", pd.NaT, -1.1],
                [6, 2, Type.CATEGORICAL, 2000, "B", pd.NaT, -1.3],
                [7, 2, Type.CATEGORICAL, 2010, "A", pd.NaT, -1.5],
                [8, 2, Type.CATEGORICAL, 2010, "B", pd.NaT, -1.7],
                [9, 3, Type.DATETIME, None, None, datetime(2000, 1, 1, 0, 0, 0), 1.1],
                [10, 3, Type.DATETIME, None, None, datetime(2000, 2, 1, 0, 0, 0), 1.3],
                [11, 3, Type.DATETIME, None, None, datetime(2000, 3, 1, 0, 0, 0), 1.5],
                [12, 3, Type.DATETIME, None, None, datetime(2000, 4, 1, 0, 0, 0), 1.7],
            ],
            columns=[
                "id",
                "time_series__id",
                "type",
                "step_year",
                "step_category",
                "step_datetime",
                "value",
            ],
        )

    @pytest.fixture(scope="class")
    def expected_mixed_df(
        self,
        test_mixed_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return test_mixed_df.copy()

    def test_datapoint_bulk_insert(
        self,
        service: DataPointService,
        test_df: pd.DataFrame,
        expected_df: pd.DataFrame,
        infer_type: bool,
    ):
        upsert_df = test_df.drop(columns=["id"])
        if infer_type:
            upsert_df = upsert_df.drop(columns=["type"])

        service.bulk_upsert(upsert_df)
        ret_df = service.tabulate()
        pdt.assert_frame_equal(expected_df, ret_df, check_like=True)

    def test_datapoint_bulk_update(
        self,
        service: DataPointService,
        test_df: pd.DataFrame,
        expected_df: pd.DataFrame,
        infer_type: bool,
    ):
        update_df = test_df.drop(columns=["id"])
        if infer_type:
            update_df = update_df.drop(columns=["type"])

        update_df["value"] = -99.99
        expected_df["value"] = -99.99
        service.bulk_upsert(update_df)
        ret_df = service.tabulate()
        pdt.assert_frame_equal(expected_df, ret_df, check_like=True)

    def test_datapoint_bulk_delete(
        self,
        service: DataPointService,
        test_df: pd.DataFrame,
        expected_df: pd.DataFrame,
        infer_type: bool,
    ):
        delete_df = test_df.drop(columns=["id", "value"])
        if infer_type:
            delete_df = delete_df.drop(columns=["type"])

        service.bulk_delete(delete_df)
        ret_df = service.tabulate()
        assert ret_df.empty


class TestDatapointBulkAnnualInferType(DataPointBulkOperationsTest):
    @pytest.fixture(scope="class")
    def infer_type(self) -> bool:
        return True

    @pytest.fixture(scope="class")
    def test_df(
        self,
        test_annual_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return test_annual_df

    @pytest.fixture(scope="class")
    def expected_df(
        self,
        expected_annual_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return expected_annual_df


class TestDatapointBulkAnnualWithType(DataPointBulkOperationsTest):
    @pytest.fixture(scope="class")
    def infer_type(self) -> bool:
        return False

    @pytest.fixture(scope="class")
    def test_df(
        self,
        test_annual_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return test_annual_df

    @pytest.fixture(scope="class")
    def expected_df(
        self,
        expected_annual_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return expected_annual_df


class TestDatapointBulkCategoricalInferType(DataPointBulkOperationsTest):
    @pytest.fixture(scope="class")
    def infer_type(self) -> bool:
        return True

    @pytest.fixture(scope="class")
    def test_df(
        self,
        test_categorical_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return test_categorical_df

    @pytest.fixture(scope="class")
    def expected_df(
        self,
        expected_categorical_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return expected_categorical_df


class TestDatapointBulkCategoricalWithType(DataPointBulkOperationsTest):
    @pytest.fixture(scope="class")
    def infer_type(self) -> bool:
        return False

    @pytest.fixture(scope="class")
    def test_df(
        self,
        test_categorical_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return test_categorical_df

    @pytest.fixture(scope="class")
    def expected_df(
        self,
        expected_categorical_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return expected_categorical_df


class TestDatapointBulkDatetimeInferType(DataPointBulkOperationsTest):
    @pytest.fixture(scope="class")
    def infer_type(self) -> bool:
        return True

    @pytest.fixture(scope="class")
    def test_df(
        self,
        test_datetime_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return test_datetime_df

    @pytest.fixture(scope="class")
    def expected_df(
        self,
        expected_datetime_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return expected_datetime_df


class TestDatapointBulkDatetimeWithType(DataPointBulkOperationsTest):
    @pytest.fixture(scope="class")
    def infer_type(self) -> bool:
        return False

    @pytest.fixture(scope="class")
    def test_df(
        self,
        test_datetime_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return test_datetime_df

    @pytest.fixture(scope="class")
    def expected_df(
        self,
        expected_datetime_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return expected_datetime_df


class TestDatapointBulkMixedInferType(DataPointBulkOperationsTest):
    @pytest.fixture(scope="class")
    def infer_type(self) -> bool:
        return True

    @pytest.fixture(scope="class")
    def test_df(
        self,
        test_datetime_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return test_datetime_df

    @pytest.fixture(scope="class")
    def expected_df(
        self,
        expected_datetime_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return expected_datetime_df


class TestDatapointBulkMixedWithType(DataPointBulkOperationsTest):
    @pytest.fixture(scope="class")
    def infer_type(self) -> bool:
        return False

    @pytest.fixture(scope="class")
    def test_df(
        self,
        test_mixed_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return test_mixed_df

    @pytest.fixture(scope="class")
    def expected_df(
        self,
        expected_mixed_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return expected_mixed_df
