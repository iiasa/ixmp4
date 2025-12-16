from datetime import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.iamc.datapoint.service import DataPointService
from ixmp4.data.iamc.datapoint.type import Type
from ixmp4.data.iamc.reverter import DataPointReverterRepository
from ixmp4.data.iamc.timeseries.service import TimeSeriesService
from ixmp4.data.region.service import RegionService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.data.unit.service import UnitService
from ixmp4.data.versions.model import Operation
from ixmp4.transport import Transport
from tests import auth, backends
from tests.base import DataFrameTest
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
    ) -> None:
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


class DataPointBulkOperationsTest(DataFrameTest, DataPointServiceTest):
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
        exp_df["step_year"] = exp_df["step_year"].astype("Int64")
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
        exp_df["step_year"] = exp_df["step_year"].astype("Int64")
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
        exp_df = test_mixed_df.copy()
        exp_df["step_year"] = exp_df["step_year"].astype("Int64")
        return exp_df

    def test_datapoint_bulk_insert(
        self,
        service: DataPointService,
        test_df: pd.DataFrame,
        expected_df: pd.DataFrame,
        infer_type: bool,
    ) -> None:
        upsert_df = test_df.drop(columns=["id"])
        if infer_type:
            upsert_df = upsert_df.drop(columns=["type"])

        service.bulk_upsert(upsert_df)
        ret_df = service.tabulate()
        pdt.assert_frame_equal(expected_df, ret_df, check_like=True)

    def test_datapoint_tabulate(
        self,
        service: DataPointService,
        test_df: pd.DataFrame,
        expected_df: pd.DataFrame,
        infer_type: bool,
    ) -> None:
        ret_df = service.tabulate()
        pdt.assert_frame_equal(expected_df, ret_df, check_like=True)

        ret_df = service.tabulate(join_parameters=True)
        assert "region" in ret_df.columns
        assert "variable" in ret_df.columns
        assert "unit" in ret_df.columns

        ret_df = service.tabulate(join_runs=True)
        assert "model" in ret_df.columns
        assert "scenario" in ret_df.columns
        assert "version" in ret_df.columns

        ret_df = service.tabulate(join_run_id=True)
        assert "run__id" in ret_df.columns

    def test_datapoint_bulk_update(
        self,
        service: DataPointService,
        test_df: pd.DataFrame,
        expected_df: pd.DataFrame,
        infer_type: bool,
    ) -> None:
        update_df = test_df.drop(columns=["id"])
        if infer_type:
            update_df = update_df.drop(columns=["type"])

        update_df["value"] = -99.99
        expected_df = expected_df.copy()
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
    ) -> None:
        delete_df = test_df.drop(columns=["id", "value"])
        if infer_type:
            delete_df = delete_df.drop(columns=["type"])

        service.bulk_delete(delete_df)
        ret_df = service.tabulate()
        assert ret_df.empty

    @pytest.fixture(scope="class")
    def tx_after_insert(self, test_df: pd.DataFrame) -> int:
        return 14 + len(test_df)

    @pytest.fixture(scope="class")
    def tx_after_update(self, tx_after_insert: int, test_df: pd.DataFrame) -> int:
        return tx_after_insert + len(test_df)

    @pytest.fixture(scope="class")
    def tx_after_delete(self, tx_after_update: int) -> int:
        return tx_after_update + 2

    def test_datapoint_revert_data(
        self,
        versioning_service: DataPointService,
        tx_after_insert: int,
        tx_after_update: int,
        tx_after_delete: int,
        test_df: pd.DataFrame,
        run: Run,
    ) -> None:
        reverter_repo = DataPointReverterRepository(versioning_service.executor)

        # insert revert data
        expected_insert_revert_df = test_df.copy()

        expected_insert_revert_df["revert_operation_type"] = Operation.DELETE.value
        revert_insert_df = reverter_repo.tabulate_revert_ops(
            tx_after_insert, 1, run.id
        ).drop(columns=["transaction_id", "end_transaction_id", "operation_type"])
        revert_insert_df = self.drop_empty_columns(revert_insert_df)
        pdt.assert_frame_equal(
            self.canonical_sort(expected_insert_revert_df),
            self.canonical_sort(revert_insert_df),
            check_like=True,
        )

        # update revert data
        revert_update_df = reverter_repo.tabulate_revert_ops(
            tx_after_update, tx_after_insert, run.id
        ).drop(columns=["transaction_id", "end_transaction_id", "operation_type"])
        revert_update_df = self.drop_empty_columns(revert_update_df)

        expected_revert_update_df = test_df.copy()
        expected_revert_update_df["revert_operation_type"] = Operation.UPDATE.value
        pdt.assert_frame_equal(
            self.canonical_sort(expected_revert_update_df),
            self.canonical_sort(revert_update_df),
            check_like=True,
        )

        # delete revert data
        revert_delete_df = reverter_repo.tabulate_revert_ops(
            tx_after_delete, tx_after_update, run.id
        ).drop(columns=["transaction_id", "end_transaction_id", "operation_type"])
        revert_delete_df = self.drop_empty_columns(revert_delete_df)
        expected_revert_delete_df = test_df.copy()
        expected_revert_delete_df["revert_operation_type"] = Operation.INSERT.value
        expected_revert_delete_df["value"] = -99.99

        pdt.assert_frame_equal(
            self.canonical_sort(expected_revert_delete_df),
            self.canonical_sort(revert_delete_df),
            check_like=True,
        )

    def test_datapoint_versions(
        self,
        versioning_service: DataPointService,
        tx_after_insert: int,
        tx_after_update: int,
        tx_after_delete: int,
        test_df: pd.DataFrame,
    ) -> None:
        # TODO: Uncommit valid_at_tx tests when version filter is implemented

        # insert valid version records
        insert_versions_df = test_df.copy()
        insert_versions_df["operation_type"] = Operation.INSERT.value

        ret_insert_versions_df = versioning_service.versions.tabulate(
            {"valid_at_transaction": tx_after_insert}
        )
        ret_insert_versions_df = self.drop_empty_columns(ret_insert_versions_df)
        ret_insert_versions_df = ret_insert_versions_df.drop(
            columns=["transaction_id", "end_transaction_id"]
        )
        pdt.assert_frame_equal(
            self.canonical_sort(insert_versions_df),
            self.canonical_sort(ret_insert_versions_df),
            check_like=True,
        )

        # update valid version records
        update_versions_df = test_df.copy()
        update_versions_df["operation_type"] = Operation.UPDATE.value
        update_versions_df["value"] = -99.99

        ret_update_versions_df = versioning_service.versions.tabulate(
            {"valid_at_transaction": tx_after_update}
        )
        ret_update_versions_df = self.drop_empty_columns(ret_update_versions_df)
        ret_update_versions_df = ret_update_versions_df.drop(
            columns=["transaction_id", "end_transaction_id"]
        )
        pdt.assert_frame_equal(
            self.canonical_sort(update_versions_df),
            self.canonical_sort(ret_update_versions_df),
            check_like=True,
        )

        # delete valid version records
        delete_versions_df = update_versions_df.copy()
        delete_versions_df["operation_type"] = Operation.DELETE.value
        ret_delete_versions_df = versioning_service.versions.tabulate(
            {"valid_at_transaction": tx_after_delete}
        )
        assert ret_delete_versions_df.empty

        # all version records
        expected_versions_df = pd.concat(
            [insert_versions_df, update_versions_df, delete_versions_df],
            ignore_index=True,
        )
        ret_versions_df = (
            versioning_service.versions.tabulate()
            .drop(columns=["transaction_id", "end_transaction_id"])
            .dropna(how="all", axis="columns")
        )

        pdt.assert_frame_equal(
            self.canonical_sort(expected_versions_df),
            self.canonical_sort(ret_versions_df),
            check_like=True,
        )


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


class DataPointAuthTest(DataPointServiceTest):
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
    def timeseries(self, transport: Transport) -> TimeSeriesService:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return TimeSeriesService(direct)

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

    @pytest.fixture(scope="class")
    def test_ts_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                ["Region 1", "Variable 1", "Unit 1"],
                ["Region 1", "Variable 2", "Unit 2"],
                ["Region 2", "Variable 1", "Unit 1"],
            ],
            columns=["region", "variable", "unit"],
        )

    @pytest.fixture(scope="class")
    def run(
        self,
        runs: RunService,
    ) -> Run:
        run = runs.create("Model", "Scenario")
        assert run.id == 1
        return run

    @pytest.fixture(scope="class")
    def test_df(
        self,
        regions: RegionService,
        units: UnitService,
        run: Run,
        timeseries: TimeSeriesService,
        test_ts_df: pd.DataFrame,
    ) -> pd.DataFrame:
        self.create_related(regions, units)
        test_ts_df["run__id"] = run.id
        timeseries.bulk_upsert(test_ts_df)
        return pd.DataFrame(
            [
                [1, Type.ANNUAL, 2000, 1.1],
                [1, Type.ANNUAL, 2010, 1.3],
                [1, Type.ANNUAL, 2020, 1.5],
                [1, Type.ANNUAL, 2030, 1.7],
            ],
            columns=[
                "time_series__id",
                "type",
                "step_year",
                "value",
            ],
        )


class TestDataPointAuthSarahPrivate(
    auth.SarahTest, auth.PrivatePlatformTest, DataPointAuthTest
):
    def test_datapoint_bulk_insert(
        self,
        service: DataPointService,
        test_df: pd.DataFrame,
    ) -> None:
        service.bulk_upsert(test_df)

    def test_datapoint_tabulate(self, service: DataPointService) -> None:
        ret_df = service.tabulate()
        assert len(ret_df) == 4

    def test_datapoint_bulk_delete(
        self, service: DataPointService, test_df: pd.DataFrame
    ) -> None:
        delete_df = test_df.drop(columns=["value"])
        service.bulk_delete(delete_df)


class TestDataPointAuthAlicePrivate(
    auth.AliceTest, auth.PrivatePlatformTest, DataPointAuthTest
):
    def test_datapoint_bulk_insert(
        self,
        service: DataPointService,
        unauthorized_service: DataPointService,
        test_df: pd.DataFrame,
    ) -> None:
        with pytest.raises(Forbidden):
            service.bulk_upsert(test_df)
        unauthorized_service.bulk_upsert(test_df)

    def test_datapoint_tabulate(self, service: DataPointService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_datapoint_bulk_delete(
        self,
        service: DataPointService,
        unauthorized_service: DataPointService,
        test_df: pd.DataFrame,
    ) -> None:
        delete_df = test_df.drop(columns=["value"])
        with pytest.raises(Forbidden):
            service.bulk_delete(delete_df)
        unauthorized_service.bulk_upsert(test_df)


class TestDataPointAuthBobPrivate(
    auth.BobTest, auth.PrivatePlatformTest, DataPointAuthTest
):
    def test_datapoint_bulk_insert(
        self,
        service: DataPointService,
        test_df: pd.DataFrame,
    ) -> None:
        service.bulk_upsert(test_df)

    def test_datapoint_tabulate(self, service: DataPointService) -> None:
        ret_df = service.tabulate()
        assert len(ret_df) == 4

    def test_datapoint_bulk_delete(
        self, service: DataPointService, test_df: pd.DataFrame
    ) -> None:
        delete_df = test_df.drop(columns=["value"])
        service.bulk_delete(delete_df)


class TestDataPointAuthCarinaPrivate(
    auth.CarinaTest, auth.PrivatePlatformTest, DataPointAuthTest
):
    @pytest.fixture(scope="class")
    def run1(
        self,
        runs: RunService,
    ) -> Run:
        run1 = runs.create("Model 1", "Scenario")
        assert run1.id == 2
        return run1

    @pytest.fixture(scope="class")
    def run2(
        self,
        runs: RunService,
    ) -> Run:
        run2 = runs.create("Model 2", "Scenario")
        assert run2.id == 3
        return run2

    @pytest.fixture(scope="class")
    def test_df1(
        self,
        run1: Run,
        timeseries: TimeSeriesService,
        test_ts_df: pd.DataFrame,
    ) -> pd.DataFrame:
        test_ts_df1 = test_ts_df.copy()
        test_ts_df1["run__id"] = run1.id
        timeseries.bulk_upsert(test_ts_df1)
        return pd.DataFrame(
            [
                [4, Type.CATEGORICAL, 2000, "A", 1.1],
                [4, Type.CATEGORICAL, 2010, "A", 1.3],
                [5, Type.CATEGORICAL, 2000, "B", 1.5],
                [6, Type.CATEGORICAL, 2010, "B", 1.7],
            ],
            columns=[
                "time_series__id",
                "type",
                "step_year",
                "step_category",
                "value",
            ],
        )

    @pytest.fixture(scope="class")
    def test_df2(
        self,
        run2: Run,
        timeseries: TimeSeriesService,
        test_ts_df: pd.DataFrame,
    ) -> pd.DataFrame:
        test_ts_df2 = test_ts_df.copy()
        test_ts_df2["run__id"] = run2.id
        timeseries.bulk_upsert(test_ts_df2)
        return pd.DataFrame(
            [
                [7, Type.DATETIME, datetime(2000, 1, 1, 0, 0, 0), 1.1],
                [7, Type.DATETIME, datetime(2000, 2, 1, 0, 0, 0), 1.3],
                [9, Type.DATETIME, datetime(2000, 3, 1, 0, 0, 0), 1.5],
                [9, Type.DATETIME, datetime(2000, 4, 1, 0, 0, 0), 1.7],
            ],
            columns=[
                "time_series__id",
                "type",
                "step_datetime",
                "value",
            ],
        )

    def test_datapoint_bulk_insert(
        self,
        service: DataPointService,
        unauthorized_service: DataPointService,
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

    def test_datapoint_tabulate(self, service: DataPointService) -> None:
        ret_df = service.tabulate()
        assert len(ret_df) == 8

    def test_datapoint_bulk_delete(
        self,
        service: DataPointService,
        test_df: pd.DataFrame,
        test_df1: pd.DataFrame,
        test_df2: pd.DataFrame,
    ) -> None:
        delete_df = test_df.drop(columns=["value"])
        with pytest.raises(Forbidden):
            service.bulk_delete(delete_df)

        delete_df1 = test_df1.drop(columns=["value"])
        service.bulk_delete(delete_df1)

        delete_df2 = test_df2.drop(columns=["value"])
        with pytest.raises(Forbidden):
            service.bulk_delete(delete_df2)
