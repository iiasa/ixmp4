import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4
from tests import backends
from tests.base import DataFrameTest

from .base import PlatformTest

platform = backends.get_platform_fixture(scope="class")


class IamcTest(DataFrameTest, PlatformTest):
    @pytest.fixture(scope="class")
    def run(
        self,
        platform: ixmp4.Platform,
    ) -> ixmp4.Run:
        run = platform.runs.create("Model", "Scenario")
        assert run.id == 1
        return run

    @pytest.fixture(scope="class")
    def units(
        self,
        platform: ixmp4.Platform,
    ) -> list[ixmp4.Unit]:
        return [platform.units.create("Unit 1"), platform.units.create("Unit 2")]

    @pytest.fixture(scope="class")
    def regions(
        self,
        platform: ixmp4.Platform,
    ) -> list[ixmp4.Region]:
        return [
            platform.regions.create("Region 1", "default"),
            platform.regions.create("Region 2", "default"),
        ]


class IamcDataTest(IamcTest):
    @pytest.fixture(scope="class")
    def test_data_upsert(
        self,
        test_data_add: pd.DataFrame,
    ) -> pd.DataFrame:
        test_data_upsert = test_data_add.copy()
        test_data_upsert["value"] = -9.994599945
        return test_data_upsert

    def test_iamc_data_add(
        self,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
        test_data_type: ixmp4.iamc.DataPoint.Type | None,
    ):
        with run.transact("Full Addition"):
            run.iamc.add(test_data_add, type=test_data_type)

    def test_iamc_data_tabulate_after_add(
        self,
        platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
        test_data_type: ixmp4.iamc.DataPoint.Type | None,
    ):
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            test_data_add, ret.drop(columns=["type"]), check_like=True
        )

        test_data_platform = test_data_add.copy()
        test_data_platform["model"] = run.model.name
        test_data_platform["scenario"] = run.scenario.name
        test_data_platform["version"] = run.version

        ret_platform = platform.iamc.tabulate(run={"default_only": False})
        pdt.assert_frame_equal(
            test_data_platform, ret_platform.drop(columns=["type"]), check_like=True
        )

    def test_iamc_data_remove_partial(
        self,
        run: ixmp4.Run,
        test_data_remove: pd.DataFrame,
        test_data_type: ixmp4.iamc.DataPoint.Type | None,
    ):
        with run.transact("Partial Removal"):
            run.iamc.remove(test_data_remove, type=test_data_type)

    def test_iamc_data_remaining_after_remove_partial(
        self,
        run: ixmp4.Run,
        test_data_remaining: pd.DataFrame,
        test_data_type: ixmp4.iamc.DataPoint.Type | None,
    ):
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            test_data_remaining, ret.drop(columns=["type"]), check_like=True
        )

    def test_iamc_data_upsert(
        self,
        run: ixmp4.Run,
        test_data_upsert: pd.DataFrame,
        test_data_type: ixmp4.iamc.DataPoint.Type | None,
    ):
        with run.transact("Upsert"):
            run.iamc.add(test_data_upsert, type=test_data_type)

    def test_iamc_data_tabulate_after_upsert(
        self,
        platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_upsert: pd.DataFrame,
    ):
        ret = run.iamc.tabulate()
        ret = ret.drop(columns=["type"])
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_upsert),
            self.canonical_sort(ret),
            check_like=True,
        )

        test_data_platform = test_data_upsert.copy()
        test_data_platform["model"] = run.model.name
        test_data_platform["scenario"] = run.scenario.name
        test_data_platform["version"] = run.version
        ret_platform = platform.iamc.tabulate(run={"default_only": False}).drop(
            columns=["type"]
        )
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_platform),
            self.canonical_sort(ret_platform),
            check_like=True,
        )

    def test_iamc_data_remove_full(
        self,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
        test_data_type: ixmp4.iamc.DataPoint.Type | None,
    ):
        test_data_remove_full = test_data_add.drop(columns=["value"])
        with run.transact("Full Removal"):
            run.iamc.remove(test_data_remove_full, type=test_data_type)

    def test_iamc_data_tabulate_empty(
        self,
        run: ixmp4.Run,
    ):
        ret = run.iamc.tabulate()
        assert ret.columns.sort_values().to_list() == [
            "region",
            "type",
            "unit",
            "value",
            "variable",
        ]
        assert ret.empty

    def test_iamc_data_versioning(self, versioning_platform: ixmp4.Platform) -> None:
        expected_versions = pd.DataFrame(
            [],
            columns=[
                "id",
                "name",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_platform.backend.iamc.datapoints.pandas_versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf)


class IamcDataRollbackTest(IamcTest):
    def test_iamc_data_removal_failure(
        self,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
        test_data_remove: pd.DataFrame,
    ):
        try:
            with run.transact("Add and remove iamc data failure"):
                run.iamc.add(test_data_add)
                run.checkpoints.create("Add iamc data")
                run.iamc.remove(test_data_remove)
                raise Exception("Whoops!!!")
        except Exception:
            pass

    def test_iamc_data_versioning_after_removal_failure(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
        test_data_remove: pd.DataFrame,
    ):
        ret = run.iamc.tabulate().drop(columns=["type"])
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_add),
            self.canonical_sort(ret),
            check_like=True,
        )

    def test_iamc_data_non_versioning_after_removal_failure(
        self,
        non_versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_remove: pd.DataFrame,
        test_data_remaining: pd.DataFrame,
    ):
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_remaining),
            self.canonical_sort(ret),
            check_like=True,
        )

    def test_iamc_data_upsert_failure(
        self,
        run: ixmp4.Run,
        test_data_upsert: pd.DataFrame,
    ):
        try:
            with run.transact("Upsert iamc data"):
                run.iamc.add(test_data_upsert)
                raise Exception("Whoops!!!")
        except Exception:
            pass

    def test_iamc_data_versioning_after_upsert_failure(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
    ):
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_add),
            self.canonical_sort(ret),
            check_like=True,
        )

    def test_iamc_data_non_versioning_after_upsert_failure(
        self,
        non_versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_upsert: pd.DataFrame,
    ):
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_upsert),
            self.canonical_sort(ret),
            check_like=True,
        )


class IamcDataAnnual:
    @pytest.fixture(scope="class")
    def test_data_add(
        self,
        regions: list[ixmp4.Region],
        units: list[ixmp4.Unit],
    ) -> pd.DataFrame:
        df = pd.DataFrame(
            [
                ["Region 1", "Unit 1", "Variable 1", 2000, 1.1],
                ["Region 1", "Unit 1", "Variable 1", 2010, 1.3],
                ["Region 1", "Unit 2", "Variable 2", 2020, 1.5],
                ["Region 1", "Unit 2", "Variable 2", 2030, 1.7],
                ["Region 2", "Unit 1", "Variable 1", 2000, 2.1],
                ["Region 2", "Unit 1", "Variable 1", 2010, 2.3],
                ["Region 2", "Unit 2", "Variable 2", 2020, 2.5],
                ["Region 2", "Unit 2", "Variable 2", 2030, 2.7],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        df["year"] = df["year"].astype("Int64")
        return df

    @pytest.fixture(scope="class")
    def test_data_remove(
        self,
        regions: list[ixmp4.Region],
        units: list[ixmp4.Unit],
    ) -> pd.DataFrame:
        df = pd.DataFrame(
            [
                ["Region 1", "Unit 1", "Variable 1", 2000, 1.1],
                ["Region 1", "Unit 1", "Variable 1", 2010, 1.3],
                ["Region 1", "Unit 2", "Variable 2", 2020, 1.5],
                ["Region 1", "Unit 2", "Variable 2", 2030, 1.7],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        df["year"] = df["year"].astype("Int64")
        return df

    @pytest.fixture(scope="class")
    def test_data_remaining(
        self,
        regions: list[ixmp4.Region],
        units: list[ixmp4.Unit],
    ) -> pd.DataFrame:
        df = pd.DataFrame(
            [
                ["Region 2", "Unit 1", "Variable 1", 2000, 2.1],
                ["Region 2", "Unit 1", "Variable 1", 2010, 2.3],
                ["Region 2", "Unit 2", "Variable 2", 2020, 2.5],
                ["Region 2", "Unit 2", "Variable 2", 2030, 2.7],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        df["year"] = df["year"].astype("Int64")
        return df

    @pytest.fixture(scope="class")
    def test_data_upsert(
        self,
        test_data_add: pd.DataFrame,
    ) -> pd.DataFrame:
        test_data_upsert = test_data_add.copy()
        test_data_upsert["value"] = np.sin(test_data_upsert["value"])
        return test_data_upsert


class TestIamcDataAnnualInferType(IamcDataAnnual, IamcDataTest):
    @pytest.fixture(scope="class")
    def test_data_type(self) -> ixmp4.iamc.DataPoint.Type | None:
        return None


class TestIamcDataAnnualWithType(IamcDataAnnual, IamcDataTest):
    @pytest.fixture(scope="class")
    def test_data_type(self) -> ixmp4.iamc.DataPoint.Type | None:
        return ixmp4.iamc.DataPoint.Type.ANNUAL


class TestIamcDataAnnualRollback(IamcDataAnnual, IamcDataRollbackTest):
    pass


class TestIamcDataRunLock(IamcDataAnnual, IamcTest):
    def test_iamc_data_requires_lock(
        self,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
        test_data_remove: pd.DataFrame,
    ) -> None:
        with pytest.raises(ixmp4.Run.LockRequired):
            run.iamc.add(test_data_add)

        with run.transact("Add iamc data"):
            run.iamc.add(test_data_add)

        with pytest.raises(ixmp4.Run.LockRequired):
            run.iamc.remove(test_data_remove)
