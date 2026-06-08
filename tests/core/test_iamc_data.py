import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
import sqlalchemy as sa
from sqlalchemy import orm
from toolkit.db.executor import SessionExecutor

import ixmp4
from ixmp4.data.iamc.datapoint.type import Type
from ixmp4.data.iamc.measurand.db import Measurand
from ixmp4.data.versions.transaction import TransactionRepository
from tests import backends
from tests.base import DataFrameTest
from tests.custom_exception import CustomException

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
        test_data_type: Type | None,
    ) -> None:
        with run.transact("Full Addition"):
            run.iamc.add(test_data_add, type=test_data_type)

    def test_iamc_data_tabulate_after_add(
        self,
        platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
        test_data_type: Type | None,
    ) -> None:
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(test_data_add, ret, check_like=True)

        test_data_platform = test_data_add.copy()
        test_data_platform["model"] = run.model.name
        test_data_platform["scenario"] = run.scenario.name
        test_data_platform["version"] = run.version

        ret_platform = platform.iamc.tabulate(run={"default_only": False})
        pdt.assert_frame_equal(test_data_platform, ret_platform, check_like=True)

    def test_iamc_data_facade_name_filter_shorthands(
        self,
        run: ixmp4.Run,
    ) -> None:
        ret_region = run.iamc.tabulate(region="Region 1")
        ret_region_explicit = run.iamc.tabulate(region={"name__like": "Region 1"})
        pdt.assert_frame_equal(
            self.canonical_sort(ret_region),
            self.canonical_sort(ret_region_explicit),
            check_like=True,
        )

        ret_unit = run.iamc.tabulate(unit=["Unit 1", "Unit 2"])
        ret_unit_explicit = run.iamc.tabulate(unit={"name__in": ["Unit 1", "Unit 2"]})
        pdt.assert_frame_equal(
            self.canonical_sort(ret_unit),
            self.canonical_sort(ret_unit_explicit),
            check_like=True,
        )

        ret_variable = run.iamc.tabulate(variable="Variable 1")
        ret_variable_explicit = run.iamc.tabulate(variable={"name__like": "Variable 1"})
        pdt.assert_frame_equal(
            self.canonical_sort(ret_variable),
            self.canonical_sort(ret_variable_explicit),
            check_like=True,
        )

        ret_variable_in = run.iamc.tabulate(variable=["Variable 1", "Variable 2"])
        ret_variable_in_explicit = run.iamc.tabulate(
            variable={"name__in": ["Variable 1", "Variable 2"]}
        )
        pdt.assert_frame_equal(
            self.canonical_sort(ret_variable_in),
            self.canonical_sort(ret_variable_in_explicit),
            check_like=True,
        )

    def test_iamc_data_remove_partial(
        self,
        run: ixmp4.Run,
        test_data_remove: pd.DataFrame,
        test_data_type: Type | None,
    ) -> None:
        with run.transact("Partial Removal"):
            run.iamc.remove(test_data_remove, type=test_data_type)

    def test_iamc_data_remaining_after_remove_partial(
        self,
        run: ixmp4.Run,
        test_data_remaining: pd.DataFrame,
        test_data_type: Type | None,
    ) -> None:
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(test_data_remaining, ret, check_like=True)

    def test_iamc_data_upsert(
        self,
        run: ixmp4.Run,
        test_data_upsert: pd.DataFrame,
        test_data_type: Type | None,
    ) -> None:
        with run.transact("Upsert"):
            run.iamc.add(test_data_upsert, type=test_data_type)

    def test_iamc_data_tabulate_after_upsert(
        self,
        platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_upsert: pd.DataFrame,
    ) -> None:
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_upsert),
            self.canonical_sort(ret),
            check_like=True,
        )

        test_data_platform = test_data_upsert.copy()
        test_data_platform["model"] = run.model.name
        test_data_platform["scenario"] = run.scenario.name
        test_data_platform["version"] = run.version
        ret_platform = platform.iamc.tabulate(run={"default_only": False})
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_platform),
            self.canonical_sort(ret_platform),
            check_like=True,
        )

    def test_iamc_data_remove_full(
        self,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
        test_data_type: Type | None,
    ) -> None:
        test_data_remove_full = test_data_add.drop(columns=["value"])
        with run.transact("Full Removal"):
            run.iamc.remove(test_data_remove_full, type=test_data_type)

    def test_iamc_data_tabulate_empty(
        self,
        run: ixmp4.Run,
    ) -> None:
        ret = run.iamc.tabulate()
        assert ret.empty

    def test_iamc_data_versioning(self, versioning_platform: ixmp4.Platform) -> None:
        pass  # TODO: Test core versioning api once its implemented


class IamcDataRollbackTest(IamcTest):
    def latest_transaction_id(self, run: ixmp4.Run) -> int:
        executor = SessionExecutor(
            self.get_direct_or_skip(run._backend.transport).session
        )
        return TransactionRepository(executor).latest().id

    def _get_direct_session(self, run: ixmp4.Run) -> orm.Session:
        return self.get_direct_or_skip(run._backend.transport).session

    def timeseries_df(self, run: ixmp4.Run) -> pd.DataFrame:
        return run._backend.iamc.timeseries.tabulate(
            join_parameters=True, run={"id": run.id, "default_only": False}
        ).drop(columns=["id", "run__id"])

    def expected_timeseries_df(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df[["region", "variable", "unit"]].drop_duplicates().reset_index(drop=True)
        )

    def clear_iamc_data(self, run: ixmp4.Run, df: pd.DataFrame) -> None:
        if self.timeseries_df(run).empty:
            return
        with run.transact("Clear iamc data for rollback test"):
            run.iamc.remove(df.drop(columns=["value"]))

    def delete_measurands_for_variable(
        self, run: ixmp4.Run, variable_name: str
    ) -> None:
        session = self._get_direct_session(run)
        variable = run._backend.iamc.variables.get_by_name(variable_name)
        session.execute(
            sa.delete(Measurand).where(Measurand.variable__id == variable.id)
        )
        session.commit()

    def delete_measurands_for_unit(self, run: ixmp4.Run, unit_name: str) -> None:
        session = self._get_direct_session(run)
        unit = run._backend.units.get_by_name(unit_name)
        session.execute(sa.delete(Measurand).where(Measurand.unit__id == unit.id))
        session.commit()

    def test_iamc_data_removal_failure(
        self,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
        test_data_remove: pd.DataFrame,
    ) -> None:
        try:
            with run.transact("Add and remove iamc data failure"):
                run.iamc.add(test_data_add)
                run.checkpoints.create("Add iamc data")
                run.iamc.remove(test_data_remove)
                raise CustomException
        except CustomException:
            pass

    def test_iamc_data_versioning_after_removal_failure(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
        test_data_remove: pd.DataFrame,
    ) -> None:
        ret = run.iamc.tabulate()
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
    ) -> None:
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
    ) -> None:
        try:
            with run.transact("Upsert iamc data"):
                run.iamc.add(test_data_upsert)
                raise CustomException
        except CustomException:
            pass

    def test_iamc_data_versioning_after_upsert_failure(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
    ) -> None:
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
    ) -> None:
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_upsert),
            self.canonical_sort(ret),
            check_like=True,
        )

    def test_iamc_data_new_timeseries_failure(
        self,
        run: ixmp4.Run,
        test_data_new_timeseries: pd.DataFrame,
    ) -> None:
        try:
            with run.transact("Add new iamc timeseries failure"):
                run.iamc.add(test_data_new_timeseries)
                raise CustomException
        except CustomException:
            pass

    def test_iamc_data_versioning_after_new_timeseries_failure(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
    ) -> None:
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_add),
            self.canonical_sort(ret),
            check_like=True,
        )
        pdt.assert_frame_equal(
            self.canonical_sort(self.expected_timeseries_df(test_data_add)),
            self.canonical_sort(self.timeseries_df(run)),
            check_like=True,
        )

    def test_iamc_data_non_versioning_after_new_timeseries_failure(
        self,
        non_versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_upsert: pd.DataFrame,
        test_data_new_timeseries: pd.DataFrame,
    ) -> None:
        expected = pd.concat(
            [test_data_upsert, test_data_new_timeseries], ignore_index=True
        )
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            self.canonical_sort(expected),
            self.canonical_sort(ret),
            check_like=True,
        )
        pdt.assert_frame_equal(
            self.canonical_sort(self.expected_timeseries_df(expected)),
            self.canonical_sort(self.timeseries_df(run)),
            check_like=True,
        )

    def test_iamc_data_full_timeseries_removal_failure(
        self,
        run: ixmp4.Run,
        test_data_remove_full_timeseries: pd.DataFrame,
    ) -> None:
        try:
            with run.transact("Remove iamc timeseries failure"):
                run.iamc.remove(test_data_remove_full_timeseries)
                raise CustomException
        except CustomException:
            pass

    def test_iamc_data_versioning_after_full_timeseries_removal_failure(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
    ) -> None:
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_add),
            self.canonical_sort(ret),
            check_like=True,
        )
        pdt.assert_frame_equal(
            self.canonical_sort(self.expected_timeseries_df(test_data_add)),
            self.canonical_sort(self.timeseries_df(run)),
            check_like=True,
        )

    def test_iamc_data_non_versioning_after_full_timeseries_removal_failure(
        self,
        non_versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_upsert_after_full_timeseries_removal: pd.DataFrame,
    ) -> None:
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_upsert_after_full_timeseries_removal),
            self.canonical_sort(ret),
            check_like=True,
        )
        pdt.assert_frame_equal(
            self.canonical_sort(
                self.expected_timeseries_df(
                    test_data_upsert_after_full_timeseries_removal
                )
            ),
            self.canonical_sort(self.timeseries_df(run)),
            check_like=True,
        )

    def test_iamc_data_revert_recreates_deleted_variable(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
    ) -> None:
        tx_id = self.latest_transaction_id(run)
        self.clear_iamc_data(run, test_data_add)
        self.delete_measurands_for_variable(run, "Variable 1")
        versioning_platform.iamc.variables.delete("Variable 1")

        run._service.revert(run.id, tx_id)

        assert versioning_platform.iamc.variables.get_by_name("Variable 1").name == (
            "Variable 1"
        )
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_add),
            self.canonical_sort(ret),
            check_like=True,
        )

    def test_iamc_data_revert_with_deleted_region_raises_not_found(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
    ) -> None:
        tx_id = self.latest_transaction_id(run)
        self.clear_iamc_data(run, test_data_add)
        versioning_platform.regions.delete("Region 1")

        with pytest.raises(ixmp4.NotFound):
            run._service.revert(run.id, tx_id)

    def test_iamc_data_revert_with_deleted_unit_raises_not_found(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
    ) -> None:
        tx_id = self.latest_transaction_id(run)
        self.clear_iamc_data(run, test_data_add)
        self.delete_measurands_for_unit(run, "Unit 1")
        versioning_platform.units.delete("Unit 1")

        with pytest.raises(ixmp4.NotFound):
            run._service.revert(run.id, tx_id)


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

    @pytest.fixture(scope="class")
    def test_data_new_timeseries(self) -> pd.DataFrame:
        df = pd.DataFrame(
            [["Region 1", "Unit 2", "Variable 1", 2040, 9.9]],
            columns=["region", "unit", "variable", "year", "value"],
        )
        df["year"] = df["year"].astype("Int64")
        return df

    @pytest.fixture(scope="class")
    def test_data_remove_full_timeseries(self) -> pd.DataFrame:
        df = pd.DataFrame(
            [
                ["Region 1", "Unit 1", "Variable 1", 2000],
                ["Region 1", "Unit 1", "Variable 1", 2010],
            ],
            columns=["region", "unit", "variable", "year"],
        )
        df["year"] = df["year"].astype("Int64")
        return df

    @pytest.fixture(scope="class")
    def test_data_upsert_after_full_timeseries_removal(
        self,
        test_data_new_timeseries: pd.DataFrame,
        test_data_upsert: pd.DataFrame,
    ) -> pd.DataFrame:
        expected = pd.concat(
            [test_data_upsert, test_data_new_timeseries], ignore_index=True
        )
        return expected[
            ~(
                (expected["region"] == "Region 1")
                & (expected["unit"] == "Unit 1")
                & (expected["variable"] == "Variable 1")
            )
        ].reset_index(drop=True)


class TestIamcDataAnnualInferType(IamcDataAnnual, IamcDataTest):
    @pytest.fixture(scope="class")
    def test_data_type(self) -> Type | None:
        return None


class TestIamcDataAnnualWithType(IamcDataAnnual, IamcDataTest):
    @pytest.fixture(scope="class")
    def test_data_type(self) -> Type | None:
        return Type.ANNUAL


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


class TestIamcDataStringType(IamcDataAnnual, IamcTest):
    """Tests that ``type`` accepts plain strings (case-insensitive) in add/remove."""

    def test_add_with_uppercase_string_type(
        self,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
    ) -> None:
        with run.transact("add uppercase"):
            run.iamc.add(test_data_add, type="ANNUAL")
        assert not run.iamc.tabulate().empty

    def test_add_with_lowercase_string_type(
        self,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
    ) -> None:
        # Clear data from previous test first
        with run.transact("clear"):
            run.iamc.remove(test_data_add, type="ANNUAL")

        with run.transact("add lowercase"):
            run.iamc.add(test_data_add, type="annual")
        assert not run.iamc.tabulate().empty

    def test_remove_with_mixed_case_string_type(
        self,
        run: ixmp4.Run,
        test_data_remove: pd.DataFrame,
        test_data_remaining: pd.DataFrame,
    ) -> None:
        with run.transact("remove partial"):
            run.iamc.remove(test_data_remove, type="Annual")
        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            self.canonical_sort(test_data_remaining),
            self.canonical_sort(ret),
            check_like=True,
        )

    def test_invalid_string_type_raises(
        self,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
    ) -> None:
        with pytest.raises(KeyError):
            with run.transact("bad type"):
                run.iamc.add(test_data_add, type="NOT_A_TYPE")


class IamcDataInputTest(IamcTest):
    @pytest.fixture(autouse=True)
    def _ensure_regions_and_units(
        self,
        regions: list[ixmp4.Region],
        units: list[ixmp4.Unit],
    ) -> None:
        # Ensure referenced region/unit names exist for fixture-provided input rows.
        _ = (regions, units)

    @pytest.fixture
    def expected_data(self) -> pd.DataFrame:
        raise NotImplementedError

    @pytest.fixture
    def input_data(self, expected_data: pd.DataFrame) -> pd.DataFrame:
        return expected_data.copy()

    def _canonical_sort_mixed_safe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Computes a sort index with categorical and object dtype special
        case handling. Then uses the index to sort the original df."""
        sorted_cols = df.columns.sort_values().to_list()
        sort_df = df.copy()

        for col in sorted_cols:
            series = sort_df[col]

            if isinstance(series.dtype, pd.CategoricalDtype):
                # Avoid unordered categorical sort errors.
                sort_df[col] = series.astype("string")
                continue

            if pd.api.types.is_object_dtype(series):
                try:
                    series.sort_values()
                except TypeError:
                    # Normalize only non-orderable mixed object columns.
                    sort_df[col] = series.map(
                        lambda v: "" if pd.isna(v) else f"{type(v).__name__}:{v}"
                    )

        order = sort_df.sort_values(by=sorted_cols).index
        return df.loc[order].reset_index(drop=True)

    def test_iamc_data_input(
        self,
        run: ixmp4.Run,
        input_data: pd.DataFrame,
        expected_data: pd.DataFrame,
    ) -> None:
        with run.transact("add iamc input data"):
            run.iamc.add(input_data)

        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            self._canonical_sort_mixed_safe(expected_data),
            self._canonical_sort_mixed_safe(ret),
            check_like=True,
        )

        with run.transact("remove iamc input data"):
            run.iamc.remove(input_data.drop(columns=["value"]))

        assert run.iamc.tabulate().empty


class TestAnnualIamcInputData(IamcDataInputTest):
    @pytest.fixture(scope="class")
    def expected_data(
        self,
        regions: list[ixmp4.Region],
        units: list[ixmp4.Unit],
    ) -> pd.DataFrame:
        return pd.DataFrame(
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
        ).astype({"year": "Int64"})

    @pytest.fixture
    def input_data(self, expected_data: pd.DataFrame) -> pd.DataFrame:
        input_df = expected_data.copy()
        input_df["region"] = input_df["region"].astype("category")
        input_df["unit"] = input_df["unit"].astype("category")
        input_df["variable"] = input_df["variable"].astype("category")
        input_df["year"] = input_df["year"].astype("int32")
        input_df["value"] = input_df["value"].astype("float32")
        return input_df


class TestCategoricalIamcInputData(IamcDataInputTest):
    @pytest.fixture
    def expected_data(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                ["Region 1", "Variable 1", "Unit 1", 2000, "Summer", 1.1],
                ["Region 2", "Variable 2", "Unit 2", 2010, "Winter", 2.3],
            ],
            columns=["region", "variable", "unit", "year", "subannual", "value"],
        ).astype({"year": "Int64"})

    @pytest.fixture
    def input_data(self, expected_data: pd.DataFrame) -> pd.DataFrame:
        input_df = expected_data.copy()
        input_df["region"] = input_df["region"].astype("category")
        input_df["unit"] = input_df["unit"].astype("category")
        input_df["variable"] = input_df["variable"].astype("category")
        input_df["year"] = input_df["year"].astype("int32")
        input_df["subannual"] = input_df["subannual"].astype("category")
        input_df["value"] = input_df["value"].astype("float32")
        return input_df


class TestDatetimeIamcInputData(IamcDataInputTest):
    @pytest.fixture
    def expected_data(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [
                    "Region 1",
                    "Variable 1",
                    "Unit 1",
                    pd.Timestamp("2000-01-01 00:00:00"),
                    1.1,
                ],
                [
                    "Region 2",
                    "Variable 2",
                    "Unit 2",
                    pd.Timestamp("2010-06-01 12:34:56"),
                    2.3,
                ],
            ],
            columns=["region", "variable", "unit", "time", "value"],
        )

    @pytest.fixture
    def input_data(self, expected_data: pd.DataFrame) -> pd.DataFrame:
        input_df = expected_data.copy()
        input_df["region"] = input_df["region"].astype("category")
        input_df["unit"] = input_df["unit"].astype("category")
        input_df["variable"] = input_df["variable"].astype("category")
        input_df["time"] = (
            input_df["time"].dt.strftime("%Y-%m-%d %H:%M:%S").astype("string")
        )
        input_df["value"] = input_df["value"].astype("float32")
        return input_df


class TestMixedIamcInputData(IamcDataInputTest):
    @pytest.fixture
    def expected_data(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                # ANNUAL
                ["Region 1", "Variable 1", "Unit 1", 2000, None, 0.1],
                ["Region 2", "Variable 2", "Unit 2", 2010, None, 0.23],
                # CATEGORICAL
                ["Region 1", "Variable 1", "Unit 1", 2000, "Summer", 1.1],
                ["Region 2", "Variable 2", "Unit 2", 2010, "Winter", 2.3],
                # DATETIME
                [
                    "Region 1",
                    "Variable 1",
                    "Unit 1",
                    pd.Timestamp("2000-01-01 00:00:00"),
                    None,
                    101.0,
                ],
                [
                    "Region 2",
                    "Variable 2",
                    "Unit 2",
                    pd.Timestamp("2010-06-01 12:34:56"),
                    None,
                    3.14,
                ],
            ],
            columns=["region", "variable", "unit", "time", "subannual", "value"],
        )


class TestObjectStringsIamcInputData(TestAnnualIamcInputData):
    @pytest.fixture
    def input_data(self, expected_data: pd.DataFrame) -> pd.DataFrame:
        input_df = expected_data.copy()
        input_df["region"] = input_df["region"].astype("object")
        input_df["unit"] = input_df["unit"].astype("object")
        input_df["variable"] = input_df["variable"].astype("object")
        return input_df
