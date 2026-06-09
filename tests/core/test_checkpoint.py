import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4
from ixmp4.base_exceptions import OperationNotSupported
from ixmp4.data.checkpoint.exceptions import CheckpointNotFound
from ixmp4.data.versions.model import Operation
from tests import backends
from tests.base import DataFrameTest

from .base import PlatformTest

platform = backends.get_platform_fixture(scope="class")


class CheckpointTest(PlatformTest):
    @pytest.fixture(scope="class")
    def run(self, versioning_platform: ixmp4.Platform) -> ixmp4.Run:
        run = versioning_platform.runs.create("Model", "Scenario")
        run.set_as_default()
        return run


class TestCheckpoint(CheckpointTest):
    def test_checkpoint_properties(
        self,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Create checkpoint for property test"):
            checkpoint = run.checkpoints.create("test message")

        assert isinstance(checkpoint.id, int)
        assert checkpoint.id == 1
        assert checkpoint.message == "test message"
        assert checkpoint.run__id == run.id
        assert isinstance(checkpoint.run__id, int)
        assert isinstance(checkpoint.transaction__id, int)

        assert str(checkpoint) == (
            "<Checkpoint message='test message' "
            f"transaction__id={checkpoint.transaction__id} "
            f"run__id={checkpoint.run__id} id=1>"
        )
        assert str(checkpoint) == repr(checkpoint)

    def test_checkpoint_next_prev(
        self,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Create checkpoints for prev/next test"):
            cp1 = run.checkpoints.create("first")
            cp2 = run.checkpoints.create("second")

        last_cp = run.checkpoints.list()[-1]

        assert last_cp.next is None
        assert last_cp.previous is not None
        assert last_cp.previous.id == cp2.id
        assert cp2.previous is not None
        assert cp2.previous.id == cp1.id
        assert cp1.next is not None
        assert cp1.next.id == cp2.id


class TestCheckpointMeta(CheckpointTest):
    def test_checkpoint_view_meta(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Set initial meta"):
            run.meta = {"key1": 1, "key2": "hello"}

        with run.transact("Update meta"):
            run.meta["key1"] = 99

        # "Set initial meta" checkpoint
        view = run.checkpoints.list()[-2]
        meta = view.meta

        assert meta["key1"] == 1
        assert meta["key2"] == "hello"

    def test_checkpoint_view_meta_empty(
        self,
        versioning_platform: ixmp4.Platform,
    ) -> None:
        run = versioning_platform.runs.create("ModelMetaEmpty", "ScenarioMetaEmpty")
        with run.transact("Create checkpoint without meta"):
            checkpoint = run.checkpoints.create("empty meta")

        assert checkpoint.meta == {}

    def test_checkpoint_view_revert(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Set meta for revert test"):
            run.meta = {"revert_key": "original"}
            checkpoint = run.checkpoints.create("before modification")

        with run.transact("Modify meta"):
            run.meta["revert_key"] = "modified"

        assert run.meta["revert_key"] == "modified"

        checkpoint.revert()

        run_after = versioning_platform.runs.get("Model", "Scenario", version=1)
        assert run_after.meta["revert_key"] == "original"


class TestCheckpointOptimizationViews(CheckpointTest):
    @pytest.mark.parametrize(
        "view_name",
        ["scalars", "tables", "parameters", "equations", "variables", "indexsets"],
    )
    def test_checkpoint_optimization_subview_tabulate(
        self,
        run: ixmp4.Run,
        view_name: str,
    ) -> None:
        with run.transact(f"Create checkpoint for {view_name} view"):
            checkpoint = run.checkpoints.create(f"before {view_name} view")

        result = getattr(checkpoint.optimization, view_name).tabulate()

        assert isinstance(result, pd.DataFrame)


class IamcCheckpointViewData(DataFrameTest):
    @pytest.fixture(scope="class")
    def test_data_add(self, versioning_platform: ixmp4.Platform) -> pd.DataFrame:
        versioning_platform.regions.create("Region A", "default")
        versioning_platform.units.create("Unit A")
        versioning_platform.iamc.variables.create("Variable A")

        df = pd.DataFrame(
            {
                "region": ["Region A"],
                "variable": ["Variable A"],
                "unit": ["Unit A"],
                "year": [2020],
                "value": [1.0],
            }
        )
        df["year"] = df["year"].astype("Int64")
        return df

    @pytest.fixture(scope="class")
    def test_data_update(self, test_data_add: pd.DataFrame) -> pd.DataFrame:
        df = test_data_add.copy()
        df["value"] = 2.0
        return df

    @pytest.fixture(scope="class")
    def test_data_remove(self, test_data_add: pd.DataFrame) -> pd.DataFrame:
        df = test_data_add.copy()
        df["year"] = pd.Series([2030], dtype="Int64")
        df["value"] = 3.0
        return df


class TestCheckpointIamcView(IamcCheckpointViewData, CheckpointTest):
    def test_checkpoint_iamc_tabulate_empty(
        self,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Create checkpoint before IAMC data"):
            checkpoint = run.checkpoints.create("before iamc add")

        result = checkpoint.iamc.tabulate()

        assert result.empty
        assert list(result.columns) == ["region", "variable", "unit", "value"]

    def test_checkpoint_iamc_tabulate_returns_checkpoint_state(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        test_data_add: pd.DataFrame,
        test_data_update: pd.DataFrame,
    ) -> None:
        with run.transact("Add IAMC data and checkpoint"):
            run.iamc.add(test_data_add)
            checkpoint = run.checkpoints.create("after iamc add")

        with run.transact("Update IAMC data"):
            run.iamc.add(test_data_update)

        result = checkpoint.iamc.tabulate()

        pdt.assert_frame_equal(
            self.canonical_sort(result),
            self.canonical_sort(test_data_add),
            check_like=True,
        )

    def test_checkpoint_iamc_difference_since_previous(
        self,
        versioning_platform: ixmp4.Platform,
        test_data_add: pd.DataFrame,
        test_data_update: pd.DataFrame,
        test_data_remove: pd.DataFrame,
    ) -> None:
        run = versioning_platform.runs.create("ModelDiff", "ScenarioDiff")

        with run.transact("checkpoint 1"):
            run.iamc.add(test_data_add)
            cp1 = run.checkpoints.create("cp1")
        assert cp1.transaction__id is not None
        prev_tx_id = cp1.transaction__id

        with run.transact("checkpoint 2"):
            run.iamc.add(test_data_update)
            run.iamc.add(test_data_remove)
            cp2 = run.checkpoints.create("cp2")
        assert cp2.transaction__id is not None
        tx_id = cp2.transaction__id

        diff = cp2.iamc.difference()

        diff_cmp = diff[
            [
                "region",
                "variable",
                "unit",
                "step_year",
                "value",
                "type",
                "operation_type",
            ]
        ]
        expected = pd.concat([test_data_update, test_data_remove], ignore_index=True)
        expected = expected.rename(columns={"year": "step_year"})
        expected["step_year"] = expected["step_year"].astype("Int64")
        expected["type"] = "ANNUAL"
        expected["operation_type"] = [
            int(Operation.UPDATE),
            int(Operation.INSERT),
        ]
        pdt.assert_frame_equal(
            self.canonical_sort(diff_cmp),
            self.canonical_sort(expected),
            check_like=True,
        )
        assert (diff["transaction_id"] <= tx_id).all()
        assert (diff["transaction_id"] > prev_tx_id).all()

    def test_checkpoint_iamc_difference_first_checkpoint_includes_initial_versions(
        self,
        versioning_platform: ixmp4.Platform,
        test_data_add: pd.DataFrame,
    ) -> None:
        run = versioning_platform.runs.create("ModelFirstDiff", "ScenarioFirstDiff")

        with run.transact("first checkpoint"):
            run.iamc.add(test_data_add)
            cp1 = run.checkpoints.create("cp1")
        assert cp1.transaction__id is not None
        tx_id = cp1.transaction__id

        diff = cp1.iamc.difference()

        diff_cmp = diff[
            [
                "region",
                "variable",
                "unit",
                "step_year",
                "value",
                "type",
                "operation_type",
                "transaction_id",
                "end_transaction_id",
            ]
        ].reset_index(drop=True)
        expected = test_data_add.rename(columns={"year": "step_year"}).copy()
        expected["step_year"] = expected["step_year"].astype("Int64")
        expected["type"] = "ANNUAL"
        expected["operation_type"] = int(Operation.INSERT)
        expected["transaction_id"] = tx_id
        expected["end_transaction_id"] = None
        pdt.assert_frame_equal(
            self.canonical_sort(diff_cmp),
            self.canonical_sort(expected),
            check_like=True,
        )


class TestRunCheckpointsView(CheckpointTest):
    def test_run_checkpoints_getitem_raises_for_other_run_checkpoint(
        self,
        versioning_platform: ixmp4.Platform,
    ) -> None:
        run1 = versioning_platform.runs.create("ModelGetItem", "ScenarioGetItem1")
        run2 = versioning_platform.runs.create("ModelGetItem", "ScenarioGetItem2")

        with run1.transact("Create checkpoint in run1"):
            checkpoint = run1.checkpoints.create("run1 checkpoint")

        with pytest.raises(CheckpointNotFound):
            run2.checkpoints.delete(checkpoint.id)

    def test_run_checkpoints_tabulate(
        self,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Create checkpoint for tabulate"):
            run.checkpoints.create("for tabulate")

        checkpoints = run.checkpoints.tabulate()

        assert isinstance(checkpoints, pd.DataFrame)
        assert {"id", "message", "transaction__id"}.issubset(checkpoints.columns)


class TestCheckpointNonVersioning(CheckpointTest):
    def test_checkpoint_view_raises_on_non_versioning(
        self,
        non_versioning_platform: ixmp4.Platform,
    ) -> None:
        run = non_versioning_platform.runs.create("ModelNV", "ScenarioNV")
        # SQLite checkpoint has transaction__id=None
        with run.transact("Set meta"):
            run.meta = {"k": "v"}
            checkpoint = run.checkpoints.create("no-tx")

        with pytest.raises(OperationNotSupported):
            _ = checkpoint.meta

    def test_checkpoint_iamc_view_raises_on_non_versioning(
        self,
        non_versioning_platform: ixmp4.Platform,
    ) -> None:
        run = non_versioning_platform.runs.create("ModelNVIamc", "ScenarioNVIamc")
        with run.transact("Create checkpoint without transaction id"):
            checkpoint = run.checkpoints.create("no-tx")

        with pytest.raises(OperationNotSupported):
            checkpoint.iamc.tabulate()

    def test_checkpoint_view_revert_raises_on_non_versioning(
        self,
        non_versioning_platform: ixmp4.Platform,
    ) -> None:
        run = non_versioning_platform.runs.create("ModelNVRevert", "ScenarioNVRevert")
        with run.transact("Create checkpoint for non-versioning revert"):
            checkpoint = run.checkpoints.create("no-tx")

        with pytest.raises(OperationNotSupported):
            checkpoint.revert()

    @pytest.mark.parametrize(
        "view_name",
        ["scalars", "tables", "parameters", "equations", "variables", "indexsets"],
    )
    def test_checkpoint_optimization_subviews_raise_on_non_versioning(
        self,
        non_versioning_platform: ixmp4.Platform,
        view_name: str,
    ) -> None:
        run = non_versioning_platform.runs.create(
            "ModelNVOpt", f"ScenarioNVOpt-{view_name}"
        )
        with run.transact("Set meta and checkpoint"):
            run.meta = {"k": "v"}
            checkpoint = run.checkpoints.create("no-tx")

        with pytest.raises(OperationNotSupported):
            getattr(checkpoint.optimization, view_name).tabulate()
