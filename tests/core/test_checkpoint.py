import pandas as pd
import pytest

import ixmp4
from ixmp4.base_exceptions import OperationNotSupported
from ixmp4.data.checkpoint.exceptions import CheckpointNotFound
from tests import backends

from .base import PlatformTest

platform = backends.get_platform_fixture(scope="class")


class CheckpointTest(PlatformTest):
    @pytest.fixture(scope="class")
    def run(self, versioning_platform: ixmp4.Platform) -> ixmp4.Run:
        run = versioning_platform.runs.create("Model", "Scenario")
        run.set_as_default()
        return run


class TestCheckpointMeta(CheckpointTest):
    def test_checkpoint_properties(
        self,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Create checkpoint for property test"):
            checkpoint = run.checkpoints.create("for checkpoint property")

        cp = run.checkpoints[checkpoint.id]

        assert cp.id == checkpoint.id
        assert cp.run__id == run.id

    def test_checkpoint_view_meta(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Set initial meta"):
            run.meta = {"key1": 1, "key2": "hello"}
            checkpoint = run.checkpoints.create("after meta set")

        with run.transact("Update meta"):
            run.meta["key1"] = 99

        view = run.checkpoints[checkpoint.id]
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

        meta = run.checkpoints[checkpoint.id].meta

        assert meta == {}

    def test_checkpoint_view_revert(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Set meta for revert test"):
            run.meta = {"revert_key": "original"}
            checkpoint = run.checkpoints.create("before revert")

        with run.transact("Modify meta"):
            run.meta["revert_key"] = "modified"

        assert run.meta["revert_key"] == "modified"

        run.checkpoints[checkpoint.id].revert()

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

        view = run.checkpoints[checkpoint.id]
        result = getattr(view.optimization, view_name).tabulate()

        assert isinstance(result, pd.DataFrame)


class TestCheckpointIamcView(CheckpointTest):
    def test_checkpoint_iamc_tabulate_empty(
        self,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Create checkpoint before IAMC data"):
            checkpoint = run.checkpoints.create("before iamc add")

        result = run.checkpoints[checkpoint.id].iamc.tabulate()

        assert result.empty
        assert list(result.columns) == ["region", "variable", "unit", "value"]

    def test_checkpoint_iamc_tabulate_returns_checkpoint_state(
        self,
        versioning_platform: ixmp4.Platform,
        run: ixmp4.Run,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        versioning_platform.regions.create("Region A", "default")
        versioning_platform.units.create("Unit A")
        versioning_platform.iamc.variables.create("Variable A")

        base = pd.DataFrame(
            {
                "region": ["Region A"],
                "variable": ["Variable A"],
                "unit": ["Unit A"],
                "year": [2020],
                "value": [1.0],
            }
        )
        updated = base.copy()
        updated["value"] = 2.0

        with run.transact("Add IAMC data and checkpoint"):
            run.iamc.add(base)
            checkpoint = run.checkpoints.create("after iamc add")

        with run.transact("Update IAMC data"):
            run.iamc.add(updated)

        result = run.checkpoints[checkpoint.id].iamc.tabulate()

        assert isinstance(result, pd.DataFrame)
        assert result.loc[0, "value"] == 1.0


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
            _ = run2.checkpoints[checkpoint.id]

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
        view = run.checkpoints[checkpoint.id]
        with pytest.raises(OperationNotSupported):
            _ = view.meta

    def test_checkpoint_iamc_view_raises_on_non_versioning(
        self,
        non_versioning_platform: ixmp4.Platform,
    ) -> None:
        run = non_versioning_platform.runs.create("ModelNVIamc", "ScenarioNVIamc")
        with run.transact("Create checkpoint without transaction id"):
            checkpoint = run.checkpoints.create("no-tx")

        view = run.checkpoints[checkpoint.id]
        with pytest.raises(OperationNotSupported):
            view.iamc.tabulate()

    def test_checkpoint_view_revert_raises_on_non_versioning(
        self,
        non_versioning_platform: ixmp4.Platform,
    ) -> None:
        run = non_versioning_platform.runs.create("ModelNVRevert", "ScenarioNVRevert")
        with run.transact("Create checkpoint for non-versioning revert"):
            checkpoint = run.checkpoints.create("no-tx")

        with pytest.raises(OperationNotSupported):
            run.checkpoints[checkpoint.id].revert()

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

        view = run.checkpoints[checkpoint.id]
        with pytest.raises(OperationNotSupported):
            getattr(view.optimization, view_name).tabulate()
