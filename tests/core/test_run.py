import datetime
import threading
import time
from typing import Any

import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4
from ixmp4.base_exceptions import OperationNotSupported
from tests import backends

from .base import PlatformTest

platform = backends.get_platform_fixture(scope="class")


class TestRun:
    def test_create_run(
        self, platform: ixmp4.Platform, fake_time: datetime.datetime
    ) -> None:
        run1 = platform.runs.create("Model", "Scenario")
        run1.set_as_default()

        run2 = platform.runs.create("Model", "Scenario")
        run3 = platform.runs.create("Other Model", "Scenario")
        run4 = platform.runs.create("Other Model", "Other Scenario")

        assert run1.id == 1
        assert run1.model.name == "Model"
        assert run1.scenario.name == "Scenario"
        assert run1.version == 1

        assert run1.created_at == fake_time.replace(tzinfo=None)
        assert run1.created_by == "@unknown"

        assert str(run1) == "<Run model='Model' scenario='Scenario' version=1 id=1>"

        assert run2.id == 2
        assert run2.version == 2

        assert run3.id == 3
        assert run4.id == 4

    def test_tabulate_run(self, platform: ixmp4.Platform) -> None:
        ret_df = platform.runs.tabulate(default_only=False)
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "model" in ret_df.columns
        assert "scenario" in ret_df.columns
        assert "version" in ret_df.columns

    def test_tabulate_run_facade_model_scenario_filters(
        self, platform: ixmp4.Platform
    ) -> None:
        shorthand = platform.runs.tabulate(
            model="Model", scenario="Scenario", default_only=False
        )
        explicit = platform.runs.tabulate(
            model={"name__like": "Model"},
            scenario={"name__like": "Scenario"},
            default_only=False,
        )
        pdt.assert_frame_equal(
            shorthand.sort_values(["id"]).reset_index(drop=True),
            explicit.sort_values(["id"]).reset_index(drop=True),
            check_like=True,
        )

    def test_tabulate_run_hides_internal_columns_by_default(
        self, platform: ixmp4.Platform
    ) -> None:
        ret_df = platform.runs.tabulate(default_only=False)
        assert "model__id" not in ret_df.columns
        assert "scenario__id" not in ret_df.columns
        assert "lock_transaction" not in ret_df.columns

    def test_tabulate_run_shows_internal_columns_when_requested(
        self, platform: ixmp4.Platform
    ) -> None:
        ret_df = platform.runs.tabulate(
            default_only=False, include_internal_columns=True
        )
        assert "model__id" in ret_df.columns
        assert "scenario__id" in ret_df.columns
        assert "lock_transaction" in ret_df.columns

    def test_list_run(self, platform: ixmp4.Platform) -> None:
        assert len(platform.runs.list(default_only=False)) == 4

    def test_delete_run_via_func_obj(self, platform: ixmp4.Platform) -> None:
        run1 = platform.runs.get("Model", "Scenario")
        platform.runs.delete(run1)
        run2 = platform.runs.get("Model", "Scenario", version=2)
        platform.runs.delete(run2)

    def test_delete_run_via_func_id(self, platform: ixmp4.Platform) -> None:
        platform.runs.delete(3)

    def test_delete_run_via_obj(self, platform: ixmp4.Platform) -> None:
        run4 = platform.runs.get("Other Model", "Other Scenario", version=1)
        run4.delete()

    def test_runs_empty(self, platform: ixmp4.Platform) -> None:
        assert platform.runs.tabulate().empty
        assert len(platform.runs.list()) == 0


class TestRunLocking:
    def test_lock_sets_is_locked(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Lock Model", "Lock Scenario")

        assert not run.is_locked
        assert not run.owns_lock

        run.lock()

        assert run.is_locked

    def test_unlock_clears_is_locked(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Unlock Model", "Unlock Scenario")

        run.lock()
        run.unlock()

        assert not run.is_locked
        assert not run.owns_lock

        run.unlock()

        assert not run.is_locked
        assert not run.owns_lock

    def test_lock_check_false_raises_for_same_object(
        self, platform: ixmp4.Platform
    ) -> None:
        run = platform.runs.create("Lock Model", "Check False")

        run.lock()
        run.lock()  # skips lock due to check=True

        with pytest.raises(ixmp4.Run.IsLocked):
            run.lock(check=False)

    def test_lock_timeout(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Timeout Model", "Timeout Scenario")
        run1 = platform.runs.get(run.model.name, run.scenario.name, version=run.version)
        run2 = platform.runs.get(run.model.name, run.scenario.name, version=run.version)
        sync_lock = threading.Lock()
        sync_lock.acquire(timeout=1)

        def background_task() -> None:
            run1.lock()
            sync_lock.release()
            time.sleep(0.5)
            run1.unlock()

        thread = threading.Thread(target=background_task)
        thread.start()

        sync_lock.acquire(timeout=1)

        run2.lock(timeout=5)

        assert run2.is_locked
        assert run2.owns_lock

        run2.unlock()
        thread.join()
        sync_lock.release()

    def test_force_unlock_refreshes_stale_state(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Force Unlock Model", "Force Unlock Scenario")
        owner = platform.runs.get(
            run.model.name, run.scenario.name, version=run.version
        )
        other = platform.runs.get(
            run.model.name, run.scenario.name, version=run.version
        )

        owner.lock()

        assert owner.is_locked
        assert not other.is_locked

        with pytest.raises(ixmp4.Run.LockRequired, match="Trying to unlock"):
            other.unlock(check=False)

        other.unlock(force=True)

        assert not other.is_locked
        assert not platform.runs.get(
            run.model.name, run.scenario.name, version=run.version
        ).is_locked


class TestRunRevert(PlatformTest):
    def test_revert_requires_lock(self, versioning_platform: ixmp4.Platform) -> None:
        run = versioning_platform.runs.create("Revert Model", "Lock Required")

        with pytest.raises(ixmp4.Run.LockRequired):
            run.revert()

    def test_revert_without_checkpoint_uses_lock_transaction(
        self, versioning_platform: ixmp4.Platform
    ) -> None:
        run = versioning_platform.runs.create("Revert Model", "No Checkpoint")

        with run.transact("Add and revert to origin"):
            run.meta["state"] = "draft"

            assert dict(run.meta) == {"state": "draft"}

            run.revert()

            assert dict(run.meta) == {}

        reloaded = versioning_platform.runs.get(
            run.model.name, run.scenario.name, version=run.version
        )
        assert dict(reloaded.meta) == {}

    def test_revert_without_transaction_id_uses_latest_checkpoint(
        self, versioning_platform: ixmp4.Platform
    ) -> None:
        run = versioning_platform.runs.create("Revert Model", "Latest Checkpoint")

        with run.transact("Revert to checkpoint"):
            run.meta["state"] = "checkpoint"
            checkpoint = run.checkpoints.create("checkpoint state")
            run.meta["state"] = "mutated"
            run.meta["extra"] = True

            assert dict(run.meta) == {"state": "mutated", "extra": True}

            run.revert()

            assert checkpoint.transaction__id is not None
            assert dict(run.meta) == {"state": "checkpoint"}

        reloaded = versioning_platform.runs.get(
            run.model.name, run.scenario.name, version=run.version
        )
        assert dict(reloaded.meta) == {"state": "checkpoint"}

    def test_revert_to_explicit_transaction_across_transactions(
        self, versioning_platform: ixmp4.Platform
    ) -> None:
        run = versioning_platform.runs.create("Revert Model", "Explicit Transaction")

        with run.transact("Create checkpoint"):
            run.meta["state"] = "checkpoint"
            checkpoint = run.checkpoints.create("before mutation")

        assert checkpoint.transaction__id is not None

        with run.transact("Mutate and revert"):
            run.meta["state"] = "mutated"
            run.meta["extra"] = "value"

            run.revert(checkpoint.transaction__id)

            assert dict(run.meta) == {"state": "checkpoint"}

        reloaded = versioning_platform.runs.get(
            run.model.name, run.scenario.name, version=run.version
        )
        assert dict(reloaded.meta) == {"state": "checkpoint"}

    def test_revert_raises_on_non_versioning(
        self, non_versioning_platform: ixmp4.Platform
    ) -> None:
        run = non_versioning_platform.runs.create("Revert Model", "Non Versioning")
        run.lock()

        try:
            with pytest.raises(OperationNotSupported):
                run.revert()
        finally:
            run.unlock(force=True)


class TestRunClone:
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

    @pytest.fixture(scope="class")
    def test_data_iamc(
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
    def test_data_meta(self) -> dict[str, Any]:
        return {
            "test_bool": False,
            "test_str": "test",
            "test_int": 13,
            "test_float": 3.14,
        }

    @pytest.fixture(scope="class")
    def test_data_idxset1(
        self,
    ) -> list[str]:
        return ["do", "re", "mi", "fa", "so", "la", "ti"]

    @pytest.fixture(scope="class")
    def test_data_idxset2(
        self,
    ) -> list[float]:
        return [3, 1, 4]

    @pytest.fixture(scope="class")
    def test_data_equation1(self) -> dict[str, list[Any]]:
        return {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }

    @pytest.fixture(scope="class")
    def test_data_parameter1(self) -> dict[str, list[Any]]:
        return {
            "units": ["Unit 1", "Unit 1", "Unit 2"],
            "values": [1.2, 1.5, -3],
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }

    @pytest.fixture(scope="class")
    def test_data_table1(self) -> dict[str, list[Any]]:
        return {
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }

    @pytest.fixture(scope="class")
    def test_data_variable1(self) -> dict[str, list[Any]]:
        return {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet 1": ["so", "la", "ti"],
            "IndexSet 2": [4, 1, 1],
        }

    @pytest.fixture(scope="class")
    def run(
        self,
        platform: ixmp4.Platform,
        test_data_iamc: pd.DataFrame,
        test_data_meta: dict[str, Any],
        test_data_idxset1: list[str],
        test_data_idxset2: list[float],
        test_data_equation1: dict[str, list[Any]],
        test_data_parameter1: dict[str, list[Any]],
        test_data_table1: dict[str, list[Any]],
        test_data_variable1: dict[str, list[Any]],
    ) -> ixmp4.Run:
        run = platform.runs.create("Model", "Scenario")
        assert run.id == 1

        with run.transact("Add meta indicators"):
            run.meta = test_data_meta

        with run.transact("Add IAMC data"):
            run.iamc.add(test_data_iamc)

        with run.transact("Add Optimization data"):
            indexset1 = run.optimization.indexsets.create("IndexSet 1")
            indexset2 = run.optimization.indexsets.create("IndexSet 2")
            indexset1.add_data(test_data_idxset1)
            indexset2.add_data(test_data_idxset2)

            run.optimization.scalars.create("Scalar 1", 1.23, "Unit 1")

            equation1 = run.optimization.equations.create(
                "Equation 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
            )
            equation1.add_data(test_data_equation1)

            parameter1 = run.optimization.parameters.create(
                "Parameter 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
            )
            parameter1.add_data(test_data_parameter1)

            table1 = run.optimization.tables.create(
                "Table 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
            )
            table1.add_data(test_data_table1)

            variable1 = run.optimization.variables.create(
                "Variable 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
            )
            variable1.add_data(test_data_variable1)

        return run

    def test_clone_run(
        self,
        run: ixmp4.Run,
        test_data_iamc: pd.DataFrame,
        test_data_meta: dict[str, Any],
        test_data_idxset1: list[str],
        test_data_idxset2: list[float],
        test_data_equation1: dict[str, list[Any]],
        test_data_parameter1: dict[str, list[Any]],
        test_data_table1: dict[str, list[Any]],
        test_data_variable1: dict[str, list[Any]],
    ) -> None:
        cloned_run = run.clone()

        assert cloned_run.model.name == run.model.name
        assert cloned_run.scenario.name == run.scenario.name
        assert dict(cloned_run.meta) == test_data_meta

        cloned_df = cloned_run.iamc.tabulate()
        pdt.assert_frame_equal(cloned_df, test_data_iamc, check_like=True)

        indexset1 = cloned_run.optimization.indexsets.get_by_name("IndexSet 1")
        indexset2 = cloned_run.optimization.indexsets.get_by_name("IndexSet 2")
        assert indexset1.data == test_data_idxset1
        assert indexset2.data == test_data_idxset2

        scalar1 = cloned_run.optimization.scalars.get_by_name("Scalar 1")
        assert scalar1.unit.name == "Unit 1"
        assert scalar1.value == 1.23

        equation1 = cloned_run.optimization.equations.get_by_name("Equation 1")
        assert equation1.data == test_data_equation1

        parameter1 = cloned_run.optimization.parameters.get_by_name("Parameter 1")
        assert parameter1.data == test_data_parameter1

        table1 = cloned_run.optimization.tables.get_by_name("Table 1")
        assert table1.data == test_data_table1

        variable1 = cloned_run.optimization.variables.get_by_name("Variable 1")
        assert variable1.data == test_data_variable1


class TestRunTransact:
    @pytest.fixture(scope="class")
    def run(self, platform: ixmp4.Platform) -> ixmp4.Run:
        return platform.runs.create("Model", "Scenario")

    def test_transact_timeout(self, platform: ixmp4.Platform, run: ixmp4.Run) -> None:
        run1 = platform.runs.get(run.model.name, run.scenario.name, version=run.version)
        run2 = platform.runs.get(run.model.name, run.scenario.name, version=run.version)
        sync_lock = threading.Lock()
        sync_lock.acquire(timeout=1)

        def background_task() -> None:
            with run1.transact("Background transaction"):
                sync_lock.release()
                time.sleep(0.5)

        thread = threading.Thread(target=background_task)
        thread.start()

        sync_lock.acquire(timeout=1)

        with run2.transact("Test transaction", timeout=5):
            run2.meta["timeout"] = "awaited"

        assert run2.meta["timeout"] == "awaited"
        thread.join()
        sync_lock.release()

    def test_transact_timeout_failure(
        self, platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        run1 = platform.runs.get(run.model.name, run.scenario.name, version=run.version)
        run2 = platform.runs.get(run.model.name, run.scenario.name, version=run.version)
        sync_lock = threading.Lock()
        sync_lock.acquire(timeout=1)

        def background_task() -> None:
            with run1.transact("Background transaction"):
                sync_lock.release()
                time.sleep(2)

        thread = threading.Thread(target=background_task)
        thread.start()

        sync_lock.acquire(timeout=1)

        with pytest.raises(ixmp4.Run.IsLocked):
            with run2.transact("Test transaction", timeout=0.5):
                run2.meta["timeout"] = "failed"

        assert run2.meta == {"timeout": "awaited"}
        thread.join()
        sync_lock.release()

    def test_transact_is_locked(self, platform: ixmp4.Platform, run: ixmp4.Run) -> None:
        run1 = platform.runs.get(run.model.name, run.scenario.name, version=run.version)
        run2 = platform.runs.get(run.model.name, run.scenario.name, version=run.version)
        sync_lock = threading.Lock()
        sync_lock.acquire(timeout=1)

        def background_task() -> None:
            with run1.transact("Background transaction"):
                sync_lock.release()
                time.sleep(2)

        thread = threading.Thread(target=background_task)
        thread.start()

        sync_lock.acquire(timeout=1)

        with pytest.raises(ixmp4.Run.IsLocked):
            with run2.transact("Test transaction"):
                run2.meta["locked"] = "already"

        assert run2.meta == {"timeout": "awaited"}
        thread.join()
        sync_lock.release()

    def test_transact_nested_raises(
        self, platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        with pytest.raises(ixmp4.Run.IsLocked, match="[Nn]ested"):
            with run.transact("outer"):
                with run.transact("inner"):
                    pass
