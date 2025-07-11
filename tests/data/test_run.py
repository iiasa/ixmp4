import numpy as np
import pandas as pd
import pytest

import ixmp4
import ixmp4.data
import ixmp4.data.abstract
from ixmp4.core.exceptions import NoDefaultRunVersion

from ..utils import assert_unordered_equality


class TestDataRun:
    def test_create_run(self, platform: ixmp4.Platform) -> None:
        run1 = platform.backend.runs.create("Model", "Scenario")
        assert run1.model.name == "Model"
        assert run1.scenario.name == "Scenario"
        assert run1.version == 1
        assert not run1.is_default

        expected_run_versions = pd.DataFrame(
            [
                [
                    1,
                    1,
                    1,
                    1,
                    False,
                    run1.created_at,
                    "@unknown",
                    None,
                    None,
                    None,
                    1,
                    None,
                    0,
                ],
            ],
            columns=[
                "id",
                "model__id",
                "scenario__id",
                "version",
                "is_default",
                "created_at",
                "created_by",
                "updated_at",
                "updated_by",
                "lock_transaction",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )

        vdf = platform.backend.runs.tabulate_versions()
        assert_unordered_equality(expected_run_versions, vdf, check_dtype=False)

    def test_create_run_increment_version(self, platform: ixmp4.Platform) -> None:
        run1 = platform.backend.runs.create("Model", "Scenario")
        run2 = platform.backend.runs.create("Model", "Scenario")
        assert run2.model.name == "Model"
        assert run2.scenario.name == "Scenario"
        assert run2.version == 2
        assert not run2.is_default

        expected_run_versions = pd.DataFrame(
            [
                [
                    1,
                    1,
                    1,
                    1,
                    False,
                    run1.created_at,
                    "@unknown",
                    None,
                    None,
                    None,
                    1,
                    None,
                    0,
                ],
                [
                    2,
                    1,
                    1,
                    2,
                    False,
                    run2.created_at,
                    "@unknown",
                    None,
                    None,
                    None,
                    2,
                    None,
                    0,
                ],
            ],
            columns=[
                "id",
                "model__id",
                "scenario__id",
                "version",
                "is_default",
                "created_at",
                "created_by",
                "updated_at",
                "updated_by",
                "lock_transaction",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )

        vdf = platform.backend.runs.tabulate_versions()
        assert_unordered_equality(expected_run_versions, vdf, check_dtype=False)

    def test_get_run_versions(self, platform: ixmp4.Platform) -> None:
        run1a = platform.backend.runs.create("Model", "Scenario")
        run2a = platform.backend.runs.create("Model", "Scenario")
        platform.backend.runs.set_as_default_version(run2a.id)
        run3a = platform.backend.runs.create("Model", "Scenario")

        run1b = platform.backend.runs.get("Model", "Scenario", 1)
        assert run1a.id == run1b.id

        run2b = platform.backend.runs.get("Model", "Scenario", 2)
        assert run2a.id == run2b.id
        run2c = platform.backend.runs.get_default_version("Model", "Scenario")
        assert run2a.id == run2c.id

        run3b = platform.backend.runs.get("Model", "Scenario", 3)
        assert run3a.id == run3b.id

        expected_run_versions = pd.DataFrame(
            [
                [1, 1, 1, 1, False, None, 1, None, 0],
                [2, 1, 1, 2, False, None, 2, 3, 0],
                [2, 1, 1, 2, True, None, 3, None, 1],
                [3, 1, 1, 3, False, None, 4, None, 0],
            ],
            columns=[
                "id",
                "model__id",
                "scenario__id",
                "version",
                "is_default",
                "lock_transaction",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        ).replace({np.nan: None})

        vdf = platform.backend.runs.tabulate_versions()
        self.drop_audit_info(vdf)

        assert_unordered_equality(expected_run_versions, vdf, check_dtype=False)

    def test_get_run_no_default_version(self, platform: ixmp4.Platform) -> None:
        with pytest.raises(NoDefaultRunVersion):
            platform.backend.runs.get_default_version("Model", "Scenario")

    def test_get_or_create_run(self, platform: ixmp4.Platform) -> None:
        run1 = platform.backend.runs.create("Model", "Scenario")
        run2 = platform.backend.runs.get_or_create("Model", "Scenario")
        assert run1.id != run2.id
        assert run2.version == 2

        platform.backend.runs.set_as_default_version(run1.id)

        run3 = platform.backend.runs.get_or_create("Model", "Scenario")
        assert run1.id == run3.id

    def test_get_run_by_id(self, platform: ixmp4.Platform) -> None:
        expected = platform.backend.runs.create("Model", "Scenario")
        result = platform.backend.runs.get_by_id(id=expected.id)
        assert expected.id == result.id

        with pytest.raises(ixmp4.data.abstract.Run.NotFound):
            _ = platform.backend.runs.get_by_id(id=expected.id + 1)

    def test_list_run(self, platform: ixmp4.Platform) -> None:
        run1 = platform.backend.runs.create("Model", "Scenario")
        platform.backend.runs.create("Model", "Scenario")

        runs = platform.backend.runs.list(default_only=False)
        assert runs[0].id == 1
        assert runs[0].version == 1
        assert runs[0].model.name == "Model"
        assert runs[0].scenario.name == "Scenario"
        assert runs[1].id == 2
        assert runs[1].version == 2

        platform.backend.runs.set_as_default_version(run1.id)
        [run] = platform.backend.runs.list(default_only=True)

        assert run1.id == run.id

    def test_tabulate_run(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        platform.backend.runs.set_as_default_version(run.id)
        platform.backend.runs.create("Model", "Scenario")

        true_runs = pd.DataFrame(
            [
                [1, True, 1, 1, 1, None],
                [2, False, 1, 1, 2, None],
            ],
            columns=[
                "id",
                "is_default",
                "model__id",
                "scenario__id",
                "version",
                "lock_transaction",
            ],
        )

        runs = platform.backend.runs.tabulate(default_only=False)
        self.drop_audit_info(runs)
        assert_unordered_equality(runs, true_runs)

        runs = platform.backend.runs.tabulate(default_only=False, iamc=False)
        self.drop_audit_info(runs)
        assert_unordered_equality(runs, true_runs)

        runs = platform.backend.runs.tabulate(default_only=False, iamc={})
        assert runs.empty

        runs = platform.backend.runs.tabulate(default_only=False, iamc=True)
        assert runs.empty

    def drop_audit_info(self, df: pd.DataFrame) -> None:
        df.drop(
            inplace=True,
            columns=["created_by", "created_at", "updated_by", "updated_at"],
        )
