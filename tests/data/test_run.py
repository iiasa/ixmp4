import pandas as pd
import pytest

from ixmp4.core.exceptions import NoDefaultRunVersion

from ..utils import all_platforms, assert_unordered_equality


@all_platforms
class TestDataRun:
    def test_create_run(self, test_mp):
        run1 = test_mp.backend.runs.create("Model", "Scenario")
        assert run1.model.name == "Model"
        assert run1.scenario.name == "Scenario"
        assert run1.version == 1
        assert not run1.is_default

    def test_create_run_increment_version(self, test_mp):
        test_mp.backend.runs.create("Model", "Scenario")
        run2 = test_mp.backend.runs.create("Model", "Scenario")
        assert run2.model.name == "Model"
        assert run2.scenario.name == "Scenario"
        assert run2.version == 2
        assert not run2.is_default

    def test_get_run_versions(self, test_mp):
        run1a = test_mp.backend.runs.create("Model", "Scenario")
        run2a = test_mp.backend.runs.create("Model", "Scenario")
        test_mp.backend.runs.set_as_default_version(run2a.id)
        run3a = test_mp.backend.runs.create("Model", "Scenario")

        run1b = test_mp.backend.runs.get("Model", "Scenario", 1)
        assert run1a.id == run1b.id

        run2b = test_mp.backend.runs.get("Model", "Scenario", 2)
        assert run2a.id == run2b.id
        run2c = test_mp.backend.runs.get_default_version("Model", "Scenario")
        assert run2a.id == run2c.id

        run3b = test_mp.backend.runs.get("Model", "Scenario", 3)
        assert run3a.id == run3b.id

    def test_get_run_no_default_version(self, test_mp):
        with pytest.raises(NoDefaultRunVersion):
            test_mp.backend.runs.get_default_version("Model", "Scenario")

    def test_get_or_create_run(self, test_mp):
        run1 = test_mp.backend.runs.create("Model", "Scenario")
        run2 = test_mp.backend.runs.get_or_create("Model", "Scenario")
        assert run1.id != run2.id
        assert run2.version == 2

        test_mp.backend.runs.set_as_default_version(run1.id)

        run3 = test_mp.backend.runs.get_or_create("Model", "Scenario")
        assert run1.id == run3.id

    def test_list_run(self, test_mp):
        run1 = test_mp.backend.runs.create("Model", "Scenario")
        test_mp.backend.runs.create("Model", "Scenario")

        runs = test_mp.backend.runs.list(default_only=False)
        assert runs[0].id == 1
        assert runs[0].version == 1
        assert runs[0].model.name == "Model"
        assert runs[0].scenario.name == "Scenario"
        assert runs[1].id == 2
        assert runs[1].version == 2

        test_mp.backend.runs.set_as_default_version(run1.id)
        [run] = test_mp.backend.runs.list(default_only=True)

        assert run1.id == run.id

    def test_tabulate_run(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        test_mp.backend.runs.set_as_default_version(run.id)
        test_mp.backend.runs.create("Model", "Scenario")

        true_runs = pd.DataFrame(
            [
                [1, True, 1, 1, 1],
                [2, False, 1, 1, 2],
            ],
            columns=[
                "id",
                "is_default",
                "model__id",
                "scenario__id",
                "version",
            ],
        )

        runs = test_mp.backend.runs.tabulate(default_only=False)
        assert_unordered_equality(runs, true_runs)
