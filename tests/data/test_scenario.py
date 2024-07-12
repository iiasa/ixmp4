import pandas as pd
import pytest

import ixmp4
from ixmp4 import Scenario

from ..utils import assert_unordered_equality, create_filter_test_data


class TestDataScenario:
    def test_create_scenario(self, platform: ixmp4.Platform):
        scenario = platform.backend.scenarios.create("Scenario")
        assert scenario.name == "Scenario"
        assert scenario.created_at is not None
        assert scenario.created_by == "@unknown"

    def test_scenario_unique(self, platform: ixmp4.Platform):
        platform.backend.scenarios.create("Scenario")

        with pytest.raises(Scenario.NotUnique):
            platform.scenarios.create("Scenario")

    def test_get_scenario(self, platform: ixmp4.Platform):
        scenario1 = platform.backend.scenarios.create("Scenario")
        scenario2 = platform.backend.scenarios.get("Scenario")
        assert scenario1 == scenario2

    def test_scenario_not_found(self, platform: ixmp4.Platform):
        with pytest.raises(Scenario.NotFound):
            platform.scenarios.get("Scenario")

    def test_list_scenario(self, platform: ixmp4.Platform):
        platform.runs.create("Model", "Scenario 1")
        platform.runs.create("Model", "Scenario 2")

        scenarios = sorted(platform.backend.scenarios.list(), key=lambda x: x.id)

        assert scenarios[0].id == 1
        assert scenarios[0].name == "Scenario 1"
        assert scenarios[1].id == 2
        assert scenarios[1].name == "Scenario 2"

    def test_tabulate_scenario(self, platform: ixmp4.Platform):
        platform.runs.create("Model", "Scenario 1")
        platform.runs.create("Model", "Scenario 2")

        true_scenarios = pd.DataFrame(
            [
                [1, "Scenario 1"],
                [2, "Scenario 2"],
            ],
            columns=["id", "name"],
        )

        scenarios = platform.backend.scenarios.tabulate()
        assert_unordered_equality(
            scenarios.drop(columns=["created_at", "created_by"]), true_scenarios
        )

    def test_map_scenario(self, platform: ixmp4.Platform):
        platform.runs.create("Model", "Scenario 1")
        platform.runs.create("Model", "Scenario 2")

        assert platform.backend.scenarios.map() == {1: "Scenario 1", 2: "Scenario 2"}

    def test_filter_scenario(self, platform: ixmp4.Platform):
        run1, _ = create_filter_test_data(platform)

        res = platform.backend.scenarios.tabulate(
            iamc={
                "region": {"name": "Region 1"},
                "run": {"default_only": False},
            }
        )
        assert sorted(res["name"].tolist()) == ["Scenario 1"]

        res = platform.backend.scenarios.tabulate(
            iamc={
                "region": {"name": "Region 3"},
                "run": {"default_only": False},
            }
        )
        assert sorted(res["name"].tolist()) == ["Scenario 1", "Scenario 2"]

        run1.set_as_default()
        res = platform.backend.scenarios.tabulate(
            iamc={
                "variable": {"name": "Variable 1"},
                "unit": {"name__in": ["Unit 3", "Unit 4"]},
                "run": {"default_only": True},
            }
        )
        assert res["name"].tolist() == ["Scenario 2"]

        res = platform.backend.scenarios.tabulate(
            iamc={
                "run": {"default_only": False, "model": {"name": "Model 2"}},
            }
        )

        assert sorted(res["name"].tolist()) == ["Scenario 2"]
