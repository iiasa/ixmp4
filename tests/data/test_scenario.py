import pandas as pd
import pytest

from ixmp4 import Scenario

from ..utils import all_platforms, assert_unordered_equality, create_filter_test_data


@all_platforms
class TestDataScenario:
    def test_create_scenario(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        scenario = test_mp.backend.scenarios.create("Scenario")
        assert scenario.name == "Scenario"
        assert scenario.created_at is not None
        assert scenario.created_by == "@unknown"

    def test_scenario_unique(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        test_mp.backend.scenarios.create("Scenario")

        with pytest.raises(Scenario.NotUnique):
            test_mp.scenarios.create("Scenario")

    def test_get_scenario(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        scenario1 = test_mp.backend.scenarios.create("Scenario")
        scenario2 = test_mp.backend.scenarios.get("Scenario")
        assert scenario1 == scenario2

    def test_scenario_not_found(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        with pytest.raises(Scenario.NotFound):
            test_mp.scenarios.get("Scenario")

    def test_list_scenario(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        test_mp.runs.create("Model", "Scenario 1")
        test_mp.runs.create("Model", "Scenario 2")

        scenarios = sorted(test_mp.backend.scenarios.list(), key=lambda x: x.id)

        assert scenarios[0].id == 1
        assert scenarios[0].name == "Scenario 1"
        assert scenarios[1].id == 2
        assert scenarios[1].name == "Scenario 2"

    def test_tabulate_scenario(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        test_mp.runs.create("Model", "Scenario 1")
        test_mp.runs.create("Model", "Scenario 2")

        true_scenarios = pd.DataFrame(
            [
                [1, "Scenario 1"],
                [2, "Scenario 2"],
            ],
            columns=["id", "name"],
        )

        scenarios = test_mp.backend.scenarios.tabulate()
        assert_unordered_equality(
            scenarios.drop(columns=["created_at", "created_by"]), true_scenarios
        )

    def test_map_scenario(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        test_mp.runs.create("Model", "Scenario 1")
        test_mp.runs.create("Model", "Scenario 2")

        assert test_mp.backend.scenarios.map() == {1: "Scenario 1", 2: "Scenario 2"}

    def test_filter_scenario(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run1, _ = create_filter_test_data(test_mp)

        res = test_mp.backend.scenarios.tabulate(
            iamc={
                "region": {"name": "Region 1"},
                "run": {"default_only": False},
            }
        )
        assert sorted(res["name"].tolist()) == ["Scenario 1"]

        res = test_mp.backend.scenarios.tabulate(
            iamc={
                "region": {"name": "Region 3"},
                "run": {"default_only": False},
            }
        )
        assert sorted(res["name"].tolist()) == ["Scenario 1", "Scenario 2"]

        run1.set_as_default()
        res = test_mp.backend.scenarios.tabulate(
            iamc={
                "variable": {"name": "Variable 1"},
                "unit": {"name__in": ["Unit 3", "Unit 4"]},
                "run": {"default_only": True},
            }
        )
        assert res["name"].tolist() == ["Scenario 2"]

        res = test_mp.backend.scenarios.tabulate(
            iamc={
                "run": {"default_only": False, "model": {"name": "Model 2"}},
            }
        )

        assert sorted(res["name"].tolist()) == ["Scenario 2"]
