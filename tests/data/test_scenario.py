import pandas as pd
import pytest

from ixmp4 import Scenario

from ..utils import assert_unordered_equality, all_platforms, create_filter_test_data


@all_platforms
def test_create_scenario(test_mp):
    scenario = test_mp.backend.scenarios.create("Scenario")
    assert scenario.name == "Scenario"
    assert scenario.created_at is not None
    assert scenario.created_by == "@unknown"


@all_platforms
def test_scenario_unique(test_mp):
    test_mp.backend.scenarios.create("Scenario")

    with pytest.raises(Scenario.NotUnique):
        test_mp.scenarios.create("Scenario")


@all_platforms
def test_get_scenario(test_mp):
    scenario1 = test_mp.backend.scenarios.create("Scenario")
    scenario2 = test_mp.backend.scenarios.get("Scenario")
    assert scenario1 == scenario2


@all_platforms
def test_scenario_not_found(test_mp):
    with pytest.raises(Scenario.NotFound):
        test_mp.scenarios.get("Scenario")


@all_platforms
def test_list_scenario(test_mp):
    test_mp.Run("Model", "Scenario 1", version="new")
    test_mp.Run("Model", "Scenario 2", version="new")

    scenarios = sorted(test_mp.backend.scenarios.list(), key=lambda x: x.id)

    assert scenarios[0].id == 1
    assert scenarios[0].name == "Scenario 1"
    assert scenarios[1].id == 2
    assert scenarios[1].name == "Scenario 2"


@all_platforms
def test_tabulate_scenario(test_mp):
    test_mp.Run("Model", "Scenario 1", version="new")
    test_mp.Run("Model", "Scenario 2", version="new")

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


@all_platforms
def test_filter_scenario(test_mp):
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
