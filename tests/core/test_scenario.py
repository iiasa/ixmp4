import pytest
import pandas as pd

from ixmp4 import Scenario

from ..utils import assert_unordered_equality, all_platforms


@all_platforms
def test_retrieve_scenario(test_mp):
    scenario1 = test_mp.scenarios.create("Scenario")
    scenario2 = test_mp.scenarios.get("Scenario")

    assert scenario1.id == scenario2.id


@all_platforms
def test_scenario_unqiue(test_mp):
    test_mp.scenarios.create("Scenario")

    with pytest.raises(Scenario.NotUnique):
        test_mp.scenarios.create("Scenario")


def create_testcase_scenarios(test_mp):
    scenario = test_mp.scenarios.create("Scenario")
    scenario2 = test_mp.scenarios.create("Scenario 2")
    return scenario, scenario2


@all_platforms
def test_list_scenario(test_mp):
    scenarios = create_testcase_scenarios(test_mp)
    scenario, _ = scenarios

    a = [s.id for s in scenarios]
    b = [s.id for s in test_mp.scenarios.list()]
    assert not (set(a) ^ set(b))

    a = [scenario.id]
    b = [s.id for s in test_mp.scenarios.list(name="Scenario")]
    assert not (set(a) ^ set(b))


def df_from_list(scenarios):
    return pd.DataFrame(
        [[s.id, s.name, s.created_at, s.created_by] for s in scenarios],
        columns=["id", "name", "created_at", "created_by"],
    )


@all_platforms
def test_tabulate_scenario(test_mp):
    scenarios = create_testcase_scenarios(test_mp)
    scenario, _ = scenarios

    a = df_from_list(scenarios)
    b = test_mp.scenarios.tabulate()
    assert_unordered_equality(a, b, check_dtype=False)

    a = df_from_list([scenario])
    b = test_mp.scenarios.tabulate(name="Scenario")
    assert_unordered_equality(a, b, check_dtype=False)


@all_platforms
def test_retrieve_docs(test_mp):
    test_mp.scenarios.create("Scenario")
    docs_scenario1 = test_mp.scenarios.set_docs(
        "Scenario", "Description of test Scenario"
    )
    docs_scenario2 = test_mp.scenarios.get_docs("Scenario")

    assert docs_scenario1 == docs_scenario2

    scenario2 = test_mp.scenarios.create("Scenario2")

    assert scenario2.docs is None

    scenario2.docs = "Description of test Scenario2"

    assert test_mp.scenarios.get_docs("Scenario2") == scenario2.docs


@all_platforms
def test_delete_docs(test_mp):
    scenario = test_mp.scenarios.create("Scenario")
    scenario.docs = "Description of test Scenario"
    scenario.docs = None

    assert scenario.docs is None

    scenario.docs = "Second description of test Scenario"
    del scenario.docs

    assert scenario.docs is None

    scenario.docs = "Third description of test Scenario"
    test_mp.scenarios.delete_docs("Scenario")

    assert scenario.docs is None
