import pandas as pd
import pytest

import ixmp4
from ixmp4 import Scenario

from ..utils import assert_unordered_equality


def create_testcase_scenarios(platform):
    scenario = platform.scenarios.create("Scenario")
    scenario2 = platform.scenarios.create("Scenario 2")
    return scenario, scenario2


def df_from_list(scenarios):
    return pd.DataFrame(
        [[s.id, s.name, s.created_at, s.created_by] for s in scenarios],
        columns=["id", "name", "created_at", "created_by"],
    )


class TestCoreScenario:
    def test_retrieve_scenario(self, platform: ixmp4.Platform):
        scenario1 = platform.scenarios.create("Scenario")
        scenario2 = platform.scenarios.get("Scenario")

        assert scenario1.id == scenario2.id

    def test_scenario_unqiue(self, platform: ixmp4.Platform):
        platform.scenarios.create("Scenario")

        with pytest.raises(Scenario.NotUnique):
            platform.scenarios.create("Scenario")

    def test_list_scenario(self, platform: ixmp4.Platform):
        scenarios = create_testcase_scenarios(platform)
        scenario, _ = scenarios

        a = [s.id for s in scenarios]
        b = [s.id for s in platform.scenarios.list()]
        assert not (set(a) ^ set(b))

        a = [scenario.id]
        b = [s.id for s in platform.scenarios.list(name="Scenario")]
        assert not (set(a) ^ set(b))

    def test_tabulate_scenario(self, platform: ixmp4.Platform):
        scenarios = create_testcase_scenarios(platform)
        scenario, _ = scenarios

        a = df_from_list(scenarios)
        b = platform.scenarios.tabulate()
        assert_unordered_equality(a, b, check_dtype=False)

        a = df_from_list([scenario])
        b = platform.scenarios.tabulate(name="Scenario")
        assert_unordered_equality(a, b, check_dtype=False)

    def test_retrieve_docs(self, platform: ixmp4.Platform):
        platform.scenarios.create("Scenario")
        docs_scenario1 = platform.scenarios.set_docs(
            "Scenario", "Description of test Scenario"
        )
        docs_scenario2 = platform.scenarios.get_docs("Scenario")

        assert docs_scenario1 == docs_scenario2

        scenario2 = platform.scenarios.create("Scenario2")

        assert scenario2.docs is None

        scenario2.docs = "Description of test Scenario2"

        assert platform.scenarios.get_docs("Scenario2") == scenario2.docs

    def test_delete_docs(self, platform: ixmp4.Platform):
        scenario = platform.scenarios.create("Scenario")
        scenario.docs = "Description of test Scenario"
        scenario.docs = None

        assert scenario.docs is None

        scenario.docs = "Second description of test Scenario"
        del scenario.docs

        assert scenario.docs is None

        scenario.docs = "Third description of test Scenario"
        platform.scenarios.delete_docs("Scenario")

        assert scenario.docs is None
