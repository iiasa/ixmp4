import datetime

import pytest

import ixmp4
from ixmp4 import Scenario
from tests import backends

platform = backends.get_platform_fixture(scope="class")


class TestScenario:
    def test_create_scenario(
        self, platform: ixmp4.Platform, fake_time: datetime.datetime
    ) -> None:
        scenario1 = platform.scenarios.create("Scenario 1")
        scenario2 = platform.scenarios.create("Scenario 2")
        scenario3 = platform.scenarios.create("Scenario 3")
        scenario4 = platform.scenarios.create("Scenario 4")

        assert scenario1.id == 1
        assert scenario1.name == "Scenario 1"
        assert scenario1.created_at == fake_time.replace(tzinfo=None)
        assert scenario1.created_by == "@unknown"
        assert scenario1.docs is None
        assert str(scenario1) == "<Scenario 1 name='Scenario 1'>"

        assert scenario2.id == 2
        assert scenario3.id == 3
        assert scenario4.id == 4

    def test_tabulate_scenario(self, platform: ixmp4.Platform) -> None:
        ret_df = platform.scenarios.tabulate()
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "name" in ret_df.columns
        assert "created_at" in ret_df.columns
        assert "created_by" in ret_df.columns

    def test_list_scenario(self, platform: ixmp4.Platform) -> None:
        assert len(platform.scenarios.list()) == 4

    def test_delete_scenario_via_func_obj(self, platform: ixmp4.Platform) -> None:
        scenario1 = platform.scenarios.get_by_name("Scenario 1")
        platform.scenarios.delete(scenario1)

    def test_delete_scenario_via_func_id(self, platform: ixmp4.Platform) -> None:
        platform.scenarios.delete(2)

    def test_delete_scenario_via_func_name(self, platform: ixmp4.Platform) -> None:
        platform.scenarios.delete("Scenario 3")

    def test_delete_scenario_via_obj(self, platform: ixmp4.Platform) -> None:
        scenario4 = platform.scenarios.get_by_name("Scenario 4")
        scenario4.delete()

    def test_scenarios_empty(self, platform: ixmp4.Platform) -> None:
        assert platform.scenarios.tabulate().empty
        assert len(platform.scenarios.list()) == 0


class TestScenarioUnique:
    def test_scenario_unqiue(self, platform: ixmp4.Platform) -> None:
        platform.scenarios.create("Scenario")

        with pytest.raises(Scenario.NotUnique):
            platform.scenarios.create("Scenario")


class TestScenarioDocs:
    def test_create_docs_via_func(self, platform: ixmp4.Platform) -> None:
        scenario1 = platform.scenarios.create("Scenario 1")

        scenario1_docs1 = platform.scenarios.set_docs(
            "Scenario 1", "Description of Scenario 1"
        )
        scenario1_docs2 = platform.scenarios.get_docs("Scenario 1")

        assert scenario1_docs1 == scenario1_docs2
        assert scenario1.docs == scenario1_docs1

    def test_create_docs_via_object(self, platform: ixmp4.Platform) -> None:
        scenario2 = platform.scenarios.create("Scenario 2")
        scenario2.docs = "Description of Scenario 2"

        assert platform.scenarios.get_docs("Scenario 2") == scenario2.docs

    def test_create_docs_via_setattr(self, platform: ixmp4.Platform) -> None:
        scenario3 = platform.scenarios.create("Scenario 3")
        setattr(scenario3, "docs", "Description of Scenario 3")

        assert platform.scenarios.get_docs("Scenario 3") == scenario3.docs

    def test_list_docs(self, platform: ixmp4.Platform) -> None:
        assert platform.scenarios.list_docs() == [
            "Description of Scenario 1",
            "Description of Scenario 2",
            "Description of Scenario 3",
        ]

        assert platform.scenarios.list_docs(id=3) == ["Description of Scenario 3"]

        assert platform.scenarios.list_docs(id__in=[1]) == ["Description of Scenario 1"]

    def test_delete_docs_via_func(self, platform: ixmp4.Platform) -> None:
        scenario1 = platform.scenarios.get_by_name("Scenario 1")
        platform.scenarios.delete_docs("Scenario 1")
        scenario1 = platform.scenarios.get_by_name("Scenario 1")
        assert scenario1.docs is None

    def test_delete_docs_set_none(self, platform: ixmp4.Platform) -> None:
        scenario2 = platform.scenarios.get_by_name("Scenario 2")
        scenario2.docs = None
        scenario2 = platform.scenarios.get_by_name("Scenario 2")
        assert scenario2.docs is None

    def test_delete_docs_del(self, platform: ixmp4.Platform) -> None:
        scenario3 = platform.scenarios.get_by_name("Scenario 3")
        del scenario3.docs
        scenario3 = platform.scenarios.get_by_name("Scenario 3")
        assert scenario3.docs is None

    def test_docs_empty(self, platform: ixmp4.Platform) -> None:
        assert len(platform.scenarios.list_docs()) == 0
