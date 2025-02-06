import pandas as pd
import pytest

import ixmp4
from ixmp4 import Scenario

from ..fixtures import FilterIamcDataset
from ..utils import assert_unordered_equality


class TestDataScenario:
    filter = FilterIamcDataset()

    def test_create_scenario(self, platform: ixmp4.Platform) -> None:
        scenario = platform.backend.scenarios.create("Scenario")
        assert scenario.name == "Scenario"
        assert scenario.created_at is not None
        assert scenario.created_by == "@unknown"

        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Scenario",
                    scenario.created_at,
                    "@unknown",
                    1,
                    None,
                    0,
                ],
            ],
            columns=[
                "id",
                "name",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = platform.backend.scenarios.tabulate_versions()
        assert_unordered_equality(expected_versions, vdf, check_dtype=False)

    def test_scenario_unique(self, platform: ixmp4.Platform) -> None:
        platform.backend.scenarios.create("Scenario")

        with pytest.raises(Scenario.NotUnique):
            platform.scenarios.create("Scenario")

    def test_get_scenario(self, platform: ixmp4.Platform) -> None:
        scenario1 = platform.backend.scenarios.create("Scenario")
        scenario2 = platform.backend.scenarios.get("Scenario")
        assert scenario1 == scenario2

    def test_scenario_not_found(self, platform: ixmp4.Platform) -> None:
        with pytest.raises(Scenario.NotFound):
            platform.scenarios.get("Scenario")

    def test_list_scenario(self, platform: ixmp4.Platform) -> None:
        platform.runs.create("Model", "Scenario 1")
        platform.runs.create("Model", "Scenario 2")

        scenarios = sorted(platform.backend.scenarios.list(), key=lambda x: x.id)

        assert scenarios[0].id == 1
        assert scenarios[0].name == "Scenario 1"
        assert scenarios[1].id == 2
        assert scenarios[1].name == "Scenario 2"

    def test_tabulate_scenario(self, platform: ixmp4.Platform) -> None:
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

    def test_map_scenario(self, platform: ixmp4.Platform) -> None:
        platform.runs.create("Model", "Scenario 1")
        platform.runs.create("Model", "Scenario 2")

        assert platform.backend.scenarios.map() == {1: "Scenario 1", 2: "Scenario 2"}

    def test_filter_scenario(self, platform: ixmp4.Platform) -> None:
        run1, run2 = self.filter.load_dataset(platform)

        res = platform.backend.scenarios.tabulate(
            iamc={
                "region": {"name": "Region 1"},
                "run": {"default_only": False},
            }
        )
        assert sorted(res["name"].tolist()) == ["Scenario 1"]

        res = platform.backend.scenarios.tabulate(
            iamc={
                "region": {"name__in": ["Region 2", "Region 3"]},
                "run": {"default_only": False},
            }
        )
        assert sorted(res["name"].tolist()) == ["Scenario 1", "Scenario 2"]

        res = platform.backend.scenarios.tabulate(
            iamc={
                "unit": {"name__in": ["Unit 2", "Unit 4"]},
                "run": {"default_only": True},
            }
        )
        assert res["name"].tolist() == ["Scenario 1"]

        res = platform.backend.scenarios.tabulate(
            iamc={
                "run": {"default_only": False, "model": {"name": "Model 2"}},
            }
        )

        assert sorted(res["name"].tolist()) == ["Scenario 2"]
