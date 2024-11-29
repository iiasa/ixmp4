import pandas as pd
import pytest

import ixmp4
from ixmp4 import Model

from ..fixtures import FilterIamcDataset
from ..utils import assert_unordered_equality


class TestDataModel:
    filter = FilterIamcDataset()

    def test_create_model(self, platform: ixmp4.Platform) -> None:
        model = platform.backend.models.create("Model")
        assert model.name == "Model"
        assert model.created_at is not None
        assert model.created_by == "@unknown"

    def test_model_unique(self, platform: ixmp4.Platform) -> None:
        platform.backend.models.create("Model")

        with pytest.raises(Model.NotUnique):
            platform.models.create("Model")

    def test_get_model(self, platform: ixmp4.Platform) -> None:
        model1 = platform.backend.models.create("Model")
        model2 = platform.backend.models.get("Model")
        assert model1 == model2

    def test_model_not_found(self, platform: ixmp4.Platform) -> None:
        with pytest.raises(Model.NotFound):
            platform.models.get("Model")

    def test_list_model(self, platform: ixmp4.Platform) -> None:
        platform.runs.create("Model 1", "Scenario")
        platform.runs.create("Model 2", "Scenario")

        models = sorted(platform.backend.models.list(), key=lambda x: x.id)
        assert models[0].id == 1
        assert models[0].name == "Model 1"
        assert models[1].id == 2
        assert models[1].name == "Model 2"

    def test_tabulate_model(self, platform: ixmp4.Platform) -> None:
        platform.runs.create("Model 1", "Scenario")
        platform.runs.create("Model 2", "Scenario")

        true_models = pd.DataFrame(
            [
                [1, "Model 1"],
                [2, "Model 2"],
            ],
            columns=["id", "name"],
        )

        models = platform.backend.models.tabulate()
        assert_unordered_equality(
            models.drop(columns=["created_at", "created_by"]), true_models
        )

    def test_map_model(self, platform: ixmp4.Platform) -> None:
        platform.runs.create("Model 1", "Scenario")
        platform.runs.create("Model 2", "Scenario")
        assert platform.backend.models.map() == {1: "Model 1", 2: "Model 2"}

    def test_filter_model(self, platform: ixmp4.Platform) -> None:
        run1, run2 = self.filter.load_dataset(platform)
        run2.set_as_default()

        res = platform.backend.models.tabulate(name__like="Model *")
        assert sorted(res["name"].tolist()) == ["Model 1", "Model 2"]

        res = platform.backend.models.tabulate(
            iamc={
                "region": {"name": "Region 1"},
            }
        )
        assert sorted(res["name"].tolist()) == ["Model 1"]

        res = platform.backend.models.tabulate(
            iamc={
                "region": {"name__in": ["Region 1", "Region 2"]},
            }
        )
        assert sorted(res["name"].tolist()) == ["Model 1", "Model 2"]

        run2.unset_as_default()
        res = platform.backend.models.tabulate(
            iamc={
                "variable": {"name": "Variable 5"},
                "unit": {"name__in": ["Unit 3", "Unit 4"]},
                "run": {"default_only": True},
            }
        )
        assert res["name"].tolist() == []

        res = platform.backend.models.tabulate(
            iamc={
                "variable": {"name": "Variable 5"},
                "unit": {"name__in": ["Unit 3", "Unit 4"]},
                "run": {"default_only": False},
            }
        )
        assert sorted(res["name"].tolist()) == ["Model 2"]
