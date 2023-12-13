import pandas as pd
import pytest

from ixmp4 import Model

from ..utils import all_platforms, assert_unordered_equality, create_filter_test_data


@all_platforms
class TestDataModel:
    def test_create_model(self, test_mp):
        model = test_mp.backend.models.create("Model")
        assert model.name == "Model"
        assert model.created_at is not None
        assert model.created_by == "@unknown"

    def test_model_unique(self, test_mp):
        test_mp.backend.models.create("Model")

        with pytest.raises(Model.NotUnique):
            test_mp.models.create("Model")

    def test_get_model(self, test_mp):
        model1 = test_mp.backend.models.create("Model")
        model2 = test_mp.backend.models.get("Model")
        assert model1 == model2

    def test_model_not_found(self, test_mp):
        with pytest.raises(Model.NotFound):
            test_mp.models.get("Model")

    def test_list_model(self, test_mp):
        test_mp.runs.create("Model 1", "Scenario")
        test_mp.runs.create("Model 2", "Scenario")

        models = sorted(test_mp.backend.models.list(), key=lambda x: x.id)
        assert models[0].id == 1
        assert models[0].name == "Model 1"
        assert models[1].id == 2
        assert models[1].name == "Model 2"

    def test_tabulate_model(self, test_mp):
        test_mp.runs.create("Model 1", "Scenario")
        test_mp.runs.create("Model 2", "Scenario")

        true_models = pd.DataFrame(
            [
                [1, "Model 1"],
                [2, "Model 2"],
            ],
            columns=["id", "name"],
        )

        models = test_mp.backend.models.tabulate()
        assert_unordered_equality(
            models.drop(columns=["created_at", "created_by"]), true_models
        )

    def test_map_model(self, test_mp):
        test_mp.runs.create("Model 1", "Scenario")
        test_mp.runs.create("Model 2", "Scenario")
        assert test_mp.backend.models.map() == {1: "Model 1", 2: "Model 2"}

    def test_filter_model(self, test_mp):
        run1, _ = create_filter_test_data(test_mp)

        res = test_mp.backend.models.tabulate(name__like="Model *")
        assert sorted(res["name"].tolist()) == ["Model 1", "Model 2"]

        res = test_mp.backend.models.tabulate(
            iamc={
                "region": {"name": "Region 1"},
                "run": {"default_only": False},
            }
        )
        assert sorted(res["name"].tolist()) == ["Model 1"]

        res = test_mp.backend.models.tabulate(
            iamc={
                "region": {"name": "Region 3"},
                "run": {"default_only": False},
            }
        )
        assert sorted(res["name"].tolist()) == ["Model 1", "Model 2"]

        run1.set_as_default()
        res = test_mp.backend.models.tabulate(
            iamc={
                "variable": {"name": "Variable 1"},
                "unit": {"name__in": ["Unit 3", "Unit 4"]},
                "run": {"default_only": True},
            }
        )
        assert res["name"].tolist() == ["Model 2"]

        res = test_mp.backend.models.tabulate(
            iamc={
                "run": {"default_only": False, "scenario": {"name": "Scenario 2"}},
            }
        )

        assert sorted(res["name"].tolist()) == ["Model 2"]
