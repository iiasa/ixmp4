import pytest

from ixmp4 import Variable

from ..utils import all_platforms, create_filter_test_data


@all_platforms
class TestDataIamcVariable:
    def test_create_iamc_variable(self, test_mp):
        variable = test_mp.backend.iamc.variables.create("Variable")
        assert variable.name == "Variable"
        assert variable.created_at is not None
        assert variable.created_by == "@unknown"

    def test_iamc_variable_unique(self, test_mp):
        test_mp.backend.iamc.variables.create("Variable")

        with pytest.raises(Variable.NotUnique):
            test_mp.iamc.variables.create("Variable")

    def test_iamc_variable_not_found(self, test_mp):
        with pytest.raises(Variable.NotFound):
            test_mp.iamc.variables.get("Variable")

    def test_filter_iamc_variable(self, test_mp):
        run1, run2 = create_filter_test_data(test_mp)
        res = test_mp.backend.iamc.variables.tabulate(unit={"name": "Unit 1"})
        assert sorted(res["name"].tolist()) == ["Variable 1", "Variable 2"]

        run2.set_as_default()
        res = test_mp.backend.iamc.variables.tabulate(
            unit={"name": "Unit 3"}, region={"name": "Region 3"}
        )
        assert sorted(res["name"].tolist()) == ["Variable 1"]

        run1.set_as_default()
        res = test_mp.backend.iamc.variables.tabulate(
            unit={"name": "Unit 3"},
            region={"name__in": ["Region 4", "Region 5"]},
            run={"model": {"name": "Model 1"}, "default_only": True},
        )
        assert res["name"].tolist() == []

        res = test_mp.backend.iamc.variables.tabulate(
            unit={"name": "Unit 3"},
            region={"name__in": ["Region 4", "Region 5"]},
            run={"default_only": False},
        )
        assert sorted(res["name"].tolist()) == ["Variable 2"]

        res = test_mp.backend.iamc.variables.tabulate()

        assert sorted(res["name"].tolist()) == [
            "Variable 1",
            "Variable 2",
        ]
